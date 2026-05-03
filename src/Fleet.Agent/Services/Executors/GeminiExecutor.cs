using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Text;
using System.Text.Encodings.Web;
using System.Text.Json;
using System.Threading.Channels;
using Fleet.Agent.Configuration;
using Fleet.Agent.Models;
using Microsoft.Extensions.Options;

namespace Fleet.Agent.Services.Executors;

/// <summary>
/// Manages a persistent `gemini` process with streaming I/O.
/// Messages are sent via stdin; responses stream from stdout in NDJSON.
/// </summary>
public sealed class GeminiExecutor : IAgentExecutor
{
    private readonly AgentOptions _config;
    private readonly ILogger<GeminiExecutor> _logger;
    private readonly PromptBuilder _promptBuilder;

    private Process? _process;
    private StreamWriter? _stdin;
    private StreamReader? _stdout;
    private readonly SemaphoreSlim _sendLock = new(1, 1);
    private string? _lastSessionId;
    private int _messageCount;
    private DateTimeOffset _lastActivity = DateTimeOffset.MinValue;
    private volatile bool _restartRequested;
    private ExecutionStats? _previousCumulativeStats;

    private Channel<GeminiStreamEvent>? _eventChannel;
    private CancellationTokenSource? _readerCts;

    public string? LastSessionId => _lastSessionId;
    public DateTimeOffset LastActivity => _lastActivity;
    public bool IsProcessWarm => _process is not null && !_process.HasExited && _messageCount > 0;

    public GeminiExecutor(IOptions<AgentOptions> config, ILogger<GeminiExecutor> logger, PromptBuilder promptBuilder)
    {
        _config = config.Value;
        _logger = logger;
        _promptBuilder = promptBuilder;
    }

    public async IAsyncEnumerable<AgentProgress> ExecuteAsync(
        string task,
        IReadOnlyList<MessageImage>? images = null,
        [EnumeratorCancellation] CancellationToken ct = default)
    {
        _lastActivity = DateTimeOffset.UtcNow;
        await _sendLock.WaitAsync(ct);

        try
        {
            var attempt = 0;
            while (attempt < 2)
            {
                attempt++;
                EnsureProcess();
                DrainStaleTurnEvents();

                try
                {
                    // Gemini CLI persistent mode: send raw text or JSON if supported.
                    // For now, sending the task as plain text to stdin.
                    // If images are present, we might need a different approach or assume the CLI handles it if we can pass them.
                    // ClaudeExecutor uses a JSON wrapper, we'll try sending the task directly or wrapped if gemini supports it.
                    // Based on gemini --help, it's mostly a REPL.
                    
                    var message = task;
                    if (images is { Count: > 0 })
                    {
                        // Gemini CLI might not support image input via stdin persistent mode easily yet,
                        // or it might require a specific format. For now, we'll append a note.
                        message = $"[Image context provided] {task}";
                    }

                    _logger.LogInformation("Sending message to gemini (message #{Count})", _messageCount + 1);
                    await _stdin!.WriteLineAsync(message.AsMemory(), ct);
                    await _stdin.FlushAsync();
                }
                catch (IOException) when (attempt < 2)
                {
                    _logger.LogWarning("Write to stdin failed (attempt {Attempt}), restarting process", attempt);
                    await KillProcessAsync();
                    continue;
                }

                var needsRetry = false;
                while (!ct.IsCancellationRequested)
                {
                    GeminiStreamEvent? evt;
                    try
                    {
                        evt = await _eventChannel!.Reader.ReadAsync(ct);
                    }
                    catch (OperationCanceledException)
                    {
                        await KillProcessAsync();
                        throw;
                    }
                    catch (ChannelClosedException)
                    {
                        evt = null;
                    }

                    if (evt is null)
                    {
                        if (attempt < 2)
                        {
                            _logger.LogWarning("Process died mid-response (attempt {Attempt}), restarting", attempt);
                            await KillProcessAsync();
                            needsRetry = true;
                            break;
                        }
                        _logger.LogError("Process died after retry, giving up");
                        yield return new AgentProgress { IsSignificant = true, Summary = "Gemini process died unexpectedly", EventType = "error" };
                        yield break;
                    }

                    var progress = ParseProgress(evt);
                    yield return progress;

                    if (evt.Type == "result")
                    {
                        if (_restartRequested)
                        {
                            _restartRequested = false;
                            await KillProcessAsync();
                            _messageCount = 0;
                        }
                        _messageCount++;
                        yield break;
                    }
                }
                if (!needsRetry) break;
            }
        }
        finally
        {
            _sendLock.Release();
        }
    }

    public async IAsyncEnumerable<AgentProgress> SendCommandAsync(string command, [EnumeratorCancellation] CancellationToken ct = default)
    {
        _lastActivity = DateTimeOffset.UtcNow;
        await _sendLock.WaitAsync(ct);
        try
        {
            if (_process is null || _process.HasExited)
            {
                yield return new AgentProgress { IsSignificant = true, Summary = "No running process.", EventType = "error" };
                yield break;
            }

            DrainStaleTurnEvents();
            await _stdin!.WriteLineAsync(command.AsMemory(), ct);
            await _stdin.FlushAsync();

            while (!ct.IsCancellationRequested)
            {
                GeminiStreamEvent? evt;
                try { evt = await _eventChannel!.Reader.ReadAsync(ct); }
                catch (ChannelClosedException) { break; }

                yield return ParseProgress(evt);
                if (evt.Type == "result") yield break;
            }
        }
        finally { _sendLock.Release(); }
    }

    public IReadOnlyCollection<BackgroundTaskInfo> GetActiveBackgroundTasks() => Array.Empty<BackgroundTaskInfo>();

    public Task<bool> CancelBackgroundTaskAsync(string taskId, CancellationToken ct = default) => Task.FromResult(false);

    public void RequestRestart() => _restartRequested = true;

    public async Task StopProcessAsync()
    {
        await _sendLock.WaitAsync();
        try { await KillProcessAsync(); _messageCount = 0; _lastSessionId = null; }
        finally { _sendLock.Release(); }
    }

    public async Task<bool> TryStopProcessAsync()
    {
        if (!await _sendLock.WaitAsync(TimeSpan.Zero)) return false;
        try { await KillProcessAsync(); _messageCount = 0; _lastSessionId = null; return true; }
        finally { _sendLock.Release(); }
    }

    public async ValueTask DisposeAsync()
    {
        await KillProcessAsync();
        _sendLock.Dispose();
    }

    private void EnsureProcess()
    {
        if (_process is not null && !_process.HasExited) return;

        var resumeId = _lastSessionId;
        _process?.Dispose();
        _messageCount = 0;

        var args = BuildArgs(resumeId);
        _logger.LogInformation("Starting persistent gemini process: {Args}", args);

        var psi = new ProcessStartInfo
        {
            FileName = "gemini",
            Arguments = args,
            WorkingDirectory = _config.WorkDir,
            RedirectStandardInput = true,
            RedirectStandardOutput = true,
            RedirectStandardError = true,
            UseShellExecute = false,
            CreateNoWindow = true,
        };

        _process = Process.Start(psi) ?? throw new InvalidOperationException("Failed to start gemini process");
        _stdin = _process.StandardInput;
        _stdout = _process.StandardOutput;

        _readerCts = new CancellationTokenSource();
        _eventChannel = Channel.CreateUnbounded<GeminiStreamEvent>(new UnboundedChannelOptions { SingleReader = true });
        _ = Task.Run(() => ReadStdoutLoopAsync(_readerCts.Token));

        // Stderr logging
        var stderr = _process.StandardError;
        _ = Task.Run(async () => {
            while (true) {
                var line = await stderr.ReadLineAsync();
                if (line is null) break;
                _logger.LogWarning("[gemini stderr] {Line}", line);
            }
        });
    }

    private string BuildArgs(string? resumeId)
    {
        var sb = new StringBuilder();
        sb.Append("--output-format stream-json --skip-trust ");
        if (resumeId != null) sb.Append($"--resume \"{resumeId}\" ");
        sb.Append($"--model \"{_config.Model}\" ");
        
        // Gemini CLI doesn't have --append-system-prompt-file exactly, 
        // but we can try to pass the system prompt or rely on GEMINI.md.
        // For now, we just pass the model.
        
        return sb.ToString().Trim();
    }

    private async Task ReadStdoutLoopAsync(CancellationToken ct)
    {
        try
        {
            while (!ct.IsCancellationRequested)
            {
                var line = await _stdout!.ReadLineAsync(ct);
                if (line is null) break;
                if (string.IsNullOrWhiteSpace(line)) continue;

                try
                {
                    var evt = JsonSerializer.Deserialize<GeminiStreamEvent>(line);
                    if (evt != null)
                    {
                        if (evt.SessionId != null) _lastSessionId = evt.SessionId;
                        _eventChannel!.Writer.TryWrite(evt);
                    }
                }
                catch { /* skip non-json */ }
            }
        }
        finally { _eventChannel?.Writer.TryComplete(); }
    }

    private async Task KillProcessAsync()
    {
        if (_process == null) return;
        _readerCts?.Cancel();
        try { if (!_process.HasExited) _process.Kill(true); await _process.WaitForExitAsync(); } catch { }
        _process.Dispose(); _process = null;
    }

    private void DrainStaleTurnEvents()
    {
        while (_eventChannel?.Reader.TryRead(out _) == true) { }
    }

    private AgentProgress ParseProgress(GeminiStreamEvent evt)
    {
        return evt.Type switch
        {
            "init" => new AgentProgress { IsSignificant = false, Summary = $"Session initialized: {evt.Model}", EventType = evt.Type, SessionId = evt.SessionId },
            "message" => new AgentProgress 
            { 
                IsSignificant = evt.Delta != true, 
                Summary = evt.Content != null ? Truncate(evt.Content, 500) : "...", 
                EventType = evt.Type,
                FinalResult = evt.Delta != true ? evt.Content : null
            },
            "tool_use" => new AgentProgress
            {
                IsSignificant = true,
                Summary = $"Using tool: {evt.ToolName}",
                EventType = evt.Type,
                ToolName = evt.ToolName,
                ToolArgs = evt.Arguments != null ? JsonSerializer.Serialize(evt.Arguments) : null
            },
            "result" => new AgentProgress
            {
                IsSignificant = true,
                Summary = evt.Status == "success" ? "Task completed" : $"Task {evt.Status}",
                EventType = evt.Type,
                IsErrorResult = evt.Status != "success",
                Stats = MapStats(evt.Stats)
            },
            _ => new AgentProgress { IsSignificant = false, Summary = $"Event: {evt.Type}", EventType = evt.Type }
        };
    }

    private ExecutionStats? MapStats(GeminiStats? stats)
    {
        if (stats == null) return null;
        return new ExecutionStats
        {
            InputTokens = stats.InputTokens,
            OutputTokens = stats.OutputTokens,
            DurationMs = stats.DurationMs
        };
    }

    private static string Truncate(string text, int max) => text.Length <= max ? text : text[..max] + "...";
}
