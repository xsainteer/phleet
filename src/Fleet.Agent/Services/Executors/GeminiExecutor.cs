using Fleet.Agent.Models;

namespace Fleet.Agent.Services.Executors;

public class GeminiExecutor : IAgentExecutor
{
    public ValueTask DisposeAsync()
    {
        throw new NotImplementedException();
    }

    public IAsyncEnumerable<AgentProgress> ExecuteAsync(string task, IReadOnlyList<MessageImage>? images = null, CancellationToken ct = default)
    {
        throw new NotImplementedException();
    }

    public Task StopProcessAsync()
    {
        throw new NotImplementedException();
    }

    public Task<bool> TryStopProcessAsync()
    {
        throw new NotImplementedException();
    }

    public void RequestRestart()
    {
        throw new NotImplementedException();
    }

    public IAsyncEnumerable<AgentProgress> SendCommandAsync(string command, CancellationToken ct = default)
    {
        throw new NotImplementedException();
    }

    public bool IsProcessWarm { get; }
    public string? LastSessionId { get; }
    public DateTimeOffset LastActivity { get; }
    public IReadOnlyCollection<BackgroundTaskInfo> GetActiveBackgroundTasks()
    {
        throw new NotImplementedException();
    }

    public Task<bool> CancelBackgroundTaskAsync(string taskId, CancellationToken ct = default)
    {
        throw new NotImplementedException();
    }
}