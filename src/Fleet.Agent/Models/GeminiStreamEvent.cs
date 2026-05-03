using System.Text.Json.Serialization;

namespace Fleet.Agent.Models;

/// <summary>
/// Represents a single event from `gemini --output-format stream-json`.
/// </summary>
public sealed class GeminiStreamEvent
{
    [JsonPropertyName("type")]
    public string Type { get; set; } = string.Empty;

    [JsonPropertyName("timestamp")]
    public DateTimeOffset? Timestamp { get; set; }

    // --- init event fields ---
    [JsonPropertyName("session_id")]
    public string? SessionId { get; set; }

    [JsonPropertyName("model")]
    public string? Model { get; set; }

    // --- message event fields ---
    [JsonPropertyName("role")]
    public string? Role { get; set; }

    [JsonPropertyName("content")]
    public string? Content { get; set; }

    [JsonPropertyName("delta")]
    public bool? Delta { get; set; }

    // --- result event fields ---
    [JsonPropertyName("status")]
    public string? Status { get; set; }

    [JsonPropertyName("stats")]
    public GeminiStats? Stats { get; set; }

    [JsonPropertyName("error")]
    public string? Error { get; set; }

    // --- tool_use event fields ---
    [JsonPropertyName("tool_call_id")]
    public string? ToolCallId { get; set; }

    [JsonPropertyName("name")]
    public string? ToolName { get; set; }

    [JsonPropertyName("arguments")]
    public Dictionary<string, object>? Arguments { get; set; }

    [JsonExtensionData]
    public Dictionary<string, object>? ExtensionData { get; set; }
}

public sealed class GeminiStats
{
    [JsonPropertyName("total_tokens")]
    public int TotalTokens { get; set; }

    [JsonPropertyName("input_tokens")]
    public int InputTokens { get; set; }

    [JsonPropertyName("output_tokens")]
    public int OutputTokens { get; set; }

    [JsonPropertyName("duration_ms")]
    public int DurationMs { get; set; }

    [JsonPropertyName("tool_calls")]
    public int ToolCalls { get; set; }
}
