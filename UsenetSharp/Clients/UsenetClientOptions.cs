namespace UsenetSharp.Clients;

/// <summary>
/// Configures NNTP operation timeouts and connection reuse behavior.
/// </summary>
public sealed record UsenetClientOptions
{
    /// <summary>
    /// Gets the maximum idle time allowed for an individual network read or write.
    /// </summary>
    public TimeSpan ReadTimeout { get; init; } = TimeSpan.FromSeconds(10);

    /// <summary>
    /// Gets the maximum number of bytes drained after a body consumer stops reading.
    /// </summary>
    public long AbandonedBodyDrainLimit { get; init; } = 1024 * 1024;
}
