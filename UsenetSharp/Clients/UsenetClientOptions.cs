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

    /// <summary>
    /// Gets whether <see cref="UsenetClient.DecodedBodyAsync(UsenetSharp.Models.SegmentId, CancellationToken)"/>
    /// validates decoded content against the CRC32 value in the yEnc trailer.
    /// </summary>
    /// <remarks>
    /// Disabled by default for backward compatibility. When enabled, a missing or
    /// mismatched CRC32 value fails the decoded response stream.
    /// </remarks>
    public bool ValidateDecodedBodyCrc32 { get; init; }
}
