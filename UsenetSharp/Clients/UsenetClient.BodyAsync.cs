using System.IO.Pipelines;
using System.Runtime.ExceptionServices;
using UsenetSharp.Exceptions;
using UsenetSharp.Models;

namespace UsenetSharp.Clients;

public partial class UsenetClient
{
    public Task<UsenetBodyResponse> BodyAsync(SegmentId segmentId, CancellationToken cancellationToken)
    {
        return BodyAsync(segmentId, null, cancellationToken);
    }

    public async Task<UsenetBodyResponse> BodyAsync
    (
        SegmentId segmentId,
        Action<ArticleBodyResult>? onConnectionReadyAgain,
        CancellationToken cancellationToken
    )
    {
        ThrowIfDisposed();
        var validatedSegmentId = ValidateSegmentId(segmentId);
        try
        {
            await _commandLock.WaitAsync(cancellationToken).ConfigureAwait(false);
        }
        catch
        {
            onConnectionReadyAgain?.Invoke(ArticleBodyResult.NotRetrieved);
            throw;
        }

        var isReadBodyToPipeAsyncStarted = false;
        CancellationTokenSource? operationCts = null;

        try
        {
            ThrowIfDisposed();
            ThrowIfUnhealthy();
            ThrowIfNotConnected();
            operationCts = CreateOperationTokenSource(cancellationToken);

            // Send BODY command with message-id
            await WriteLineAsync($"BODY <{validatedSegmentId}>".AsMemory(), operationCts.Token).ConfigureAwait(false);
            var response = await ReadLineAsync(operationCts.Token).ConfigureAwait(false);
            var responseCode = ParseResponseCode(response);

            // Article retrieved - body follows
            if (responseCode == (int)UsenetResponseType.ArticleRetrievedBodyFollows)
            {
                // Create a pipe for streaming the body data
                var pipe = new Pipe(new PipeOptions(
                    pauseWriterThreshold: 1024 * 1024,
                    resumeWriterThreshold: 512 * 1024));

                // Start background task to read the body and write to pipe
                isReadBodyToPipeAsyncStarted = true;
                _ = ReadBodyToPipeAsync(
                    pipe.Writer, operationCts, cancellationToken, onConnectionReadyAgain);
                operationCts = null;

                // Return immediately with the stream and headers
                return new UsenetBodyResponse
                {
                    SegmentId = segmentId,
                    ResponseCode = responseCode,
                    ResponseMessage = response!,
                    Stream = pipe.Reader.AsStream(),
                };
            }

            return new UsenetBodyResponse()
            {
                ResponseCode = responseCode,
                ResponseMessage = response!,
                SegmentId = segmentId,
                Stream = null
            };
        }
        finally
        {
            if (!isReadBodyToPipeAsyncStarted)
            {
                operationCts?.Dispose();
                _commandLock.Release();
                onConnectionReadyAgain?.Invoke(ArticleBodyResult.NotRetrieved);
            }
        }
    }

    private async Task ReadBodyToPipeAsync(
        PipeWriter writer,
        CancellationTokenSource operationCts,
        CancellationToken callerCancellationToken,
        Action<ArticleBodyResult>? onConnectionReadyAgain)
    {
        Exception? failure = null;
        try
        {
            if (_reader == null)
            {
                throw new UsenetNotConnectedException("The NNTP connection closed before the article body was read.");
            }

            var shouldWrite = true;
            long drainedBytes = 0;
            var cancellationToken = operationCts.Token;
            using var readTimeoutCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);

            // Read lines until we encounter the termination sequence (single dot on a line)
            while (true)
            {
                ReadOnlyMemory<byte>? lineMemory;
                try
                {
                    lineMemory = await ReadLineBytesAsync(readTimeoutCts, cancellationToken).ConfigureAwait(false);
                }
                catch (IOException)
                {
                    throw new UsenetProtocolException(
                        "The NNTP connection closed before the article body terminator was received.");
                }

                if (!lineMemory.HasValue)
                {
                    throw new UsenetProtocolException(
                        "The NNTP connection closed before the article body terminator was received.");
                }

                var line = lineMemory.Value.Span;

                // Check for NNTP termination sequence (single dot)
                if (line.Length == 1 && line[0] == (byte)'.')
                {
                    break;
                }

                if (!shouldWrite)
                {
                    drainedBytes += line.Length + 2;
                    if (drainedBytes > _options.AbandonedBodyDrainLimit)
                    {
                        throw new UsenetProtocolException(
                            "The abandoned NNTP body exceeded the configured drain limit.");
                    }

                    continue;
                }

                // NNTP escaping: Lines starting with ".." should have the first dot removed
                if (line.Length >= 2 && line[0] == (byte)'.' && line[1] == (byte)'.')
                {
                    line = line[1..];
                }

                // Copy protocol bytes directly so yEnc data never round-trips through UTF-16.
                var destination = writer.GetSpan(line.Length + 2);
                line.CopyTo(destination);
                destination[line.Length] = (byte)'\r';
                destination[line.Length + 1] = (byte)'\n';
                writer.Advance(line.Length + 2);

                // Flush periodically to make data available for reading
                var result = await writer.FlushAsync(cancellationToken).ConfigureAwait(false);
                if (result.IsCompleted || result.IsCanceled)
                {
                    shouldWrite = false;
                }
            }
        }
        catch (OperationCanceledException e) when (callerCancellationToken.IsCancellationRequested)
        {
            failure = e;
            var drainFailure = await TryDrainBodyAsync().ConfigureAwait(false);
            if (drainFailure != null)
            {
                RecordBackgroundFailure(drainFailure);
            }
        }
        catch (Exception e)
        {
            failure = e;
            RecordBackgroundFailure(e);
        }
        finally
        {
            await writer.CompleteAsync(failure).ConfigureAwait(false);
            operationCts.Dispose();
            _commandLock.Release();
            try
            {
                onConnectionReadyAgain?.Invoke(
                    failure == null ? ArticleBodyResult.Retrieved : ArticleBodyResult.NotRetrieved);
            }
            catch
            {
                // User callbacks must not fault the unobserved background transfer task.
            }
        }
    }

    private async Task<Exception?> TryDrainBodyAsync()
    {
        try
        {
            using var drainCts = CreateOperationTokenSource(CancellationToken.None);
            using var timeoutCts = CancellationTokenSource.CreateLinkedTokenSource(drainCts.Token);
            long drainedBytes = 0;

            while (true)
            {
                ReadOnlyMemory<byte>? line;
                try
                {
                    line = await ReadLineBytesAsync(timeoutCts, drainCts.Token).ConfigureAwait(false);
                }
                catch (IOException)
                {
                    return new UsenetProtocolException(
                        "The NNTP connection closed while draining a cancelled body.");
                }

                if (!line.HasValue)
                {
                    return new UsenetProtocolException(
                        "The NNTP connection closed while draining a cancelled body.");
                }

                var bytes = line.Value.Span;
                if (bytes.Length == 1 && bytes[0] == (byte)'.')
                {
                    return null;
                }

                drainedBytes += bytes.Length + 2;
                if (drainedBytes > _options.AbandonedBodyDrainLimit)
                {
                    return new UsenetProtocolException(
                        "The cancelled NNTP body exceeded the configured drain limit.");
                }
            }
        }
        catch (Exception e)
        {
            return e;
        }
    }

    private void RecordBackgroundFailure(Exception failure)
    {
        lock (_stateLock)
        {
            _backgroundException = ExceptionDispatchInfo.Capture(failure);
        }
    }
}
