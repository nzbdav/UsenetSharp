using System.Buffers;
using System.Text;
using UsenetSharp.Exceptions;

namespace UsenetSharp.Clients;

internal sealed class NntpLineReader(Stream stream, int maximumLineLength = 64 * 1024) : IDisposable
{
    private const int BufferSize = 8192;
    private readonly byte[] _buffer = ArrayPool<byte>.Shared.Rent(BufferSize);
    private int _position;
    private int _length;
    private bool _disposed;

    public async ValueTask<string?> ReadLineAsync(CancellationToken cancellationToken)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        using var line = new MemoryStream();

        while (true)
        {
            if (_position >= _length)
            {
                _position = 0;
                _length = await stream.ReadAsync(_buffer.AsMemory(0, BufferSize), cancellationToken)
                    .ConfigureAwait(false);
                if (_length == 0)
                {
                    return line.Length == 0 ? null : DecodeLine(line);
                }
            }

            var available = _buffer.AsSpan(_position, _length - _position);
            var newlineIndex = available.IndexOf((byte)'\n');
            var count = newlineIndex >= 0 ? newlineIndex : available.Length;

            if (line.Length + count > maximumLineLength)
            {
                throw new UsenetProtocolException(
                    $"NNTP response line exceeded the {maximumLineLength}-byte limit.");
            }

            line.Write(available[..count]);
            _position += count;

            if (newlineIndex >= 0)
            {
                _position++;
                return DecodeLine(line);
            }
        }
    }

    private static string DecodeLine(MemoryStream line)
    {
        var bytes = line.GetBuffer().AsSpan(0, checked((int)line.Length));
        if (!bytes.IsEmpty && bytes[^1] == (byte)'\r')
        {
            bytes = bytes[..^1];
        }

        return Encoding.Latin1.GetString(bytes);
    }

    public void Dispose()
    {
        if (_disposed)
        {
            return;
        }

        _disposed = true;
        ArrayPool<byte>.Shared.Return(_buffer);
    }
}
