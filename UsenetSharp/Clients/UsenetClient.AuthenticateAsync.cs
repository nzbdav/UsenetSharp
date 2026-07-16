using UsenetSharp.Models;

namespace UsenetSharp.Clients;

public partial class UsenetClient
{
    public async Task<UsenetResponse> AuthenticateAsync(string user, string pass, CancellationToken cancellationToken)
    {
        ThrowIfDisposed();
        ValidateCommandValue(user, nameof(user), 497);
        ValidateCommandValue(pass, nameof(pass), 497);
        await _commandLock.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            ThrowIfDisposed();
            ThrowIfUnhealthy();
            ThrowIfNotConnected();
            using var operationCts = CreateOperationTokenSource(cancellationToken);

            var (userResponseCode, userResponse) = await ExchangeSingleLineAsync(
                ct => new ValueTask(WriteLineAsync($"AUTHINFO USER {user}".AsMemory(), ct)),
                operationCts.Token).ConfigureAwait(false);

            // Password required
            if (userResponseCode == (int)UsenetResponseType.PasswordRequired)
            {
                var (passResponseCode, passResponse) = await ExchangeSingleLineAsync(
                    ct => new ValueTask(WriteLineAsync($"AUTHINFO PASS {pass}".AsMemory(), ct)),
                    operationCts.Token).ConfigureAwait(false);

                return new UsenetResponse()
                {
                    ResponseCode = passResponseCode,
                    ResponseMessage = passResponse,
                };
            }

            return new UsenetResponse()
            {
                ResponseCode = userResponseCode,
                ResponseMessage = userResponse,
            };
        }
        finally
        {
            _commandLock.Release();
        }
    }
}
