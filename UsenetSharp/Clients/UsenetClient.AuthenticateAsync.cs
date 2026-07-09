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

            // Send AUTHINFO USER command
            await WriteLineAsync($"AUTHINFO USER {user}".AsMemory(), operationCts.Token).ConfigureAwait(false);
            var userResponse = await ReadLineAsync(operationCts.Token).ConfigureAwait(false);
            var userResponseCode = ParseResponseCode(userResponse);

            // Password required
            if (userResponseCode == (int)UsenetResponseType.PasswordRequired)
            {
                // Send AUTHINFO PASS command
                await WriteLineAsync($"AUTHINFO PASS {pass}".AsMemory(), operationCts.Token).ConfigureAwait(false);
                var passResponse = await ReadLineAsync(operationCts.Token).ConfigureAwait(false);
                var passResponseCode = ParseResponseCode(passResponse);

                return new UsenetResponse()
                {
                    ResponseCode = passResponseCode,
                    ResponseMessage = passResponse!,
                };
            }

            return new UsenetResponse()
            {
                ResponseCode = userResponseCode,
                ResponseMessage = userResponse!,
            };
        }
        finally
        {
            _commandLock.Release();
        }
    }
}
