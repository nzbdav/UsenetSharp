# Security Policy

## Supported versions

Security fixes are provided for the latest released version. Please upgrade to
the newest package before reporting an issue that may already be resolved.

## Reporting a vulnerability

Do not open a public issue for a suspected vulnerability.

Use GitHub's
[private vulnerability reporting](https://github.com/hoivikaj/UsenetSharp/security/advisories/new)
to send a description, reproduction steps, affected versions, and any proposed
mitigation. You should receive an acknowledgement within seven days. We will
coordinate validation, remediation, and disclosure through the private
advisory.

If private reporting is unavailable, open a discussion that asks a maintainer
for a private contact channel without including vulnerability details.

## Security expectations

UsenetSharp relies on platform certificate validation for TLS connections.
NNTP credentials sent over a connection created with `useSsl: false` are
plaintext by protocol design. Do not use plaintext authentication on an
untrusted network.
