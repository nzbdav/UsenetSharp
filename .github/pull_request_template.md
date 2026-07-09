## Summary

- What changed?
- Why is the change needed?

## Validation

- [ ] `dotnet restore --locked-mode`
- [ ] `dotnet build --configuration Release --no-restore`
- [ ] `dotnet test --configuration Release --no-build --filter "TestCategory!=Integration"`
- [ ] `dotnet pack UsenetSharp/UsenetSharp.csproj --configuration Release --no-build`
- [ ] Documentation and regression tests are updated where needed.
- [ ] No credentials, tokens, or private article data are included.

## Compatibility

Describe any public API, package, protocol, performance, or lifecycle impact.
