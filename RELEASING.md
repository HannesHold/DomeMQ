# Releasing DomeMQ

Maintainer notes for cutting a release. The workflow is `.github/workflows/release.yml`. It runs on a push of a tag that matches `v*`, for example `v0.1.0`. It does not run on commits to `master`.

The version published by that job is the tag with the leading `v` removed. `v0.1.0` publishes `0.1.0`.

## One-time setup

### NuGet Trusted Publishing

`DomeMQ.Protocol`, `DomeMQ.Client`, and `DomeMQ.Server` are pushed to `https://api.nuget.org/v3/index.json` with [Trusted Publishing](https://aka.ms/nuget/trusted-publishing). The release job has `id-token: write`, so GitHub issues an OIDC token for the job. `NuGet/login@v1` exchanges that token for a short-lived credential used only by that job. The repository does not store a long-lived NuGet key.

On nuget.org, create a Trusted Publishing policy for the account that owns the packages:

- Repository owner: `HannesHold`
- Repository: `DomeMQ`
- Workflow file: `release.yml`
- Environment: none. The release job does not set a GitHub environment.

In this GitHub repository, add an Actions secret `NUGET_USER` set to that nuget.org username. The login step passes the secret as `user`.

If `NUGET_USER` is missing or empty, the workflow logs that it is skipping NuGet publish and the job still succeeds. The container and the GitHub Release are still published.

### GitHub Container Registry

The image is `ghcr.io/<owner>/<repo>:<version>`. Owner and repository are lowercased, so a tag `v0.1.0` on `HannesHold/DomeMQ` pushes `ghcr.io/hanneshold/domemq:0.1.0`.

The first push creates the package. GitHub creates it private. Open the package on GHCR and change the visibility to public once. Later releases of the same package stay public.

## What a tag publishes

- NuGet packages `DomeMQ.Protocol`, `DomeMQ.Client`, and `DomeMQ.Server`, when `NUGET_USER` is set. The broker host and the Inspector are not packed.
- Container `ghcr.io/<owner>/<repo>:<version>`.
- A GitHub Release titled `DomeMQ vX.Y.Z` with these assets:
  - `DomeMQ-X.Y.Z-linux-x64.zip`
  - `DomeMQ-X.Y.Z-linux-arm64.zip`
  - `DomeMQ-X.Y.Z-win-x64.zip`
  - `DomeMQ-Inspector-X.Y.Z-linux-x64.zip`
  - `DomeMQ-Inspector-X.Y.Z-win-x64.zip`

The broker zips are self-contained single-file builds and include `appsettings.json`. The Inspector zips are self-contained folders (not single-file) for `linux-x64` and `win-x64`.

## Cut a release

1. Set `<Version>` in `Directory.Build.props` to `X.Y.Z` (for example `0.2.0`).
2. Commit that change.
3. Tag the commit `vX.Y.Z`. The tag must match the version in `Directory.Build.props`.
4. Push the commit and the tag. Pushing the tag starts the release workflow.

```bash
git add Directory.Build.props
git commit -m "Set version to X.Y.Z."
git tag vX.Y.Z
git push origin master
git push origin vX.Y.Z
```
