## Release pipeline

This repository now uses a GitHub Actions release pipeline that cuts SemVer
releases from `main`, publishes binaries with GoReleaser, and publishes
multi-arch container images to GHCR.

### Trigger and flow

1. Merge a PR into `main`.
2. `/Users/cam/Documents/Repos/cam3ron2/github-stats/.github/workflows/release.yml`
   runs.
3. `semantic-release` evaluates Conventional Commits since the previous tag,
   updates `CHANGELOG.md`, creates a SemVer tag (`vX.Y.Z`), and creates the
   GitHub release.
4. GoReleaser builds release binaries and uploads archives/checksums to that
   release.
5. Docker Buildx builds and pushes a multi-arch image for `linux/amd64` and
   `linux/arm64` to `ghcr.io/<owner>/<repo>`.
6. The pipeline updates
   `/Users/cam/Documents/Repos/cam3ron2/github-stats/deploy/kustomize/base/app-deployment.yaml`
   to the new release tag and commits that change back to `main` with
   `[skip ci]`.

### Image tags

Each release publishes:

- `ghcr.io/<owner>/<repo>:vX.Y.Z`
- `ghcr.io/<owner>/<repo>:X.Y.Z`
- `ghcr.io/<owner>/<repo>:latest`

### Required repository settings

- Enforce Conventional Commits in PR titles or commits (recommended).
- Ensure branch protection requires CI on `main` before merge.
- Ensure Actions have permission to write:
  - `contents: write` for tags/releases/changelog commit
  - `packages: write` for GHCR image publishing

### GitOps alignment

- Release versioning is derived from commits on `main`.
- Tags are immutable release references.
- Release artifacts (binary archives and container images) are produced from the
  tagged commit.
- Deployment manifest state is updated in Git to the released image tag.
- Changelog is automatically generated and committed back to Git.
