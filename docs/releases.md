# Releases

Release Please keeps a release PR with the next version and changelog. Its Python strategy updates `pyproject.toml`; the extra-file rule updates only this project's version in `uv.lock`.

Merge the release PR after its checks pass. Release Please then creates the `vX.Y.Z` tag and GitHub release. The published release triggers the existing Docker workflow. AI release notes run against that exact tag.

The lockfile selector uses `name.value` because Release Please’s TOML parser wraps scalar values. It was checked against the version bundled with the action.

The migration starts from the last published version, `v1.38.3`. Unreleased commits stay in the first release PR.

## One-time GitHub App setup

Before merging this migration:

1. Create a GitHub App with repository permissions **Contents: read/write**, **Pull requests: read/write**, and **Issues: read/write**. No webhook is needed.
2. Install it on this repository only. It does not need permission to bypass branch protection.
3. Set repository variable `RELEASE_APP_ID` to the App ID.
4. Generate a private key and store it as repository secret `RELEASE_APP_PRIVATE_KEY`.

The workflow requests a short-lived installation token for this repository. The default `GITHUB_TOKEN` would suppress CI runs on generated PRs and the Docker workflow on published releases.

After merging, check that the first release PR runs CI, Codecov, and Cubic. Keep the existing required checks. If checks do not run, check the App installation and any bot filters in the review service.

## Recovery

The Release workflow can be dispatched manually. If AI notes fail after publication, the release already exists; do not delete its tag or make another release to retry the notes. The Docker workflow can be dispatched separately with the published tag.
