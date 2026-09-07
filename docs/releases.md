# Releases

Release Please keeps a release PR with the next version and changelog. Its Python strategy updates `pyproject.toml`; the extra-file rule updates only this project's version in `uv.lock`.

Merge the release PR after its checks pass. Release Please then creates the `vX.Y.Z` tag and GitHub release. The published release triggers the existing Docker workflow. AI release notes run against that exact tag.

The lockfile selector uses `name.value` because Release Please’s TOML parser wraps scalar values. It was checked against the version bundled with the action.

The migration starts from the last published version, `v1.38.3`. Unreleased commits stay in the first release PR.

## GitHub token

Store a personal access token as repository secret `RELEASE_PLEASE_TOKEN`. A fine-grained token needs access to this repository with **Contents: read/write**, **Pull requests: read/write**, and **Issues: read/write**. Renew the secret when the token expires or is rotated.

The personal token lets generated PRs trigger CI and published releases trigger Docker publishing. The default `GITHUB_TOKEN` suppresses those follow-up workflows. Keep the existing branch protection and required checks.

After merging, check that the first release PR runs CI, Codecov, and Cubic. If checks do not run, check token permissions and any bot filters in the review service.

## Recovery

The Release workflow can be dispatched manually. If AI notes fail after publication, the release already exists; do not delete its tag or make another release to retry the notes. The Docker workflow can be dispatched separately with the published tag.
