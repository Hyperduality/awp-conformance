# AGENTS.md

awp-conformance is the conformance suite for the Agent World Protocol, published on PyPI as `awp-conformance`. It tests a world at a URL, or an agent against its own harness world, and reports a verdict per requirement ID for one specification revision. That revision is pinned as the `spec/` submodule and named by `SPEC_REVISION`.

## Checks

```bash
git submodule update --init
uv sync
uv run ruff check && uv run ruff format --check
uv run mypy
uv run pytest                                  # against awp-sim, awp-demo, and deliberately broken worlds
uv run python scripts/sync_spec.py --check
```

CI runs all of these. It also runs the suite against awp-sim in each configuration it has a fixture for.

## Code

- The suite shares no code with any implementation. `src/` never imports `awp` or `awp_sim`, which are development dependencies for the tests only.
- The suite speaks the protocol from the bundled schemas, the lifecycle table, and the requirement matrix. `src/awp_conformance/_spec/` is copied from `spec/` by `scripts/sync_spec.py`, so never edit it by hand.
- Name each test after the requirements it covers.
- A requirement the suite doesn't exercise is reported `untested`, never `pass`.
- `fixtures/` holds the fixtures for awp-sim. The CI of awp-sim fetches them from this repository's release tags.
- Comment only what the code can't say.

## Commits and pull requests

- Branch from `main` and open a pull request. Merge once CI passes.
- Write the title as one plain sentence in sentence case, with no trailing period, saying what changed: `Never land the frame-gaps seq gap on a resync frame`. A release commit is titled `Release 0.1.0a4`.
- Add a body only when the title can't carry the reason: one or two short sentences.
- Write commits the way a person on the project would. No `Co-Authored-By` trailers, no "Generated with" lines, and no other mention of AI tools, in commits or in PRs.
- The PR title matches the commit title, and the description is a few lines at most.

## Moving to a new draft revision

1. Check out the revision's tag (`spec-v0.1-draft.N`) in `spec/`.
2. Run `uv run python scripts/sync_spec.py`.
3. Update `SPEC_REVISION` in `src/awp_conformance/__init__.py`.
4. Fix whatever the tests report.

## Releasing

1. In a pull request:
   - Set the version with `uv version <version>`, for example `0.1.0a5`.
   - Add a `CHANGELOG.md` entry that starts "Targets specification revision `0.1-draft.N`." and lists what changed in the verdicts.
2. Once it is merged, tag the merge commit and push the tag:

   ```bash
   git tag -a v0.1.0a5 -m "awp-conformance 0.1.0a5 (AWP 0.1-draft.N)"
   git push origin v0.1.0a5
   ```

3. `release.yml` checks that the tag matches the version, then builds the package. It publishes through PyPI trusted publishing once someone approves the `pypi` environment.
   - A maintainer gives that approval. Agents never approve deployments, and never publish with a token.
4. After the release, update the suite pin in the CI of awp-python, awp-sim, and awp-typescript.
   - Regenerate their committed reports with the new version.
   - Update the suite version on the docs site's registry and badges pages.
