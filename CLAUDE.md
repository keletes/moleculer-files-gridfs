# moleculer-files-gridfs

`@keletes/moleculer-files-gridfs`: GridFS storage driver for [`@keletes/moleculer-files`](https://github.com/keletes/moleculer-files), on the current `mongodb` driver (6.x). Used by business-cad, business-documentation and utilities-buffer. Design and usage are in `README.md`.

## Shape of this repo (like `@keletes/moleculer-db-adapter-mongo`)

- **Published to GitHub Packages** as `@keletes/moleculer-files-gridfs` (`"@keletes/moleculer-files-gridfs": "^X.Y.Z"`); `.npmrc` maps the `@keletes` scope to `https://npm.pkg.github.com`, no token in the repo. Installing `@keletes/*` in CI needs the org secret `PACKAGES_READ_TOKEN` (`NODE_AUTH_TOKEN` on install steps).
- **`dist/` is built at release time** (`@semantic-release/exec` runs `yarn build`) and shipped in the published package only; it is gitignored, never committed. `package.json` `version` is the placeholder `0.0.0-development`.
- `yarn.lock` is committed (CI installs with `--frozen-lockfile`).
- `mongodb`, `moleculer` and `@keletes/moleculer-files` are peer dependencies; `@keletes/moleculer-files` is also a devDependency (`^0.3.0`), bump the range when the base releases something the driver needs.

## Commands

```
yarn verify       # typecheck + lint (no mongod)
yarn test         # jest against a real throwaway mongod (mongodb-memory-server); = verify:int
yarn build        # tsc -> dist/
```

CI (`verify.yml`) runs `verify` plus `test` on ubuntu x64 and ubuntu-24.04-arm; all three are required checks on `master`.

## Gotchas

- **mongod version is pinned in `package.json` `config.mongodbMemoryServer.version`**, read by both mongodb-memory-server's postinstall and `test/pin.ts`/`test/global-setup.ts`. Do not hard-code the version elsewhere. `MONGOMS_VERSION` overrides. `test/tests/mongod-pin.spec.ts` fails if the running mongod is not the pinned version or not built for the host arch. When bumping the pin, check a build exists for `ubuntu-24.04` on both x64 and arm64.
- The CI mongod cache key is `hashFiles('package.json')`; any package.json change re-downloads once.
- Moleculer copies the adapter instance when it merges the service schema: in tests use `service.adapter`, not the instance you passed in.
- Consumers reach into `adapter.bucketFS`, `adapter.db` and `adapter.client` (utilities-buffer); keep them public.
- Files are addressed by `filename` and versioned (`metadata.version`, a string); `get` returns the latest revision. `remove` takes a GridFS `_id` (hex) or a filename (all revisions).

## Releases: trunk-based, Conventional Commits

**Every PR title must follow [Conventional Commits](https://www.conventionalcommits.org/)** (`.github/workflows/pr-title.yml`). Squash-merge uses the PR title as the `master` commit message, which `tag.yml` feeds to `semantic-release`:

| Prefix | Effect |
|---|---|
| `fix:` | patch |
| `feat:` | minor |
| `feat!:` or `BREAKING CHANGE:` footer | major |
| `chore:`, `docs:`, `style:`, `refactor:`, `perf:`, `test:`, `ci:`, `build:` | no release |

On a release, semantic-release builds `dist/`, publishes the package to GitHub Packages, tags `vX.Y.Z` and publishes a GitHub Release. No release commit is pushed. **Never bump the version or push a `v*` tag by hand.**

`tag.yml` creates the tag and release with the GitHub App token (org secrets `DEPLOY_APP_ID`, `DEPLOY_APP_PRIVATE_KEY`; the app must be installed on this repo) and publishes with `GITHUB_TOKEN` (`packages: write`). Node 22 is required for `yarn install`.

## Branching: check merge status first

*Always* check whether the current branch has already been merged into `master` before starting work: `git fetch origin`, `git branch -r --merged origin/master`, `git cherry origin/master`, and (squash merges) `gh pr list --head <branch> --state merged`. If merged, branch from an up-to-date `origin/master`. Every change goes through a feature branch and PR; `master` is protected. After a PR merges, check `master` out and fast-forward it before starting anything else.
