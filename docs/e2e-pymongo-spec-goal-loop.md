# Goal: PyMongo E2E Compatibility Test Suite

You are working in `/Users/smarzola/projects/mongolino`.

Your objective is to add a functional/e2e test suite that validates `mongolino` through a real MongoDB driver, runs locally with `uv`, and runs in GitHub Actions CI. Use PyMongo and pytest for the first end-to-end layer. Use MongoDB's official specifications as design inspiration, especially the Unified Test Format and CRUD API spec, but do not attempt to run the upstream test corpus wholesale.

The suite should prove observable MongoDB wire compatibility for the documented `mongolino` subset: handshake, insert, find, update, delete, and explicit errors. Keep the suite deterministic, local-first, CI-friendly, and honest about unsupported features.

## Repository Rules

Follow `AGENTS.md`.

Important reminders:

- Keep the server implementation in Rust and the durable storage layer in SQLite.
- Treat MongoDB wire compatibility as observable behavior: validate changes with real client handshakes when possible.
- Run `cargo fmt` and `cargo test` before handing off code changes.
- Keep unsupported MongoDB commands explicit by returning command errors instead of silently accepting behavior.
- Do not use Docker or external MongoDB services for this goal. The test server is the locally built `mongolino` binary using a temporary SQLite file.
- Use `uv` for Python dependency management and test execution.
- Keep repo docs concise and tool-neutral where possible, but it is appropriate for this goal to document the e2e command because it is part of the supported test workflow.
- Do not revert unrelated user changes.
- Prefer the repo's existing patterns and docs style.
- If e2e tests expose a real product gap in the documented subset, fix the product rather than weakening the test.
- At the end of each milestone, run verification, mark the milestone done in this file, commit the completed milestone, and report the commit hash before continuing.

## External References

Use these as source-grounded inspiration:

- MongoDB specifications repo: `https://github.com/mongodb/specifications`
  - The repo describes itself as holding in-progress and completed specifications for MongoDB features, drivers, and associated products.
- Unified Test Format: `https://specifications.readthedocs.io/en/latest/unified-test-format/unified-test-format/`
  - It defines a shared YAML/JSON schema for specification tests that run operations against a MongoDB deployment.
  - It is broader than `mongolino` and includes many features that require real MongoDB server behavior. Use its structure and terminology as inspiration only.
- CRUD API spec: `https://specifications.readthedocs.io/en/latest/crud/crud/`
  - Use it to choose operation names, expected high-level behavior, and result assertions for insert/find/update/delete.
- uv GitHub Actions docs: `https://docs.astral.sh/uv/guides/integration/github/`
  - Use `astral-sh/setup-uv`, `uv sync --locked --all-extras --dev` or the appropriate locked sync command for this repo, and `uv run pytest ...`.
- PyMongo docs: `https://www.mongodb.com/docs/languages/python/pymongo-driver/current/`
  - Use PyMongo as the real client. Configure connection options conservatively for a standalone localhost server.

## Target State

By the end, the repo should have:

- A committed `pyproject.toml` configured for a non-packaged Python test harness, with pytest and PyMongo managed by `uv`.
- A committed `uv.lock`.
- A `tests/e2e/` pytest suite that starts `mongolino`, connects with PyMongo over TCP, runs operations, and tears down cleanly.
- A reusable pytest fixture that:
  - builds or locates `target/debug/mongolino`;
  - binds the server to a free localhost port;
  - uses a temporary SQLite file;
  - waits for `ping` to succeed;
  - yields a configured `MongoClient`;
  - terminates the server process reliably;
  - captures server stdout/stderr on failure.
- A small spec-inspired corpus under `tests/spec_corpus/` and a pytest runner that executes only the subset `mongolino` supports.
- GitHub Actions CI that runs Rust checks and PyMongo e2e checks on every push and pull request.
- README development docs that explain local Rust and e2e verification commands.

## Current State

The repo currently has:

- Rust server implementation in `src/main.rs`.
- Rust unit tests in `src/main.rs`; current full suite passes with 39 tests.
- `Cargo.toml` and `Cargo.lock`.
- README documentation for the MongoDB interface surface.
- Goal-loop docs for the CRUD uplift and adversarial fixes.

The repo currently does not have:

- Python test dependencies.
- `pyproject.toml` for e2e tooling.
- `uv.lock`.
- `tests/e2e/` or `tests/spec_corpus/`.
- GitHub Actions workflows.
- Real-driver CI coverage.

Known constraints:

- `mongolino` is not a full MongoDB server.
- Server-side cursors and `getMore` are unsupported.
- Auth, retryable writes, sessions beyond accepting `lsid`, transactions, write concern, collation, and secondary indexes are unsupported.
- PyMongo may send driver-level commands or options that `mongolino` does not yet support. Configure the client to avoid unsupported features where possible, and let the e2e suite expose product gaps only for the documented subset.

## Definition Of Done

The goal is complete only when:

1. `uv sync --locked --dev` or the repo's chosen locked uv sync command succeeds from a clean checkout.
2. `uv run pytest tests/e2e` starts the local `mongolino` binary and passes.
3. The e2e suite uses PyMongo, not hand-rolled raw socket calls, for the main compatibility assertions.
4. The e2e suite covers handshake, insert, find, update, delete, and explicit error behavior.
5. The e2e suite includes not-happy-path and adversarial tests for duplicate keys, unsupported operators, malformed updates, invalid delete options, projection edge cases, numeric `$inc` overflow, and process startup failures.
6. The spec-inspired corpus runner executes committed local YAML or JSON cases and supports skip/xfail metadata for unsupported MongoDB features.
7. CI runs `cargo fmt -- --check`, `cargo test`, `cargo build`, locked uv sync, and `uv run pytest tests/e2e`.
8. README documents local e2e commands and CI expectations concisely.
9. `cargo fmt`, `cargo test`, and `uv run pytest tests/e2e` pass locally.
10. Milestone checkboxes in this file are marked `[x]` as work completes.
11. Each completed milestone has a focused commit.
12. Final verification commands pass or any unrelated/environmental failures are documented with evidence.

## Milestone Checklist

When a milestone is complete:

1. Run the milestone's verification commands.
2. Update this checklist by changing the milestone from `[ ]` to `[x]`.
3. Add a short status note under that milestone with the date, exact commands run, and commit hash if available.
4. Commit the code, tests, docs, and checklist/status update with a focused commit message.
5. Report the commit hash in the goal-loop status before starting the next milestone.

- [ ] Milestone 0: Python/uv test tooling foundation
- [ ] Milestone 1: PyMongo server fixture and handshake smoke
- [ ] Milestone 2: CRUD e2e behavior coverage
- [ ] Milestone 3: Spec-inspired corpus runner
- [ ] Milestone 4: CI and documentation

## Milestone 0: Python/uv Test Tooling Foundation

Problem:

- The repo has Rust tests but no Python e2e tooling.
- CI and local developers need a reproducible way to install PyMongo and pytest without relying on global Python packages.

Desired behavior:

- The repo can install the Python e2e environment through `uv`.
- The Python test project is explicitly non-packaged; it exists only to run tests.
- Basic pytest discovery works before server orchestration is introduced.

Acceptance criteria:

- Add `pyproject.toml` with:
  - a project name appropriate for test tooling;
  - `requires-python` compatible with GitHub Actions and local uv Python;
  - pytest and PyMongo as dev dependencies or dependency-group entries;
  - `tool.uv.package = false` unless there is a strong reason to package the repo.
- Generate and commit `uv.lock`.
- Add a minimal `tests/e2e/test_environment.py` proving pytest runs and can import `pymongo`.
- Add a pytest marker such as `e2e` if useful.
- Do not add broad repo policy text to `AGENTS.md`.
- Milestone status is marked done in this file and committed.

Likely files:

- `pyproject.toml`
- `uv.lock`
- `tests/e2e/test_environment.py`
- `docs/e2e-pymongo-spec-goal-loop.md`

Verification:

```bash
uv sync --locked --dev
uv run pytest tests/e2e/test_environment.py
```

If sandbox cache permissions block uv, use a temporary cache path such as:

```bash
UV_CACHE_DIR=/private/tmp/mongolino-uv-cache uv sync --locked --dev
UV_CACHE_DIR=/private/tmp/mongolino-uv-cache uv run pytest tests/e2e/test_environment.py
```

Commit requirement:

- Commit after marking this milestone done and adding the status note.

## Milestone 1: PyMongo Server Fixture and Handshake Smoke

Problem:

- Real compatibility must be tested through a driver over TCP.
- Tests need to start and stop the server reliably without fixed ports or persistent database files.

Desired behavior:

- Pytest can start `mongolino`, connect with PyMongo, run handshake/ping checks, and shut down the server cleanly.
- Failures include enough server logs to debug startup, handshake, and command errors.

Acceptance criteria:

- Add `tests/e2e/conftest.py` with fixtures for:
  - locating or building `target/debug/mongolino`;
  - allocating a free port on `127.0.0.1`;
  - creating a temp SQLite path;
  - starting the process with `--addr` and `--db`;
  - polling `client.admin.command("ping")` until success or timeout;
  - yielding a PyMongo client configured with conservative localhost options;
  - terminating the process on teardown;
  - dumping stdout/stderr when startup or teardown fails.
- Client options should avoid unsupported features where possible:
  - `directConnection=true`;
  - short server selection and connect timeouts;
  - `retryWrites=false`;
  - no auth.
- Add handshake smoke tests for:
  - `ping`;
  - `hello` or equivalent `admin.command`;
  - `buildInfo` or `server_info()` if PyMongo can call it against `mongolino`.
- Add startup adversarial tests or fixture-level tests for:
  - missing binary gives a clear failure;
  - server exits early produces captured logs;
  - port allocation does not use a hard-coded port.
- If PyMongo sends an unsupported command during normal connection, fix `mongolino` if the command is reasonable for basic driver compatibility; otherwise configure the client or document the limitation explicitly.
- Milestone status is marked done in this file and committed.

Likely files:

- `tests/e2e/conftest.py`
- `tests/e2e/test_handshake.py`
- `src/main.rs` if basic driver compatibility exposes a product gap
- `docs/e2e-pymongo-spec-goal-loop.md`

Verification:

```bash
cargo build
uv run pytest tests/e2e/test_handshake.py
cargo test
```

Commit requirement:

- Commit after marking this milestone done and adding the status note.

## Milestone 2: CRUD E2E Behavior Coverage

Problem:

- Rust unit tests exercise internals, but PyMongo may send different wire message shapes and command options.
- The documented CRUD subset should be proven through normal driver APIs.

Desired behavior:

- A real PyMongo client can perform the documented CRUD subset against `mongolino`.
- Tests assert both return values and persisted data, including error preservation behavior.

Acceptance criteria:

- Add e2e tests for insert:
  - `insert_one` with explicit `_id`;
  - generated `_id`;
  - `insert_many` ordered and unordered behavior if PyMongo can express the supported subset;
  - duplicate `_id` failure preserves original document.
- Add e2e tests for find:
  - `find_one` by `_id`;
  - field equality;
  - dotted path matching;
  - supported operators `$eq`, `$ne`, `$gt`, `$gte`, `$lt`, `$lte`, `$in`, `$nin`, `$exists`;
  - logical operators `$and`, `$or`, `$nor`, and `$not` where PyMongo sends the expected command;
  - projection including `_id`-only projection, `_id: 0`, and inclusion with `_id: 0`;
  - sort, skip, limit, and batch size behavior documented for closed cursors.
- Add e2e tests for update:
  - `update_one` with `$set`;
  - `update_many` with `$inc`;
  - `$unset`;
  - replacement update if PyMongo exposes it cleanly;
  - upsert;
  - duplicate key or `_id` immutability errors preserve existing data;
  - `$inc` integer overflow returns an error and preserves existing data.
- Add e2e tests for delete:
  - `delete_one`;
  - `delete_many`;
  - repeated delete is a no-op with zero deleted count.
- Add explicit error tests for:
  - unsupported query operator such as `$where` or `$regex`;
  - unsupported update operator such as `$push`;
  - invalid delete command through `db.command` if PyMongo helpers cannot express it;
  - unsupported command remains explicit.
- Keep tests isolated by using unique database and collection names per test. Do not rely on `drop` unless `drop` is implemented; prefer unique names.
- Mark tests with clear helper assertions so failures identify whether driver connection, command response, or persisted data is wrong.
- If driver-level tests expose missing basic compatibility, fix `src/main.rs` and add Rust regression tests where appropriate.
- Milestone status is marked done in this file and committed.

Likely files:

- `tests/e2e/test_crud.py`
- `tests/e2e/test_errors.py`
- `tests/e2e/conftest.py`
- `src/main.rs` if e2e reveals server gaps
- `docs/e2e-pymongo-spec-goal-loop.md`

Verification:

```bash
cargo fmt
cargo test
cargo build
uv run pytest tests/e2e
```

Commit requirement:

- Commit after marking this milestone done and adding the status note.

## Milestone 3: Spec-Inspired Corpus Runner

Problem:

- A pile of imperative pytest tests is useful, but the suite should also be able to grow toward standardized MongoDB-style cases.
- The upstream Unified Test Format is too broad for `mongolino` today, but its structure is useful for long-term compatibility tracking.

Desired behavior:

- The repo has a small local YAML or JSON corpus inspired by MongoDB's Unified Test Format and CRUD spec.
- A pytest runner executes the local corpus against PyMongo and supports explicit skip/xfail metadata for unsupported features.

Scope controls:

- Do not vendor the full upstream MongoDB specification repository.
- Do not copy large upstream YAML test files wholesale.
- Do not imply official compliance with MongoDB's full test suite.
- Keep the runner intentionally small and documented as `mongolino`'s local compatibility corpus.

Acceptance criteria:

- Add `tests/spec_corpus/` with local cases covering:
  - handshake/ping;
  - insert duplicate key preservation;
  - find projection and supported operators;
  - update `$set`, `$inc`, upsert, and unsupported operator failure;
  - delete one/many;
  - unsupported command failure.
- Each corpus file includes metadata:
  - test name;
  - source inspiration such as `crud` or `unified-test-format`;
  - supported status or skip reason;
  - setup documents;
  - operations;
  - expected result or expected error;
  - expected final documents where applicable.
- Add `tests/e2e/test_spec_corpus.py` to load and run the local corpus.
- Use PyYAML or JSON only if it is managed in `pyproject.toml` and locked in `uv.lock`.
- The runner should reject malformed local corpus files with useful assertion messages.
- Add adversarial corpus runner tests or cases for:
  - unknown operation in corpus;
  - unsupported expected assertion shape;
  - malformed setup document;
  - skipped unsupported feature case is reported as skipped, not passed.
- Document in code comments or README that this is a local subset inspired by MongoDB specifications, not an official compliance claim.
- Milestone status is marked done in this file and committed.

Likely files:

- `tests/spec_corpus/*.yaml` or `tests/spec_corpus/*.json`
- `tests/e2e/test_spec_corpus.py`
- `pyproject.toml`
- `uv.lock`
- `docs/e2e-pymongo-spec-goal-loop.md`

Verification:

```bash
uv sync --locked --dev
uv run pytest tests/e2e/test_spec_corpus.py
uv run pytest tests/e2e
```

Commit requirement:

- Commit after marking this milestone done and adding the status note.

## Milestone 4: CI and Documentation

Problem:

- The suite needs to run consistently in CI and be discoverable locally.
- CI should catch both internal Rust regressions and real-driver compatibility regressions.

Desired behavior:

- GitHub Actions runs Rust checks and PyMongo e2e checks on pushes and pull requests.
- README concisely documents the commands developers and agents should run.

Acceptance criteria:

- Add a GitHub Actions workflow under `.github/workflows/`.
- The workflow runs on `push` and `pull_request`.
- The workflow includes:
  - checkout;
  - Rust toolchain availability using the default runner or a minimal official action if needed;
  - `cargo fmt -- --check`;
  - `cargo test`;
  - `cargo build`;
  - `astral-sh/setup-uv`;
  - locked uv sync;
  - `uv run pytest tests/e2e`.
- If GitHub Actions needs a specific Python version, set it explicitly through uv or setup-python in the simplest maintainable way.
- Do not add Docker services or MongoDB services.
- Add README development instructions for:
  - Rust-only checks;
  - e2e checks;
  - what the e2e suite starts locally;
  - how the local spec corpus relates to MongoDB's official specifications.
- Run final local verification.
- Milestone status is marked done in this file and committed.

Likely files:

- `.github/workflows/ci.yml`
- `README.md`
- `docs/e2e-pymongo-spec-goal-loop.md`

Verification:

```bash
cargo fmt -- --check
cargo test
cargo build
uv sync --locked --dev
uv run pytest tests/e2e
```

Commit requirement:

- Commit after marking this milestone done and adding the status note.

## Final Verification

Before the goal is complete, run:

```bash
cargo fmt -- --check
cargo test
cargo build
uv sync --locked --dev
uv run pytest tests/e2e
```

If `uv` fails because of sandbox cache permissions, rerun with a temp cache under `/private/tmp` and record the exact command. Do not write sandbox cache workaround prose into project docs unless it is necessary for normal local use outside Codex.

If PyMongo exposes a product gap, either fix the documented subset or explicitly document and test the limitation. Do not silently skip a failing e2e path without a named reason.

## Final Response Required

When complete, report:

- target state achieved or not achieved;
- commits made, with hashes;
- files changed;
- exact verification commands run and results;
- whether CI was added and what it runs;
- known residual risks or follow-up issues.
