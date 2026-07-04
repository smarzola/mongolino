# Goal: Application Compatibility Adversarial Fixes

You are working in `/Users/smarzola/projects/mongolino`.

Your objective is to fix the adversarial review findings from the application compatibility uplift without broadening scope. The implementation already added cursor, catalog, metadata, and index compatibility. This follow-up must make the cursor edge behavior non-livelocking, align docs with the implemented index cleanup behavior, and add regression tests that would have caught both issues.

## Repository Rules

Follow `AGENTS.md`.

Important reminders:

- Keep the server implementation in Rust and the durable storage layer in SQLite.
- Treat MongoDB wire compatibility as observable behavior: validate changes with real client handshakes when possible.
- Run `cargo fmt` and `cargo test` before handing off code changes.
- Keep unsupported MongoDB commands explicit by returning command errors instead of silently accepting behavior.
- Use the existing PyMongo e2e suite for real driver verification.
- Use `uv` for Python tooling.
- Do not use Docker or external MongoDB services for this goal.
- Do not revert unrelated user or agent changes.
- This is a fix pass, not a refactor pass.

## Target State

By the end:

- `getMore` cannot return an empty `nextBatch` while leaving the same live cursor open because of `batchSize: 0`.
- Direct command callers get deterministic, explicit behavior for malformed or unsafe `getMore` batch sizes.
- README compatibility text accurately says `drop` removes user index metadata and maintained index entries.
- Rust and PyMongo e2e tests cover the adversarial cursor case and the lifecycle/index cleanup documentation path where practical.

## Current State

The application compatibility uplift is implemented on `master` but not yet pushed. Current review findings:

- `src/main.rs` accepts `getMore` with `batchSize: 0`, loops zero times, returns an empty `nextBatch`, and keeps the cursor alive. Repeated direct calls can livelock a client.
- `README.md` still says index cleanup will become relevant once user indexes exist, even though the implementation now supports user indexes and deletes their metadata/entries on `drop`.

## Definition Of Done

1. `getMore` has explicit non-livelocking behavior for `batchSize: 0`; prefer a command error unless you find MongoDB/PyMongo compatibility evidence that a different non-livelocking behavior is more appropriate.
2. Negative, zero, non-integer, unsupported-option, missing-collection, and invalid-id `getMore` paths are covered by Rust tests.
3. A PyMongo/direct-command e2e test proves `batchSize: 0` does not leave clients in an empty-batch live-cursor loop.
4. README compatibility table accurately describes `drop` index cleanup now that user indexes exist.
5. Existing cursor, lifecycle, and index tests still pass.
6. `cargo fmt -- --check`, `cargo test`, `cargo build`, `UV_CACHE_DIR=/private/tmp/mongolino-uv-cache uv lock --check`, `UV_CACHE_DIR=/private/tmp/mongolino-uv-cache uv sync --locked --dev`, and `UV_CACHE_DIR=/private/tmp/mongolino-uv-cache uv run --locked pytest tests/e2e` pass locally. Use unsandboxed execution for localhost binding if needed.
7. This file is updated with `[x]` milestone status, a short status note, exact commands run, and commit hash.
8. The finished fix is committed with a focused commit message.

## Milestone Checklist

When the milestone is complete:

1. Run the milestone's verification commands.
2. Update this checklist by changing the milestone from `[ ]` to `[x]`.
3. Add a short status note with the date, exact commands run, and commit hash if available.
4. Commit the code, tests, docs, and status-note update with a focused commit message.
5. Report the commit hash in the final response.

- [x] Milestone 1: Cursor livelock guard and docs correction

Status note 2026-07-04:

- Implemented explicit `getMore` command error for `batchSize: 0` (`code: 9`, `batchSize must be positive`) before cursor mutation, leaving the cursor usable for a valid follow-up `getMore`.
- Updated README `drop` compatibility text and Rust lifecycle coverage to assert user index metadata and maintained index entries are removed.
- Verification commands run:
  - `cargo fmt -- --check` - passed.
  - `cargo test get_more` - passed after correcting the implementation target.
  - `cargo test` - passed after correcting the drop index-entry fixture.
  - `cargo build` - passed.
  - `UV_CACHE_DIR=/private/tmp/mongolino-uv-cache uv lock --check` - passed.
  - `UV_CACHE_DIR=/private/tmp/mongolino-uv-cache uv sync --locked --dev` - passed.
  - `UV_CACHE_DIR=/private/tmp/mongolino-uv-cache uv run --locked pytest tests/e2e` - sandboxed run failed on localhost bind with `PermissionError: [Errno 1] Operation not permitted`; unsandboxed rerun passed with 73 passed, 1 skipped.
- Commit hash: pending at time of note.

## Milestone 1: Cursor Livelock Guard and Docs Correction

Problem:

- `getMore` with `batchSize: 0` can produce an empty batch while preserving the cursor, which is hostile to direct command callers and hides an easy infinite-loop pattern.
- README has stale lifecycle text for `drop` now that user indexes and index entries exist.

Desired behavior:

- `getMore` must either reject `batchSize: 0` with a command error or otherwise close/progress the cursor in a way that cannot livelock. Prefer an explicit command error for this project's honest subset unless compatibility evidence says otherwise.
- The compatibility table should state that `drop` removes collection documents, the catalog entry, user index metadata, and maintained index entries.

Acceptance criteria:

- Update `src/main.rs` so `getMore` cannot keep a cursor alive with an empty returned batch caused by `batchSize: 0`.
- Add or update Rust tests near existing cursor tests for:
  - `getMore` rejects or safely handles `batchSize: 0`;
  - cursor state remains usable or is deterministically closed according to the chosen behavior;
  - malformed `getMore` cases still return command errors.
- Add or update `tests/e2e/test_cursors.py` with a direct `database.command(...)` adversarial test for `batchSize: 0`.
- Update `README.md` `drop` row to match the implemented index cleanup behavior.
- Update this file's checklist/status note.
- Commit the finished fix.

Likely files:

- `src/main.rs`
- `tests/e2e/test_cursors.py`
- `README.md`
- `docs/application-compatibility-adversarial-fix-goal-loop.md`

Verification:

```bash
cargo fmt -- --check
cargo test get_more
cargo test
cargo build
UV_CACHE_DIR=/private/tmp/mongolino-uv-cache uv lock --check
UV_CACHE_DIR=/private/tmp/mongolino-uv-cache uv sync --locked --dev
UV_CACHE_DIR=/private/tmp/mongolino-uv-cache uv run --locked pytest tests/e2e
```

Commit requirement:

- Commit after marking this milestone done and adding the status note.

## Final Response Requirements

Report:

- files changed;
- exact verification commands and pass/fail result;
- commit hash;
- any residual risk or compatibility choice, especially the chosen `getMore batchSize: 0` behavior.
