# Goal: Query and Atomic Compatibility Adversarial Fixes

You are working in `/Users/smarzola/projects/mongolino`.

Your objective is to fix adversarial review findings from the query and atomic modification compatibility uplift without broadening scope. The implementation already passed Rust tests and the full PyMongo e2e suite, so this is a hardening pass for malformed command shapes that currently slip through.

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

- A malformed `findAndModify` command cannot include both `findAndModify` and `findandmodify` command-name aliases and still execute.
- The preserved PyMongo `count_documents()` `$group` shape is exact: only `{ "_id": 1, "n": { "$sum": 1 } }` is accepted.
- General or malformed `$group` documents with extra group fields or extra accumulator fields return explicit command errors.
- Rust and PyMongo/direct-command e2e tests cover these adversarial cases.

## Current State

The query/atomic compatibility uplift is implemented on local `master` and has passed:

- `cargo fmt -- --check`
- `cargo test`
- `cargo build`
- `UV_CACHE_DIR=/private/tmp/mongolino-uv-cache uv lock --check`
- `UV_CACHE_DIR=/private/tmp/mongolino-uv-cache uv sync --locked --dev`
- unsandboxed `UV_CACHE_DIR=/private/tmp/mongolino-uv-cache uv run --locked pytest tests/e2e` with 88 passed, 1 skipped

Adversarial review findings:

1. `findAndModify` dispatch accepts both command aliases in the same BSON command document because both `findAndModify` and `findandmodify` are listed as allowed keys. A command document should have one command key. Accepting both hides a malformed command.
2. `is_count_documents_group` accepts `$group` documents with extra fields, for example `{ "_id": 1, "n": { "$sum": 1 }, "extra": { "$sum": 1 } }`, and accepts an `n` accumulator document with extra keys. That silently treats a malformed/general `$group` as the narrow count path.

## Definition Of Done

1. `findAndModify` / `findandmodify` rejects a command document containing both aliases with an explicit command error before mutating storage.
2. The exact PyMongo `count_documents()` `$group` shape remains supported.
3. `$group` rejects extra group fields and extra accumulator fields with explicit command errors.
4. Rust tests cover both fixes.
5. PyMongo/direct-command e2e tests cover both fixes.
6. Existing `findAndModify`, aggregation, metadata, cursor, and spec corpus tests still pass.
7. `cargo fmt -- --check`, `cargo test`, `cargo build`, `UV_CACHE_DIR=/private/tmp/mongolino-uv-cache uv lock --check`, `UV_CACHE_DIR=/private/tmp/mongolino-uv-cache uv sync --locked --dev`, and `UV_CACHE_DIR=/private/tmp/mongolino-uv-cache uv run --locked pytest tests/e2e` pass locally. Use unsandboxed execution for localhost binding if needed.
8. This file is updated with `[x]` milestone status, a short status note, exact commands run, and commit hash.
9. The finished fix is committed with a focused commit message.

## Milestone Checklist

When the milestone is complete:

1. Run the milestone's verification commands.
2. Update this checklist by changing the milestone from `[ ]` to `[x]`.
3. Add a short status note with the date, exact commands run, and commit hash if available.
4. Commit the code, tests, docs, and status-note update with a focused commit message.
5. Report the commit hash in the final response.

- [ ] Milestone 1: Reject ambiguous command aliases and exact-match count `$group`

## Milestone 1: Reject Ambiguous Command Aliases and Exact-Match Count `$group`

Problem:

- Ambiguous command aliases and permissive `$group` shape checks can mask malformed inputs as supported behavior.
- This violates the repo rule that unsupported MongoDB commands and shapes must return explicit errors instead of being silently accepted.

Desired behavior:

- `findAndModify` commands have one command key only.
- `$group` support remains intentionally narrow and exact for PyMongo `count_documents()`.

Acceptance criteria:

- Update `src/main.rs` so `findAndModify` / `findandmodify` rejects command documents containing both alias keys.
- Add or update Rust tests near existing `find_and_modify_rejects_malformed_and_unsupported_shapes`.
- Update `is_count_documents_group` so it requires:
  - exactly two group keys: `_id` and `n`;
  - `_id` equal to integer `1`;
  - `n` equal to a document containing exactly one key, `$sum`;
  - `$sum` equal to integer `1`.
- Add or update Rust tests near `aggregate_pipeline_rejects_malformed_and_unsupported_stages` for extra group fields and extra accumulator fields.
- Add e2e tests using direct `database.command(...)`:
  - ambiguous `findAndModify` alias command fails explicitly and leaves the target document unchanged;
  - malformed `$group` with extra fields fails explicitly.
- Update this file's checklist/status note.
- Commit the finished fix.

Likely files:

- `src/main.rs`
- `tests/e2e/test_find_and_modify.py`
- `tests/e2e/test_aggregation.py`
- `docs/query-atomic-adversarial-fix-goal-loop.md`

Verification:

```bash
cargo fmt -- --check
cargo test find_and_modify
cargo test aggregate
cargo test
cargo build
UV_CACHE_DIR=/private/tmp/mongolino-uv-cache uv lock --check
UV_CACHE_DIR=/private/tmp/mongolino-uv-cache uv sync --locked --dev
UV_CACHE_DIR=/private/tmp/mongolino-uv-cache uv run --locked pytest tests/e2e/test_find_and_modify.py tests/e2e/test_aggregation.py
UV_CACHE_DIR=/private/tmp/mongolino-uv-cache uv run --locked pytest tests/e2e
```

Commit requirement:

- Commit after marking this milestone done and adding the status note.

## Final Response Requirements

Report:

- files changed;
- exact verification commands and pass/fail result;
- commit hash;
- residual risks or compatibility choices.
