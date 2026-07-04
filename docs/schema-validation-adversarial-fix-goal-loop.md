# Goal: Schema Validation Adversarial Fixes

You are working in `/Users/smarzola/projects/mongolino`.

Your objective is to fix adversarial review findings from the schema validation compatibility uplift without broadening scope. The implementation already passes Rust tests, but the parent unsandboxed PyMongo e2e run found a real driver mismatch in `findAndModify` bypass behavior.

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

- PyMongo `find_one_and_update(..., bypass_document_validation=True)` works against a validated collection.
- Direct command callers can use either `bypassDocumentValidation` or PyMongo's observed `bypass_document_validation` field for `findAndModify`.
- If both bypass spellings are present with conflicting boolean values, `findAndModify` returns an explicit command error before mutating storage.
- Non-boolean values for either bypass spelling return explicit command errors.
- Existing insert/update bypass behavior remains unchanged.

## Current State

The parent unsandboxed e2e run failed:

```text
tests/e2e/test_validation.py::test_find_and_modify_enforces_validator_and_bypass
pymongo.errors.OperationFailure: bypass_document_validation is not supported for this command
```

The server currently accepts only `bypassDocumentValidation` in `findAndModify`, but PyMongo's `find_one_and_update(..., bypass_document_validation=True)` sends `bypass_document_validation`.

## Definition Of Done

1. `findAndModify` accepts `bypass_document_validation` as a supported alias for the same bypass behavior as `bypassDocumentValidation`.
2. `findAndModify` rejects conflicting `bypassDocumentValidation` and `bypass_document_validation` values explicitly.
3. `findAndModify` rejects non-boolean values for either bypass spelling explicitly.
4. Bypass still does not bypass `_id` immutability or unique index enforcement.
5. Rust tests cover alias success, conflict rejection, and non-boolean rejection.
6. PyMongo e2e `test_find_and_modify_enforces_validator_and_bypass` passes without weakening the assertion.
7. Full `tests/e2e/test_validation.py` passes under real localhost execution.
8. `cargo fmt -- --check`, `cargo test`, `cargo build`, `UV_CACHE_DIR=/private/tmp/mongolino-uv-cache uv lock --check`, `UV_CACHE_DIR=/private/tmp/mongolino-uv-cache uv sync --locked --dev`, and `UV_CACHE_DIR=/private/tmp/mongolino-uv-cache uv run --locked pytest tests/e2e` pass locally. Use unsandboxed execution for localhost binding if needed.
9. This file is updated with `[x]` milestone status, a short status note, exact commands run, and commit hash.
10. The finished fix is committed with a focused commit message.

## Milestone Checklist

When the milestone is complete:

1. Run the milestone's verification commands.
2. Update this checklist by changing the milestone from `[ ]` to `[x]`.
3. Add a short status note with the date, exact commands run, and commit hash if available.
4. Commit the code, tests, docs, and status-note update with a focused commit message.
5. Report the commit hash in the final response.

- [x] Milestone 1: PyMongo findAndModify bypass alias

Status note 2026-07-04:

- Implemented `findAndModify` support for PyMongo's observed `bypass_document_validation` alias while preserving explicit command errors for conflicting alias values and non-boolean bypass fields.
- Also fixed the parent-review `collMod` finding: `validator: {}` clears stored `validator`, `validationLevel`, and `validationAction`, while same-command explicit supported `validationLevel` / `validationAction` values are retained.
- Verification run:
  - `cargo fmt -- --check` passed.
  - `cargo test validation_find_and_modify` passed.
  - `cargo test validation` passed.
  - `cargo test` passed.
  - `cargo build` passed.
  - `UV_CACHE_DIR=/private/tmp/mongolino-uv-cache uv lock --check` passed.
  - `UV_CACHE_DIR=/private/tmp/mongolino-uv-cache uv sync --locked --dev` passed.
  - `UV_CACHE_DIR=/private/tmp/mongolino-uv-cache uv run --locked pytest tests/e2e/test_validation.py` failed in the sandbox before server startup with `PermissionError: [Errno 1] Operation not permitted` while binding `127.0.0.1`; the same command passed outside the sandbox with 10 passed.
  - `UV_CACHE_DIR=/private/tmp/mongolino-uv-cache uv run --locked pytest tests/e2e` passed outside the sandbox with 100 passed, 1 skipped.
- Commit hash: final amended commit reported in handoff.

## Milestone 1: PyMongo findAndModify Bypass Alias

Problem:

- PyMongo sends `bypass_document_validation` for `find_one_and_update`, while the command implementation only accepts `bypassDocumentValidation`.
- This makes the implemented bypass behavior fail through the real driver even though direct-command tests can pass.

Desired behavior:

- `findAndModify` supports the observed PyMongo field while preserving explicit validation for malformed inputs.

Acceptance criteria:

- Update `src/main.rs` to allow `bypass_document_validation` only on `findAndModify` / `findandmodify`.
- Parse both bypass aliases through one helper that:
  - accepts either alias;
  - accepts both aliases only when values are equal booleans;
  - rejects conflicting booleans;
  - rejects non-boolean values with an explicit command error.
- Add Rust tests near existing validation/find-and-modify tests for alias success, conflicting aliases, and non-boolean alias.
- Keep insert/update command allowed keys unchanged unless real PyMongo evidence requires otherwise.
- Add or keep e2e coverage using PyMongo helper behavior.
- Update this file's checklist/status note.
- Commit the finished fix.

Likely files:

- `src/main.rs`
- `tests/e2e/test_validation.py`
- `docs/schema-validation-adversarial-fix-goal-loop.md`

Verification:

```bash
cargo fmt -- --check
cargo test validation_find_and_modify
cargo test validation
cargo test
cargo build
UV_CACHE_DIR=/private/tmp/mongolino-uv-cache uv lock --check
UV_CACHE_DIR=/private/tmp/mongolino-uv-cache uv sync --locked --dev
UV_CACHE_DIR=/private/tmp/mongolino-uv-cache uv run --locked pytest tests/e2e/test_validation.py
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
