# Goal: Fix E2E Suite Adversarial Review Findings

You are working in `/Users/smarzola/projects/mongolino`.

Your objective is to fix the focused findings from adversarial review of the PyMongo e2e suite. The suite already passes outside the sandbox and covers the intended CRUD compatibility subset. Keep this goal scoped to test/CI hardening and do not refactor unrelated server behavior.

## Repository Rules

Follow `AGENTS.md`.

Important reminders:

- Keep the server implementation in Rust and the durable storage layer in SQLite.
- Treat MongoDB wire compatibility as observable behavior.
- Run `cargo fmt`, `cargo test`, and the PyMongo e2e suite before handing off.
- Keep unsupported MongoDB behavior explicit by returning command errors instead of silently accepting behavior.
- Use `uv` for Python tooling.
- You are not alone in the codebase. Do not revert edits made by others; adjust your work to accommodate existing changes.
- Keep changes scoped to the findings below.

## Target State

By the end:

- CI explicitly ensures a current stable Rust toolchain with `rustfmt` is available instead of trusting runner-preinstalled state.
- CI and docs use locked uv execution for e2e test runs where practical.
- Corpus-runner meta-tests that validate malformed local corpus definitions do not start a real `mongolino` server unless the behavior under test needs server I/O.
- The e2e suite still passes outside the sandbox and keeps its one intentional unsupported-regex skip.

## Current Findings

1. CI Rust setup is implicit:
   - `.github/workflows/ci.yml` currently runs `rustup show` but otherwise trusts the GitHub runner's preinstalled Rust state.
   - This is more brittle than explicitly installing or selecting stable Rust and `rustfmt`.

2. Locked uv execution is incomplete:
   - CI uses `uv sync --locked --dev`, but the following `uv run pytest tests/e2e` can be made more explicit with `uv run --locked pytest tests/e2e`.
   - README should match the CI/local command where practical.

3. Some corpus-runner meta-tests unnecessarily require localhost:
   - `tests/e2e/test_spec_corpus.py` includes negative tests such as unknown operation, unsupported assertion shape, and skipped unsupported feature reporting.
   - Some of these use the `collection` fixture even though the assertions are pure runner behavior and do not need to start `mongolino`.
   - In restricted environments this causes unrelated localhost bind failures to obscure pure validation regressions.

## Definition Of Done

The goal is complete only when:

1. CI explicitly prepares stable Rust and `rustfmt`.
2. CI runs the e2e suite with locked uv execution.
3. README e2e instructions match the locked local command unless there is a clear reason not to.
4. Pure corpus-runner meta-tests no longer require `collection` or `mongolino_server` when no server I/O is needed.
5. Tests that do need a live server still use the real PyMongo fixture.
6. `cargo fmt -- --check`, `cargo test`, `cargo build`, `UV_CACHE_DIR=/private/tmp/mongolino-uv-cache uv lock --check`, `UV_CACHE_DIR=/private/tmp/mongolino-uv-cache uv sync --locked --dev`, and `UV_CACHE_DIR=/private/tmp/mongolino-uv-cache uv run --locked pytest tests/e2e` pass, using escalated/unsandboxed execution for localhost binding if needed.
7. This file has a status note with exact commands run and the final commit hash.
8. The completed fix is committed with a focused commit message.

## Milestone Checklist

When the milestone is complete:

1. Run the verification commands.
2. Update this checklist by changing the milestone from `[ ]` to `[x]`.
3. Add a short status note under the milestone with the date, exact commands run, and commit hash if available.
4. Commit the code, tests, docs, and checklist/status update with a focused commit message.
5. Report the commit hash in the goal-loop status.

- [x] Milestone 1: Harden e2e CI and corpus meta-tests

Status note 2026-07-04 CEST:

- `cargo fmt -- --check`: passed.
- `cargo test`: passed, 39 passed.
- `cargo build`: passed.
- `UV_CACHE_DIR=/private/tmp/mongolino-uv-cache uv lock --check`: passed, resolved 9 packages.
- `UV_CACHE_DIR=/private/tmp/mongolino-uv-cache uv sync --locked --dev`: passed, checked 7 packages.
- `UV_CACHE_DIR=/private/tmp/mongolino-uv-cache uv run --locked pytest tests/e2e`: sandboxed run failed because `sock.bind(("127.0.0.1", 0))` raised `PermissionError: [Errno 1] Operation not permitted`.
- `UV_CACHE_DIR=/private/tmp/mongolino-uv-cache uv run --locked pytest tests/e2e`: passed outside the sandbox, 40 passed and 1 skipped.
- Commit hash: pending until commit; final hash reported in handoff.

## Milestone 1: Harden E2E CI and Corpus Meta-Tests

Problem:

- The implementation is functionally good, but adversarial review found a few reliability issues in CI and in the structure of corpus-runner meta-tests.

Desired behavior:

- CI is reproducible about Rust toolchain setup.
- Local and CI uv commands stay locked.
- Corpus-runner tests separate pure validation behavior from live server behavior.

Acceptance criteria:

- Update `.github/workflows/ci.yml` to explicitly select or install stable Rust and ensure `rustfmt` is available before running `cargo fmt -- --check`.
- Update CI's e2e step to run `uv run --locked pytest tests/e2e`.
- Update README development commands to use `uv run --locked pytest tests/e2e`.
- Refactor `tests/e2e/test_spec_corpus.py` so tests that only verify skip handling, unknown operations, malformed setup, or unsupported assertion shapes avoid the live `collection` fixture when possible.
- Preserve live-server coverage for actual corpus cases and operation execution tests.
- Keep the unsupported regex corpus case as an intentional skip.
- Milestone status is marked done in this file and committed.

Likely files:

- `.github/workflows/ci.yml`
- `README.md`
- `tests/e2e/test_spec_corpus.py`
- `docs/e2e-adversarial-review-fix-goal-loop.md`

Verification:

```bash
cargo fmt -- --check
cargo test
cargo build
UV_CACHE_DIR=/private/tmp/mongolino-uv-cache uv lock --check
UV_CACHE_DIR=/private/tmp/mongolino-uv-cache uv sync --locked --dev
UV_CACHE_DIR=/private/tmp/mongolino-uv-cache uv run --locked pytest tests/e2e
```

Commit requirement:

- Commit after marking this milestone done and adding the status note.

## Final Response Required

When complete, report:

- findings fixed;
- commit hash;
- files changed;
- exact verification commands run and results;
- any residual risk.
