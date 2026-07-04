# Goal: Fix Index Omission Sentinel Lifecycle Cleanup

You are working in `/Users/smarzola/projects/mongolino`.

Your objective is to fix a lifecycle cleanup issue found during parent review of
the index pushdown array-omission fix. The new `index_multikey_omissions`
sentinel table is cleaned by index rebuild, document refresh, delete, and
`dropIndexes`, but `drop` and `dropDatabase` still delete only `index_entries`.
That can leave stale sentinel rows after collection or database lifecycle
commands.

This is a narrow adversarial fix for index uplift 1.

## Repository Rules

Follow `AGENTS.md`.

Important reminders:

- Keep the server implementation in Rust and SQLite.
- Run `cargo fmt` and `cargo test` before handoff.
- Keep unsupported behavior explicit.
- Do not revert unrelated user or agent changes.
- You are not alone in the codebase. Work with current state.
- Commit the completed fix with focused commit messages.

## Required Fix

- `drop` must delete `index_multikey_omissions` rows for the dropped namespace.
- `dropDatabase` must delete `index_multikey_omissions` rows for every dropped
  namespace.
- Existing `dropIndexes` cleanup must continue to work.
- Add tests proving sentinel rows are removed by `drop`, `dropDatabase`, and
  `dropIndexes: "*"`.

## Acceptance Criteria

1. No stale `index_multikey_omissions` rows remain after `drop`.
2. No stale `index_multikey_omissions` rows remain after `dropDatabase`.
3. Existing index metadata and index entry cleanup behavior does not regress.
4. Verification commands pass:

```bash
cargo fmt -- --check
cargo test drop
cargo test index
cargo test
UV_CACHE_DIR=/private/tmp/mongolino-uv-cache uv run --locked pytest tests/e2e/test_lifecycle.py tests/e2e/test_indexes.py
```

Use unsandboxed execution for PyMongo e2e if localhost binding is blocked.

## Final Response Requirements

Report commits, changed files, tests added, verification outcomes, and any
residual lifecycle risks.
