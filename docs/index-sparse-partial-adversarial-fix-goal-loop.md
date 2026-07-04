# Goal: Fix Sparse Partial CreateIndexes Command Shape

You are working in `/Users/smarzola/projects/mongolino`.

Your objective is to fix an adversarial compatibility issue found in the sparse
and partial index uplift. The implementation accepts top-level `sparse` and
`partialFilterExpression` options on the `createIndexes` command and propagates
them to every index spec. MongoDB drivers send these options inside individual
index specs, not as command-level options. Accepting them at command level can
silently create surprising index definitions and weakens the repo rule that
unsupported command shapes return explicit errors.

This is a focused fix loop for index uplift 2.

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

- Remove top-level `sparse` and `partialFilterExpression` from the allowed
  `createIndexes` command keys.
- Remove propagation of command-level sparse/partial options into each index
  spec.
- Keep per-index-spec `sparse` and `partialFilterExpression` support intact.
- Add tests proving command-level sparse/partial options are explicit command
  errors.
- Ensure PyMongo `create_index(..., sparse=True)` and
  `create_index(..., partialFilterExpression=...)` still work.

## Acceptance Criteria

1. `createIndexes` with top-level `sparse` returns command error code `72`.
2. `createIndexes` with top-level `partialFilterExpression` returns command
   error code `72`.
3. Per-index-spec sparse and partial metadata roundtrip still passes.
4. Sparse/partial unique and planner tests still pass.
5. Verification commands pass:

```bash
cargo fmt -- --check
cargo test index
cargo test partial
cargo test
UV_CACHE_DIR=/private/tmp/mongolino-uv-cache uv run --locked pytest tests/e2e/test_indexes.py tests/e2e/test_metadata.py tests/e2e/test_aggregation.py
```

Use unsandboxed execution for PyMongo e2e if localhost binding is blocked.

## Final Response Requirements

Report commits, changed files, tests added, verification outcomes, and residual
sparse/partial command-shape risks.

## Status

Status 2026-07-04: Complete. Removed command-level `sparse` and
`partialFilterExpression` support from `createIndexes` while preserving
per-index-spec sparse and partial parsing. Added Rust and PyMongo e2e coverage
for explicit code `72` errors on top-level sparse/partial options.
