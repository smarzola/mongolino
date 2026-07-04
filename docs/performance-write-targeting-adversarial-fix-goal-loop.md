# Goal: Fix Write Targeting Unique Numeric Pushdown Correctness

You are working in `/Users/smarzola/projects/mongolino`.

Your objective is to fix an adversarial correctness issue found in the SQLite write-targeting performance uplift. The unique conflict pushdown uses maintained `index_entries` for single-field scalar unique checks, but it currently permits numeric BSON values. Existing matcher and unique-key behavior must not allow a fast path to accept duplicates that the Rust fallback would reject.

This is a fix loop for performance uplift 3. Preserve the write-targeting performance uplift for safe cases, but prefer fallback over fast wrong duplicate-key behavior.

## Repository Rules

Follow `AGENTS.md`.

Important reminders:

- Keep the server implementation in Rust and SQLite.
- Treat MongoDB wire compatibility as observable behavior.
- Run `cargo fmt` and `cargo test` before handoff.
- Keep unsupported behavior explicit.
- Do not revert unrelated user or agent changes.
- You are not alone in the codebase. Other agents may have touched nearby files; work with current state and do not revert unrelated edits.
- Commit the completed fix with focused commit messages.

## Bug

Current behavior risk:

- A collection has a unique single-field index on `n`.
- Documents include or are updated/upserted with numerically equal values represented as different BSON numeric types, such as `Int32(1)`, `Int64(1)`, and `Double(1.0)`.
- `unique_conflict_check_with_index_entries_tx` can return `Ok(true)` after looking up only the type-tagged `index_entries.key_value`.
- Because `ensure_unique_constraints_tx` treats `Ok(true)` as handled, it skips the full Rust fallback scan.
- This can allow numeric duplicates if project semantics consider those values conflicting.

This is the same safety class as the count-pushdown numeric issue fixed by making numeric indexed count filters fall back.

## Required Fix

Choose the safest implementation:

- Preferred: make unique conflict pushdown fall back for numeric BSON values until the index encoding can represent cross-type numeric equality correctly.
- Alternative: query all equivalent numeric key encodings and prove correctness for `Int32`, `Int64`, and finite `Double` without false positives. Only choose this if it is simpler and clearly correct.

Do not change matcher or unique-key semantics to fit the optimization.

## Acceptance Criteria

1. Unique conflict pushdown no longer handles numeric values through the type-tagged `index_entries` shortcut.
2. Numeric unique conflicts are still caught by the Rust fallback scan for insert, update, upsert, and findAndModify where supported.
3. Non-numeric unique scalar pushdown still uses `index_entries`.
4. Update/delete/findAndModify target narrowing remains unchanged and still validates candidates through the Rust matcher.
5. Rust tests explicitly cover numeric unique fallback and duplicate rejection.
6. PyMongo e2e tests cover numeric unique conflict behavior through a real client.
7. Benchmark budget still passes.
8. `docs/performance-sqlite-write-targeting-goal-loop.md` and/or `docs/performance-baseline.md` note the numeric unique fallback honestly.
9. Verification commands pass:

```bash
cargo fmt -- --check
cargo test unique
cargo test update
cargo test find_and_modify
cargo test
cargo run --bin mongolino-bench -- --profile ci --check-budget
UV_CACHE_DIR=/private/tmp/mongolino-uv-cache uv run --locked pytest tests/e2e/test_indexes.py tests/e2e/test_find_and_modify.py tests/e2e/test_update_operators.py
```

Use unsandboxed execution for PyMongo e2e if localhost binding is blocked.

## Final Response Requirements

Report:

- commits made;
- files changed;
- exact fix strategy chosen;
- tests added;
- verification commands and outcomes;
- any remaining unique/index limitations.

## Status

Status 2026-07-04: Implemented. Numeric BSON values no longer use the
type-tagged `index_entries` unique-conflict shortcut. The Rust unique fallback
now canonicalizes numeric key parts through the same numeric comparison domain
used by the matcher, so `Int32(1)`, `Int64(1)`, and `Double(1.0)` conflict
during insert, update, upsert, and findAndModify checks. Non-numeric scalar
unique pushdown remains enabled.
