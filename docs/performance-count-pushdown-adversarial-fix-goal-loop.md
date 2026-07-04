# Goal: Fix Count Pushdown Mixed Numeric Correctness

You are working in `/Users/smarzola/projects/mongolino`.

Your objective is to fix an adversarial correctness issue found in the SQLite count pushdown uplift. The new count planner pushes exact scalar equality counts through maintained `index_entries`, but numeric BSON equality in the Rust matcher is cross-type: `Int32(1)`, `Int64(1)`, and `Double(1.0)` compare equal. `index_entries.key_value` is type-tagged through `id_key_from_bson`, so a numeric indexed-count pushdown can undercount mixed numeric representations.

This is a fix loop for performance uplift 2. Keep the performance uplift for safe cases, but do not allow a fast wrong count.

## Status 2026-07-04

- Fixed by making indexed scalar count planning fall back for numeric BSON values, preserving matcher semantics for `Int32`, `Int64`, and `Double` cross-type equality.
- Preserved SQLite count pushdown for empty filters, exact `_id` equality, and non-numeric indexed scalar equality.
- Added Rust planner/count/aggregation tests and PyMongo e2e coverage for mixed numeric indexed counts.

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

- A collection has an index on `n`.
- Documents include `{n: 1_i32}`, `{n: 1_i64}`, and `{n: 1.0}`.
- Existing matcher semantics match all three for `{n: 1_i32}`, `{n: 1_i64}`, or `{n: 1.0}`.
- Current pushed-down count through `index_entries` can count only the matching type-tagged key, returning `1` instead of `3`.
- The same risk applies to aggregation pipelines of the exact shape `[{$match: {n: <numeric>}}, {$count: "total"}]` because they reuse `pushed_down_count`.

## Required Fix

Choose the safest implementation that preserves correctness:

- Preferred: make `plan_count` fall back for numeric indexed scalar equality until index encoding or SQL lookup can represent cross-type numeric equality correctly.
- Alternative: query all equivalent numeric key encodings and prove it handles `Int32`, `Int64`, and finite `Double` equivalence without false positives. Only choose this if it is simpler and clearly correct.

Do not change matcher semantics to fit the optimization.

## Acceptance Criteria

1. Indexed count pushdown no longer undercounts mixed numeric BSON values.
2. Aggregation `$match` + `$count` no longer undercounts mixed numeric BSON values.
3. Non-numeric indexed scalar count pushdown still works and remains fast.
4. Empty count and `_id` equality count pushdown still work.
5. Planner tests explicitly cover numeric equality fallback or equivalent numeric SQL lookup.
6. PyMongo e2e tests cover mixed numeric count behavior through a real client.
7. Benchmark budget still passes.
8. `docs/performance-sqlite-count-pushdown-goal-loop.md` and/or `docs/performance-baseline.md` note the numeric fallback/residual limitation honestly.
9. Verification commands pass:

```bash
cargo fmt -- --check
cargo test count
cargo test aggregate_match_count
cargo test
cargo run --bin mongolino-bench -- --profile ci --check-budget
UV_CACHE_DIR=/private/tmp/mongolino-uv-cache uv run --locked pytest tests/e2e/test_metadata.py tests/e2e/test_aggregation.py tests/e2e/test_indexes.py
```

Use unsandboxed execution for PyMongo e2e if localhost binding is blocked.

## Final Response Requirements

Report:

- commits made;
- files changed;
- exact fix strategy chosen;
- tests added;
- verification commands and outcomes;
- any remaining numeric/index limitations.
