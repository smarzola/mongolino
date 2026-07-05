# Goal: Fix `$unwind`/`$group` Side-Table Adversarial Gaps

You are working in `/Users/smarzola/projects/mongolino`.

Your objective is to fix adversarial issues found in commit `3c4311a`
(`Add unwind group occurrence pushdown`) before the performance uplift is
considered complete.

The implementation added `unwind_group_entries` and
`unwind_group_omissions`, then optimized the exact default `$unwind` plus
same-path `$group` count shape. Parent review found at least one correctness
gap that can return wrong results on existing databases after upgrade.

## Repository Rules

Follow `AGENTS.md`.

Important reminders:

- Keep the server implementation in Rust and the durable storage layer in
  SQLite.
- Treat MongoDB wire compatibility as observable behavior: validate changes
  with real client handshakes when possible.
- Run `cargo fmt` and `cargo test` before handing off code changes.
- Keep unsupported MongoDB commands explicit by returning command errors
  instead of silently accepting behavior.
- Do not revert unrelated user or agent changes.
- You are not alone in the codebase. Work with the current state and do not
  revert unrelated edits.

## Adversarial Findings To Fix

### Finding 1: Existing Databases Can Fast-Path With Empty Side Tables

`init_connection` creates the new side-table schema, but an existing database
can already contain `documents` and single-field indexes created before the
side-table feature existed. In that state:

- `indexes_for_namespace` returns a usable `tags_1` index;
- `unwind_group_entries_safe_for_planner` sees no omissions;
- `aggregate_unwind_group_pushdown` uses the fast path;
- `optimized_unwind_group_count` reads zero side-table rows;
- the aggregate returns an empty result instead of the Rust executor's true
  `$unwind`/`$group` counts.

Fix this with a robust migration/backfill or a safety gate. The preferred fix
is to make initialization backfill/rebuild occurrence rows for existing
indexes, using the same logic as normal index rebuilds, and record a schema
migration marker so it is idempotent. A conservative fallback gate is acceptable
only if it proves the side table was built for the namespace/index before
optimizing.

### Finding 2: Optimized Row Order Should Match Document Scan Order

The Rust executor reads collection documents with `ORDER BY created_at`.
The optimized path currently orders by `d.rowid, e.occurrence`. Prefer
`ORDER BY d.created_at, d.rowid, e.occurrence` or an equivalent deterministic
ordering that matches current collection scan order as closely as possible.

## Definition Of Done

The fix is complete only when:

1. Existing databases with documents and pre-existing simple single-field
   indexes cannot return empty or stale optimized `$unwind`/`$group` results.
2. Initialization is idempotent and does not duplicate side-table rows.
3. Backfill/rebuild respects all existing safety fences:
   - simple collation only;
   - no partial index source;
   - one row per default `$unwind` occurrence;
   - omissions for unsupported values;
   - missing/null direct fields and empty arrays contribute zero rows.
4. Optimized output order is aligned with the current collection scan order.
5. Regression tests cover:
   - an old-style database with documents and an index but no side-table rows;
   - repeated `init_connection` does not duplicate rows;
   - unsupported pre-existing values create omissions and force fallback;
   - ordering remains the same as the Rust executor for representative inputs.
6. `docs/aggregation-unwind-group-side-table-goal-loop.md` is updated with a
   fix status note and residual risks if any.
7. `docs/performance-baseline.md` is updated only if you rerun benchmarks.
8. Verification passes:
   - `cargo fmt -- --check`
   - `cargo test unwind_group`
   - `cargo test aggregate`
   - `cargo test index`
   - `cargo test`

## Checkpoint Protocol

When complete:

1. Run the verification commands.
2. Update this file by marking the checklist item done and adding a status note
   with the exact commands run.
3. Update the main side-table goal file with the fix status.
4. Commit the code, tests, docs, and status updates with a focused commit
   message.
5. Report the commit hash and any skipped verification.

## Milestone Checklist

- [x] Fix side-table initialization/backfill and optimized ordering

## Milestone: Fix Side-Table Initialization/Backfill And Ordering

Likely files:

- `src/main.rs`
- `docs/aggregation-unwind-group-side-table-goal-loop.md`
- this file
- `docs/performance-baseline.md` only if benchmarks are rerun

Verification:

```bash
cargo fmt -- --check
cargo test unwind_group
cargo test aggregate
cargo test index
cargo test
```

Final response requirements:

- commit hash;
- files changed;
- exact commands run and pass/fail;
- summary of how existing databases are protected;
- residual risks.

Status:

- 2026-07-05: Added an idempotent
  `unwind_group_side_table_backfill_v1` schema migration that runs during
  `init_connection` after index metadata migrations and rebuilds existing
  index entries through the same rebuild path used by normal index creation,
  which also repopulates `unwind_group_entries` and
  `unwind_group_omissions`. The optimized aggregate query now orders
  occurrence rows by `documents.created_at`, `documents.rowid`, and occurrence;
  the Rust collection scan helper now uses the same deterministic
  `created_at, rowid` tie-break. Added regressions for legacy databases with
  pre-existing indexes but no side-table rows, repeated initialization
  idempotence, unsupported legacy values creating omissions and forcing
  fallback, and optimized order alignment with the Rust executor. Verification
  run: initial `cargo fmt -- --check` failed only on formatting before
  `cargo fmt`; final `cargo fmt -- --check` passed; `cargo test unwind_group`
  passed; `cargo test aggregate` passed; `cargo test index` passed; `cargo
  test` passed. Benchmarks were not rerun, so `docs/performance-baseline.md`
  was unchanged. Commit hash reported after checkpoint commit.
