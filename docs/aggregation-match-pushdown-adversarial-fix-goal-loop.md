# Goal: Fix First-Stage Aggregation Match Pushdown Review Findings

You are working in `/Users/smarzola/projects/mongolino`.

Your objective is to address parent adversarial review findings for commit
`9c38ef2` (`Add aggregation match candidate narrowing`). The implementation is
directionally correct, but the review found coverage and documentation gaps
around the new early candidate-loading path. Fix those gaps without widening
planner semantics.

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
- Do not optimize by weakening matcher semantics.
- Do not revert unrelated user or agent changes.
- You are not alone in the codebase. Do not revert the writeConcern durability
  commits or the first-stage aggregation pushdown commit.

## Parent Review Findings

1. **Missing targeted error-ordering test.**

   `aggregate_pipeline_documents` now calls `aggregate_initial_documents`
   before iterating stages. Command-level validation currently happens before
   that, but the test suite should explicitly prove a safe first-stage `$match`
   does not hide a malformed later stage or change the returned command error.

2. **Missing targeted collation fallback test.**

   `_id` candidate support was added to the shared candidate loader. The code
   checks `id_equality_safe_for_collation`, so string `_id` equality under
   non-simple collation should fall back rather than narrow unsafely. Add a
   test that proves first-stage aggregation `$match` preserves non-simple
   collation string equality semantics for `_id` rather than using binary
   `id_key` lookup.

3. **Performance note wording is stale.**

   `docs/performance-baseline.md` says the first-stage aggregation benchmark
   was recorded from a dirty working tree based on `db507d8`. After integration
   this should be made clearer and not read like an accidental or final dirty
   state. Use measured numbers already recorded unless you rerun benchmarks.

4. **Roadmap status should include parent review/fix status.**

   Update `docs/query-planner-pushdown-roadmap-goal-loop.md` with a short
   parent-review status note and this fix-loop reference.

## Definition Of Done

The fix is complete only when:

1. A Rust test proves safe first-stage `$match` plus a malformed later stage
   returns the same explicit command error path and does not mutate/sweep
   unexpectedly.
2. A Rust test proves non-simple collation string `_id` matching in first-stage
   aggregation falls back safely and returns all collation-equal string `_id`
   documents.
3. Existing first-stage `$match` pushdown behavior remains unchanged for safe
   `_id` under simple collation and safe indexed equality.
4. Performance docs no longer imply an accidental final dirty working tree.
5. The planner roadmap records the parent review and fix status.
6. `cargo fmt -- --check`, `cargo test aggregation`, and `cargo test collation`
   pass.

## Checkpoint Protocol

When complete:

1. Run the verification commands.
2. Update this file with a status note containing the date, commands run, and
   commit hash if available.
3. Commit the tests/docs/status update with a focused commit message.
4. Report changed files, verification results, and residual risks.

## Likely Files

- `src/main.rs`
- `docs/performance-baseline.md`
- `docs/query-planner-pushdown-roadmap-goal-loop.md`
- this file

## Verification Commands

```bash
cargo fmt -- --check
cargo test aggregation
cargo test collation
```

## Final Response Requirements

Report:

- review findings fixed;
- files changed;
- exact verification commands and results;
- commit hash if committed;
- any remaining risk.

## Status Notes

Status note 2026-07-05:

- Added targeted Rust coverage for safe first-stage `$match` followed by a
  malformed later `$lookup` stage, comparing the returned command error with
  the existing malformed-stage path and verifying the invalid read does not
  sweep TTL-expired documents.
- Added targeted Rust coverage for first-stage aggregation `$match` on string
  `_id` under `{ locale: "en", strength: 2 }`, proving candidate loading falls
  back to the full namespace and the aggregate returns all collation-equal
  string `_id` documents.
- Clarified the first-stage aggregation benchmark note in
  `docs/performance-baseline.md` so the recorded working-tree measurement is
  described as an integration-time benchmark rather than an accidental final
  dirty state.
- Updated `docs/query-planner-pushdown-roadmap-goal-loop.md` with the parent
  review and fix-loop status.
- Verification run:
  - `cargo fmt -- --check`: passed.
  - `cargo test aggregation`: passed, 6 tests in `src/main.rs` and 6 tests
    via the bench target.
  - `cargo test collation`: passed, 14 tests in `src/main.rs` and 14 tests
    via the bench target.
- Commit hash: `fc377a3`.
