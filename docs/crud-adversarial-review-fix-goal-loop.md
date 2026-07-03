# Goal: Fix CRUD Adversarial Review Findings

You are working in `/Users/smarzola/projects/mongolino`.

Your objective is to fix the adversarial review findings from the first CRUD compatibility implementation pass. The broad CRUD uplift is already implemented in prior commits; keep this goal focused on correctness holes found during review, add regression tests for each hole, and preserve the documented MongoDB subset.

## Repository Rules

Follow `AGENTS.md`.

Important reminders:

- Keep the server implementation in Rust and the durable storage layer in SQLite.
- Treat MongoDB wire compatibility as observable behavior.
- Run `cargo fmt` and `cargo test` before handing off.
- Keep unsupported MongoDB behavior explicit by returning command errors instead of silently accepting behavior.
- You are not alone in the codebase. Do not revert edits made by others; adjust your work to accommodate existing changes.
- Keep changes scoped to the findings below.

## Target State

By the end:

- `_id`-only projections behave as projections, not as if no projection was supplied.
- `$inc` handles mixed integer operands exactly and rejects overflow instead of losing precision through `f64` conversion.
- OP_MSG parsing rejects multiple body sections instead of silently accepting the last body.
- Each fix has a focused regression test, including adversarial cases.

## Current Findings

1. Projection-only `_id` bug:
   - `parse_projection` currently returns `None` when the projection document contains only `_id`.
   - Result: `{ projection: { _id: 0 } }` returns full documents with `_id` instead of excluding `_id`.
   - Result: `{ projection: { _id: 1 } }` returns full documents instead of only `_id`.
   - Relevant code: `src/main.rs`, `parse_projection` and `apply_projection`.

2. `$inc` mixed-integer precision/overflow bug:
   - `add_numeric_bson` only handles exact checked addition for `Int64 + Int64`.
   - Mixed integer cases such as `Int64(i64::MAX) + Int32(1)` fall through through `f64` conversion and can silently lose precision or saturate when cast back.
   - Relevant code: `src/main.rs`, `add_numeric_bson` and `$inc` tests.

3. OP_MSG multiple-body hardening gap:
   - `parse_op_msg_document` accepts multiple section kind `0` body documents and silently keeps the last one.
   - The parser should reject multiple body sections with an explicit protocol error.
   - Relevant code: `src/main.rs`, `parse_op_msg_document`.

## Definition Of Done

The goal is complete only when:

1. `_id`-only projection tests fail before the fix and pass after the fix.
2. `$inc` mixed integer overflow tests fail before the fix and pass after the fix.
3. OP_MSG multiple-body tests fail before the fix and pass after the fix.
4. Existing CRUD tests still pass.
5. `cargo fmt` and `cargo test` pass.
6. This file has a status note with exact commands run and the final commit hash.
7. The completed fix is committed with a focused commit message.

## Milestone Checklist

When the milestone is complete:

1. Run the verification commands.
2. Update this checklist by changing the milestone from `[ ]` to `[x]`.
3. Add a short status note under the milestone with the date, exact commands run, and commit hash if available.
4. Commit the code, tests, docs, and checklist/status update with a focused commit message.
5. Report the commit hash in the goal-loop status.

- [x] Milestone 1: Fix adversarial review findings

Status note, 2026-07-03:

- Commands run: `cargo fmt`; `cargo test projection`; `cargo test update`; `cargo test op_msg`; `cargo test`.
- Results: all commands passed; full suite passed with 39 tests.
- Commit hash: pending until the milestone commit is created; final hash reported in handoff.

## Milestone 1: Fix Adversarial Review Findings

Problem:

- The first CRUD implementation passed its suite, but review found three correctness gaps that can produce misleading MongoDB behavior or silently accept malformed wire input.

Desired behavior:

- `find` projection should distinguish no projection from `_id`-only projection.
- `$inc` should use exact checked integer addition when both operands are integer BSON values, including mixed `Int32`/`Int64` combinations.
- OP_MSG parsing should return a protocol error if more than one body section is present.

Acceptance criteria:

- Add tests for `{ "projection": { "_id": 0 } }` showing returned documents do not contain `_id` and still contain other fields.
- Add tests for `{ "projection": { "_id": 1 } }` showing returned documents contain only `_id`.
- Add tests for inclusion projection with `_id: 0` to ensure the existing behavior remains correct.
- Add tests for `$inc` with:
  - `Int64(i64::MAX) + Int32(1)` returning a write error and preserving the original document;
  - `Int64(i64::MAX - 1) + Int32(1)` succeeding exactly as `Int64(i64::MAX)`;
  - `Int32(i32::MAX) + Int32(1)` promoting to `Int64`.
- Add a test that an OP_MSG payload with two kind `0` body sections returns a protocol error.
- Keep existing tests passing.
- Milestone status is marked done in this file and committed.

Likely files:

- `src/main.rs`
- `docs/crud-adversarial-review-fix-goal-loop.md`

Verification:

```bash
cargo fmt
cargo test projection
cargo test update
cargo test op_msg
cargo test
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
