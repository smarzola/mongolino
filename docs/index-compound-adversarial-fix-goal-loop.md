# Goal: Fix Index Pushdown Array False Negatives

You are working in `/Users/smarzola/projects/mongolino`.

Your objective is to fix an adversarial correctness issue found in the compound
index planner uplift. The new compound planner, and the older single-field
planner, can use maintained `index_entries` for scalar equality. However,
`index_entries` currently omit array-valued indexed fields, while the Rust
matcher treats scalar equality as matching scalar elements inside arrays. This
can produce fast wrong results by excluding documents that a collection scan
would match.

This is a fix loop for index uplift 1. Preserve safe scalar-index performance,
but do not allow index pushdown to hide documents with unsupported multikey
values.

## Repository Rules

Follow `AGENTS.md`.

Important reminders:

- Keep the server implementation in Rust and SQLite.
- Treat MongoDB wire compatibility as observable behavior.
- Run `cargo fmt` and `cargo test` before handoff.
- Keep unsupported behavior explicit.
- Do not revert unrelated user or agent changes.
- You are not alone in the codebase. Other agents may have touched nearby files;
  work with current state and do not revert unrelated edits.
- Commit the completed fix with focused commit messages.

## Bug

Current behavior risk:

- A collection has an index on `tags`, or a compound index on `{tags: 1,
  active: 1}`.
- A document contains `{tags: ["a"], active: true}`.
- The Rust matcher treats `{tags: "a"}` and `{tags: "a", active: true}` as
  matching that document.
- The index-entry builder omits the array-valued key.
- The planner can still use the index for scalar equality and return only
  indexed candidates, incorrectly omitting the array document.

This affects:

- `find` candidate narrowing;
- `count` pushdown;
- aggregation `$match` + `$count` pushdown;
- update/delete/findAndModify target narrowing.

## Required Fix

Choose the safest implementation that preserves correctness:

- Preferred: add a conservative planner guard so an index is used only when it
  is known not to have omitted multikey/array values for the relevant key
  paths. This may use maintained sentinel metadata/entries, a cheap transaction
  check, or another durable mechanism.
- Alternative: implement enough scalar multikey entry maintenance to make the
  planner equivalent for scalar array elements. Only choose this if it is small
  and fully tested; do not attempt full multikey semantics here.

Do not weaken the Rust matcher. Do not silently drop array matches.

## Acceptance Criteria

1. Scalar equality `find` with a single-field index returns the same documents
   as a collection scan when indexed fields contain arrays.
2. Full-key compound equality `find` returns the same documents as a collection
   scan when any indexed key path contains arrays.
3. `count` and aggregation `$match` + `$count` do not undercount array matches
   through single-field or compound indexes.
4. update/delete/findAndModify target narrowing does not miss array-backed
   matches.
5. Clean scalar datasets still use index pushdown and keep the compound
   benchmark targets met.
6. Existing numeric, null/missing, document, and unsupported operator fallbacks
   remain correct.
7. PyMongo e2e tests cover the array false-negative cases through real driver
   paths.
8. Docs note that full multikey indexing is still reserved for the later
   multikey uplift, while current planners fall back when unsupported array
   values are present.
9. Verification commands pass:

```bash
cargo fmt -- --check
cargo test planner
cargo test count
cargo test find
cargo test update
cargo test find_and_modify
cargo test
cargo run --bin mongolino-bench -- --profile ci --check-budget
UV_CACHE_DIR=/private/tmp/mongolino-uv-cache uv run --locked pytest tests/e2e/test_indexes.py tests/e2e/test_metadata.py tests/e2e/test_aggregation.py tests/e2e/test_crud.py tests/e2e/test_find_and_modify.py
```

Use unsandboxed execution for PyMongo e2e if localhost binding is blocked.

## Final Response Requirements

Report:

- commits made;
- files changed;
- exact guard or multikey-lite strategy chosen;
- tests added;
- performance impact on clean scalar compound benchmarks;
- verification commands and outcomes;
- remaining multikey limitations.
