# Goal: Update Pipeline And Array Filters Adversarial Fix

You are working in `/Users/smarzola/projects/mongolino`.

This is a focused follow-up to `docs/update-pipeline-arrayfilters-goal-loop.md`.
The large uplift is mostly complete, but parent adversarial review found a
compatibility gap in first-positional `$` binding for scalar arrays queried with
`$elemMatch`.

## Repository Rules

Follow `AGENTS.md`.

- Keep the server implementation in Rust and the durable storage layer in
  SQLite.
- Treat MongoDB wire compatibility as observable behavior.
- Keep unsupported MongoDB behavior explicit with write or command errors.
- Do not revert unrelated user or agent changes.
- You are not alone in the codebase. Work with the current state and do not
  undo nearby edits you did not make.
- Use focused commits. Commit the final fix, tests, and checklist/status update.

## Bug To Fix

The query matcher supports scalar-array `$elemMatch` predicates such as:

```javascript
{ scores: { $elemMatch: { $gte: 5, $lt: 10 } } }
```

However, first-positional update binding currently converts `$elemMatch` into an
`ArrayFilterPredicate` with a document predicate. During positional element
matching, scalar array elements are rejected before the operator document is
evaluated. As a result, this natural shape fails even though the query matched
the document:

```javascript
db.users.updateOne(
  { scores: { $elemMatch: { $gte: 5, $lt: 10 } } },
  { $set: { "scores.$": 99 } }
)
```

Expected behavior for the supported subset:

- The update should bind `$` to the first scalar element satisfying the
  `$elemMatch` operator document.
- The implementation should preserve existing document-array `$elemMatch`
  behavior.
- Unsupported or ambiguous positional shapes should still return explicit write
  errors.
- Failing positional updates must not partially mutate documents.

## Required Implementation

1. Update first-positional predicate extraction so `$elemMatch` can represent
   both scalar operator predicates and document predicates.
2. Reuse existing matcher semantics where possible. In particular, mirror the
   existing `matches_elem_match_value` split:
   - scalar/operator-only `$elemMatch` predicates should run against the scalar
     array element;
   - document `$elemMatch` predicates should run against document array
     elements;
   - logical predicates already supported by the matcher should continue to
     work for document elements.
3. Preserve collation behavior for string comparisons inside positional
   binding.
4. Preserve all existing validation:
   - empty `$elemMatch` remains invalid;
   - unsupported operators remain explicit errors;
   - missing array path, scalar parent traversal, nested arrays, and multiple
     positional segments remain rejected.
5. Do not widen the update language beyond the documented subset.

## Required Tests

Add Rust unit coverage in `src/main.rs` near the current positional update tests:

- Happy path: scalar array `$elemMatch` with numeric range binds and updates the
  first matching scalar element only.
- Happy path: scalar array `$elemMatch` with regex/string comparison respects
  collation where applicable.
- Regression path: document-array `$elemMatch` still binds the first document
  element matching all predicates.
- Not-happy path: unsupported `$elemMatch` operator returns a write error and
  leaves the document unchanged.
- Adversarial path: multi-document update where the first matched document would
  update successfully but a later matched document fails positional binding must
  leave all matched documents unchanged.

Add PyMongo e2e coverage in `tests/e2e/test_update_operators.py`:

- `update_one` scalar-array `$elemMatch` plus `"scores.$"` updates the first
  matching element.
- A failing scalar-array positional update does not mutate the document.

Add `find_one_and_update` coverage in `tests/e2e/test_find_and_modify.py` if the
same bug applies there.

## Verification

Run focused verification:

```bash
cargo fmt -- --check
cargo test positional
cargo test update
cargo test find_and_modify
UV_CACHE_DIR=/private/tmp/mongolino-uv-cache uv run --locked pytest tests/e2e/test_update_operators.py tests/e2e/test_find_and_modify.py
```

If the sandbox blocks localhost binding for PyMongo e2e, report that exact
failure and rerun the same command unsandboxed if available.

Run final broad verification if the fix touches shared matcher logic:

```bash
cargo test
cargo build
```

## Completion

- [x] Fix positional scalar `$elemMatch` binding.
- [x] Add happy-path, not-happy-path, and adversarial Rust coverage.
- [x] Add PyMongo e2e coverage.
- [x] Run verification and record exact commands/results below.
- [x] Commit the completed fix.

## Status Notes

2026-07-05:

- Fixed first-positional `$` predicate extraction so direct `$elemMatch`
  predicates are represented separately from arrayFilters and matched through
  the existing query matcher `matches_elem_match_value` split. Scalar/operator
  `$elemMatch` predicates now bind scalar array elements, while document-array
  `$elemMatch` predicates continue to bind document elements.
- Added Rust coverage for numeric scalar `$elemMatch`, collation-aware string
  scalar `$elemMatch`, document-array `$elemMatch` regression, unsupported
  `$elemMatch` operator errors preserving documents, and multi-document
  positional update failure preserving all matched documents.
- Added PyMongo coverage for `update_one` scalar-array `$elemMatch` with
  `"scores.$"`, no-mutation invalid scalar `$elemMatch`, and
  `find_one_and_update` scalar-array `$elemMatch`.
- Verification:
  - `cargo fmt -- --check` passed.
  - `cargo test positional` passed: 5 main tests and 5 bench-target tests.
  - `cargo test update` passed: 24 main tests and 24 bench-target tests.
  - `cargo test find_and_modify` passed: 13 main tests and 13 bench-target
    tests.
  - `UV_CACHE_DIR=/private/tmp/mongolino-uv-cache uv run --locked pytest tests/e2e/test_update_operators.py tests/e2e/test_find_and_modify.py`
    failed in the sandbox with `PermissionError: [Errno 1] Operation not
    permitted` from `sock.bind(("127.0.0.1", 0))`.
  - `cargo build` passed with existing dead-code warnings.
  - The same PyMongo command rerun unsandboxed after rebuilding passed:
    38 passed in 20.23s.
  - `cargo test` passed: 185 main tests and 187 bench-target tests.

Parent review 2026-07-05:

- Reviewed commit `407038a` and accepted the fix. The new
  `PositionalQueryPredicate::ElemMatch` path preserves arrayFilters behavior
  while delegating `$elemMatch` element matching to the existing query matcher
  split, so scalar/operator and document predicates follow the same semantics as
  reads.
- Re-ran focused verification: `cargo fmt -- --check`, `cargo test
  positional`, `cargo test update`, and `cargo test find_and_modify` all
  passed.
- Confirmed the sandboxed focused PyMongo command still fails before test
  execution on localhost bind permission; the same command passed unsandboxed
  with 38 tests passed in 20.25s.
- Re-ran broad verification from the parent context: `cargo test` passed with
  185 main tests and 187 bench-target tests, `cargo build` passed with existing
  dead-code warnings, `cargo run --bin mongolino-bench -- --profile ci
  --check-budget` passed, `UV_CACHE_DIR=/private/tmp/mongolino-uv-cache uv lock
  --check` passed, `UV_CACHE_DIR=/private/tmp/mongolino-uv-cache uv sync
  --locked --dev` passed, and full unsandboxed
  `UV_CACHE_DIR=/private/tmp/mongolino-uv-cache uv run --locked pytest
  tests/e2e` passed with 214 tests in 111.44s.
