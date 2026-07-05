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

- [ ] Fix positional scalar `$elemMatch` binding.
- [ ] Add happy-path, not-happy-path, and adversarial Rust coverage.
- [ ] Add PyMongo e2e coverage.
- [ ] Run verification and record exact commands/results below.
- [ ] Commit the completed fix.

## Status Notes

