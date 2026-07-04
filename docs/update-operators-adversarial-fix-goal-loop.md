# Goal: Update Operators Adversarial Fix Loop

You are working in `/Users/smarzola/projects/mongolino`.

Your objective is to fix the adversarial review finding from the rich update operators uplift: `$pull` currently removes scalar values by equality and supports top-level operator predicates, but it does not apply Mongo-style document element predicates to arrays of documents. This leaves a compatibility gap for common PyMongo/ODM updates such as:

```python
collection.update_one(
    {"_id": "u1"},
    {"$pull": {"items": {"kind": "a", "score": {"$gte": 2}}}},
)
```

The implementation should remove matching array documents using the existing query matcher subset, preserve explicit errors for unsupported predicates, and keep all existing write invariants intact.

## Repository Rules

Follow `AGENTS.md`.

Important reminders:

- Keep the server implementation in Rust and the durable storage layer in SQLite.
- Treat MongoDB wire compatibility as observable behavior: validate changes with real client handshakes when possible.
- Run `cargo fmt` and `cargo test` before handing off code changes.
- Keep unsupported MongoDB commands explicit by returning command errors instead of silently accepting behavior.
- Use the existing PyMongo e2e suite for real driver verification.
- Use `uv` for Python tooling.
- Do not use Docker or external MongoDB services for this goal.
- Do not revert unrelated user or agent changes.
- You are not alone in the codebase. Other agents may have touched nearby files; work with current state and do not revert unrelated edits.

## Review Finding

Current behavior:

- `$pull` with a scalar operator document works, for example `{"$pull": {"scores": {"$gte": 3}}}`.
- `$pull` with exact document equality works only when the whole array element exactly equals the condition, for example `{"$pull": {"docs": {"kind": "a"}}}` removes `{"kind": "a"}`.
- `$pull` with document element predicates does not work for documents with additional fields or nested operator predicates. For example, pulling `{"kind": "a"}` should remove `{"kind": "a", "score": 5}` from an array of documents, but exact equality will retain it.

Expected behavior for this goal:

- When the `$pull` condition is a document and the array element is also a document, apply the existing matcher subset to the element document.
- This should support literal field predicates and nested supported field operators, for example:
  - `{"kind": "a"}` matches document elements with `kind == "a"` even when they have other fields.
  - `{"kind": "a", "score": {"$gte": 2}}` matches document elements satisfying both predicates.
  - dotted paths should work if the existing matcher supports them.
- When the `$pull` condition is a top-level operator document and the array element is scalar, preserve current scalar predicate behavior.
- Unsupported matcher operators inside `$pull` must return a write error and preserve the original document.
- Existing scalar equality, scalar operator predicates, `$pullAll`, `$push`, `$addToSet`, `$pop`, validation, unique indexes, index refresh, `update`, `update_many`, and `findAndModify` behavior must not regress.

## Definition Of Done

The fix is complete only when:

1. `$pull` applies existing matcher-subset predicates to document elements.
2. `$pull` still supports scalar equality and scalar operator predicates.
3. Unsupported `$pull` predicate operators return explicit write errors.
4. A failing `$pull` predicate does not partially mutate or persist the document.
5. `update_one`, `update_many`, and `find_one_and_update` have coverage for document element predicates where practical.
6. PyMongo e2e tests include happy, not-happy, and adversarial document-predicate cases.
7. Rust unit tests or spec corpus coverage captures the same compatibility behavior.
8. README/docs remain accurate if the supported `$pull` subset wording needs tightening.
9. `cargo fmt -- --check`, `cargo test`, `cargo build`, `UV_CACHE_DIR=/private/tmp/mongolino-uv-cache uv lock --check`, `UV_CACHE_DIR=/private/tmp/mongolino-uv-cache uv sync --locked --dev`, and `UV_CACHE_DIR=/private/tmp/mongolino-uv-cache uv run --locked pytest tests/e2e` pass locally. Use unsandboxed execution for localhost binding if needed.
10. This file has a completed status note with exact commands and the final commit hash.
11. The completed fix is committed with a focused commit.

## Milestone Checklist

- [ ] Milestone 0: Reproduce the gap with failing tests
- [ ] Milestone 1: Implement document element predicate matching for `$pull`
- [ ] Milestone 2: Harden unsupported-predicate and no-partial-write behavior
- [ ] Milestone 3: Final docs, verification, and commit

## Milestone 0: Reproduce The Gap

Add tests that fail before the fix:

- PyMongo e2e: `$pull` removes documents matching a literal predicate even when elements have additional fields.
- PyMongo e2e: `$pull` removes documents matching a mixed literal/operator predicate, for example `{"kind": "a", "score": {"$gte": 2}}`.
- PyMongo e2e or Rust/spec corpus: `find_one_and_update` returns the expected post-image after a document-predicate `$pull`.

Verification:

```bash
cargo fmt -- --check
cargo test update
UV_CACHE_DIR=/private/tmp/mongolino-uv-cache uv run --locked pytest tests/e2e/test_update_operators.py
```

Expected before implementation: the new tests should expose the defect.

## Milestone 1: Implement The Fix

Implement the smallest safe change in `src/main.rs`:

- Route `$pull` document conditions against document array elements through the existing matcher subset.
- Preserve current top-level operator behavior for scalar array elements.
- Keep exact equality behavior where matcher semantics do not apply.
- Convert matcher errors into write errors using existing error flow.

Likely files:

- `src/main.rs`
- `tests/e2e/test_update_operators.py`
- `tests/spec_corpus/update_array_operators.json` or a focused new spec corpus file

Verification:

```bash
cargo fmt -- --check
cargo test update
cargo test
```

## Milestone 2: Harden Adversarial Paths

Add or confirm tests for:

- Unsupported `$pull` predicates such as `{"name": {"$regex": "^a"}}` return `WriteError`.
- The document remains unchanged after an unsupported predicate.
- `update_many` applies document-predicate `$pull` consistently across multiple matching documents.
- Existing scalar predicate `$pull` still works.
- Existing exact scalar/document equality does not regress.

Verification:

```bash
cargo fmt -- --check
cargo test update
UV_CACHE_DIR=/private/tmp/mongolino-uv-cache uv run --locked pytest tests/e2e/test_update_operators.py
```

## Milestone 3: Final Verification And Commit

Update this checklist and add a status note with:

- date;
- exact commands run;
- any sandbox limitation encountered;
- final commit hash.

Run:

```bash
cargo fmt -- --check
cargo test
cargo build
UV_CACHE_DIR=/private/tmp/mongolino-uv-cache uv lock --check
UV_CACHE_DIR=/private/tmp/mongolino-uv-cache uv sync --locked --dev
UV_CACHE_DIR=/private/tmp/mongolino-uv-cache uv run --locked pytest tests/e2e
```

Commit requirement:

- Commit with a focused message such as `Fix pull document predicate matching`.
