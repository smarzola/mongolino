# Goal: Update Operators Logical Pull Predicate Fix Loop

You are working in `/Users/smarzola/projects/mongolino`.

Your objective is to close the remaining adversarial gap from the rich update operators uplift: `$pull` document element predicates now use the existing matcher subset for ordinary field predicates, but top-level logical predicates such as `$or`, `$and`, and `$nor` still route through scalar operator matching and return an unsupported operator error.

This goal is intentionally narrow. Do not broaden update operator support beyond the documented matcher subset.

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

The current `$pull` matcher logic roughly does this:

- If the condition document has any top-level `$` key, treat it as a scalar operator document with `matches_operator_document`.
- Else, if the array element and condition are both documents, use `matches_filter`.

That preserves scalar predicates like `{"$gte": 3}`, but it mishandles document array predicates such as:

```python
collection.update_one(
    {"_id": "u1"},
    {"$pull": {"items": {"$or": [{"kind": "archived"}, {"score": {"$lte": 0}}]}}},
)
```

The repo's existing matcher subset supports top-level `$and`, `$or`, and `$nor` for normal queries. `$pull` should be able to apply that same subset to document array elements.

## Target Behavior

- `$pull` with a document array element and a top-level logical predicate applies `matches_filter` to that element.
- Supported logical predicates are exactly the existing top-level query matcher subset: `$and`, `$or`, and `$nor`.
- Existing scalar array predicates still work, for example `{"$pull": {"scores": {"$gte": 3}}}`.
- Existing whole-document scalar operator predicates still work where already supported, for example `{"$pull": {"docs": {"$eq": {"kind": "a"}}}}`.
- Unsupported top-level operators remain explicit write errors.
- Mixed logical and unsupported top-level operators must not be accepted silently.
- Errors must preserve the original document.

## Definition Of Done

The fix is complete only when:

1. `$pull` supports top-level `$and`, `$or`, and `$nor` predicates against document array elements.
2. `$pull` still supports field predicates against document array elements.
3. `$pull` still supports scalar equality and scalar operator predicates.
4. Existing whole-document `$eq` behavior is preserved.
5. Unsupported or malformed logical predicates return write errors and preserve data.
6. PyMongo e2e tests include happy, not-happy, and adversarial cases.
7. Spec corpus or Rust tests cover the behavior.
8. `cargo fmt -- --check`, `cargo test`, `cargo build`, `UV_CACHE_DIR=/private/tmp/mongolino-uv-cache uv lock --check`, `UV_CACHE_DIR=/private/tmp/mongolino-uv-cache uv sync --locked --dev`, and `UV_CACHE_DIR=/private/tmp/mongolino-uv-cache uv run --locked pytest tests/e2e` pass locally. Use unsandboxed execution for localhost binding if needed.
9. This file has a completed status note with exact commands and the final commit hash.
10. The completed fix is committed with a focused commit.

## Milestone Checklist

- [x] Milestone 0: Add failing logical `$pull` tests
- [x] Milestone 1: Route document logical predicates through the query matcher
- [x] Milestone 2: Harden scalar preservation and unsupported logical errors
- [x] Milestone 3: Final verification and commit

## Milestone 0: Add Failing Logical `$pull` Tests

Add tests that expose the current defect:

- PyMongo e2e: `$pull` removes document array elements matching a top-level `$or`.
- PyMongo e2e: `$pull` supports a top-level `$and` or `$nor` predicate against document array elements.
- Spec corpus or Rust coverage mirrors at least one logical predicate case.

Verification:

```bash
cargo fmt -- --check
cargo test update
UV_CACHE_DIR=/private/tmp/mongolino-uv-cache uv run --locked pytest tests/e2e/test_update_operators.py
```

Expected before implementation: the new logical `$pull` tests should fail.

## Milestone 1: Implement Logical Predicate Routing

Implement the smallest safe change in `src/main.rs`:

- For `$pull`, when both the array element and condition are documents, route top-level logical predicates through `matches_filter`.
- Preserve scalar operator behavior for scalar array elements.
- Preserve whole-document operator behavior such as `$eq` where it already worked.
- Do not accept new query operators beyond the existing matcher subset.

Likely files:

- `src/main.rs`
- `tests/e2e/test_update_operators.py`
- `tests/spec_corpus/update_array_operators.json`

Verification:

```bash
cargo fmt -- --check
cargo test update
cargo test
```

## Milestone 2: Harden Adversarial Paths

Add or confirm tests for:

- Unsupported top-level operator on document elements, for example `$where`, returns a write error.
- Malformed logical operands return write errors.
- Mixed logical and unsupported top-level operators do not partially mutate data.
- Scalar `$pull` predicates still work.
- Whole-document `$eq` still works if it previously worked.

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

- Commit with a focused message such as `Fix logical pull predicates`.

Status 2026-07-04: Complete. Implemented `$pull` routing for document array elements with supported top-level logical predicates through the existing query matcher, while preserving scalar predicate and whole-document `$eq` behavior. Added PyMongo e2e, Rust, and spec corpus coverage for `$or`, `$and`, `$nor`, no-match behavior, malformed logical operands, and mixed logical plus unsupported top-level operators. Verification run:

```bash
cargo fmt
git diff --check
UV_CACHE_DIR=/private/tmp/mongolino-uv-cache uv run --locked python -m json.tool tests/spec_corpus/update_array_operators.json
cargo fmt -- --check
cargo test update
cargo test pull_document_arrays_supports_logical_predicates
cargo test
cargo build
UV_CACHE_DIR=/private/tmp/mongolino-uv-cache uv lock --check
UV_CACHE_DIR=/private/tmp/mongolino-uv-cache uv sync --locked --dev
UV_CACHE_DIR=/private/tmp/mongolino-uv-cache uv run --locked pytest tests/e2e
```

Results: formatter check, JSON parsing, focused Rust tests, full Rust tests, build, uv lock check, and uv sync passed. The sandboxed PyMongo e2e command collected 114 items but failed at `tests/e2e/conftest.py:103` with `PermissionError: [Errno 1] Operation not permitted` while binding `127.0.0.1` (`2 failed, 6 passed, 1 skipped, 105 errors`). The same `UV_CACHE_DIR=/private/tmp/mongolino-uv-cache uv run --locked pytest tests/e2e` command passed outside the sandbox with `113 passed, 1 skipped in 57.43s`. Fix commit: `4bc3759c73312b29a302c2c92040746ff9491ea8`.
