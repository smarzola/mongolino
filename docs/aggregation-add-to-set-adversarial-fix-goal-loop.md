# Goal: Aggregation `$addToSet` Equality Adversarial Fix Loop

You are working in `/Users/smarzola/projects/mongolino`.

Your objective is to fix the adversarial review finding from the aggregation `$unwind` and `$group` uplift: `$group` `$addToSet` currently uses query-style equality for uniqueness. Query equality intentionally treats an array field as matching a scalar element, but aggregation `$addToSet` uniqueness must compare whole evaluated values. As implemented, a grouped value of `[1, 2]` can incorrectly deduplicate scalar `1` depending on stream order.

This goal is intentionally narrow. Do not broaden aggregation support beyond the documented subset.

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

Current aggregation `$addToSet` state uses `bson_values_equal` to detect duplicates.

That helper is correct for query matching, but it is not correct for aggregation set membership because it treats arrays specially:

- existing value `[1, 2]`;
- incoming value `1`;
- query equality says the array matches scalar `1`;
- aggregation `$addToSet` should keep both values because `[1, 2]` and `1` are different whole BSON values.

The code already has `aggregation_values_equal`, which compares whole BSON values recursively and is a better fit for group keys and `$addToSet` set membership.

## Target Behavior

- `$group` `$addToSet` must use whole-value aggregation equality, not query matcher equality.
- Numeric values that are equal across BSON numeric widths should still deduplicate consistently, matching the existing `aggregation_values_equal` behavior.
- Arrays are compared as arrays, not as match containers.
- Documents are compared as whole documents using the existing aggregation equality helper.
- `$push` is unchanged and keeps every evaluated value in stream order.
- Existing `$addToSet` scalar/document behavior remains deterministic.
- Unsupported aggregation expression shapes remain explicit errors.

## Definition Of Done

The fix is complete only when:

1. `$group` `$addToSet` uses whole-value aggregation equality.
2. Array values and scalar values are not incorrectly deduplicated against each other.
3. Array duplicate detection is order-sensitive within arrays unless the existing aggregation equality explicitly says otherwise.
4. Numeric duplicate detection still deduplicates equal numeric values across supported numeric BSON types.
5. Document duplicate detection remains deterministic.
6. PyMongo e2e tests include happy, not-happy, and adversarial cases for `$addToSet` with scalars, arrays, and documents.
7. Rust tests or spec corpus coverage captures the same bug.
8. `cargo fmt -- --check`, `cargo test`, `cargo build`, `UV_CACHE_DIR=/private/tmp/mongolino-uv-cache uv lock --check`, `UV_CACHE_DIR=/private/tmp/mongolino-uv-cache uv sync --locked --dev`, and `UV_CACHE_DIR=/private/tmp/mongolino-uv-cache uv run --locked pytest tests/e2e` pass locally. Use unsandboxed execution for localhost binding if needed.
9. This file has a completed status note with exact commands and the final commit hash.
10. The completed fix is committed with a focused commit.

## Milestone Checklist

- [x] Milestone 0: Add failing `$addToSet` whole-value equality tests
- [x] Milestone 1: Switch `$addToSet` uniqueness to aggregation equality
- [x] Milestone 2: Harden scalar, array, document, and numeric cases
- [x] Milestone 3: Final verification and commit

## Milestone 0: Add Failing Tests

Add tests that expose the current defect:

- PyMongo e2e: group values where one document contributes an array value such as `[1, 2]` and another contributes scalar `1`; `$addToSet` must keep both values.
- PyMongo e2e: reverse the stream order or otherwise prove the result is not order-dependent.
- Spec corpus or Rust coverage mirrors at least one array-versus-scalar case.

Verification:

```bash
cargo fmt -- --check
cargo test aggregate
UV_CACHE_DIR=/private/tmp/mongolino-uv-cache uv run --locked pytest tests/e2e/test_aggregation.py
```

Expected before implementation: the new array-versus-scalar case should fail.

## Milestone 1: Implement The Fix

Implement the smallest safe change in `src/main.rs`:

- In `$group` `$addToSet` accumulator state, replace query equality with whole-value aggregation equality.
- Reuse `aggregation_values_equal` unless there is a concrete reason it is insufficient.
- Do not change query matcher semantics.
- Do not change update `$addToSet` semantics in this goal.

Likely files:

- `src/main.rs`
- `tests/e2e/test_aggregation.py`
- `tests/spec_corpus/aggregation_pipeline.json`

Verification:

```bash
cargo fmt -- --check
cargo test aggregate
cargo test
```

## Milestone 2: Harden Adversarial Paths

Add or confirm tests for:

- duplicate scalar values still deduplicate;
- equal numeric values across supported numeric BSON widths still deduplicate;
- duplicate arrays deduplicate only when arrays are equal as arrays;
- scalar values and array values remain distinct;
- duplicate document values deduplicate deterministically;
- `$push` still includes all values including duplicates.

Verification:

```bash
cargo fmt -- --check
cargo test aggregate
UV_CACHE_DIR=/private/tmp/mongolino-uv-cache uv run --locked pytest tests/e2e/test_aggregation.py
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

- Commit with a focused message such as `Fix aggregation addToSet equality`.

## Status Note

Completed on 2026-07-04.

Fix commit:

- `59b349bfc7c2fe6669e020a6d92b48b29b2fb962` (`Fix aggregation addToSet equality`)

Commands run:

```bash
cargo fmt
python3 -m json.tool tests/spec_corpus/aggregation_pipeline.json
cargo fmt -- --check
cargo test aggregate
git diff --check
cargo test
cargo build
UV_CACHE_DIR=/private/tmp/mongolino-uv-cache uv lock --check
UV_CACHE_DIR=/private/tmp/mongolino-uv-cache uv sync --locked --dev
UV_CACHE_DIR=/private/tmp/mongolino-uv-cache uv run --locked pytest tests/e2e
UV_CACHE_DIR=/private/tmp/mongolino-uv-cache uv run --locked pytest tests/e2e
```

Results:

- `cargo fmt -- --check`: passed.
- `cargo test aggregate`: passed, 12 tests.
- `git diff --check`: passed.
- `cargo test`: passed, 107 tests.
- `cargo build`: passed.
- `UV_CACHE_DIR=/private/tmp/mongolino-uv-cache uv lock --check`: passed.
- `UV_CACHE_DIR=/private/tmp/mongolino-uv-cache uv sync --locked --dev`: passed.
- First `UV_CACHE_DIR=/private/tmp/mongolino-uv-cache uv run --locked pytest tests/e2e` sandboxed run failed before server startup because localhost binding is blocked in the sandbox: `PermissionError: [Errno 1] Operation not permitted` at `sock.bind(("127.0.0.1", 0))` in `tests/e2e/conftest.py:103`.
- Second `UV_CACHE_DIR=/private/tmp/mongolino-uv-cache uv run --locked pytest tests/e2e` run outside the sandbox passed: 119 passed, 1 skipped.
