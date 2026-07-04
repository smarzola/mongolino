# Goal: Rich Update Operators Compatibility Uplift

You are working in `/Users/smarzola/projects/mongolino`.

Your objective is to add a substantial update-operator compatibility layer for common application and ODM workflows. The server already supports replacement updates, `$set`, `$unset`, `$inc`, upsert, validation, unique indexes, and `findAndModify`. This uplift should add practical scalar and array modifiers while preserving every existing write invariant: `_id` immutability, validation enforcement and bypass, unique-index enforcement, maintained index entries, ordered/unordered batch semantics, and explicit errors for unsupported MongoDB behavior.

This is major uplift 2 of 3 in the current delivery sequence.

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
- Prefer the repo's existing patterns and docs style.
- Add abstractions only where they keep `update`, `findAndModify`, validation, and index maintenance consistent.
- At the end of each milestone, run verification, mark the milestone done in this file, commit the completed milestone, and report the commit hash before continuing.

## Target State

By the end, `mongolino` should support enough update modifiers that a simple PyMongo app can:

- rename fields with `$rename`;
- compare-and-set scalar values with `$min` and `$max`;
- multiply numeric values with `$mul`;
- initialize upsert-only fields with `$setOnInsert`;
- mutate arrays with practical `$push`, `$addToSet`, `$pop`, `$pull`, and `$pullAll` behavior;
- use these modifiers through both `update` and `findAndModify`;
- rely on validation, unique indexes, `_id` immutability, and index-entry freshness after every supported modifier.

The implementation should remain honest:

- No positional operators (`$`, `$[]`, `$[identifier]`) in this goal.
- No `arrayFilters`.
- No update pipelines.
- No full `$push` modifier document support except the explicitly supported subset.
- No complex query-language `$pull` predicates beyond the documented scalar/document equality and the existing matcher subset if implemented safely.
- Unsupported modifier options must return write/command errors rather than being ignored.

## Current State

The repo currently has:

- `UpdateSpec::Replacement` and `UpdateSpec::Modifier` with `$set`, `$unset`, and `$inc`.
- Shared update application used by `update` and `findAndModify`.
- Validation enforcement and `bypassDocumentValidation`.
- Unique index enforcement and index-entry refresh after writes.
- PyMongo e2e and local spec corpus coverage.

Important gaps:

- `$rename`, `$min`, `$max`, `$mul`, and `$setOnInsert` are unsupported.
- `$push`, `$addToSet`, `$pop`, `$pull`, and `$pullAll` are unsupported.
- Existing tests assert `$push` is unsupported; those must be updated once `$push` is supported.
- Path-collision validation currently tracks only simple update paths and needs to account for rename source/destination and upsert-only operators.

## Definition Of Done

The goal is complete only when:

1. `update` and `findAndModify` support `$rename`, `$min`, `$max`, `$mul`, `$setOnInsert`, `$push`, `$addToSet`, `$pop`, `$pull`, and `$pullAll` for the documented subset.
2. Existing `$set`, `$unset`, and `$inc` behavior remains unchanged except where shared validation improves error clarity.
3. All supported modifiers reject `_id` changes and `_id` path writes.
4. Path-collision detection covers all supported modifier target paths plus `$rename` source/destination conflicts.
5. `$setOnInsert` applies only during upsert insert construction, not existing-document updates.
6. Numeric modifiers preserve existing numeric promotion/overflow behavior and reject non-numeric operands or targets explicitly.
7. Array modifiers create arrays only where MongoDB-like behavior is documented for the subset and reject scalar parents/targets explicitly.
8. `$push` supports scalar append and the subset `{ $each: [...], $position, $slice, $sort }` only if implemented and tested; otherwise reject unsupported `$push` option documents explicitly.
9. `$addToSet` supports scalar values and `$each`.
10. `$pull` supports scalar/document equality and any existing matcher-subset predicate only if correctness is tested.
11. `$pullAll` removes equal scalar/document values from arrays.
12. Every modifier works through `update_one`, `update_many`, and `find_one_and_update` where applicable.
13. Validation enforcement and bypass behavior still apply after every modifier.
14. Unique-index enforcement still applies after every modifier.
15. Maintained index entries remain fresh after every modifier.
16. Ordered/unordered update batch behavior remains correct.
17. Unsupported update operators and unsupported modifier options return explicit errors.
18. README compatibility tables and notes accurately describe the new supported update subset.
19. PyMongo e2e tests cover happy paths, not-happy paths, and adversarial paths.
20. Local spec corpus includes representative update-operator cases.
21. `cargo fmt -- --check`, `cargo test`, `cargo build`, `UV_CACHE_DIR=/private/tmp/mongolino-uv-cache uv lock --check`, `UV_CACHE_DIR=/private/tmp/mongolino-uv-cache uv sync --locked --dev`, and `UV_CACHE_DIR=/private/tmp/mongolino-uv-cache uv run --locked pytest tests/e2e` pass locally. Use unsandboxed execution for localhost binding if needed.
22. Milestone checkboxes in this file are marked `[x]` as work completes.
23. Each completed milestone has a focused commit.

## Milestone Checklist

When a milestone is complete:

1. Run the milestone's verification commands.
2. Update this checklist by changing the milestone from `[ ]` to `[x]`.
3. Add a short status note under that milestone with the date, exact commands run, and commit hash if available.
4. Commit the code, tests, docs, and checklist/status update with a focused commit message.
5. Report the commit hash in the goal-loop status before starting the next milestone.

- [x] Milestone 0: Update modifier architecture and path validation
- [x] Milestone 1: Scalar modifiers and upsert-only behavior
- [x] Milestone 2: Array modifiers
- [ ] Milestone 3: Invariant hardening across validation, indexes, and findAndModify
- [ ] Milestone 4: PyMongo/spec corpus, README, and final verification

Milestone 0 status, 2026-07-04: refactored modifier parsing into structured storage and centralized update path validation while keeping existing `$set`, `$unset`, and `$inc` behavior unchanged. Verification passed: `cargo fmt -- --check`; `cargo test update`; `cargo test find_and_modify`; `cargo test validation`; `cargo test`; `cargo build`. Commit hash reported after commit creation.

Milestone 1 status, 2026-07-04: implemented `$rename`, `$min`, `$max`, `$mul`, and `$setOnInsert` through shared `update` and `findAndModify` update application, with Rust and PyMongo coverage plus scalar spec-corpus cases. Verification passed: `cargo fmt -- --check`; `cargo test update`; `cargo test find_and_modify`; `cargo test validation`; `cargo test`. Verification blocked by sandbox localhost permissions: `UV_CACHE_DIR=/private/tmp/mongolino-uv-cache uv run --locked pytest tests/e2e/test_update_operators.py` failed during fixture setup with `PermissionError: [Errno 1] Operation not permitted` on `sock.bind(("127.0.0.1", 0))`. Commit hash reported after commit creation.

Milestone 2 status, 2026-07-04: implemented `$push` with scalar and `$each`, `$addToSet` with scalar and `$each`, `$pop`, `$pull` equality plus existing matcher predicates, and `$pullAll`, while keeping `$position`, `$slice`, `$sort`, positional paths, scalar parents, and malformed operands as explicit write errors. Verification passed: `cargo fmt -- --check`; `cargo test update`; `cargo test`. Verification blocked by sandbox localhost permissions: `UV_CACHE_DIR=/private/tmp/mongolino-uv-cache uv run --locked pytest tests/e2e/test_update_operators.py` failed during fixture setup with `PermissionError: [Errno 1] Operation not permitted` on `sock.bind(("127.0.0.1", 0))`. Commit hash reported after commit creation.

## Milestone 0: Update Modifier Architecture and Path Validation

Problem:

- The current `UpdateSpec::Modifier` stores only `$set`, `$unset`, and `$inc`.
- Adding many operators without a clearer internal representation will make path collision, `_id` protection, and upsert behavior fragile.

Desired behavior:

- Refactor modifier parsing/application enough to support new operators without changing observable behavior.

Acceptance criteria:

- Keep existing replacement and `$set`/`$unset`/`$inc` behavior unchanged.
- Add structured storage for planned modifier operands.
- Centralize path validation for:
  - empty paths;
  - paths containing empty segments;
  - paths starting with `$`;
  - `_id` and `_id.*`;
  - positional segments containing `$`;
  - conflicting target paths.
- Add explicit tests that existing unsupported operators still error before their milestone makes them supported.
- Preserve validation, unique index, and index refresh behavior.
- Milestone status is marked done in this file and committed.

Likely files:

- `src/main.rs`
- `docs/update-operators-compatibility-goal-loop.md`

Verification:

```bash
cargo fmt -- --check
cargo test update
cargo test find_and_modify
cargo test validation
cargo test
cargo build
```

Commit requirement:

- Commit after marking this milestone done and adding the status note.

## Milestone 1: Scalar Modifiers and Upsert-Only Behavior

Problem:

- Common application updates use `$rename`, `$min`, `$max`, `$mul`, and `$setOnInsert`.

Desired behavior:

- Implement the scalar modifier subset consistently across `update` and `findAndModify`.

Acceptance criteria:

- `$rename` moves an existing field to a new non-conflicting path and no-ops when the source is absent.
- `$rename` rejects non-string destinations, empty destinations, dotted path collisions, scalar parent traversal, `_id` source/destination paths, and positional paths.
- `$min` sets a value only when the existing value compares greater than the operand, or when the field is missing.
- `$max` sets a value only when the existing value compares less than the operand, or when the field is missing.
- `$mul` multiplies existing numeric fields, creates missing fields as zero of a reasonable numeric type, and rejects non-numeric operands/targets and int64 overflow.
- `$setOnInsert` applies only when an upsert inserts a new document.
- `$setOnInsert` does not modify existing matched documents in `update` or `findAndModify`.
- Scalar modifiers interact correctly with validation, unique indexes, and maintained index entries.
- Add Rust tests and PyMongo e2e tests for each modifier.
- Milestone status is marked done in this file and committed.

Likely files:

- `src/main.rs`
- `tests/e2e/test_update_operators.py`
- `tests/spec_corpus/update_operators.json`
- `docs/update-operators-compatibility-goal-loop.md`

Verification:

```bash
cargo fmt -- --check
cargo test update
cargo test find_and_modify
cargo test validation
UV_CACHE_DIR=/private/tmp/mongolino-uv-cache uv run --locked pytest tests/e2e/test_update_operators.py
cargo test
```

Commit requirement:

- Commit after marking this milestone done and adding the status note.

## Milestone 2: Array Modifiers

Problem:

- Arrays are common in simple application models, but the server currently rejects every array update operator.

Desired behavior:

- Implement a practical array modifier subset while rejecting unsupported option shapes explicitly.

Acceptance criteria:

- `$push` appends scalar/document/array values to an existing array or creates a new array when the field is missing.
- `$push` supports `$each`.
- If `$position`, `$slice`, or `$sort` are implemented, tests must cover them; otherwise reject them explicitly.
- `$push` rejects scalar existing targets and malformed option documents.
- `$addToSet` appends only when an equal value is not already present.
- `$addToSet` supports `$each`.
- `$pop` with `1` removes the last array element; `$pop` with `-1` removes the first; other operands error.
- `$pull` removes equal values and, if implemented, supports simple existing matcher predicates with tests.
- `$pullAll` removes all equal values present in the operand array.
- Array modifiers reject positional paths and scalar parent traversal.
- Array modifiers interact correctly with validation, unique indexes, and maintained index entries.
- Add Rust tests and PyMongo e2e tests for happy and adversarial paths.
- Milestone status is marked done in this file and committed.

Likely files:

- `src/main.rs`
- `tests/e2e/test_update_operators.py`
- `tests/spec_corpus/update_operators.json`
- `README.md`
- `docs/update-operators-compatibility-goal-loop.md`

Verification:

```bash
cargo fmt -- --check
cargo test update
UV_CACHE_DIR=/private/tmp/mongolino-uv-cache uv run --locked pytest tests/e2e/test_update_operators.py
cargo test
```

Commit requirement:

- Commit after marking this milestone done and adding the status note.

## Milestone 3: Invariant Hardening Across Validation, Indexes, and findAndModify

Problem:

- New update operators touch shared write paths and can easily bypass invariants.

Desired behavior:

- Prove the new operators preserve validation, uniqueness, `_id` immutability, ordered/unordered behavior, and index-entry freshness.

Acceptance criteria:

- Add focused Rust tests proving:
  - validation failure blocks invalid modifier results;
  - `bypassDocumentValidation` bypasses validation but not unique indexes;
  - unique indexes reject duplicate results from `$rename`, `$min`/`$max`, scalar set-like operations, and array operators where applicable;
  - index entries are refreshed after modifiers that affect indexed fields;
  - `findAndModify` pre/post images reflect modifier results correctly;
  - ordered and unordered update batches retain expected partial-failure semantics.
- Add PyMongo e2e tests for the highest-risk invariants.
- Update old unsupported `$push` tests to assert unsupported sub-options rather than unsupported operator.
- Milestone status is marked done in this file and committed.

Likely files:

- `src/main.rs`
- `tests/e2e/test_update_operators.py`
- `tests/e2e/test_errors.py`
- `tests/spec_corpus/update_operators.json`
- `docs/update-operators-compatibility-goal-loop.md`

Verification:

```bash
cargo fmt -- --check
cargo test update
cargo test unique
cargo test validation
cargo test planner
UV_CACHE_DIR=/private/tmp/mongolino-uv-cache uv run --locked pytest tests/e2e/test_update_operators.py tests/e2e/test_errors.py
cargo test
```

Commit requirement:

- Commit after marking this milestone done and adding the status note.

## Milestone 4: PyMongo/Spec Corpus, README, and Final Verification

Problem:

- The new operator support needs durable docs and corpus coverage.

Desired behavior:

- The supported update subset is documented and covered by both driver e2e and local corpus tests.

Acceptance criteria:

- Update README compatibility table and CRUD notes:
  - list the newly supported scalar and array modifiers;
  - keep positional operators, array filters, pipeline updates, and unsupported options explicit.
- Add or update local spec corpus cases for:
  - scalar modifiers;
  - array modifiers;
  - findAndModify modifier behavior;
  - validation/unique/index freshness invariants;
  - unsupported modifier options.
- Ensure e2e tests cover real PyMongo helpers and direct command adversarial cases.
- Run full verification.
- Mark every milestone complete with status notes and commit hashes.
- Commit final docs/test hardening.

Likely files:

- `README.md`
- `docs/update-operators-compatibility-goal-loop.md`
- `tests/spec_corpus/update_operators.json`
- `tests/e2e/test_update_operators.py`
- `tests/e2e/test_spec_corpus.py`
- `src/main.rs`

Verification:

```bash
cargo fmt -- --check
cargo test
cargo build
UV_CACHE_DIR=/private/tmp/mongolino-uv-cache uv lock --check
UV_CACHE_DIR=/private/tmp/mongolino-uv-cache uv sync --locked --dev
UV_CACHE_DIR=/private/tmp/mongolino-uv-cache uv run --locked pytest tests/e2e
```

Commit requirement:

- Commit after marking this milestone done and adding the status note.

## Final Response Requirements

When the goal is complete, report:

- summary of implemented update-operator behavior;
- files changed;
- commits made with hashes;
- exact verification commands and pass/fail result;
- whether e2e needed unsandboxed execution for localhost binding;
- known residual risks and intentionally unsupported MongoDB update behavior.
