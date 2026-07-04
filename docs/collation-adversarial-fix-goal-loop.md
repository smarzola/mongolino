# Goal: Fix Collation Compound Prefix Planner Keys

You are working in `/Users/smarzola/projects/mongolino`.

Your objective is to harden the Collation Compatibility uplift after parent
adversarial review. The implemented collation surface is broad and mostly
coherent, but the review found a blocking index-planning bug: compound-prefix
planner keys are encoded with the index collation when documents are indexed,
but query-side compound-prefix keys are still encoded with binary
`id_key_from_bson`. For non-simple case-insensitive collation indexes, prefix
queries can therefore return an empty candidate set even though matching
documents exist.

## Repository Rules

Follow `AGENTS.md`.

Important reminders:

- Keep the server implementation in Rust and durable storage in SQLite.
- Treat MongoDB wire compatibility as observable behavior.
- Unsupported MongoDB behavior must return explicit command errors.
- Do not weaken matcher, validator, planner, unique-index, TTL, or collation
  semantics.
- Do not revert unrelated user or agent changes.
- You are not alone in the codebase; work with current state.
- Use focused commits and update this file as milestones complete.

## Parent Review Finding

The parent review inspected commits through `5f75628` and found this issue:

- `compound_planner_keys_for_document` stores compound-prefix index entries
  using `spec.collation.id_key_from_bson(value)`.
- `prefix_planner_key_for_filter` builds query prefix keys using binary
  `id_key_from_bson(value)`.
- As a result, a compound index such as
  `{ account: 1, created: 1 }` with `{ locale: "en", strength: 2 }` stores
  prefix keys like `str-ci:acme`, but a query such as
  `{ account: "ACME" }` asks SQLite for a binary key like `str:ACME`.
- The affected paths include unhinted candidate narrowing, count planning,
  hinted reads, update target selection, delete target selection, and
  findAndModify target selection.
- Existing tests cover single-field collation indexes, but not
  collation-aware compound prefix indexes.

This can silently return zero matches and is a correctness bug because matcher
validation never runs over documents excluded by the incorrect index lookup.

## Target State

- Query-side compound prefix planner keys are encoded with the same collation
  as document-side compound prefix entries.
- Every caller of compound prefix planning gets correct behavior for matching
  collation-aware compound indexes.
- Unsafe or unsupported collation/index shapes remain explicit fallback or
  errors as already designed.
- Binary/simple collation behavior remains unchanged.
- Existing range and full compound equality behavior remain unchanged.

## Definition Of Done

The fix is complete only when:

1. `prefix_planner_key_for_filter` or its replacement uses `spec.collation` for
   each prefix equality component.
2. Any other compound-prefix query-side key builders are audited and fixed if
   they have the same binary-vs-collation mismatch.
3. Rust tests cover case-insensitive compound-prefix index reads and target
   selection for:
   - `find`;
   - `count`;
   - update;
   - delete;
   - findAndModify.
4. Rust tests prove hinted collation-aware compound-prefix indexes work and
   incompatible collation hints still error without mutation.
5. PyMongo e2e tests cover the same class through real driver commands.
6. Existing simple/binary compound-prefix planner tests still pass.
7. Existing collation, planner, hint, update/delete/findAndModify, TTL, and
   full e2e tests pass.
8. This file is updated with completed checkboxes, status notes, and commit
   hashes.

## Milestone Checklist

When a milestone is complete:

1. Run the milestone's verification commands.
2. Update this file by changing the milestone from `[ ]` to `[x]`.
3. Add a short status note with the date, exact commands run, and commit hash.
4. Commit the code, tests, docs, and status update with a focused commit
   message.
5. Report the commit hash in the goal-loop status before continuing.

- [ ] Milestone 0: Fix collation-aware compound prefix key construction
- [ ] Milestone 1: PyMongo coverage and full verification

## Milestone 0: Fix Collation-Aware Compound Prefix Key Construction

Problem:

- Query-side compound prefix keys use binary encoding while document-side
  entries use index collation encoding.

Desired behavior:

- Build prefix planner keys using the owning `IndexSpec` collation.
- Keep simple/binary behavior byte-for-byte compatible.
- Add focused Rust tests that would fail before the fix.

Acceptance criteria:

- Rust tests prove a case-insensitive compound index on
  `{ account: 1, created: 1 }` correctly serves:
  - `find({account: "ACME"}, collation=en strength 2)`;
  - `count({account: "ACME"}, collation=en strength 2)`;
  - `update({account: "ACME"}, ...)`;
  - `delete({account: "ACME"}, ...)`;
  - findAndModify with `{account: "ACME"}`.
- Rust tests prove an explicit hint to that compound index works under the
  matching collation and an incompatible/simple collation hint errors without
  mutation where appropriate.
- Existing simple compound-prefix tests continue to pass.
- Milestone status is marked done in this file and committed.

Likely files:

- `src/main.rs`
- `docs/collation-adversarial-fix-goal-loop.md`

Verification:

```bash
cargo fmt -- --check
cargo test collation
cargo test compound_prefix
cargo test hint
cargo test update
cargo test delete
cargo test find_and_modify
```

## Milestone 1: PyMongo Coverage And Full Verification

Problem:

- The bug is driver-visible and affects normal PyMongo workflows.

Desired behavior:

- Add PyMongo e2e coverage for collation-aware compound-prefix indexes across
  read and write paths.
- Run full verification.

Acceptance criteria:

- PyMongo e2e covers:
  - compound collation index metadata;
  - `find` using prefix equality under matching collation;
  - `count_documents` using prefix equality under matching collation;
  - `update_many`/`delete_one` target selection under matching collation;
  - find_one_and_update or find_one_and_delete under matching collation;
  - incompatible hint collation error without mutation.
- Full verification passes:

```bash
cargo fmt -- --check
cargo test
cargo build
cargo run --bin mongolino-bench -- --profile ci --check-budget
UV_CACHE_DIR=/private/tmp/mongolino-uv-cache uv lock --check
UV_CACHE_DIR=/private/tmp/mongolino-uv-cache uv sync --locked --dev
UV_CACHE_DIR=/private/tmp/mongolino-uv-cache uv run --locked pytest tests/e2e
```

- Milestone status is marked done in this file and committed.

Likely files:

- `src/main.rs`
- `tests/e2e/test_indexes.py`
- `tests/e2e/test_crud.py`
- `tests/e2e/test_find_and_modify.py`
- `docs/collation-adversarial-fix-goal-loop.md`

## Final Response Requirements

When complete, report:

- every commit hash;
- exact tests and verification commands run;
- PyMongo e2e pass count;
- files changed;
- any residual risk or intentionally unsupported behavior.
