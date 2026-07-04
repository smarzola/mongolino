# Goal: SQLite Write Targeting And Unique Check Performance Uplift

You are working in `/Users/smarzola/projects/mongolino`.

Your objective is to deliver the third large performance uplift in the performance sequence: use SQLite as the query engine for safe write-target selection and unique-index conflict checks. The current benchmark baseline shows `update_index_refresh` at scan-like cost, and source inspection shows update, delete, findAndModify, and unique checks often decode full namespaces even when `_id` or maintained index entries can safely narrow the work.

This is performance uplift 3 of 3.

## Repository Rules

Follow `AGENTS.md`.

Important reminders:

- Keep the server implementation in Rust and the durable storage layer in SQLite.
- Treat MongoDB wire compatibility as observable behavior: validate changes with real client handshakes when possible.
- Run `cargo fmt` and `cargo test` before handing off code changes.
- Keep unsupported MongoDB commands explicit by returning command errors instead of silently accepting behavior.
- Do not require Docker or external MongoDB services.
- Do not optimize by weakening matcher semantics.
- Do not revert unrelated user or agent changes.
- You are not alone in the codebase. Other agents may have touched nearby files; work with current state and do not revert unrelated edits.
- At the end of each milestone, run verification, mark the milestone done in this file, commit the completed milestone, and report the commit hash before continuing.

## Performance Target

Use SQLite to narrow write targets and unique conflicts when behavior is equivalent:

- update/delete/findAndModify with `_id` equality should load only the primary-key candidate;
- update/delete/findAndModify with safe indexed scalar equality should load only maintained index-entry candidates, then still run the Rust matcher before mutation;
- unique-index checks for single-field scalar unique indexes should use maintained index entries instead of scanning the namespace;
- unsafe filters and compound/multikey unique checks must keep the current Rust fallback.

Baseline from `docs/performance-baseline.md` local profile:

- `update_index_refresh`: `30.707 ms/op`;
- `_id` equality `find`: `0.023 ms/op`;
- indexed scalar equality `find`: `2.098 ms/op`.

Performance success target:

- materially improve `update_index_refresh` on the local benchmark profile;
- preserve all write invariants: validation, unique indexes, `_id` immutability, ordered/unordered write errors, index-entry freshness, and findAndModify pre/post images;
- keep fallback behavior for unsupported filters unchanged.

## Current State

The current implementation:

- `apply_update_entry` scans `stored_documents_for_namespace_tx` and applies the matcher until enough targets are found.
- `apply_delete_entry` scans `stored_documents_for_namespace_tx` and applies the matcher until enough targets are found.
- `find_and_modify_target_tx` scans transaction-local documents and sorts in Rust.
- `ensure_unique_constraints_tx` checks unique conflicts by scanning stored documents for each unique index.
- `candidate_documents` and `indexed_candidate_documents` already provide a read-side model for safe narrowing outside transactions.
- `index_entries` are maintained on insert, update, delete, and drop.

Important constraints:

- Candidate narrowing is allowed only when it cannot exclude a document the Rust matcher would accept.
- Even after SQLite narrows candidates, the Rust matcher must validate candidates before mutation.
- Multi-update and delete-many must preserve deterministic created-order behavior.
- Sorted findAndModify target selection can only use candidate narrowing before existing Rust sort; it must not push sort into SQLite unless already equivalent.
- Unique checks must not use maintained index entries for unsupported unique-index shapes.

## Definition Of Done

The goal is complete only when:

1. Transaction-local candidate loading supports `_id` equality using `(namespace, id_key)`.
2. Transaction-local candidate loading supports safe indexed scalar equality using `index_entries`.
3. `apply_update_entry` uses candidate narrowing where safe and preserves ordered/unordered and multi/single semantics.
4. `apply_delete_entry` uses candidate narrowing where safe and preserves limit `1` and `0` semantics.
5. `find_and_modify_target_tx` uses candidate narrowing where safe and still applies matcher and Rust sort.
6. Unique constraint checks use `index_entries` for safe single-field scalar unique indexes where possible.
7. Unique constraint checks fall back for compound indexes, multikey/array values, missing-field semantics where not safely expressible, and unsupported shapes.
8. Maintained index entries remain fresh after insert/update/upsert/delete/drop.
9. Validation, `_id` immutability, duplicate-key errors, and findAndModify images do not regress.
10. Benchmarks include before/after evidence for `update_index_refresh`.
11. Performance budget command still passes.
12. Docs update the pushdown roadmap with completed write-targeting work and remaining aggregation/grouping work.
13. PyMongo e2e and spec corpus coverage prove update/delete/findAndModify/unique behavior through real driver paths where practical.
14. `cargo fmt -- --check`, `cargo test`, `cargo build`, `cargo run --bin mongolino-bench -- --profile ci --check-budget`, `UV_CACHE_DIR=/private/tmp/mongolino-uv-cache uv lock --check`, `UV_CACHE_DIR=/private/tmp/mongolino-uv-cache uv sync --locked --dev`, and `UV_CACHE_DIR=/private/tmp/mongolino-uv-cache uv run --locked pytest tests/e2e` pass locally. Use unsandboxed execution for localhost binding if needed.
15. Milestone checkboxes in this file are marked `[x]` as work completes.
16. Each completed milestone has a focused commit.

## Milestone Checklist

When a milestone is complete:

1. Run the milestone's verification commands.
2. Update this checklist by changing the milestone from `[ ]` to `[x]`.
3. Add a short status note under that milestone with the date, exact commands run, and commit hash if available.
4. Commit the code, tests, docs, and checklist/status update with a focused commit message.
5. Report the commit hash in the goal-loop status before starting the next milestone.

- [x] Milestone 0: Transaction candidate planner
- [ ] Milestone 1: Update/delete target narrowing
- [ ] Milestone 2: findAndModify target narrowing
- [ ] Milestone 3: Unique conflict check pushdown
- [ ] Milestone 4: Benchmarks, docs, and final verification

## Milestone 0: Transaction Candidate Planner

Problem:

- Write paths run inside transactions, but the current read-side candidate helper uses `Connection`.

Desired behavior:

- Add a transaction-local candidate loader that mirrors the safe read-side planner.

Acceptance criteria:

- Supports `_id` literal equality or `$eq`.
- Supports simple scalar equality or `$eq` through maintained single-field index entries.
- Rejects/falls back for arrays, logical operators, unsupported operators, range operators, `$in`, `$nin`, `$ne`, `$exists`, `$not`, unindexed fields, and risky dotted paths without maintained entries.
- Returns stored documents in created order for deterministic single-target behavior.
- Always returns candidates that still need Rust matcher validation.
- Add Rust tests for classification and candidate order.
- Milestone status is marked done in this file and committed.

Status 2026-07-04: Done. Added transaction-local `_id` and safe indexed scalar
candidate planning/loading, with Rust tests for classification and created-order
candidate delivery. Verification passed: `cargo fmt -- --check`,
`cargo test planner`, `cargo test update`, `cargo test delete`,
`cargo test find_and_modify`, and `cargo test`. Commit: `5b1f921`.

Likely files:

- `src/main.rs`
- `docs/performance-sqlite-write-targeting-goal-loop.md`

Verification:

```bash
cargo fmt -- --check
cargo test planner
cargo test update
cargo test delete
cargo test find_and_modify
cargo test
```

## Milestone 1: Update/Delete Target Narrowing

Problem:

- Update and delete target selection scan full namespaces even for `_id` and indexed scalar equality filters.

Acceptance criteria:

- `apply_update_entry` uses transaction candidate narrowing where safe.
- `apply_delete_entry` uses transaction candidate narrowing where safe.
- Single update/delete preserves first-match behavior.
- Multi update/delete-many preserves all-match behavior.
- Fallback filters behave exactly as before.
- Add Rust and PyMongo e2e coverage for `_id` and indexed scalar update/delete paths plus fallback cases.
- Milestone status is marked done in this file and committed.

Likely files:

- `src/main.rs`
- `tests/e2e/test_crud.py`
- `tests/e2e/test_indexes.py`
- `tests/spec_corpus/delete_one_many.json`
- `tests/spec_corpus/update_set_inc_upsert.json`
- `docs/performance-sqlite-write-targeting-goal-loop.md`

Verification:

```bash
cargo fmt -- --check
cargo test update
cargo test delete
cargo test planner
UV_CACHE_DIR=/private/tmp/mongolino-uv-cache uv run --locked pytest tests/e2e/test_crud.py tests/e2e/test_indexes.py
cargo test
```

## Milestone 2: findAndModify Target Narrowing

Problem:

- findAndModify scans every document before selecting, updating, deleting, or replacing one target.

Acceptance criteria:

- `find_and_modify_target_tx` uses transaction candidate narrowing where safe.
- Rust matcher still validates every narrowed candidate.
- Existing sort behavior remains Rust-side and unchanged.
- Pre-image/post-image, projection, upsert, delete, duplicate-key, validation, and index-entry freshness behavior do not regress.
- Add focused tests for `_id` and indexed scalar findAndModify update/delete/replace where practical.
- Milestone status is marked done in this file and committed.

Likely files:

- `src/main.rs`
- `tests/e2e/test_find_and_modify.py`
- `tests/spec_corpus/find_and_modify.json`
- `docs/performance-sqlite-write-targeting-goal-loop.md`

Verification:

```bash
cargo fmt -- --check
cargo test find_and_modify
cargo test planner
UV_CACHE_DIR=/private/tmp/mongolino-uv-cache uv run --locked pytest tests/e2e/test_find_and_modify.py
cargo test
```

## Milestone 3: Unique Conflict Check Pushdown

Problem:

- Unique checks scan the namespace even when maintained index entries already encode scalar unique keys.

Acceptance criteria:

- Safe single-field scalar unique conflicts use `index_entries` lookups.
- Excluding the current document during update works correctly.
- Missing-field/null semantics remain compatible with the current supported subset.
- Compound indexes and unsafe/multikey values fall back to current scan behavior.
- Duplicate-key error codes and messages remain compatible with existing tests.
- Add tests for insert, update, upsert, and findAndModify duplicate conflicts through the pushed-down path.
- Milestone status is marked done in this file and committed.

Likely files:

- `src/main.rs`
- `tests/e2e/test_indexes.py`
- `tests/e2e/test_find_and_modify.py`
- `tests/e2e/test_update_operators.py`
- `docs/performance-sqlite-write-targeting-goal-loop.md`

Verification:

```bash
cargo fmt -- --check
cargo test unique
cargo test update
cargo test find_and_modify
UV_CACHE_DIR=/private/tmp/mongolino-uv-cache uv run --locked pytest tests/e2e/test_indexes.py tests/e2e/test_find_and_modify.py tests/e2e/test_update_operators.py
cargo test
```

## Milestone 4: Benchmarks, Docs, And Final Verification

Acceptance criteria:

- Run smoke and local benchmarks.
- Update `docs/performance-baseline.md` with after numbers for `update_index_refresh`.
- Update roadmap to mark write-targeting and unique-check pushdown complete, and identify remaining aggregation/grouping candidates.
- Ensure CI budget passes.
- Full verification passes.
- Milestone status is marked done in this file and committed.

Verification:

```bash
cargo fmt -- --check
cargo test
cargo build
cargo run --bin mongolino-bench -- --profile smoke --json /tmp/mongolino-bench-write-smoke.json
cargo run --bin mongolino-bench -- --profile local --json /tmp/mongolino-bench-write-local.json
cargo run --bin mongolino-bench -- --profile ci --check-budget
UV_CACHE_DIR=/private/tmp/mongolino-uv-cache uv lock --check
UV_CACHE_DIR=/private/tmp/mongolino-uv-cache uv sync --locked --dev
UV_CACHE_DIR=/private/tmp/mongolino-uv-cache uv run --locked pytest tests/e2e
```

Use unsandboxed execution for the PyMongo e2e suite if the sandbox blocks localhost binding.

Commit requirement:

- Commit after marking this milestone done and adding the status note.

## Final Response Requirements

When complete, report:

- commits made;
- files changed;
- pushed-down update/delete/findAndModify/unique cases;
- fallback cases that intentionally remain Rust-side;
- benchmark before/after headline numbers;
- final verification commands and outcomes;
- known residual risks.
