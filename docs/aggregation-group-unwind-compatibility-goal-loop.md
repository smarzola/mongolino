# Goal: Aggregation `$unwind` And `$group` Compatibility Uplift

You are working in `/Users/smarzola/projects/mongolino`.

Your objective is to deliver a substantial aggregation compatibility uplift: add practical `$unwind` support and replace the current special-case `$group` support with a bounded, tested grouping engine for common analytics and ODM workflows.

This is major uplift 3 of 3 in the current delivery sequence.

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
- At the end of each milestone, run verification, mark the milestone done in this file, commit the completed milestone, and report the commit hash before continuing.

## Target State

By the end, `mongolino` should support aggregation pipelines that can:

- expand arrays with `$unwind`;
- group by `null`, constants, simple field paths, and small document key specs;
- compute common accumulators: `$sum`, `$avg`, `$min`, `$max`, `$first`, `$last`, `$push`, and `$addToSet`;
- preserve the existing PyMongo `count_documents()` `$group` shape;
- compose `$match`, `$unwind`, `$group`, `$sort`, `$skip`, `$limit`, `$project`, and `$count` in stage order;
- return results through existing aggregate cursor and `getMore` behavior;
- reject unsupported stages, malformed expression specs, and unsupported accumulators explicitly.

The implementation should remain honest:

- No general aggregation expression language.
- No `$lookup`, `$facet`, `$addFields`, `$set`, `$unset`, `$replaceRoot`, `$out`, `$merge`, `$geoNear`, or window stages.
- No arithmetic expressions beyond accumulator behavior described here.
- No collation, allowDiskUse, read concern, write concern, hint, explain, maxTimeMS, or `let`.
- No attempt to match every MongoDB numeric promotion edge. Use a documented, deterministic subset and cover it with tests.

## Current State

The repo currently has:

- `aggregate` command dispatch with cursor support.
- Sequential `$match`, `$sort`, `$skip`, `$limit`, `$project`, and `$count`.
- A special-case `$group` path only for PyMongo `count_documents()` shape: `{ "_id": 1, "n": { "$sum": 1 } }`.
- Shared matcher, projection, sort, BSON comparison, cursor, and spec-corpus test infrastructure.

Important gaps:

- `$unwind` is unsupported.
- General `$group` is unsupported.
- Accumulators are unsupported except the count-documents special case.
- The README compatibility table still says general `$group` is missing.

## Definition Of Done

The goal is complete only when:

1. `$unwind` supports string path form, for example `{ "$unwind": "$tags" }`.
2. `$unwind` supports document form with `path`, optional `preserveNullAndEmptyArrays`, and optional `includeArrayIndex`.
3. `$unwind` rejects malformed paths, unsupported options, non-boolean `preserveNullAndEmptyArrays`, and invalid `includeArrayIndex` values explicitly.
4. `$unwind` expands arrays in stream order.
5. `$unwind` drops missing, `null`, and empty-array fields by default.
6. `$unwind` preserves missing, `null`, and empty-array fields when `preserveNullAndEmptyArrays: true`.
7. `$unwind` treats non-array, non-null present values as a single output document.
8. `$group` supports `_id: null`, scalar constants, field-path strings like `"$team"`, and small document key specs such as `{ team: "$team", active: "$active" }`.
9. `$group` rejects unsupported `_id` expressions explicitly.
10. `$group` supports `$sum`, `$avg`, `$min`, `$max`, `$first`, `$last`, `$push`, and `$addToSet`.
11. `$sum` supports numeric constants and numeric field paths; missing or non-numeric field values count as `0` for this subset.
12. `$avg` supports numeric field paths and returns `null` for groups with no numeric values.
13. `$min` and `$max` use the repo's deterministic BSON ordering over evaluated non-missing values.
14. `$first` and `$last` respect incoming stream order.
15. `$push` collects evaluated values in stream order.
16. `$addToSet` collects unique values using the repo's BSON equality semantics and deterministic insertion order.
17. Field-path accumulator operands such as `"$score"` and `"$_id"` work for supported accumulators.
18. Literal scalar accumulator operands work only where explicitly documented; unsupported object/array expressions return command errors.
19. Existing PyMongo `count_documents()` aggregation shape still works.
20. Aggregation cursor batching and `getMore` continue to work after `$unwind` and `$group`.
21. Unsupported accumulators such as `$median`, malformed accumulator documents, accumulator fields named `_id`, and unknown `$unwind` options return explicit errors.
22. Existing `$match`, `$sort`, `$skip`, `$limit`, `$project`, and `$count` behavior does not regress.
23. README compatibility tables and aggregation notes accurately describe the new subset and remaining gaps.
24. PyMongo e2e tests cover happy paths, not-happy paths, and adversarial paths.
25. Local spec corpus includes representative `$unwind`, `$group`, cursor, and unsupported-shape cases.
26. `cargo fmt -- --check`, `cargo test`, `cargo build`, `UV_CACHE_DIR=/private/tmp/mongolino-uv-cache uv lock --check`, `UV_CACHE_DIR=/private/tmp/mongolino-uv-cache uv sync --locked --dev`, and `UV_CACHE_DIR=/private/tmp/mongolino-uv-cache uv run --locked pytest tests/e2e` pass locally. Use unsandboxed execution for localhost binding if needed.
27. Milestone checkboxes in this file are marked `[x]` as work completes.
28. Each completed milestone has a focused commit.

## Milestone Checklist

When a milestone is complete:

1. Run the milestone's verification commands.
2. Update this checklist by changing the milestone from `[ ]` to `[x]`.
3. Add a short status note under that milestone with the date, exact commands run, and commit hash if available.
4. Commit the code, tests, docs, and checklist/status update with a focused commit message.
5. Report the commit hash in the goal-loop status before starting the next milestone.

- [x] Milestone 0: Aggregation expression and grouping design
- [x] Milestone 1: `$unwind` stage
- [x] Milestone 2: `$group` keys and scalar accumulators
- [x] Milestone 3: Array-style accumulators and pipeline composition
- [x] Milestone 4: Adversarial coverage, README, spec corpus, and final verification

## Milestone 0: Aggregation Expression And Grouping Design

Status 2026-07-04:

- Added the internal aggregation expression subset for field paths, scalar literals, and simple document key specs, with Rust coverage for supported evaluation and malformed shapes.
- Verification passed: `cargo fmt -- --check`; `cargo test aggregate`; `cargo test`.
- Commit: `971ac7e`.

Problem:

- `$group` needs a small expression evaluator and accumulator engine, but the repo should not grow an unbounded aggregation language.

Desired behavior:

- Add internal structures that make supported group keys and accumulator operands explicit.
- Keep errors crisp for unsupported expression shapes.
- Preserve the existing count-documents group shape.

Acceptance criteria:

- Define a small aggregation value expression subset:
  - field path strings beginning with `$`, such as `"$team"` and `"$_id"`;
  - scalar literals: `null`, booleans, strings not beginning with `$`, numeric values, ObjectId, and dates where BSON support is already available;
  - document key specs for `_id` only, with simple output field names and supported nested expressions as values.
- Reject array expressions and operator documents except accumulator documents in `$group`.
- Reject empty field paths, malformed `$` paths, and field paths containing empty dotted segments.
- Add Rust tests for expression parsing/evaluation before broad accumulator work.
- Milestone status is marked done in this file and committed.

Likely files:

- `src/main.rs`
- `docs/aggregation-group-unwind-compatibility-goal-loop.md`

Verification:

```bash
cargo fmt -- --check
cargo test aggregate
cargo test
```

Commit requirement:

- Commit after marking this milestone done and adding the status note.

## Milestone 1: `$unwind` Stage

Status 2026-07-04:

- Added `$unwind` string and document forms with default drop behavior, preservation, scalar handling, array index output, and explicit option/path errors.
- Verification passed: `cargo fmt -- --check`; `cargo test aggregate`; `cargo test`; `cargo build`; `UV_CACHE_DIR=/private/tmp/mongolino-uv-cache uv run --locked pytest tests/e2e/test_aggregation.py` outside the sandbox.
- Sandboxed e2e attempt failed at localhost port allocation with `PermissionError: [Errno 1] Operation not permitted` from `tests/e2e/conftest.py:103`.
- Commit: `8a75ac9`.

Problem:

- Many aggregation workflows need to explode array fields before grouping or filtering.

Desired behavior:

- Implement a bounded `$unwind` stage in the existing sequential pipeline executor.

Acceptance criteria:

- Support string form `{ "$unwind": "$tags" }`.
- Support document form `{ "$unwind": { "path": "$tags" } }`.
- Support `preserveNullAndEmptyArrays: true|false`.
- Support `includeArrayIndex: "idx"` for array values; set the index as an integer for expanded array elements.
- Reject `includeArrayIndex` paths starting with `$`, empty strings, dotted paths that collide with the unwind target, and unsupported option keys.
- Expand arrays in original order.
- Drop missing, `null`, and empty arrays by default.
- Preserve missing and `null` fields unchanged when requested; preserve empty arrays as a document with the target field removed or documented consistently by test.
- Treat scalar present values as one output document, with `includeArrayIndex` set to `null` if included.
- Add PyMongo e2e tests for default and preserving behavior.
- Milestone status is marked done in this file and committed.

Likely files:

- `src/main.rs`
- `tests/e2e/test_aggregation.py`
- `tests/spec_corpus/aggregation_pipeline.json` or a new focused corpus file
- `docs/aggregation-group-unwind-compatibility-goal-loop.md`

Verification:

```bash
cargo fmt -- --check
cargo test aggregate
UV_CACHE_DIR=/private/tmp/mongolino-uv-cache uv run --locked pytest tests/e2e/test_aggregation.py
cargo test
```

Commit requirement:

- Commit after marking this milestone done and adding the status note.

## Milestone 2: `$group` Keys And Scalar Accumulators

Status 2026-07-04:

- Replaced the count-only `$group` branch with bounded `_id` key parsing and `$sum`, `$avg`, `$min`, `$max`, `$first`, and `$last` accumulators while preserving the PyMongo count-documents group shape.
- Verification passed: `cargo fmt -- --check`; `cargo test aggregate`; `cargo test`; `cargo build`; `UV_CACHE_DIR=/private/tmp/mongolino-uv-cache uv run --locked pytest tests/e2e/test_aggregation.py` outside the sandbox.
- Commit: `c257eba`.

Problem:

- Grouping by fields and computing numeric/min/max/first/last summaries is a major practical aggregation need.

Desired behavior:

- Replace the current `$group` special case with a real parser/executor for the documented subset.

Acceptance criteria:

- Support `_id: null`, scalar constants, field paths, and small document key specs.
- Support `$sum` with numeric constants and numeric field paths.
- Preserve PyMongo `count_documents()` output shape for `{ "_id": 1, "n": { "$sum": 1 } }`.
- Support `$avg` over numeric field paths, ignoring missing and non-numeric values and returning `null` when no numeric values exist.
- Support `$min` and `$max` over evaluated non-missing values using deterministic BSON ordering.
- Support `$first` and `$last` over evaluated values in incoming stream order.
- Reject accumulator fields named `_id`.
- Reject accumulator specs with zero keys, multiple keys, unknown accumulator operators, array operands, operator expression operands, and unsupported object operands.
- Add Rust and PyMongo e2e tests covering groups with multiple documents, missing values, non-numeric values, and stable ordering.
- Milestone status is marked done in this file and committed.

Likely files:

- `src/main.rs`
- `tests/e2e/test_aggregation.py`
- `tests/spec_corpus/aggregation_pipeline.json` or a new focused corpus file
- `docs/aggregation-group-unwind-compatibility-goal-loop.md`

Verification:

```bash
cargo fmt -- --check
cargo test aggregate
UV_CACHE_DIR=/private/tmp/mongolino-uv-cache uv run --locked pytest tests/e2e/test_aggregation.py
cargo test
```

Commit requirement:

- Commit after marking this milestone done and adding the status note.

## Milestone 3: Array-Style Accumulators And Pipeline Composition

Status 2026-07-04:

- Added `$push` and `$addToSet`, including missing field-path values as `null`, deterministic insertion order, composed `$match`/`$unwind`/`$group`/`$sort`/`$project` pipelines, and aggregate cursor `getMore` coverage with `batchSize=1`.
- Verification passed: `cargo fmt -- --check`; `cargo test aggregate`; `cargo test`; `cargo build`; `UV_CACHE_DIR=/private/tmp/mongolino-uv-cache uv run --locked pytest tests/e2e/test_aggregation.py` outside the sandbox.
- Commit: `9987467`.

Problem:

- `$push` and `$addToSet` are common for grouped list materialization, and this uplift should prove `$unwind` + `$group` composes with existing stages and cursors.

Desired behavior:

- Add `$push` and `$addToSet` accumulators and harden stage composition.

Acceptance criteria:

- `$push` collects evaluated values in stream order.
- `$addToSet` collects unique evaluated values using existing BSON equality semantics.
- Missing field-path values are represented consistently and documented by tests.
- Pipelines composing `$match`, `$unwind`, `$group`, `$sort`, `$project`, `$skip`, `$limit`, and `$count` work in stage order.
- Cursor batch iteration works for grouped/unwound result sets.
- Add e2e tests using `batchSize=1` over a grouped/unwound pipeline.
- Milestone status is marked done in this file and committed.

Likely files:

- `src/main.rs`
- `tests/e2e/test_aggregation.py`
- `tests/spec_corpus/aggregation_pipeline.json` or a new focused corpus file
- `docs/aggregation-group-unwind-compatibility-goal-loop.md`

Verification:

```bash
cargo fmt -- --check
cargo test aggregate
UV_CACHE_DIR=/private/tmp/mongolino-uv-cache uv run --locked pytest tests/e2e/test_aggregation.py
cargo test
```

Commit requirement:

- Commit after marking this milestone done and adding the status note.

## Milestone 4: Adversarial Coverage, README, Spec Corpus, And Final Verification

Status 2026-07-04:

- Added adversarial PyMongo and corpus coverage for empty input groups, unsupported group expression documents and arrays, malformed accumulator operands, unsupported `$unwind` paths, and successful reuse after command errors.
- Updated README aggregation compatibility text to describe `$unwind`, bounded `$group`, supported accumulators, cursor behavior, and remaining unsupported stages/options.
- Verification passed: `cargo fmt -- --check`; `cargo test`; `cargo build`; `UV_CACHE_DIR=/private/tmp/mongolino-uv-cache uv lock --check`; `UV_CACHE_DIR=/private/tmp/mongolino-uv-cache uv sync --locked --dev`; `UV_CACHE_DIR=/private/tmp/mongolino-uv-cache uv run --locked pytest tests/e2e` outside the sandbox (`118 passed, 1 skipped`).
- Commit: pending at status-update time; final hash reported in handoff.

Problem:

- Aggregation engines are easy to make permissive accidentally. The final milestone should harden unsupported paths and make the documented surface precise.

Acceptance criteria:

- Add explicit not-happy and adversarial tests for:
  - malformed `$unwind` operands;
  - unsupported `$unwind` options;
  - malformed `$group` specs;
  - unsupported accumulators;
  - unsupported expression documents;
  - non-array `$unwind` scalar behavior;
  - empty input groups;
  - no partial state leaks after errors.
- Update README compatibility table and aggregation notes.
- Add or update spec corpus cases for supported `$unwind`, supported `$group`, composed pipelines, and unsupported shapes.
- Ensure existing tests for unsupported `$lookup`, cursor option errors, `$count`, `$project`, and PyMongo `count_documents()` still pass.
- Milestone status is marked done in this file and committed.

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
