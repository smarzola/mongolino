# Goal: Update Pipeline And Array Filters Compatibility Uplift

You are working in `/Users/smarzola/projects/mongolino`.

Your objective is to deliver uplift 6 of the seven-uplift MongoDB compatibility
sequence: add practical update pipeline support plus a conservative positional
array update subset with `arrayFilters`.

This is a large compatibility and quality uplift. It should move the repo-local
scorecard from **77%** to at least **82%** by raising update language
compatibility from **8%** to at least **13%**, without weakening explicit errors
for unsupported update behavior.

## Repository Rules

Follow `AGENTS.md`.

Important reminders:

- Keep the server implementation in Rust and the durable storage layer in
  SQLite.
- Treat MongoDB wire compatibility as observable behavior: validate changes
  with real client handshakes when possible.
- Run `cargo fmt` and `cargo test` before handing off code changes.
- Keep unsupported MongoDB commands explicit by returning command errors instead
  of silently accepting behavior.
- Use the existing PyMongo e2e suite for real driver verification.
- Use `uv` for Python tooling.
- Do not use Docker or external MongoDB services for this goal.
- Do not revert unrelated user or agent changes.
- You are not alone in the codebase. Other agents may have touched nearby
  files; work with current state and do not revert unrelated edits.
- At the end of each milestone, run verification, mark the milestone done in
  this file, commit the completed milestone, and report the commit hash before
  continuing.

## Target State

By the end, `mongolino` should support common PyMongo update workflows that
currently fail:

- update pipelines for `update` and `findAndModify`;
- positional `$` updates for the first matching array element selected by the
  query;
- all-positional `$[]` updates across every element of one array path;
- filtered positional `$[identifier]` updates using `arrayFilters`;
- supported scalar modifiers through positional paths where safe;
- validation, unique-index, TTL, collation, upsert, ordered/unordered batch, and
  index-entry invariants after every supported update shape.

The implementation should remain honest and bounded:

- No full MongoDB update language parity.
- No nested multi-array positional traversal unless explicitly implemented and
  tested.
- No aggregation stages in update pipelines beyond the documented subset.
- No array filter operators beyond the existing matcher subset.
- No `$expr`, JavaScript, geospatial/text predicates, command `let`, write
  concern, retryable writes, transactions, or explain behavior.
- Unsupported pipeline stages, unsupported positional shapes, malformed
  `arrayFilters`, ambiguous positional matches, and unsafe paths must return
  explicit write or command errors.

## Current State

The repo currently has:

- Replacement updates.
- Modifier updates with `$set`, `$unset`, `$inc`, `$rename`, `$min`, `$max`,
  `$mul`, `$setOnInsert`, `$push`, `$addToSet`, `$pop`, `$pull`, and `$pullAll`.
- Shared update application used by `update` and `findAndModify`.
- Update target planning through `_id`, maintained indexes, hints, collation,
  and fallback scans.
- Validation enforcement and `bypassDocumentValidation`.
- Unique-index enforcement and index-entry refresh after writes.
- PyMongo e2e and spec corpus coverage for the current update subset.
- A bounded aggregation expression/stage evaluator that can be reused or
  adapted for update pipelines.

Important gaps:

- `update` requires `u` to be a document and rejects pipeline arrays.
- `findAndModify` rejects pipeline update arrays.
- `arrayFilters` is rejected on both update and findAndModify paths.
- `validate_update_path` rejects any `$` segment, so `$`, `$[]`, and `$[id]`
  positional updates are unsupported.
- Existing docs explicitly list update pipelines, array filters, and positional
  operators as unsupported.

## Definition Of Done

The goal is complete only when:

1. `update` accepts update pipeline arrays for supported stages.
2. `findAndModify` accepts update pipeline arrays for supported stages.
3. Supported update pipeline stages include `$set`/`$addFields`, `$unset`,
   `$project`, `$replaceRoot`, and `$replaceWith`, reusing the Aggregation v2
   expression subset where safe.
4. Pipeline updates reject unsupported stages, unsupported command options,
   malformed specs, and attempts to remove or change `_id`.
5. Pipeline updates work for matched documents and upsert inserts where safe and
   documented by tests.
6. Positional `$` supports updating the first matching element of a single array
   path when the query contains a supported predicate that identifies the array
   element.
7. `$[]` supports applying supported scalar modifiers to every element of one
   array path.
8. `$[identifier]` supports applying supported scalar modifiers to elements
   matching `arrayFilters`.
9. Supported positional modifiers include at least `$set`, `$unset`, `$inc`,
   `$min`, `$max`, and `$mul`; support for array modifiers through positional
   paths is optional and must be explicit if added.
10. `arrayFilters` is accepted only as an array of non-empty documents using
    one identifier each.
11. Array filter identifiers must be valid, referenced identifiers must exist,
    and unused or duplicate identifiers must return errors.
12. Array filter predicates use the existing matcher subset and preserve
    explicit errors for unsupported operators.
13. Positional updates reject `_id` changes, empty paths, scalar parent
    traversal, missing arrays where MongoDB would error for this subset,
    ambiguous nested arrays, and unsupported positional syntax.
14. All supported shapes work through `update_one`, `update_many`, and
    `find_one_and_update` where applicable.
15. Failing positional or pipeline updates do not partially mutate documents.
16. Validation enforcement and `bypassDocumentValidation` still apply.
17. Unique-index enforcement still applies.
18. Maintained index entries remain fresh after pipeline and positional updates.
19. Ordered/unordered batch behavior remains correct.
20. Invalid update entries return write errors before TTL sweeps or mutations.
21. Existing CRUD, query, index, TTL, collation, aggregation, validation, and
    cursor behavior does not regress.
22. PyMongo e2e tests cover happy paths, not-happy paths, and adversarial paths.
23. The local spec corpus includes representative update pipeline and
    positional/arrayFilters cases.
24. README compatibility tables and notes accurately describe the new supported
    update subset and remaining gaps.
25. `docs/mongodb-compatibility-uplifts-roadmap.md` marks uplift 6 complete,
    moves update language compatibility from 8% to at least 13%, and moves the
    total score from 77% to at least 82%.
26. Benchmark coverage or `docs/performance-baseline.md` is updated if the new
    update path adds meaningful performance risk.
27. Full verification passes:

```bash
cargo fmt -- --check
cargo test
cargo build
cargo run --bin mongolino-bench -- --profile ci --check-budget
UV_CACHE_DIR=/private/tmp/mongolino-uv-cache uv lock --check
UV_CACHE_DIR=/private/tmp/mongolino-uv-cache uv sync --locked --dev
UV_CACHE_DIR=/private/tmp/mongolino-uv-cache uv run --locked pytest tests/e2e
```

28. Milestone checkboxes in this file are marked `[x]` as work completes.
29. Each completed milestone has a focused commit.

## Milestone Checklist

When a milestone is complete:

1. Run the milestone's verification commands.
2. Update this checklist by changing the milestone from `[ ]` to `[x]`.
3. Add a short status note under that milestone with the date, exact commands
   run, and commit hash if available.
4. Commit the code, tests, docs, and checklist/status update with a focused
   commit message.
5. Report the commit hash in the goal-loop status before starting the next
   milestone.

- [x] Milestone 0: Update spec architecture and preflight model
- [ ] Milestone 1: Update pipeline subset
- [ ] Milestone 2: Positional `$` and `$[]`
- [ ] Milestone 3: Filtered positional `$[identifier]` and `arrayFilters`
- [ ] Milestone 4: Invariant hardening across validation, indexes, TTL, and findAndModify
- [ ] Milestone 5: PyMongo e2e, spec corpus, docs, scorecard, and benchmarks
- [ ] Milestone 6: Final verification and handoff

## Milestone 0: Update Spec Architecture And Preflight Model

Problem:

- The current `UpdateSpec` only distinguishes replacement and modifier updates.
  Pipeline updates and positional array updates need richer parsed structure
  without weakening no-mutation-on-error guarantees.

Desired behavior:

- Extend update parsing enough to represent replacement, modifier, and pipeline
  updates.
- Add a parsed representation for positional paths and array filters.
- Keep existing non-positional update behavior unchanged.

Acceptance criteria:

- `UpdateSpec` can represent pipeline update arrays without applying them yet.
- `update` and `findAndModify` parse update arrays but still reject unsupported
  pipeline stages until Milestone 1 implements them.
- Command and entry shape parsing accepts `arrayFilters` only where appropriate,
  but unsupported/unused forms remain explicit errors until implemented.
- Add helpers that parse update paths into segments:
  - field segments;
  - positional `$`;
  - all-positional `$[]`;
  - filtered positional `$[identifier]`.
- Existing non-positional paths keep current `_id`, empty segment, scalar parent,
  and collision behavior.
- Preflight validation still happens before TTL sweeps or mutations.
- Existing update, findAndModify, validation, unique-index, and array modifier
  tests pass unchanged.
- Milestone status is marked done in this file and committed.

Likely files:

- `src/main.rs`
- `docs/update-pipeline-arrayfilters-goal-loop.md`

Verification:

```bash
cargo fmt -- --check
cargo test update
cargo test find_and_modify
cargo test validation
cargo test
```

Commit requirement:

- Commit after marking this milestone done and adding the status note.

Status:

- 2026-07-05: Added parsed update architecture for document and pipeline update
  specs, parsed positional path segments, parsed `arrayFilters`, and shared
  preflight/application wiring for `update` and `findAndModify`. Verification
  passed with `cargo fmt -- --check`, `cargo test update`, `cargo test
  find_and_modify`, `cargo test validation`, and `cargo test` (180 main tests
  and 182 bench-target tests passed). Commit hash pending at checkpoint commit
  time.

## Milestone 1: Update Pipeline Subset

Problem:

- PyMongo applications often use update pipelines to compute fields from the
  current document. The server currently rejects all update arrays.

Desired behavior:

- Implement a bounded update pipeline executor over one document using the
  Aggregation v2 expression subset.

Acceptance criteria:

- `update` accepts `u` arrays containing only supported stages:
  - `$set` / `$addFields`;
  - `$unset`;
  - `$project`;
  - `$replaceRoot`;
  - `$replaceWith`.
- `findAndModify` accepts the same pipeline subset.
- Pipeline stages run sequentially over the current document.
- Expressions can reference the current document with field paths, `$$ROOT`,
  and `$$CURRENT`.
- Pipeline updates reject `_id` changes/removal.
- Pipeline updates reject unsupported stages such as `$lookup`, `$group`,
  `$unwind`, `$out`, `$merge`, and `$facet`.
- Pipeline updates reject malformed expression specs and constant-only runtime
  errors during preflight when possible, before TTL sweeps.
- Pipeline upsert insert behavior is implemented conservatively and documented:
  build the base document from equality query fields, ensure `_id`, then run the
  pipeline; reject unsupported cases that cannot safely synthesize a base
  document.
- Validation, unique indexes, and index refresh apply after pipeline updates.
- Add Rust and PyMongo tests for update_one, update_many, find_one_and_update,
  upsert, validation failure, `_id` immutability, unsupported stages, and
  no-partial-write errors.
- Milestone status is marked done in this file and committed.

Likely files:

- `src/main.rs`
- `tests/e2e/test_update_operators.py`
- `tests/e2e/test_find_and_modify.py`
- `tests/spec_corpus/update_operators.json`
- `docs/update-pipeline-arrayfilters-goal-loop.md`

Verification:

```bash
cargo fmt -- --check
cargo test update
cargo test find_and_modify
cargo test validation
UV_CACHE_DIR=/private/tmp/mongolino-uv-cache uv run --locked pytest tests/e2e/test_update_operators.py tests/e2e/test_find_and_modify.py
cargo test
```

Commit requirement:

- Commit after marking this milestone done and adding the status note.

## Milestone 2: Positional `$` And `$[]`

Problem:

- Common array updates use the first matched element (`$`) or all elements
  (`$[]`). The server currently rejects any path containing `$`.

Desired behavior:

- Implement conservative positional path resolution for one array level.

Acceptance criteria:

- Positional `$` supports paths such as `items.$.status` when the query includes
  a supported predicate for the same array path, for example
  `{ "items.kind": "open" }` or `{ "items": { "$elemMatch": { "kind": "open" } } }`.
- Positional `$` updates only the first matching element in stored document
  order.
- `$[]` supports paths such as `items.$[].score` and updates every element of
  the array.
- Supported modifiers through `$` and `$[]` include `$set`, `$unset`, `$inc`,
  `$min`, `$max`, and `$mul`.
- Positional modifiers reject `_id` writes, missing arrays, scalar targets,
  scalar parent traversal, nested arrays, multiple positional segments, and
  ambiguous query shapes.
- `update_one`, `update_many`, and `find_one_and_update` are covered.
- Existing non-positional modifiers still behave exactly as before.
- Failing positional updates leave the original document unchanged.
- Milestone status is marked done in this file and committed.

Likely files:

- `src/main.rs`
- `tests/e2e/test_update_operators.py`
- `tests/e2e/test_find_and_modify.py`
- `docs/update-pipeline-arrayfilters-goal-loop.md`

Verification:

```bash
cargo fmt -- --check
cargo test positional
cargo test update
cargo test find_and_modify
UV_CACHE_DIR=/private/tmp/mongolino-uv-cache uv run --locked pytest tests/e2e/test_update_operators.py tests/e2e/test_find_and_modify.py
cargo test
```

Commit requirement:

- Commit after marking this milestone done and adding the status note.

## Milestone 3: Filtered Positional `$[identifier]` And `arrayFilters`

Problem:

- ODMs commonly update selected array elements with `arrayFilters`, for example
  `{ "$set": { "items.$[open].status": "done" } }` with
  `arrayFilters=[{ "open.kind": "open" }]`.

Desired behavior:

- Implement a bounded single-array filtered positional subset.

Acceptance criteria:

- `update` accepts per-entry `arrayFilters`.
- `findAndModify` accepts command-level `arrayFilters`.
- `$[identifier]` path segments bind to one array filter document.
- Identifier names must start with a lowercase ASCII letter and contain only
  ASCII letters, digits, or underscores.
- Each identifier used in update paths must have exactly one filter document.
- Unused filters and duplicate filters return explicit errors.
- Array filter documents must be non-empty and use one identifier prefix, for
  example `{ "elem.score": { "$gte": 5 } }`.
- Array filter predicates use the existing matcher subset against each array
  element with the identifier prefix stripped.
- Supported modifiers through `$[identifier]` include `$set`, `$unset`, `$inc`,
  `$min`, `$max`, and `$mul`.
- Unsupported filter operators, malformed identifiers, missing arrays, scalar
  targets, nested arrays, and multiple identifiers in one path return errors.
- Failing filtered positional updates leave original documents unchanged.
- Validation, unique indexes, and index refresh apply after successful updates.
- Add Rust and PyMongo e2e coverage for happy, not-happy, and adversarial paths.
- Milestone status is marked done in this file and committed.

Likely files:

- `src/main.rs`
- `tests/e2e/test_update_operators.py`
- `tests/e2e/test_find_and_modify.py`
- `tests/spec_corpus/update_array_operators.json`
- `docs/update-pipeline-arrayfilters-goal-loop.md`

Verification:

```bash
cargo fmt -- --check
cargo test array_filter
cargo test positional
cargo test update
cargo test find_and_modify
UV_CACHE_DIR=/private/tmp/mongolino-uv-cache uv run --locked pytest tests/e2e/test_update_operators.py tests/e2e/test_find_and_modify.py
cargo test
```

Commit requirement:

- Commit after marking this milestone done and adding the status note.

## Milestone 4: Invariant Hardening Across Validation, Indexes, TTL, And findAndModify

Problem:

- Pipeline and positional updates touch shared write invariants. Bugs here can
  silently corrupt documents, indexes, or validation guarantees.

Desired behavior:

- Harden all write invariants with focused tests.

Acceptance criteria:

- Invalid pipeline, positional, and array-filter shapes return write/command
  errors before TTL sweeps or mutations.
- Ordered and unordered update batches preserve correct partial-failure
  behavior.
- Validation failures preserve original documents unless bypass is true.
- Bypass validation does not bypass `_id` immutability or unique indexes.
- Unique indexes reject duplicate results from pipeline and positional updates.
- Maintained exact, compound, multikey, sparse, partial, TTL, and collation-aware
  index entries remain fresh after successful pipeline and positional updates.
- `findAndModify` returns correct pre-image/post-image for pipeline and
  positional updates.
- Upsert behavior is documented and tested for pipeline and rejected where
  unsupported.
- Existing aggregation, query, TTL, and collation tests still pass.
- Milestone status is marked done in this file and committed.

Likely files:

- `src/main.rs`
- `tests/e2e/test_update_operators.py`
- `tests/e2e/test_find_and_modify.py`
- `tests/e2e/test_validation.py`
- `tests/e2e/test_indexes.py`
- `docs/update-pipeline-arrayfilters-goal-loop.md`

Verification:

```bash
cargo fmt -- --check
cargo test update
cargo test find_and_modify
cargo test validation
cargo test unique
cargo test ttl
cargo test collation
UV_CACHE_DIR=/private/tmp/mongolino-uv-cache uv run --locked pytest tests/e2e/test_update_operators.py tests/e2e/test_find_and_modify.py tests/e2e/test_validation.py tests/e2e/test_indexes.py
cargo test
```

Commit requirement:

- Commit after marking this milestone done and adding the status note.

## Milestone 5: PyMongo E2E, Spec Corpus, Docs, Scorecard, And Benchmarks

Problem:

- The new update surface must be visible through real drivers and documented
  honestly.

Desired behavior:

- Extend e2e tests, local corpus, README, roadmap, and benchmark evidence.

Acceptance criteria:

- PyMongo e2e covers:
  - update pipelines through update_one, update_many, find_one_and_update, and
    upsert where supported;
  - positional `$` first-match updates;
  - `$[]` all-element updates;
  - `$[identifier]` with arrayFilters;
  - validation, unique-index, TTL no-sweep-on-error, collation target selection,
    hints, and index refresh interactions;
  - unsupported pipeline stages, unsupported positional forms, malformed
    arrayFilters, unused/duplicate identifiers, and no-partial-write failures.
- Local spec corpus covers representative success and failure cases.
- README updates `update`, `findAndModify`, and update notes.
- `docs/mongodb-compatibility-uplifts-roadmap.md` marks uplift 6 complete,
  moves update language compatibility to at least 13%, moves total score to at
  least 82%, and points the next prompt to driver workflow semantics.
- Benchmark coverage or `docs/performance-baseline.md` includes representative
  positional or pipeline update cases, or documents why existing write-targeting
  budget coverage is sufficient.
- Milestone status is marked done in this file and committed.

Likely files:

- `tests/e2e/test_update_operators.py`
- `tests/e2e/test_find_and_modify.py`
- `tests/e2e/test_spec_corpus.py`
- `tests/spec_corpus/update_array_operators.json`
- `tests/spec_corpus/update_operators.json`
- `README.md`
- `docs/mongodb-compatibility-uplifts-roadmap.md`
- `docs/performance-baseline.md`
- `src/bin/mongolino-bench.rs`
- `docs/update-pipeline-arrayfilters-goal-loop.md`

Verification:

```bash
cargo fmt -- --check
cargo test update
cargo test find_and_modify
cargo run --bin mongolino-bench -- --profile ci --check-budget
UV_CACHE_DIR=/private/tmp/mongolino-uv-cache uv lock --check
UV_CACHE_DIR=/private/tmp/mongolino-uv-cache uv sync --locked --dev
UV_CACHE_DIR=/private/tmp/mongolino-uv-cache uv run --locked pytest tests/e2e/test_update_operators.py tests/e2e/test_find_and_modify.py tests/e2e/test_spec_corpus.py
cargo test
```

Commit requirement:

- Commit after marking this milestone done and adding the status note.

## Milestone 6: Final Verification And Handoff

Problem:

- The final uplift state must be verified as a whole.

Desired behavior:

- Run the full project verification suite and record exact results.

Acceptance criteria:

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

- If sandboxed PyMongo e2e is blocked by localhost binding, record the sandbox
  failure and rerun the exact command unsandboxed.
- This file contains a final status note with:
  - exact commands run;
  - Rust test counts;
  - PyMongo e2e pass count;
  - benchmark result summary;
  - commit hashes for every milestone;
  - residual unsupported update behavior.
- Milestone status is marked done in this file and committed.

Likely files:

- `docs/update-pipeline-arrayfilters-goal-loop.md`

Commit requirement:

- Commit after marking this milestone done and adding the status note.

## Final Response Requirements

When the goal is complete, report:

- the final compatibility scorecard movement;
- every commit hash created for this goal;
- milestone checklist status;
- exact verification commands run and results;
- PyMongo e2e pass count;
- benchmark result summary;
- files changed;
- any known residual update gaps or intentionally unsupported MongoDB behavior.

Do not call the goal complete if any milestone remains unchecked, verification
has not run, or README/roadmap docs still describe all update pipeline,
positional, and arrayFilters behavior as unsupported.
