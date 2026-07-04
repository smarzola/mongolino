# Goal: Index Planner v2 Compatibility Uplift

You are working in `/Users/smarzola/projects/mongolino`.

Your objective is to deliver uplift 2 of 7 in the aggressive MongoDB
compatibility sequence: make the existing SQLite-backed index catalog more
useful and observable by adding conservative planner support for prefix/range
query shapes, sort-aware reads where semantics are provably safe, `hint`, and
`explain` diagnostics.

This uplift starts after Query Language v2 moved the repo-local compatibility
scorecard in `docs/mongodb-compatibility-uplifts-roadmap.md` from **49% to
55%**. This uplift must move the scorecard from **55% to at least 62%**,
primarily through Index planning and diagnostics.

## Repository Rules

Follow `AGENTS.md`.

Important reminders:

- Keep the server implementation in Rust and the durable storage layer in
  SQLite.
- Treat MongoDB wire compatibility as observable behavior: validate changes
  with real client handshakes when possible.
- Run `cargo fmt` and `cargo test` before handing off code changes.
- Keep unsupported MongoDB commands explicit by returning command errors
  instead of silently accepting behavior.
- Do not require Docker or external MongoDB services.
- Do not optimize by weakening matcher semantics.
- Do not revert unrelated user or agent changes.
- You are not alone in the codebase. Other agents may have touched nearby
  files; work with current state and do not revert unrelated edits.
- At the end of each milestone, run verification, mark the milestone done in
  this file, commit the completed milestone, and report the commit hash before
  continuing.

## Target State

Add practical planner v2 behavior while keeping matcher validation as the final
source of truth:

- `find`, `count`, update/delete target selection, and findAndModify can narrow
  candidates with safe equality-prefix plans over existing maintained
  `index_entries`.
- A single trailing range predicate can use a range-capable maintained index
  representation for safe scalar values where BSON ordering is deterministic.
- Sort-aware reads can avoid an in-memory sort only when the result order is
  provably equivalent to MongoDB-style index order for the supported scalar
  subset.
- `hint` is accepted on `find`, `count`, `delete`, `update`, and
  findAndModify for supported index names or key-pattern documents.
- Unsupported, missing, ambiguous, or semantically unsafe hints return explicit
  command or write errors without mutating documents.
- `explain` is accepted for `find` and `count` and returns a small
  MongoDB-shaped diagnostic document showing whether the planner used collection
  scan, `_id`, exact index equality, prefix/range, sort, or hint-directed
  planning.
- Existing exact equality, sparse, partial, scalar multikey, and compound full
  equality planner behavior does not regress.

## Compatibility Target

Move `docs/mongodb-compatibility-uplifts-roadmap.md` from **55% to at least
62%**:

- Index planning and diagnostics: `8% -> at least 13%`.
- Explicit unsupported behavior remains `5%`.
- Other scorecard areas should not be inflated by this uplift.

Do not claim support for text indexes, geospatial indexes, hashed indexes,
wildcard indexes, hidden indexes, TTL, collation-aware indexes, full compound
multikey semantics, or full MongoDB explain parity in this uplift.

## Current State

Relevant current implementation:

- `IndexSpec` stores name, key document, unique/sparse/partial metadata, and is
  persisted/listed/dropped.
- `index_entries(namespace, index_name, key_value, id_key)` stores maintained
  planner keys for safe exact equality:
  - single-field scalar values;
  - full-key compound scalar values;
  - supported scalar multikey entries for single-field indexes.
- `planner_key_for_filter` only recognizes exact equality shapes for one
  single-field key or a full compound key.
- `indexed_candidate_documents`, `plan_count`, and
  `plan_transaction_candidates` use exact equality planner keys and still rely
  on Rust matcher validation after narrowing.
- Sparse and partial indexes are used only when filter implication is safe.
- Existing count pushdown uses exact index-entry counts and falls back for
  unsupported filters.
- `find` currently rejects `hint` and `explain` as unsupported command keys.
- `count`, `update`, `delete`, and findAndModify reject `hint`; `aggregate`
  rejects `hint` and `explain`.
- Sort is parsed and applied in memory by `sort_documents`.
- README is stale in places and still says compound indexes are stored but not
  planned.

Known constraints:

- Candidate narrowing must never exclude a document that the Rust matcher would
  accept.
- Numeric equality is cross-type in the matcher; type-tagged index encodings can
  be unsafe for numeric exact/range plans unless all numeric cross-type behavior
  is explicitly handled. Prefer fallback for numeric ranges until fully proven.
- Array, document, collation, regex, `$elemMatch`, `$all`, `$size`, `$type`,
  `$not`, `$ne`, `$nin`, and logical shapes should fall back unless you provide
  complete proof and tests.
- Sort pushdown must be disabled when missing values, mixed BSON types,
  multikey ambiguity, sparse/partial membership, non-covered filters, or
  descending/compound-direction edge cases make order equivalence unclear.
- `explain` should be useful and stable, not byte-for-byte MongoDB parity.

## Definition Of Done

The goal is complete only when:

1. Planner data structures distinguish collection scan, `_id`, exact index
   equality, equality-prefix, range, and sort-aware plans.
2. Safe equality-prefix candidate narrowing works for compound indexes where
   the filter constrains a leading prefix of the index key.
3. Safe single trailing range narrowing works for supported scalar values on a
   single-field index and on a compound index after an equality prefix.
4. `find` uses new planner v2 narrowing and still validates every candidate with
   `matches_filter` before returning it.
5. `count` uses new planner v2 count pushdown only when the index predicate
   fully covers the filter; otherwise it falls back to existing matcher-based
   counting.
6. update/delete/findAndModify target selection can use planner v2 narrowing for
   safe filters and still validates candidates before mutation.
7. Hints are accepted by name and by key-pattern document for supported indexes
   on `find`, `count`, `update`, `delete`, and findAndModify.
8. Hints that refer to unknown indexes, incompatible filters, unsupported index
   types/options, unsafe sparse/partial membership, or unsupported command paths
   return explicit errors without mutating documents.
9. `find` and `count` explain responses expose stable diagnostics with winning
   plan, index name when used, scan strategy, filter summary, and fallback
   reason when applicable.
10. Sort-aware planning is implemented only for proven-safe scalar cases and has
    adversarial tests for unsupported cases; if a case is not proven safe, it
    must fall back to existing in-memory sort.
11. Existing exact equality, sparse, partial, scalar multikey, full-key compound
    equality, unique enforcement, and Query v2 semantics do not regress.
12. PyMongo e2e and spec corpus cases cover happy paths, non-happy paths, and
    adversarial paths.
13. Benchmarks include representative prefix/range/sort/hint cases and CI
    budget checks catch gross regressions.
14. README and `docs/mongodb-compatibility-uplifts-roadmap.md` accurately
    document newly supported and still unsupported planner behavior.
15. Verification commands pass locally.
16. Milestone checkboxes in this file are marked `[x]` as work completes.
17. Each completed milestone has a focused commit.

## Milestone Checklist

When a milestone is complete:

1. Run the milestone's verification commands.
2. Update this file by changing the milestone from `[ ]` to `[x]`.
3. Add a short status note under that milestone with the date, exact commands
   run, and commit hash if available.
4. Commit the code, tests, docs, and checklist/status update with a focused
   commit message.
5. Report the commit hash in the goal-loop status before starting the next
   milestone.

- [x] Milestone 0: Planner v2 architecture and diagnostics model
- [x] Milestone 1: Equality-prefix candidate narrowing
- [x] Milestone 2: Safe range scans and count pushdown
- [x] Milestone 3: Hint command semantics
- [x] Milestone 4: Explain diagnostics
- [x] Milestone 5: Sort-aware read planning
- [ ] Milestone 6: Benchmarks, docs, final e2e verification

## Milestone 0: Planner v2 Architecture And Diagnostics Model

Problem:

- Existing planner helpers return only exact equality candidate sets and do not
  expose why a plan was chosen or rejected.

Desired behavior:

- Introduce planner v2 types that can represent collection scans, `_id` exact
  lookup, exact index equality, prefix equality, range scans, and sort-aware
  plans.
- Keep existing exact equality behavior intact while making future milestones
  additive.
- Add a small diagnostic structure that can later be reused by `explain`.

Acceptance criteria:

- Existing planner tests continue to pass.
- New Rust unit tests cover planner classification for exact equality, partial
  prefix, range candidate, unsupported operator fallback, sparse/partial
  membership fallback, and explicit fallback reasons.
- No command behavior changes except clearer internal planning structure.
- Milestone status is marked done in this file and committed.

Likely files:

- `src/main.rs`
- `docs/index-planner-v2-goal-loop.md`

Verification:

```bash
cargo fmt -- --check
cargo test planner
cargo test index
```

Status 2026-07-04: Added planner v2 plan/diagnostic types plus conservative
exact, equality-prefix, range, unsupported-operator, and membership fallback
classification tests. Verification passed with `cargo fmt -- --check`,
`cargo test planner`, and `cargo test index`. Commit: reported in goal-loop
status after commit creation.

## Milestone 1: Equality-Prefix Candidate Narrowing

Problem:

- Compound indexes are currently useful only for full-key equality. Real MongoDB
  workloads commonly filter on a leading compound prefix, for example index
  `{accountId: 1, status: 1, createdAt: -1}` with filter `{accountId: "a"}`.

Desired behavior:

- Maintain or derive a prefix-searchable representation for safe compound
  equality prefixes.
- Use equality-prefix plans for `find`, update/delete target selection, and
  findAndModify when the filter constrains a leading prefix of a compound index.
- Continue matcher validation after candidate narrowing.
- Preserve existing exact full-key count pushdown behavior; count prefix
  pushdown may be implemented only if every returned entry is proven covered.

Acceptance criteria:

- Prefix plans use only leading compound key fields in index order.
- Prefix plans fall back for non-leading fields, gaps, extra uncovered predicates
  that cannot be safely validated after narrowing, unsafe sparse/partial
  membership, numeric values if unsafe, arrays, documents, and unsupported
  operators.
- Rust tests cover prefix classification and candidate freshness after insert,
  update, delete, upsert, and findAndModify.
- PyMongo e2e covers compound prefix `find`, `update_many`, `delete_many`, and
  findAndModify targeting.
- Milestone status is marked done in this file and committed.

Likely files:

- `src/main.rs`
- `tests/e2e/test_indexes.py`
- `tests/e2e/test_crud.py`
- `tests/e2e/test_find_and_modify.py`
- `docs/index-planner-v2-goal-loop.md`

Verification:

```bash
cargo fmt -- --check
cargo test planner
cargo test index
cargo test update
UV_CACHE_DIR=/private/tmp/mongolino-uv-cache uv run --locked pytest tests/e2e/test_indexes.py tests/e2e/test_crud.py tests/e2e/test_find_and_modify.py
```

Use unsandboxed execution for PyMongo e2e if the sandbox blocks localhost
binding.

Status 2026-07-04: Added maintained `compound-prefix` index-entry keys for
leading compound equality prefixes and used them for `find`, update/delete
target selection, and findAndModify, with matcher validation still applied.
Verification passed with `cargo fmt -- --check`, `cargo test planner`,
`cargo test index`, `cargo test update`, and unsandboxed
`UV_CACHE_DIR=/private/tmp/mongolino-uv-cache uv run --locked pytest
tests/e2e/test_indexes.py tests/e2e/test_crud.py
tests/e2e/test_find_and_modify.py` after the sandboxed e2e run failed to bind
localhost. Commit: reported in goal-loop status after commit creation.

## Milestone 2: Safe Range Scans And Count Pushdown

Problem:

- Range predicates such as `$gt`, `$gte`, `$lt`, and `$lte` currently scan
  collection candidates even when a suitable scalar index exists.

Desired behavior:

- Add a range-capable maintained representation or safe SQL predicate over
  existing index keys for supported scalar ordering.
- Support single-field scalar range plans.
- Support compound range plans when all earlier index fields are constrained by
  safe equality and the next indexed field has a single bounded range predicate.
- Support combined lower and upper bounds on the same field.
- Use range count pushdown only when the indexed range fully covers the filter
  and BSON ordering equivalence is proven for the supported scalar type subset.

Acceptance criteria:

- Range plans support string, bool, ObjectId, and date values where ordering is
  deterministic under the existing BSON order rules.
- Numeric, array, document, null, mixed-type, multikey, regex, `$in`, `$nin`,
  `$ne`, `$not`, `$elemMatch`, `$all`, `$size`, and `$type` filters fall back
  unless fully proven and tested.
- Candidate narrowing still validates every result with the matcher.
- Count pushdown falls back when filters include non-covered predicates.
- Rust tests cover inclusive/exclusive bounds, double-sided ranges, no-match
  ranges, sparse/partial fallback, and stale-entry avoidance.
- PyMongo e2e covers `find`, `count_documents`, update/delete target selection,
  and adversarial fallback shapes.
- Milestone status is marked done in this file and committed.

Likely files:

- `src/main.rs`
- `tests/e2e/test_indexes.py`
- `tests/e2e/test_crud.py`
- `tests/spec_corpus/index_planner_v2.json`
- `docs/index-planner-v2-goal-loop.md`

Verification:

```bash
cargo fmt -- --check
cargo test range
cargo test planner
cargo test count
UV_CACHE_DIR=/private/tmp/mongolino-uv-cache uv run --locked pytest tests/e2e/test_indexes.py tests/e2e/test_crud.py tests/e2e/test_spec_corpus.py
```

Status 2026-07-04: Added maintained `range` index-entry keys for safe string,
bool, ObjectId, and date scalar ordering, wired single-field and compound
equality-prefix-plus-range plans into `find`, covered `count`, update/delete
targeting, and findAndModify, and extended matcher range validation to the same
scalar subset. Verification passed with `cargo fmt -- --check`, `cargo test
range`, `cargo test planner`, `cargo test count`, `cargo build`, and
unsandboxed `UV_CACHE_DIR=/private/tmp/mongolino-uv-cache uv run --locked
pytest tests/e2e/test_indexes.py tests/e2e/test_crud.py
tests/e2e/test_spec_corpus.py` after the sandboxed e2e run failed to bind
localhost. Commit: reported in goal-loop status after commit creation.

## Milestone 3: Hint Command Semantics

Problem:

- Common drivers and ODMs send `hint` for query, count, update, delete, and
  findAndModify flows. The server currently rejects these command keys.

Desired behavior:

- Accept `hint` as an index name string or key-pattern document on:
  - `find`;
  - `count`;
  - `update`;
  - `delete`;
  - findAndModify.
- Apply hints to planner index selection when the hinted index can safely serve
  the filter.
- Return explicit errors for unknown hints, ambiguous key-pattern matches,
  unsupported hint types, incompatible hinted filters, and hints on unsupported
  command paths.

Acceptance criteria:

- Hint by name and by key document both work for exact equality, prefix, and
  supported range plans.
- Hinted unsupported filters error explicitly rather than silently falling back
  to another index.
- Unhinted unsupported filters still fall back instead of erroring.
- Write commands with bad hints must not mutate documents.
- PyMongo e2e covers happy paths and bad-hint no-mutation cases for update,
  delete, and findAndModify.
- Milestone status is marked done in this file and committed.

Likely files:

- `src/main.rs`
- `tests/e2e/test_indexes.py`
- `tests/e2e/test_crud.py`
- `tests/e2e/test_find_and_modify.py`
- `docs/index-planner-v2-goal-loop.md`

Verification:

```bash
cargo fmt -- --check
cargo test hint
cargo test planner
UV_CACHE_DIR=/private/tmp/mongolino-uv-cache uv run --locked pytest tests/e2e/test_indexes.py tests/e2e/test_crud.py tests/e2e/test_find_and_modify.py
```

Status 2026-07-04: Added explicit hint parsing and resolution by index name or
key-pattern document for `find`, `count`, update, delete, and findAndModify,
with hinted reads/writes constrained to safe exact, prefix, and range plans and
bad write hints rejected before mutation. Verification passed with `cargo fmt
-- --check`, `cargo test hint`, `cargo test planner`, `cargo build`, and
unsandboxed `UV_CACHE_DIR=/private/tmp/mongolino-uv-cache uv run --locked
pytest tests/e2e/test_indexes.py tests/e2e/test_crud.py
tests/e2e/test_find_and_modify.py` after the sandboxed e2e path had previously
failed to bind localhost. Commit: reported in goal-loop status after commit
creation.

## Milestone 4: Explain Diagnostics

Problem:

- Without `explain`, it is hard to understand whether compatibility/performance
  improvements are actually using an index or falling back.

Desired behavior:

- Accept `explain: true` on `find` and `count`.
- Return a stable diagnostic document with:
  - `ok: 1`;
  - command namespace;
  - parsed filter summary;
  - winning plan stage;
  - index name and key pattern when used;
  - whether a hint was provided;
  - whether matcher validation remains required;
  - fallback reason when collection scan is chosen.
- Keep the shape intentionally small and documented as partial compatibility.

Acceptance criteria:

- Explain never mutates data.
- Explain validates malformed filters, hints, sort specs, bounds, and unsupported
  command keys consistently with the real command path.
- Explain covers collection scan, `_id`, exact index equality, prefix, range,
  hinted plan, and fallback cases.
- Unsupported `explain` on aggregate/update/delete/findAndModify remains
  explicit unless fully implemented and tested.
- PyMongo e2e or command-level tests cover explain response shape.
- Milestone status is marked done in this file and committed.

Likely files:

- `src/main.rs`
- `tests/e2e/test_indexes.py`
- `tests/spec_corpus/index_planner_v2.json`
- `README.md`
- `docs/index-planner-v2-goal-loop.md`

Verification:

```bash
cargo fmt -- --check
cargo test explain
cargo test planner
UV_CACHE_DIR=/private/tmp/mongolino-uv-cache uv run --locked pytest tests/e2e/test_indexes.py tests/e2e/test_spec_corpus.py
```

Status 2026-07-04: Added partial `explain: true` support for `find` and
`count`, returning stable `queryPlanner` diagnostics for collection scan, `_id`,
exact index equality, equality-prefix, range, hinted plans, and fallback
reasons while keeping aggregate/update/delete/findAndModify explain paths
explicitly unsupported. Verification passed with `cargo fmt -- --check`,
`cargo test explain`, `cargo test planner`, `cargo build`, and unsandboxed
`UV_CACHE_DIR=/private/tmp/mongolino-uv-cache uv run --locked pytest
tests/e2e/test_indexes.py tests/e2e/test_spec_corpus.py` because the sandboxed
e2e path cannot bind localhost. Commit: reported in goal-loop status after
commit creation.

## Milestone 5: Sort-Aware Read Planning

Problem:

- Sort is always applied in memory even when a safe index order could satisfy a
  common sorted query shape.

Desired behavior:

- Use index order to avoid in-memory sort only for safe scalar cases:
  - sort exactly matches a single-field index direction or its reverse; or
  - sort matches the remaining suffix of a compound index after equality-prefix
    constraints.
- Preserve existing deterministic sorting behavior for unsupported or unsafe
  cases by falling back to in-memory sort.

Acceptance criteria:

- Sort-aware plans are disabled for sparse/partial indexes unless membership and
  coverage are proven.
- Sort-aware plans are disabled for multikey indexes, missing values, mixed
  BSON types, arrays/documents, non-scalar values, non-prefix compound sorts,
  collation-sensitive strings, and filters requiring post-sort broad scans.
- Result ordering matches existing `sort_documents` behavior for all supported
  sort pushdown cases.
- PyMongo e2e covers ascending, descending, compound equality-prefix sort,
  reverse direction, skip/limit interaction, and adversarial fallback cases.
- Milestone status is marked done in this file and committed.

Likely files:

- `src/main.rs`
- `tests/e2e/test_indexes.py`
- `tests/e2e/test_crud.py`
- `tests/spec_corpus/index_planner_v2.json`
- `docs/index-planner-v2-goal-loop.md`

Verification:

```bash
cargo fmt -- --check
cargo test sort
cargo test planner
UV_CACHE_DIR=/private/tmp/mongolino-uv-cache uv run --locked pytest tests/e2e/test_indexes.py tests/e2e/test_crud.py tests/e2e/test_spec_corpus.py
```

Status 2026-07-04: Added conservative sort-aware reads over maintained range
index entries for unique, fully covered bool/ObjectId/date scalar sort keys,
including single-field sorts and compound equality-prefix suffix sorts, with
missing values, duplicate sort keys, strings, sparse/partial indexes, multikey
omissions, and broad filters falling back to the existing in-memory sorter.
Verification passed with `cargo fmt -- --check`, `cargo test sort`, `cargo test
planner`, `cargo build`, and unsandboxed `UV_CACHE_DIR=/private/tmp/mongolino-uv-cache
uv run --locked pytest tests/e2e/test_indexes.py tests/e2e/test_crud.py
tests/e2e/test_spec_corpus.py` because the sandboxed e2e path cannot bind
localhost. Commit: reported in goal-loop status after commit creation.

## Milestone 6: Benchmarks, Docs, And Final Verification

Problem:

- Planner v2 is only valuable if it is observable in compatibility docs,
  performance evidence, and real-client coverage.

Desired behavior:

- Add benchmark cases for:
  - compound prefix `find`;
  - single-field indexed range `find`;
  - compound equality-prefix plus range `find`;
  - range count pushdown when covered;
  - hinted exact/prefix/range plan overhead;
  - sort-aware read with skip/limit.
- Update performance budgets to catch gross regressions without making CI
  flaky.
- Update README and `docs/mongodb-compatibility-uplifts-roadmap.md`.
- Run final verification.

Acceptance criteria:

- Benchmarks are stable in the CI profile.
- Docs describe supported `hint`, `explain`, prefix, range, and sort planner
  behavior plus explicit unsupported cases.
- Scorecard moves to at least 62%.
- Full verification passes.
- Milestone status is marked done in this file and committed.

Likely files:

- `src/bin/mongolino-bench.rs`
- `README.md`
- `docs/performance-baseline.md`
- `docs/mongodb-compatibility-uplifts-roadmap.md`
- `docs/index-planner-v2-goal-loop.md`
- `tests/e2e/test_indexes.py`
- `tests/spec_corpus/index_planner_v2.json`

Verification:

```bash
cargo fmt -- --check
cargo test
cargo build
cargo run --bin mongolino-bench -- --profile ci --check-budget
UV_CACHE_DIR=/private/tmp/mongolino-uv-cache uv lock --check
UV_CACHE_DIR=/private/tmp/mongolino-uv-cache uv sync --locked --dev
UV_CACHE_DIR=/private/tmp/mongolino-uv-cache uv run --locked pytest tests/e2e
```

Use unsandboxed execution for PyMongo e2e if the sandbox blocks localhost
binding.

## Final Response Requirements

When complete, report:

- commits made;
- files changed;
- scorecard movement;
- supported planner v2 shapes;
- supported `hint` and `explain` shapes;
- unsupported planner/hint/explain shapes that remain explicit errors or
  fallback paths;
- benchmark headline results;
- final verification commands and outcomes;
- known residual risks.
