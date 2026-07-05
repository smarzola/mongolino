# MongoDB Compatibility Uplifts Roadmap

This roadmap tracks the seven aggressive MongoDB compatibility uplifts requested
after the index compatibility work. Each uplift must follow the established
workflow: write or update a durable goal-loop prompt, dispatch a subagent,
perform parent adversarial review, dispatch a fix subagent when issues are
found, verify locally, and push accepted work.

## Baseline

Current implementation has a practical CRUD, aggregation, validation, and index
subset, but several major MongoDB application surfaces remain unsupported or
intentionally conservative:

- Query predicates now support a practical `$regex`, `$elemMatch`, `$type`,
  `$size`, `$all`, and supported collation equality subset; JavaScript,
  geospatial/text, expression, and non-simple collation range predicates remain
  unsupported.
- Index planning now supports conservative compound prefix scans, safe scalar
  range scans, narrow sort pushdown, `hint`, and partial `explain`
  diagnostics. Broader index classes and unsafe shapes remain unsupported.
- TTL index metadata, deterministic expiration, narrow TTL `collMod` updates,
  and supported collation-aware index metadata are available for documented safe
  shapes.
- Collation support is limited to `{ locale: "simple" }` and English
  case-insensitive strength-2 equality/sort behavior on supported command paths.
  ICU collation, numeric ordering, diacritic folding, locale-specific ordering,
  non-simple range predicates, and unsafe collation/index combinations remain
  unsupported.
- Aggregation now has a bounded expression language, computed shaping stages,
  root replacement, computed group operands, and simple same-database equality
  `$lookup`; advanced stages and full expression parity remain unsupported.
- Update pipelines, positional `$`, `$[]`, and filtered `$[identifier]` with
  `arrayFilters` now support a conservative single-array scalar-modifier subset;
  nested multi-array traversal, array modifiers through positional paths, and
  unsupported pipeline stages remain explicit errors.
- Driver workflow behavior now validates session ids, accepts safe single-node
  readConcern/writeConcern no-ops, rejects transaction fields before mutation,
  and provides bounded per-connection retryable write replay for supported
  one-shot writes.

## Compatibility Scorecard

The percentages below are repo-local practical compatibility scores, not a
claim of full MongoDB parity.

| Area | Weight | Current | Target After 7 Uplifts |
| --- | ---: | ---: | ---: |
| Query predicate compatibility | 20% | 18% | 18% |
| Index planning and diagnostics | 15% | 15% | 15% |
| Index lifecycle/TTL/collation behavior | 15% | 13% | 13% |
| Aggregation compatibility | 20% | 15% | 15% |
| Update language compatibility | 15% | 13% | 13% |
| Driver workflow semantics | 10% | 7% | 7% |
| Explicit unsupported behavior and tests | 5% | 5% | 5% |
| Total | 100% | 86% | 86% |

Completion target for this seven-uplift goal: reach at least **80%** on this
repo-local scorecard while preserving explicit errors for unsupported features.

## Uplifts

1. Query Language v2
   - Complete in uplift 1. Added `$regex`, `$elemMatch`, `$type`, `$size`, and
     `$all` for the supported BSON/query subset.
   - Hardened malformed predicate errors and practical array traversal cases.
   - Prompt: `docs/query-language-v2-goal-loop.md`.

2. Index Planner v2
   - Complete in uplift 2. Added compound prefix scans, safe scalar range
     scans, narrow sort pushdown, supported `hint`, and partial `explain`
     diagnostics.
   - Prompt: `docs/index-planner-v2-goal-loop.md`.

3. TTL Index Compatibility
   - Complete in uplift 3. Added `expireAfterSeconds` index metadata,
     validation, listing, deterministic namespace-scoped expiration/sweeper
     behavior, and narrow TTL duration updates through `collMod`.
   - Compound TTL, `_id` TTL, sparse/partial TTL combinations, background TTL
     monitor timing, TTL conversion for non-TTL indexes, and collation-aware
     indexes remain unsupported.
   - Prompt to write after Index Planner v2: `docs/ttl-index-goal-loop.md`.

4. Collation Support
   - Complete in uplift 4. Added `{ locale: "simple" }` and English
     case-insensitive strength-2 collation for supported equality, sort,
     distinct, read/write target selection, aggregate `$match`/`$sort`/`$count`,
     and safe matching collation-aware indexes.
   - Unsupported ICU options, unsupported locales/strengths, non-simple string
     ranges, collation with TTL/partial indexes, and unsafe hints return
     explicit errors or fall back without using binary indexes for non-simple
     semantics.
   - Prompt: `docs/collation-compatibility-goal-loop.md`.

5. Aggregation v2
   - Complete in uplift 5. Added a bounded aggregation expression evaluator,
     computed `$project`, `$addFields`/`$set`, `$unset`, `$replaceRoot`,
     `$replaceWith`, computed group operands, and simple same-database equality
     `$lookup`.
   - Unsupported `$lookup` pipeline/`let`, cross-database lookup, broad ICU
     collation, full expression parity, `$facet`, `$bucket`, `$out`, `$merge`,
     `$geoNear`, window stages, command `let`, allowDiskUse, hint, explain,
     read concern, and write concern remain explicit command errors.
   - Prompt: `docs/aggregation-v2-goal-loop.md`.

6. Update Pipeline And Array Filters
   - Complete in uplift 6. Added update pipelines with `$set`/`$addFields`,
     `$unset`, `$project`, `$replaceRoot`, and `$replaceWith`; conservative
     positional `$`, `$[]`, and `$[identifier]` with `arrayFilters`; staged
     no-mutation-on-error preflight for matched updates; and invariant coverage
     across validation, unique indexes, maintained index entries, TTL preflight,
     and findAndModify images.
   - Nested multi-array traversal, array modifiers through positional paths,
     command `let`, unsupported pipeline stages, full aggregation expression
     parity, write concern, retryable writes, transactions, and explain behavior
     remain unsupported.
   - Prompt: `docs/update-pipeline-arrayfilters-goal-loop.md`.

7. Driver Workflow Semantics
   - Complete in uplift 7. Added shared driver workflow preflight for `lsid`,
     `readConcern`, `writeConcern`, transaction fields, and `txnNumber`; a
     validating `endSessions` stub; safe local no-op read/write concern subsets;
     explicit transaction rejection before mutation; and bounded per-connection
     retryable write replay for exact duplicate `insert`, `update`, `delete`,
     and `findAndModify` attempts.
   - Causal consistency, snapshot reads, unacknowledged writes, distributed
     durability, durable retry history across reconnects/restarts, and
     multi-operation transactions remain unsupported.
   - Prompt: `docs/driver-workflow-semantics-goal-loop.md`.

## Goal Completion Requirements

The seven-uplift goal is complete only when:

1. All seven goal-loop prompts exist and have completed milestone checklists.
2. Every uplift was implemented by a subagent.
3. Every uplift received a parent adversarial review.
4. Every review either found no blocking issues with evidence or produced a fix
   prompt handled by a subagent.
5. Full final verification passes:

```bash
cargo fmt -- --check
cargo test
cargo build
cargo run --bin mongolino-bench -- --profile ci --check-budget
UV_CACHE_DIR=/private/tmp/mongolino-uv-cache uv lock --check
UV_CACHE_DIR=/private/tmp/mongolino-uv-cache uv sync --locked --dev
UV_CACHE_DIR=/private/tmp/mongolino-uv-cache uv run --locked pytest tests/e2e
```

6. README and relevant docs accurately describe newly supported and still
   unsupported compatibility surfaces.
7. The scorecard above is updated with final evidence and reaches at least 80%.
8. The final branch is pushed to `origin/master`.
