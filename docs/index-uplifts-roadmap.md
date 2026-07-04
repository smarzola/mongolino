# Index Compatibility And Performance Uplift Roadmap

This roadmap defines the three large index uplifts for the current goal. It is
not a substitute for per-uplift goal-loop prompts; each uplift must still have
its own executable prompt, subagent implementation, adversarial review, fix
loop, verification, and push.

## Baseline

Current index support after the performance pushdown work:

- `createIndexes`, `listIndexes`, and `dropIndexes` are implemented.
- `_id_` is always listed and cannot be dropped.
- User index key specs accept ascending and descending numeric directions.
- `unique: true` is enforced for supported unique key shapes.
- Maintained `index_entries` support safe single-field scalar planner paths.
- Single-field scalar indexes can accelerate `find`, `count`, aggregation
  `$match` + `$count`, update/delete target selection, findAndModify target
  selection, and safe non-numeric unique checks.
- Unsupported index types and options return explicit command errors.

Major gaps:

- Compound indexes are stored as metadata, but maintained planner entries are
  single-field only.
- Sparse and partial index options are rejected.
- Multikey indexes do not have MongoDB-compatible array indexing semantics.
- Query hints, index choice diagnostics, and broader sort/range pushdown are
  absent.
- Text, geospatial, hashed, wildcard, collation-aware, hidden, and TTL indexes
  are unsupported.

## Compatibility Scorecard

The percentages below are a repo-local scorecard for common application index
behavior, not a claim of full MongoDB parity.

| Area | Weight | Baseline | Target After 3 Uplifts |
| --- | ---: | ---: | ---: |
| Index command surface and catalog behavior | 15% | 10% | 12% |
| Basic btree key specs and metadata | 15% | 8% | 13% |
| Unique enforcement semantics | 20% | 12% | 16% |
| Planner use for reads and writes | 25% | 8% | 20% |
| Sparse/partial/multikey practical semantics | 20% | 1% | 12% |
| Explicit unsupported behavior and tests | 5% | 4% | 5% |
| Total | 100% | 43% | 78% |

Completion target for this goal: reach at least **75%** on this scorecard while
preserving explicit errors for unsupported MongoDB index families.

## Performance Targets

Use the local benchmark profile as the headline measurement unless a prompt
states otherwise.

Baseline local numbers from `docs/performance-baseline.md`:

- `find_collection_scan`: about `31.313 ms/op`.
- `find_indexed_scalar_equality`: about `2.151 ms/op`.
- `count_simple_equality`: about `0.066 ms/op`.
- `update_index_refresh`: about `1.147 ms/op`.

Goal-level targets after all three index uplifts:

- Compound equality find on a selective compound index: **at least 10x faster**
  than collection scan and below **3 ms/op** on the local profile.
- Compound equality count: below **0.25 ms/op** on the local profile when fully
  covered by maintained index entries.
- Compound write target selection: below **2 ms/op** on the local profile for a
  selective compound indexed update benchmark.
- Sparse/partial unique conflict checks: avoid full namespace scans for safe
  non-numeric scalar keys and stay below **2 ms/op** in a local targeted
  benchmark.
- Multikey equality find for scalar array elements: at least **5x faster** than
  collection scan when a maintained multikey index is safe to use.

## Uplift 1: Compound Index Planner And Benchmarks

Target compatibility movement: **43% -> 55%**.

Target behavior:

- Maintain `index_entries` for compound scalar indexes.
- Use compound entries for exact equality predicates covering the full compound
  key.
- Use compound entries for safe read count and write target narrowing.
- Keep Rust matcher validation after narrowing.
- Add benchmark cases for compound equality find, count, and update targeting.

Prompt: `docs/index-compound-planner-goal-loop.md`.

## Uplift 2: Sparse And Partial Index Semantics

Target compatibility movement: **55% -> 67%**.

Target behavior:

- Parse and list `sparse` and `partialFilterExpression` options for supported
  predicate shapes.
- Enforce unique sparse and unique partial indexes for insert/update/upsert and
  findAndModify.
- Maintain index entries only for documents included by sparse/partial rules.
- Use sparse/partial entries for planner pushdown only when query filters imply
  index membership.
- Keep unsupported partial filter operators explicit.

Prompt to write after uplift 1 review: `docs/index-sparse-partial-goal-loop.md`.

## Uplift 3: Multikey Scalar Index Semantics

Target compatibility movement: **67% -> 78%**.

Target behavior:

- Maintain one index entry per scalar array element for supported single-field
  multikey indexes.
- Use multikey entries for scalar equality find/count and safe write target
  narrowing while preserving matcher validation.
- Define and enforce conservative unique multikey behavior with explicit errors
  where unsupported.
- Add PyMongo e2e coverage for array field queries and mutation freshness.
- Add benchmark cases for array-element indexed find/count.

Prompt to write after uplift 2 review: `docs/index-multikey-scalar-goal-loop.md`.

## Goal Completion Requirements

This goal is complete only when:

1. All three uplift prompts exist and have completed milestone checklists.
2. Each uplift was implemented by a subagent.
3. Each uplift received an adversarial parent review.
4. Each review either found no blocking issues with evidence or produced a fix
   prompt handled by a subagent.
5. `cargo fmt -- --check`, `cargo test`, `cargo build`,
   `cargo run --bin mongolino-bench -- --profile ci --check-budget`,
   `UV_CACHE_DIR=/private/tmp/mongolino-uv-cache uv lock --check`,
   `UV_CACHE_DIR=/private/tmp/mongolino-uv-cache uv sync --locked --dev`, and
   `UV_CACHE_DIR=/private/tmp/mongolino-uv-cache uv run --locked pytest
   tests/e2e` pass after the third uplift and fix loop.
6. `docs/performance-baseline.md` records before/after measurements for every
   benchmark added by the index uplifts.
7. The scorecard above is updated with final evidence and reaches at least 75%.
8. The final branch is pushed to `origin/master`.
