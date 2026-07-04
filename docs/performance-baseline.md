# Performance Baseline

Recorded on 2026-07-04 for commit `ab487d3`.

Machine note:

- macOS 26.5.1 build 25F80
- Darwin 25.5.0 arm64
- Benchmarks run through `cargo run` in the default dev profile
- Benchmark databases were temporary SQLite files created by the harness

The benchmark command exercises real `mongolino` command handlers, SQLite
schema initialization, BSON storage, index-entry maintenance, query matching,
update, and aggregation paths. It does not bind localhost and does not require
PyMongo, Docker, or an external MongoDB service.

## Commands

```sh
cargo run --bin mongolino-bench -- --profile smoke --json /tmp/mongolino-bench-smoke.json
cargo run --bin mongolino-bench -- --profile local --json /tmp/mongolino-bench-local.json
cargo run --bin mongolino-bench -- --profile ci --check-budget
```

## Smoke Results

Profile: `smoke`; seeded query dataset: 400 documents; git commit: `ab487d3`.

| Benchmark | Iterations | Elapsed ms | Ops/sec | Latency ms |
| --- | ---: | ---: | ---: | ---: |
| insert_batch_throughput | 25 | 40.07 | 31197.06 | 1.603 |
| find_id_equality | 25 | 0.63 | 39543.95 | 0.025 |
| find_collection_scan | 25 | 108.46 | 230.49 | 4.339 |
| find_indexed_scalar_equality | 25 | 6.98 | 3583.14 | 0.279 |
| count_empty_filter | 25 | 101.99 | 245.13 | 4.079 |
| count_simple_equality | 25 | 101.76 | 245.68 | 4.070 |
| update_index_refresh | 25 | 108.84 | 229.69 | 4.354 |
| aggregation_match_count | 25 | 101.80 | 245.57 | 4.072 |
| aggregation_unwind_group | 25 | 192.06 | 130.17 | 7.682 |

## Local Results

Profile: `local`; seeded query dataset: 3000 documents; git commit: `ab487d3`.

| Benchmark | Iterations | Elapsed ms | Ops/sec | Latency ms |
| --- | ---: | ---: | ---: | ---: |
| insert_batch_throughput | 100 | 274.16 | 36475.33 | 2.742 |
| find_id_equality | 100 | 2.27 | 44122.51 | 0.023 |
| find_collection_scan | 100 | 3091.44 | 32.35 | 30.914 |
| find_indexed_scalar_equality | 100 | 209.79 | 476.67 | 2.098 |
| count_empty_filter | 100 | 2929.76 | 34.13 | 29.298 |
| count_simple_equality | 100 | 3010.88 | 33.21 | 30.109 |
| update_index_refresh | 100 | 3070.73 | 32.57 | 30.707 |
| aggregation_match_count | 100 | 3003.17 | 33.30 | 30.032 |
| aggregation_unwind_group | 100 | 5733.27 | 17.44 | 57.333 |

## Count Pushdown Results

Recorded on 2026-07-04 for commit `4402545` after SQLite count pushdown.

Smoke profile: seeded query dataset 400 documents.

| Benchmark | Iterations | Elapsed ms | Ops/sec | Latency ms |
| --- | ---: | ---: | ---: | ---: |
| insert_batch_throughput | 25 | 39.03 | 32022.65 | 1.561 |
| find_id_equality | 25 | 0.70 | 35739.81 | 0.028 |
| find_collection_scan | 25 | 131.80 | 189.68 | 5.272 |
| find_indexed_scalar_equality | 25 | 7.04 | 3549.18 | 0.282 |
| count_empty_filter | 25 | 0.67 | 37059.88 | 0.027 |
| count_simple_equality | 25 | 0.90 | 27700.83 | 0.036 |
| update_index_refresh | 25 | 127.36 | 196.30 | 5.094 |
| aggregation_match_count | 25 | 0.95 | 26204.30 | 0.038 |
| aggregation_unwind_group | 25 | 195.90 | 127.62 | 7.836 |

Local profile: seeded query dataset 3000 documents.

| Benchmark | Before ms/op | After ms/op | Change |
| --- | ---: | ---: | ---: |
| count_empty_filter | 29.298 | 0.116 | 252.6x faster |
| count_simple_equality | 30.109 | 0.066 | 456.2x faster |
| aggregation_match_count | 30.032 | 0.071 | 423.0x faster |

Full local profile after count pushdown:

| Benchmark | Iterations | Elapsed ms | Ops/sec | Latency ms |
| --- | ---: | ---: | ---: | ---: |
| insert_batch_throughput | 100 | 260.60 | 38372.56 | 2.606 |
| find_id_equality | 100 | 2.25 | 44524.43 | 0.022 |
| find_collection_scan | 100 | 3091.26 | 32.35 | 30.913 |
| find_indexed_scalar_equality | 100 | 211.13 | 473.63 | 2.111 |
| count_empty_filter | 100 | 11.61 | 8609.68 | 0.116 |
| count_simple_equality | 100 | 6.65 | 15046.45 | 0.066 |
| update_index_refresh | 100 | 3078.52 | 32.48 | 30.785 |
| aggregation_match_count | 100 | 7.10 | 14084.18 | 0.071 |
| aggregation_unwind_group | 100 | 5761.48 | 17.36 | 57.615 |

## Interpretation

Current behavior has these SQLite-backed fast paths:

- `_id` equality `find` uses the SQLite primary key `(namespace, id_key)` and
  avoids decoding the namespace.
- Simple scalar equality `find` can use maintained `index_entries`, then still
  decodes matching BSON documents for final matcher compatibility.
- `count` uses SQLite for empty filters, exact `_id` equality, and exact
  non-numeric indexed scalar equality with maintained single-field index
  entries.
- Aggregation pipelines exactly shaped as `$match` followed by `$count` reuse
  the same safe count planner and avoid BSON namespace decode when the filter
  is pushdown-safe.

The remaining slow local results cluster around full namespace decode:

- collection-scan `find` decodes every document before filtering;
- unsupported count filters fall back to Rust matcher semantics, including
  arrays, logical operators, unsupported operators, multi-predicate filters,
  unindexed fields, null equality, document equality, and numeric indexed
  equality where matcher semantics compare `Int32`, `Int64`, and `Double`
  cross-type;
- general aggregation still starts by loading the full namespace into memory,
  then applies `$match`, `$count`, `$unwind`, and `$group` in Rust;
- update target selection still pays scan-like cost when the selected filter is
  not narrowed enough before applying modifiers and refreshing index entries.

Expect variance between local machines and GitHub-hosted runners. The CI budget
therefore uses intentionally coarse latency and throughput thresholds. Use JSON
outputs from the same profile on the same machine for before/after comparisons.

## SQLite Pushdown Roadmap

The target for the next two uplifts is to make SQLite the query engine whenever
that is behaviorally equivalent to the supported MongoDB subset, while keeping
the Rust BSON matcher as a compatibility fallback.

1. Push down `count` for empty filters and simple indexed equality filters.

   Baseline: `count_empty_filter` is 29.298 ms/op and
   `count_simple_equality` is 30.109 ms/op on the local profile. Both are close
   to collection-scan cost because they decode full namespaces.

   Completed: empty-filter count uses `SELECT COUNT(*) FROM documents WHERE
   namespace = ?`; `_id` equality count uses the `(namespace, id_key)` primary
   key; exact scalar equality count uses `index_entries` when a matching
   maintained single-field index exists. Skip and positive limit are applied to
   the SQL count result with the same count-command semantics.

   Fallbacks: unsupported operators, arrays, logical operators, multi-predicate
   filters, unindexed fields, null equality, document equality, and any other
   filter shape outside the conservative planner still use the Rust matcher.

   Measurement: local `count_empty_filter` improved from 29.298 to 0.116 ms/op;
   local `count_simple_equality` improved from 30.109 to 0.066 ms/op.

2. Broaden SQLite candidate narrowing for simple `find`.

   Baseline: `_id` equality is 0.023 ms/op, indexed scalar equality is
   2.098 ms/op, and collection scan is 30.914 ms/op on the local profile.

   Proposed implementation: keep `_id` equality as the model fast path, then
   use index entries more aggressively for supported simple equality filters,
   including selected dotted scalar paths if index-entry maintenance can prove
   equivalent. Continue to decode candidate BSON documents and run the Rust
   matcher before returning results.

   Correctness risks: array traversal and multikey behavior are intentionally
   limited today. Candidate narrowing must never drop a document that the Rust
   matcher would accept. Projection and sort should remain Rust-side until their
   semantics are covered.

   Measurement: compare `find_indexed_scalar_equality`,
   `find_collection_scan`, and targeted planner tests that verify candidate
   freshness after insert, update, delete, and drop.

3. Push down aggregation `$match` plus `$count` when the filter is safe.

   Baseline: `aggregation_match_count` is 30.032 ms/op on the local profile,
   matching the count and collection-scan cluster.

   Completed: pipelines exactly shaped as safe `$match` followed by `$count`
   reuse the same SQLite count planner as command `count`, and return the
   documented cursor response shape, including empty first batches for zero
   matched documents.

   Fallbacks: unsupported filters, malformed stages, and non-exact pipeline
   shapes continue through the existing Rust aggregation executor.

   Measurement: local `aggregation_match_count` improved from 30.032 to
   0.071 ms/op.

4. Explore SQLite grouping for bounded scalar `$unwind`/`$group` workloads.

   Baseline: `aggregation_unwind_group` is 57.333 ms/op on the local profile,
   the slowest benchmark because it decodes documents, expands arrays, and
   groups in Rust.

   Proposed implementation: do not start with general BSON array SQL. First
   evaluate whether maintained side tables for selected array scalar fields are
   worth the complexity, or whether this belongs after count/find pushdown.

   Correctness risks: `$unwind` preserve-null behavior, include-array-index,
   whole-value equality, and accumulator ordering are subtle. This is lower
   priority until simpler count/find pushdowns prove the measurement loop.

   Measurement: compare `aggregation_unwind_group` and the existing aggregation
   tests for `$unwind`, `$group`, `$push`, and `$addToSet`.
