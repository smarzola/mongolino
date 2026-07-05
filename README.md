# mongolino

`mongolino` is a small MongoDB wire-protocol server backed by one SQLite file.
It is not a MongoDB clone, but it is now a practical single-node compatibility
server for local applications, integration tests, and experiments that benefit
from MongoDB driver behavior without running a MongoDB daemon.

## Server Model

`mongolino` is intentionally scoped to one process, one writable primary, and
one durable SQLite database file. Within that model, several command families
are functionally complete: they validate driver workflow metadata, preserve
write invariants, maintain indexes, and return explicit errors for unsupported
MongoDB behavior.

The unsupported areas are mostly outside that model rather than accidental
omissions: no replication, sharding, authentication, change streams,
transactions, snapshot reads, unacknowledged writes, distributed write concern,
or cross-process retry history. Those features are rejected explicitly instead
of being accepted with false semantics.

## Run

```sh
cargo run -- --addr 127.0.0.1:27017 --db mongolino.sqlite3
```

Then connect with a MongoDB client:

```sh
mongosh mongodb://127.0.0.1:27017
```

Example commands:

```javascript
use app
db.users.insertOne({ _id: "u1", name: "Ada" })
db.users.insertOne({ _id: "u2", name: "Grace", age: 39, profile: { city: "London" } })
db.users.find({ age: { $gte: 38 } }, { name: 1, _id: 0 }).sort({ age: -1 }).toArray()
db.users.updateOne({ _id: "u1" }, { $set: { name: "Ada Lovelace" }, $inc: { score: 1 }, $push: { tags: "logic" } })
db.users.findOneAndUpdate({ _id: "u1" }, { $inc: { score: 1 } }, { returnDocument: "after" })
db.users.aggregate([{ $match: { age: { $gte: 38 } } }, { $sort: { age: -1 } }, { $project: { name: 1 } }]).toArray()
db.users.deleteOne({ _id: "u2" })
```

## MongoDB Interface Surface

Compatibility flags:

- `Complete in model`: functionally complete for `mongolino`'s documented
  single-node SQLite server model.
- `Broad subset`: substantial practical MongoDB behavior, with specific
  advanced features intentionally unsupported.
- `Bounded subset`: useful but deliberately constrained compatibility.
- `Metadata/no-op`: validates and stores metadata or accepts safe no-op
  semantics without claiming full MongoDB server behavior.
- `Unsupported`: returns a command or protocol error.

| Surface | Compatibility | Current behavior | Gaps |
| --- | --- | --- | --- |
| TCP listener | Complete in model | Listens on `--addr`, defaulting to `127.0.0.1:27017`. | No TLS, Unix sockets, IPv6-specific handling, or connection limits. |
| MongoDB message header | Complete in model | Reads and writes standard 16-byte wire message headers with request/response IDs. | Does not validate all cross-field semantics. |
| `OP_MSG` | Broad subset | Parses body section kind `0`, exposes document sequence section kind `1` as command arrays, and replies with `OP_MSG`. | Ignores flags; no compression. |
| `OP_QUERY` | Bounded subset | Parses legacy query payloads and replies with `OP_REPLY`. Useful for legacy handshake-style clients. | No legacy cursor behavior beyond one-document command replies. |
| Other opcodes | Unsupported | Returns an error document for unknown opcodes. | No `OP_COMPRESSED`, `OP_GET_MORE`, `OP_INSERT`, `OP_UPDATE`, `OP_DELETE`, or `OP_KILL_CURSORS`. |
| `hello` | Complete in model | Returns a standalone writable primary-style handshake with wire version and size limits. | No replica set, topology, compression, speculative auth, or server parameters. |
| `isMaster` / `ismaster` | Complete in model | Same response shape as `hello`, with `ismaster` and `helloOk`. | Same handshake gaps as `hello`. |
| `ping` | Complete in model | Returns `{ ok: 1.0 }`. | None for basic ping behavior. |
| `buildInfo` | Metadata/no-op | Returns version, allocator/storage hints, BSON size, bitness, and `ok`. | Not byte-for-byte compatible with MongoDB server build metadata. |
| `listDatabases` | Bounded subset | Lists database names derived from the collection catalog and persisted document namespaces, including databases with empty collections. | Size accounting is placeholder `0`. |
| `endSessions` | Metadata/no-op | Validates an array of session documents with BSON binary UUID `id` fields and returns `{ ok: 1.0 }`. | Sessions are not stored, expired, causally ordered, or shared across connections. |
| `create` | Bounded subset | Creates a durable empty collection catalog entry and stores supported `$jsonSchema` validators with `validationLevel: "strict"` and `validationAction: "error"`. | Collection options such as capped collections, clustered indexes, timeseries, collation, and unsupported validator features are rejected. |
| `listCollections` | Bounded subset | Lists durable catalog entries and legacy document-only namespaces, with `nameOnly`, simple `name` equality filters, and stored validator options for non-`nameOnly` calls. | Size/details are minimal; complex filters and unsupported collection metadata options are rejected. |
| `collMod` | Bounded subset | Updates or clears durable collection validator metadata for existing collections. Clearing uses `validator: {}`. Supports TTL duration updates with `index: { name, expireAfterSeconds }` for existing TTL indexes. | Only validator metadata, `validationLevel: "strict"`, `validationAction: "error"`, and TTL duration updates by index name are supported. No key-pattern index updates, non-TTL index conversion, view, timeseries, collation, or validation conversion behavior. |
| `drop` | Complete in model | Drops a collection by removing its documents, catalog entry, user index metadata, and maintained index entries. | No view, change-stream, or storage-stat side effects. |
| `dropDatabase` | Complete in model | Drops catalog entries and documents for the selected database only. | No users, roles, profiling collections, or storage statistics. |
| `count` | Broad subset | Counts documents matching the supported filter subset, with `skip` and `limit`; accepts safe no-op `readConcern` forms `{}`, `local`, and `available`; supports explicit command `hint` for safe exact/prefix/range index plans, `explain: true` diagnostics, and the supported collation subset. Runs deterministic namespace-scoped TTL sweeps before non-explain counts. PyMongo `estimated_document_count()` uses this path. | No `majority`, `linearizable`, `snapshot`, maxTimeMS, or storage-stat semantics. PyMongo `count_documents()` uses aggregate and does not get command-level count hints. Non-simple collation range filters are rejected. |
| `aggregate` | Broad subset | Runs a sequential read pipeline subset: `$match`, `$unwind`, `$group`, `$sort`, `$skip`, `$limit`, computed and inclusion/exclusion `$project`, `$addFields`/`$set`, `$unset`, `$replaceRoot`, `$replaceWith`, simple same-database equality `$lookup`, and `$count`; accepts safe no-op `readConcern` forms `{}`, `local`, and `available`; supports a bounded expression subset (`$literal`, `$ifNull`, string conversion/case operators, comparisons, boolean operators, `$cond`, and numeric arithmetic); supports bounded group keys and `$sum`, `$avg`, `$min`, `$max`, `$first`, `$last`, `$push`, and `$addToSet` over supported expressions; preserves the PyMongo `count_documents()` `$group` shape; supports the documented collation subset for `$match`, `$sort`, `$count`, expression comparisons, and simple `$lookup`; returns cursor documents and supports per-client `cursor.batchSize` with `getMore`; runs deterministic namespace-scoped TTL sweeps before reading. | No `$lookup` pipeline/`let` form, cross-database lookup, `$facet`, `$bucket`, `$sortByCount`, `$out`, `$merge`, `$geoNear`, `$redact`, window stages, server-side JavaScript, allowDiskUse, hint, unsafe read concern, write concern, explain, maxTimeMS, aggregate command `let`, broad ICU collation, or full expression/operator parity. Unsupported shapes return command errors. Non-simple collation range filters are rejected. |
| `distinct` | Broad subset | Returns unique scalar, dotted-path, and array-expanded values for documents matching the supported filter subset, ordered deterministically by BSON sort order or supported collation order; accepts safe no-op `readConcern` forms `{}`, `local`, and `available`; de-duplicates strings under the supported collation subset; runs deterministic namespace-scoped TTL sweeps before reading. | No hint, unsafe read concern, maxTimeMS, or complex array semantics beyond the documented matcher behavior. Non-simple collation range filters are rejected. |
| `insert` | Complete in model | Accepts `documents`, assigns `_id` when missing, preserves existing documents on duplicate `_id`, reports duplicate key and validation `writeErrors`, supports ordered/unordered batches, honors `bypassDocumentValidation: true`, validates `lsid`, accepts safe acknowledged `writeConcern`, and supports bounded per-connection retryable replay with `lsid + txnNumber`. | No unacknowledged writes, durable retry history across reconnects/restarts, causal consistency, transactions, or distributed write concern semantics. |
| `find` | Broad subset | Returns `firstBatch` and creates a per-client server-side cursor when more shaped results remain. Supports exact matches, dotted paths, limited array traversal, `$eq`, `$ne`, `$gt`, `$gte`, `$lt`, `$lte`, `$in`, `$nin`, `$exists`, `$regex`, `$type`, `$size`, `$all`, `$elemMatch`, `$and`, `$or`, `$nor`, `$not`, projection, sort, skip, limit, capped batch size, safe no-op `readConcern` forms `{}`, `local`, and `available`, supported collation, `hint`, `explain: true`, conservative exact/prefix/range index narrowing, sort-aware reads for unique fully covered bool/ObjectId/date scalar index keys, and deterministic namespace-scoped TTL sweeps before non-explain reads. | No `$where`, geospatial/text search, unsafe read concern, tailable cursors, text/geospatial/hashed/wildcard index planning, broad sort pushdown, or non-simple collation range filters. Unsupported operators and unsupported predicate shapes return command errors. |
| `findAndModify` / `findandmodify` | Broad subset | Supports PyMongo `find_one_and_update`, `find_one_and_replace`, and `find_one_and_delete` for one document, with filter, deterministic sort, supported collation for target matching/sort, `fields`/`projection`, pre-image or post-image return, update/replacement/pipeline upsert, supported update modifiers, the bounded update pipeline subset, conservative positional `$`, `$[]`, and `$[identifier]` with `arrayFilters`, `_id` immutability, validator enforcement, `bypassDocumentValidation: true`, unique-index enforcement, maintained index entries, supported `hint`, safe acknowledged `writeConcern`, validated `lsid`, bounded per-connection retryable replay with `lsid + txnNumber`, and deterministic namespace-scoped TTL sweeps before target selection. | No nested multi-array positional traversal, array modifiers through positional paths, unsupported update pipeline stages, maxTimeMS, `let`, transaction semantics, explain behavior, durable retry history across reconnects/restarts, or non-simple collation range filters. Unsupported shapes return command errors. |
| `getMore` | Complete in model | Returns `nextBatch` for live per-client cursors and closes cursors on exhaustion. | Cursor state is in memory, per connection, and snapshot-at-find-time. No cursor timeout, awaitData, or cross-connection cursor lookup. |
| `killCursors` | Complete in model | Removes live per-client cursors and reports `cursorsKilled` or `cursorsNotFound`. | No cross-connection cursor lookup. Malformed cursor ids return command errors. |
| `update` | Broad subset | Supports replacement updates; update pipelines with `$set`/`$addFields`, `$unset`, `$project`, `$replaceRoot`, and `$replaceWith`; `$set`, `$unset`, `$inc`, `$rename`, `$min`, `$max`, `$mul`, `$setOnInsert`; array modifiers `$push`, `$addToSet`, `$pop`, `$pull`, and `$pullAll` for the documented subset; conservative positional `$`, `$[]`, and `$[identifier]` with `arrayFilters` for supported scalar modifiers; upsert; single-update; multi-update; ordered/unordered batches; safe acknowledged `writeConcern`; validated `lsid`; bounded per-connection retryable replay with `lsid + txnNumber`; supported per-entry collation; supported `hint`; `_id` immutability; validator enforcement; `bypassDocumentValidation: true`; and duplicate-key write errors. | No nested multi-array positional traversal, array modifiers through positional paths, unsupported update pipeline stages, `$push` `$position`/`$slice`/`$sort`, transactions, explain behavior, durable retry history across reconnects/restarts, or non-simple collation range filters. |
| `delete` | Complete in model | Supports batch deletes with `q`, `limit`, supported per-entry collation, supported `hint`, safe acknowledged `writeConcern`, validated `lsid`, and bounded per-connection retryable replay with `lsid + txnNumber`; `limit: 1` deletes one deterministic match and `limit: 0` deletes all matches. | No transactions, explain behavior, durable retry history across reconnects/restarts, or non-simple collation range filters. |
| Cursors | Complete in model | `find` and `aggregate` store remaining results under a positive cursor id, PyMongo can iterate across multiple batches, exhausted cursors close with `id: 0`, and `killCursors` explicitly closes live cursors. | No cursor timeout. Cursor state is not durable and is scoped to one client connection. Invalid or exhausted cursor ids return explicit command errors for `getMore`. |
| BSON storage | Complete in model | Stores BSON blobs in SQLite, derives a stable primary key from `_id`, maintains exact, compound-prefix, range, scalar multikey, sparse, partial, TTL, and supported collation-aware index metadata/entries for supported planner shapes, stores durable collection validator metadata, and enforces the supported validator subset on document-producing writes. Inserts with operator-shaped field names store those names as data. | No document size enforcement beyond message size. Unsupported planner shapes fall back or error as documented instead of weakening matcher semantics. |
| Authentication | Unsupported | No auth challenge or credential validation. | No SCRAM, x.509, keyfile, localhost exception, users, roles, or permissions. |
| Transactions | Unsupported | `commitTransaction`, `abortTransaction`, `prepareTransaction`, `startTransaction`, and `autocommit` are explicit command errors before mutation. | No snapshot reads, causal consistency, transaction oplog/retry state, multi-operation atomicity, or rollback semantics. |
| Indexes | Broad subset | Supports `createIndexes`, `listIndexes`, and `dropIndexes` metadata for simple ascending/descending single-field and compound keys, lists virtual `_id_`, enforces supported `unique: true` indexes across insert/update/upsert, maintains safe exact, compound-prefix, range, scalar multikey, sparse, partial, single-field TTL, and supported collation-aware planner entries, accepts supported hints, exposes partial explain diagnostics, and deletes expired TTL documents at deterministic command boundaries. Supported collation metadata is persisted and listed. | Text, geospatial, hashed, wildcard, compound TTL, `_id` TTL, hidden, background monitor timing, sparse/partial TTL combinations, collation combined with TTL or partial indexes, non-TTL-to-TTL `collMod` conversion, full compound multikey planning, numeric range planning, non-simple collation range planning, string sort pushdown, broad collation sort pushdown, and full MongoDB explain parity are unsupported. Unique indexes reject array/multikey values. |

## Current Storage Model

Documents are stored as BSON blobs in SQLite:

- `namespace`: MongoDB namespace, for example `app.users`
- `id_key`: a stable key derived from `_id`
- `bson`: the original BSON document

If an inserted document does not include `_id`, `mongolino` assigns a BSON
ObjectId before writing it to SQLite.

Collection names are also stored in a durable SQLite catalog, so empty
collections created through `create` remain visible to `listCollections` and
`listDatabases`.

## Architecture Notes

The core design keeps MongoDB-visible behavior in Rust while using SQLite for
durability and safe narrowing:

- **BSON-first storage.** Documents are stored as original BSON blobs, so field
  order, BSON numeric types, ObjectIds, dates, arrays, nested documents, and
  operator-shaped field names round-trip as data. SQLite owns durability; Rust
  owns MongoDB semantics.
- **Stable `_id` keys.** Every stored document gets an `id_key` derived from
  `_id`. This gives SQLite a durable primary key without rewriting user
  documents, and lets replacement/update paths prove `_id` immutability before
  writing.
- **Maintained `index_entries`.** Indexes are represented as metadata plus
  derived key rows. Supported single-field, compound-prefix, sparse, partial,
  TTL, scalar multikey, and collation-aware entries can narrow candidates
  without changing the source BSON document.
- **Planner safety over planner cleverness.** SQLite is used only when the
  maintained entries are known to be semantically safe for the query shape.
  Every narrowed candidate still goes through the Rust matcher before a read or
  write is observed. Unsafe shapes fall back to scans or explicit errors instead
  of letting an index hide matching documents.
- **One matcher, many surfaces.** The same matcher powers `find`, `count`,
  `distinct`, aggregation `$match`, update/delete targeting, `findAndModify`,
  partial-index membership, and `$pull` document predicates. This keeps query
  semantics consistent across commands.
- **Staged writes.** Update and find-and-modify paths compute candidate
  replacement documents, validate `_id`, validators, unique indexes, and
  in-batch unique collisions before persisting. Error paths are designed to
  return before mutation when a command shape is invalid.
- **Command-bound TTL.** TTL deletion runs at deterministic command boundaries
  after command preflight. Invalid filters, unsupported stages, invalid
  workflow options, and bad hints do not accidentally sweep expired data before
  returning errors.
- **Per-client state where MongoDB expects it.** Cursors and retryable-write
  replay history live in `ClientState`, scoped to one connection. That matches
  the single-process server model while avoiding fake durable session or
  transaction claims.
- **Explicit unsupported behavior.** Unsupported MongoDB families are not
  silently ignored. Commands and options that would imply false semantics return
  command or write errors before side effects.

## CRUD Compatibility Notes

Supported query matching is bounded and explicit. Field equality
works for scalars, documents, arrays, booleans, `null`, and comparable numeric
types. Dotted paths can traverse embedded documents and arrays of documents.
Unsupported or malformed operators return command errors instead of being
silently accepted.

The shared query matcher is used by `find`, `count`, `distinct`, aggregation
`$match`, update/delete target selection, `findAndModify`, index membership
checks, and `$pull` document predicates. Supported predicate shapes include
exact BSON equality; dotted paths with limited array traversal; comparison
operators; `$in`/`$nin`; `$exists`; `$not`; logical `$and`/`$or`/`$nor`;
`$regex` with string or BSON regex operands and options `i`, `m`, and `s`;
`$type` for common aliases and BSON type codes; `$size` for exact array length;
scalar `$all`; `$all` with supported `$elemMatch` clauses; and `$elemMatch` for
scalar and document arrays. Unsupported regex options, invalid regex patterns,
malformed `$type`/`$size`/`$all`/`$elemMatch` operands, JavaScript `$where`,
geospatial/text predicates, and expression predicates remain explicit errors.

Collation support is deliberately bounded. `{ locale: "simple" }` preserves
binary semantics. `{ locale: "en", strength: 2 }` and
`{ locale: "en_US", strength: 2 }` provide case-insensitive string equality,
ordering, and distinct de-duplication on supported read/write command paths.
Unsupported options such as `numericOrdering`, `caseLevel`, `alternate`,
`maxVariable`, `backwards`, `normalization`, unsupported locales, unsupported
strengths, non-document collation values, and unknown fields return explicit
command or write errors. Non-simple string range filters are rejected instead of
using unsafe binary index semantics. Full ICU behavior, locale-specific sort
orders, diacritic folding, text/geospatial collation behavior, and broad
collation sort pushdown are not implemented.

Projection supports inclusion or exclusion mode, with `_id` as the only allowed
mode override. Sort supports top-level or dotted fields with `1` or `-1`; missing
fields sort deterministically before present fields in ascending order.

Update paths support dotted document fields and a conservative single-array
positional subset. Dotted updates through scalar parents, conflicting paths such
as `{ a: 1, "a.b": 2 }`, nested/multiple positional segments, attempts to
change `_id`, and unsupported update operators are rejected with write errors.

Supported update modifiers are intentionally bounded. Scalar modifiers include
`$set`, `$unset`, `$inc`, `$rename`, `$min`, `$max`, `$mul`, and `$setOnInsert`.
Array modifiers include `$push` with scalar values or `$each`, `$addToSet` with
scalar values or `$each`, `$pop` with `1` or `-1`, `$pull` with equality or the
supported matcher predicate subset, and `$pullAll` with scalar/document equality.
`$setOnInsert` applies only to inserted upserts. Supported scalar modifiers
through positional paths are `$set`, `$unset`, `$inc`, `$min`, `$max`, and
`$mul`; `$` selects the first matching element from a supported query predicate,
`$[]` applies to every element of one array path, and `$[identifier]` uses
`arrayFilters` documents with one lowercase identifier and the supported matcher
subset. `$push` option documents using `$position`, `$slice`, `$sort`, or
unknown options are rejected explicitly, as are array modifiers through
positional paths.

Update pipelines are supported for `$set`/`$addFields`, `$unset`, `$project`,
`$replaceRoot`, and `$replaceWith` using the bounded aggregation expression
subset. Pipeline updates preserve `_id`, can synthesize conservative upsert base
documents from equality query fields, and reject unsupported stages such as
`$lookup`, `$group`, `$unwind`, `$out`, `$merge`, and `$facet`.

`findAndModify` uses the same matcher, sort, projection, update application,
duplicate-key checks, maintained index entries, driver workflow preflight, and
retryable-write replay cache as `find`, `update`, and `delete`. It is
SQLite-transaction backed for one selected document, but it does not implement
MongoDB causal consistency, durable retry history, distributed write concern
durability, or multi-document transactions.

Driver workflow support is intentionally single-node. SQLite connections run in
WAL mode with foreign keys enabled and `synchronous=NORMAL` by default. Valid
`lsid` documents are shape-checked, `endSessions` is a validating cleanup stub,
and safe `readConcern` forms (`{}`, `local`, `available`) are accepted as local
SQLite no-ops. Safe acknowledged `writeConcern` forms (`{}`, `w: 1`,
`w: "majority"`, boolean `j`, and non-negative timeout fields) are accepted as
local acknowledged writes; `w: "majority"` does not imply replica majority
durability. `writeConcern: { j: true }` upgrades that single command to local
SQLite `synchronous=FULL` and then restores the prior connection mode. Retryable
writes replay exact duplicate `insert`, `update`, `delete`, and
`findAndModify` responses for the same `lsid + txnNumber` on the same
connection. The retry cache is bounded and in-memory; reconnects, process
restarts, transactions, snapshot reads, causal ordering, unacknowledged writes,
and distributed durability semantics are not supported.

Validation supports a small `$jsonSchema` subset for object documents:
root `bsonType: "object"`, `required`, and nested `properties` with `bsonType`
values `object`, `array`, `string`, `int`, `long`, `double`, `number`, `bool`,
`objectId`, `date`, and `null`. `number` matches int, long, and double. Nested
object properties may define their own `required` and `properties`. Dotted
validator property names, arrays/items, regex/patterns, numeric bounds, string
lengths, combinators, dependencies, collation, `validationLevel` values other
than `strict`, and `validationAction` values other than `error` are rejected
explicitly. `collMod` clears validation with `validator: {}`. Insert, update,
and find-and-modify enforce validators unless `bypassDocumentValidation: true`
is present; bypass does not disable `_id` immutability or unique indexes.

Aggregation is a document-stream subset. Each supported stage runs in order over
the current stream, so `$limit` before `$skip` is intentionally different from
`$skip` before `$limit`. Unsupported stages and unsupported expression shapes
return command errors instead of being ignored. `$unwind` supports field-path
strings and document form with `path`, `preserveNullAndEmptyArrays`, and
`includeArrayIndex`. `$group`, computed `$project`, `$addFields`/`$set`,
`$replaceRoot`, and `$replaceWith` share the bounded expression evaluator
documented in the command table. Simple `$lookup` supports only same-database
`from`/`localField`/`foreignField`/`as` equality joins; missing local or foreign
fields compare as `null`, local arrays match scalar foreign values by any
element, and unsupported pipeline/`let` forms return explicit command errors.

## Development

```sh
cargo fmt -- --check
cargo test
cargo build
```

Run repeatable local performance benchmarks without binding localhost or using
PyMongo:

```sh
cargo run --bin mongolino-bench -- --profile smoke
cargo run --bin mongolino-bench -- --profile smoke --json /tmp/mongolino-bench-smoke.json
cargo run --bin mongolino-bench -- --profile local --json /tmp/mongolino-bench-local.json
```

Run the CI-friendly smoke budget locally:

```sh
cargo run --bin mongolino-bench -- --profile ci --check-budget
```

Profiles:

- `smoke`: fast local check with small seeded collections.
- `ci`: fast budget profile used by GitHub Actions.
- `local`: larger baseline profile for before/after comparisons.

The benchmark harness creates temporary SQLite databases, seeds data through
`mongolino` command handlers, and exercises real insert, find, count, update,
index-entry refresh, and aggregation paths. Budget thresholds are intentionally
coarse: they are meant to catch severe regressions in CI, not to decide small
performance wins.

To compare normal local writes against journaled local writes, run the same
write workload with default write concern and then with `writeConcern: { j:
true }`. The latter maps to SQLite `synchronous=FULL` for each supported write
command, so expect higher write latency on durable storage; record command lines
and machine details with any numbers.

Current baseline results and SQLite pushdown targets are recorded in
`docs/performance-baseline.md`.

Run the PyMongo end-to-end suite with `uv`:

```sh
uv sync --locked --dev
uv run --locked pytest tests/e2e
```

Useful targeted subsets while developing:

```sh
uv run --locked pytest tests/e2e/test_cursors.py
uv run --locked pytest tests/e2e/test_find_and_modify.py
uv run --locked pytest tests/e2e/test_aggregation.py
uv run --locked pytest tests/e2e/test_lifecycle.py
uv run --locked pytest tests/e2e/test_indexes.py
uv run --locked pytest tests/e2e/test_metadata.py
uv run --locked pytest tests/e2e/test_update_operators.py
```

The e2e suite builds or locates `target/debug/mongolino`, starts it on a
temporary localhost port with a temporary SQLite file, connects with PyMongo,
and tears it down after each test. It does not require Docker or an external
MongoDB service.

`tests/spec_corpus/` contains a small local JSON corpus inspired by MongoDB's
Unified Test Format and CRUD API spec. It tracks the documented `mongolino`
subset only and is not an official MongoDB compliance claim.

GitHub Actions runs Rust formatting, Rust tests, a Rust build, locked uv sync,
and the PyMongo e2e suite on pushes and pull requests.

## Scope

This is not a full MongoDB replacement. The next major pieces are
broader query/update edge cases, more aggregation stages, richer index planning,
authentication behavior, durable/distributed session semantics, transactions,
and deeper driver compatibility testing.
