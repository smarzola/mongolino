# mongolino

`mongolino` is a small MongoDB wire-protocol server backed by one SQLite file.
It supports a documented single-server CRUD subset for simple client smoke
tests and local experiments.

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
db.users.updateOne({ _id: "u1" }, { $set: { name: "Ada Lovelace" }, $inc: { score: 1 } })
db.users.deleteOne({ _id: "u2" })
```

## MongoDB Interface Surface

Compatibility flags:

- `Compatible`: expected to work for the documented subset.
- `Partial`: accepted, but missing meaningful MongoDB behavior.
- `Stub`: returns a successful response only to keep clients moving.
- `Unsupported`: returns a command or protocol error.

| Surface | Compatibility | Current behavior | Gaps |
| --- | --- | --- | --- |
| TCP listener | Compatible | Listens on `--addr`, defaulting to `127.0.0.1:27017`. | No TLS, Unix sockets, IPv6-specific handling, or connection limits. |
| MongoDB message header | Compatible | Reads and writes standard 16-byte wire message headers with request/response IDs. | Does not validate all cross-field semantics. |
| `OP_MSG` | Partial | Parses body section kind `0`, exposes document sequence section kind `1` as command arrays, and replies with `OP_MSG`. | Ignores flags; no compression. |
| `OP_QUERY` | Partial | Parses legacy query payloads and replies with `OP_REPLY`. Useful for legacy handshake-style clients. | No legacy cursor behavior beyond one-document command replies. |
| Other opcodes | Unsupported | Returns an error document for unknown opcodes. | No `OP_COMPRESSED`, `OP_GET_MORE`, `OP_INSERT`, `OP_UPDATE`, `OP_DELETE`, or `OP_KILL_CURSORS`. |
| `hello` | Compatible | Returns a standalone writable primary-style handshake with wire version and size limits. | No replica set, topology, compression, speculative auth, or server parameters. |
| `isMaster` / `ismaster` | Compatible | Same response shape as `hello`, with `ismaster` and `helloOk`. | Same handshake gaps as `hello`. |
| `ping` | Compatible | Returns `{ ok: 1.0 }`. | None for basic ping behavior. |
| `buildInfo` | Partial | Returns version, allocator/storage hints, BSON size, bitness, and `ok`. | Not byte-for-byte compatible with MongoDB server build metadata. |
| `listDatabases` | Partial | Lists database names derived from the collection catalog and persisted document namespaces, including databases with empty collections. | Size accounting is placeholder `0`. |
| `endSessions` | Stub | Returns `{ ok: 1.0 }`. | Sessions are not stored, validated, expired, or attached to operations. |
| `create` | Partial | Creates a durable empty collection catalog entry. | Collection options such as validators, capped collections, clustered indexes, timeseries, and collation are unsupported. |
| `listCollections` | Partial | Lists durable catalog entries and legacy document-only namespaces, with `nameOnly` and simple `name` equality filters. | Size/details are minimal; complex filters and collection metadata options are unsupported. |
| `drop` | Partial | Drops a collection by removing its documents and catalog entry. | Index cleanup will become relevant once user indexes exist. |
| `dropDatabase` | Partial | Drops catalog entries and documents for the selected database only. | No users, roles, profiling collections, or storage statistics. |
| `count` | Partial | Counts documents matching the supported filter subset, with `skip` and `limit`. PyMongo `estimated_document_count()` uses this path. | No hint, collation, read concern, maxTimeMS, or storage-stat semantics. |
| `aggregate` | Partial | Supports only the PyMongo `count_documents()` pipeline shape: `$match`, optional `$skip`/`$limit`, and count `$group`. | General aggregation stages return command errors. |
| `distinct` | Partial | Returns unique scalar, dotted-path, and array-expanded values for documents matching the supported filter subset, ordered deterministically by BSON sort order. | No collation, hint, read concern, maxTimeMS, or complex array semantics beyond the documented matcher behavior. |
| `insert` | Partial | Accepts `documents`, assigns `_id` when missing, preserves existing documents on duplicate `_id`, reports duplicate key `writeErrors`, and supports ordered/unordered batches. | No write concern, bypass document validation, schema validation, retryable writes, or sessions beyond accepting `lsid`. |
| `find` | Partial | Returns `firstBatch` and creates a per-client server-side cursor when more shaped results remain. Supports exact matches, dotted paths, limited array traversal, `$eq`, `$ne`, `$gt`, `$gte`, `$lt`, `$lte`, `$in`, `$nin`, `$exists`, `$and`, `$or`, `$nor`, `$not`, projection, sort, skip, limit, and capped batch size. | No regex, `$where`, `$elemMatch`, geospatial/text search, collation, read concern, tailable cursors, or secondary indexes. Unsupported operators return command errors. |
| `getMore` | Partial | Returns `nextBatch` for live per-client cursors and closes cursors on exhaustion. | Cursor state is in memory, per connection, and snapshot-at-find-time. No cursor timeout, awaitData, or cross-connection cursor lookup. |
| `killCursors` | Partial | Removes live per-client cursors and reports `cursorsKilled` or `cursorsNotFound`. | No cross-connection cursor lookup. Malformed cursor ids return command errors. |
| `update` | Partial | Supports replacement updates, `$set`, `$unset`, `$inc`, upsert, single-update, multi-update, ordered/unordered batches, `_id` immutability, and duplicate-key write errors. | No array filters, positional operators, pipeline updates, `$rename`, `$push`, `$pull`, hints, collation, write concern, retryable writes, or transactions. |
| `delete` | Partial | Supports batch deletes with `q` and `limit`; `limit: 1` deletes one deterministic match and `limit: 0` deletes all matches. | No hints, collation, write concern, retryable writes, or explain behavior. |
| Cursors | Partial | `find` stores remaining results under a positive cursor id, PyMongo can iterate across multiple batches, exhausted cursors close with `id: 0`, and `killCursors` explicitly closes live cursors. | No cursor timeout. Cursor state is not durable and is scoped to one client connection. Invalid or exhausted cursor ids return explicit command errors for `getMore`. |
| BSON storage | Partial | Stores BSON blobs in SQLite and derives a stable primary key from `_id`. Inserts with operator-shaped field names store those names as data. | No typed secondary indexes, schema validation, document size enforcement beyond message size, or query planning. |
| Authentication | Unsupported | No auth challenge or credential validation. | No SCRAM, x.509, keyfile, localhost exception, users, roles, or permissions. |
| Transactions | Unsupported | No multi-operation transaction protocol. | No sessions, transaction numbers, retryable writes, snapshot reads, or rollback semantics. |
| Indexes | Partial | Supports `createIndexes`, `listIndexes`, and `dropIndexes` metadata for simple ascending/descending single-field and compound keys, lists virtual `_id_`, stores `unique: true` metadata, and rejects unsupported index options explicitly. | Unique index enforcement and secondary-index query planning are not implemented yet. Text, geospatial, hashed, wildcard, partial, sparse, TTL, hidden, collation, and background semantics are unsupported. |

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

## CRUD Compatibility Notes

Supported query matching is intentionally small and explicit. Field equality
works for scalars, documents, arrays, booleans, `null`, and comparable numeric
types. Dotted paths can traverse embedded documents and arrays of documents.
Unsupported or malformed operators return command errors instead of being
silently accepted.

Projection supports inclusion or exclusion mode, with `_id` as the only allowed
mode override. Sort supports top-level or dotted fields with `1` or `-1`; missing
fields sort deterministically before present fields in ascending order.

Update paths support dotted document fields. Dotted updates through scalar
parents, conflicting paths such as `{ a: 1, "a.b": 2 }`, attempts to change
`_id`, and unsupported update operators are rejected with write errors.

## Development

```sh
cargo fmt -- --check
cargo test
cargo build
```

Run the PyMongo end-to-end suite with `uv`:

```sh
uv sync --locked --dev
uv run --locked pytest tests/e2e
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
collection/database lifecycle commands, indexes, broader query/update operators,
authentication behavior, transactions, retryable writes, and deeper driver
compatibility testing.
