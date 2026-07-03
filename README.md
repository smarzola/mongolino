# mongolino

`mongolino` is a small MongoDB wire-protocol server backed by one SQLite file.

The first implementation is intentionally narrow: it accepts MongoDB `OP_MSG`
handshakes and supports `hello`, `isMaster`, `ping`, `buildInfo`,
`listDatabases`, basic `insert`, and basic `find`.

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
db.users.find({ _id: "u1" })
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
| `OP_MSG` | Partial | Parses body section kind `0` and replies with `OP_MSG`. Skips document sequence section kind `1`. | Does not expose document sequence payloads to command handlers; ignores flags. |
| `OP_QUERY` | Partial | Parses legacy query payloads and replies with `OP_REPLY`. Useful for legacy handshake-style clients. | No legacy cursor behavior beyond one-document command replies. |
| Other opcodes | Unsupported | Returns an error document for unknown opcodes. | No `OP_COMPRESSED`, `OP_GET_MORE`, `OP_INSERT`, `OP_UPDATE`, `OP_DELETE`, or `OP_KILL_CURSORS`. |
| `hello` | Compatible | Returns a standalone writable primary-style handshake with wire version and size limits. | No replica set, topology, compression, speculative auth, or server parameters. |
| `isMaster` / `ismaster` | Compatible | Same response shape as `hello`, with `ismaster` and `helloOk`. | Same handshake gaps as `hello`. |
| `ping` | Compatible | Returns `{ ok: 1.0 }`. | None for basic ping behavior. |
| `buildInfo` | Partial | Returns version, allocator/storage hints, BSON size, bitness, and `ok`. | Not byte-for-byte compatible with MongoDB server build metadata. |
| `listDatabases` | Partial | Lists distinct database names derived from persisted namespaces. | Size accounting is placeholder `0`; empty databases are not tracked. |
| `endSessions` | Stub | Returns `{ ok: 1.0 }`. | Sessions are not stored, validated, expired, or attached to operations. |
| `insert` | Partial | Accepts `documents`, assigns `_id` when missing, and stores each BSON document in SQLite by namespace and `_id`. | Ordered/unordered semantics, write concern, duplicate key errors, validation, bypass flags, and bulk error reporting are not implemented. |
| `find` | Partial | Returns a cursor with `id: 0` and `firstBatch`. Supports full collection scan with `batchSize` and exact `_id` lookup. | No query operators, projections, sort, skip, limit semantics, collation, read concern, getMore, tailable cursors, or non-`_id` indexes. |
| Cursors | Partial | `find` returns all selected results in `firstBatch` and closes the cursor immediately with `id: 0`. | No server-side cursor storage, `getMore`, cursor timeout, or kill cursor support. |
| BSON storage | Partial | Stores original BSON blobs in SQLite and derives a stable primary key from `_id`. | No typed secondary indexes, schema validation, document size enforcement beyond message size, or query planning. |
| Authentication | Unsupported | No auth challenge or credential validation. | No SCRAM, x.509, keyfile, localhost exception, users, roles, or permissions. |
| Transactions | Unsupported | No multi-operation transaction protocol. | No sessions, transaction numbers, retryable writes, snapshot reads, or rollback semantics. |
| Indexes | Unsupported | SQLite has only internal storage indexes for namespace scans and `_id` primary-key lookups. | MongoDB `createIndexes`, `listIndexes`, `dropIndexes`, unique indexes beyond `_id`, and query planner behavior are not implemented. |

## Current Storage Model

Documents are stored as BSON blobs in SQLite:

- `namespace`: MongoDB namespace, for example `app.users`
- `id_key`: a stable key derived from `_id`
- `bson`: the original BSON document

If an inserted document does not include `_id`, `mongolino` assigns a BSON
ObjectId before writing it to SQLite.

## Development

```sh
cargo fmt
cargo test
```

## Scope

This is not a full MongoDB replacement yet. The next major pieces are query
operator support, update/delete commands, indexes, authentication behavior, and
driver compatibility testing.
