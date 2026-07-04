use std::collections::{HashMap, VecDeque};
use std::env;
use std::io::Cursor;
use std::path::PathBuf;
use std::sync::atomic::{AtomicI32, Ordering};

use bson::{Bson, Document, doc, oid::ObjectId};
use rusqlite::{Connection, OptionalExtension, params};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

const OP_REPLY: i32 = 1;
const OP_QUERY: i32 = 2004;
const OP_MSG: i32 = 2013;
const MAX_MESSAGE_BYTES: usize = 48 * 1024 * 1024;

static NEXT_REQUEST_ID: AtomicI32 = AtomicI32::new(1);

type Result<T> = std::result::Result<T, MongolinoError>;

#[derive(Debug)]
enum MongolinoError {
    Io(std::io::Error),
    Sqlite(rusqlite::Error),
    BsonDe(bson::de::Error),
    BsonSer(bson::ser::Error),
    Protocol(String),
}

impl std::fmt::Display for MongolinoError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io(err) => write!(f, "io error: {err}"),
            Self::Sqlite(err) => write!(f, "sqlite error: {err}"),
            Self::BsonDe(err) => write!(f, "bson decode error: {err}"),
            Self::BsonSer(err) => write!(f, "bson encode error: {err}"),
            Self::Protocol(msg) => write!(f, "protocol error: {msg}"),
        }
    }
}

impl std::error::Error for MongolinoError {}

impl From<std::io::Error> for MongolinoError {
    fn from(value: std::io::Error) -> Self {
        Self::Io(value)
    }
}

impl From<rusqlite::Error> for MongolinoError {
    fn from(value: rusqlite::Error) -> Self {
        Self::Sqlite(value)
    }
}

impl From<bson::de::Error> for MongolinoError {
    fn from(value: bson::de::Error) -> Self {
        Self::BsonDe(value)
    }
}

impl From<bson::ser::Error> for MongolinoError {
    fn from(value: bson::ser::Error) -> Self {
        Self::BsonSer(value)
    }
}

#[derive(Clone, Debug)]
struct Config {
    addr: String,
    sqlite_path: PathBuf,
}

#[derive(Debug)]
struct WireMessage {
    request_id: i32,
    opcode: i32,
    payload: Vec<u8>,
}

#[derive(Debug)]
struct ClientState {
    cursors: HashMap<i64, CursorState>,
    next_cursor_id: i64,
}

impl Default for ClientState {
    fn default() -> Self {
        Self {
            cursors: HashMap::new(),
            next_cursor_id: 1,
        }
    }
}

#[derive(Debug)]
struct CursorState {
    namespace: String,
    remaining: VecDeque<Document>,
}

impl ClientState {
    fn insert_cursor(&mut self, namespace: String, remaining: Vec<Document>) -> i64 {
        let id = self.next_cursor_id;
        self.next_cursor_id += 1;
        self.cursors.insert(
            id,
            CursorState {
                namespace,
                remaining: remaining.into(),
            },
        );
        id
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let config = Config::from_env()?;
    init_database(&config.sqlite_path)?;

    let listener = TcpListener::bind(&config.addr).await?;
    println!(
        "mongolino listening on mongodb://{} with sqlite file {}",
        config.addr,
        config.sqlite_path.display()
    );

    loop {
        let (stream, peer_addr) = listener.accept().await?;
        let config = config.clone();
        tokio::spawn(async move {
            if let Err(err) = serve_client(stream, config).await {
                eprintln!("{peer_addr}: {err}");
            }
        });
    }
}

impl Config {
    fn from_env() -> Result<Self> {
        let mut addr = "127.0.0.1:27017".to_string();
        let mut sqlite_path = PathBuf::from("mongolino.sqlite3");
        let mut args = env::args().skip(1);

        while let Some(arg) = args.next() {
            match arg.as_str() {
                "--addr" => {
                    addr = args.next().ok_or_else(|| {
                        MongolinoError::Protocol("--addr requires a value".to_string())
                    })?;
                }
                "--db" => {
                    sqlite_path = PathBuf::from(args.next().ok_or_else(|| {
                        MongolinoError::Protocol("--db requires a value".to_string())
                    })?);
                }
                "--help" | "-h" => {
                    println!(
                        "mongolino\n\nUsage: mongolino [--addr HOST:PORT] [--db PATH]\n\nDefaults: --addr 127.0.0.1:27017 --db mongolino.sqlite3"
                    );
                    std::process::exit(0);
                }
                unknown => {
                    return Err(MongolinoError::Protocol(format!(
                        "unknown argument: {unknown}"
                    )));
                }
            }
        }

        Ok(Self { addr, sqlite_path })
    }
}

fn init_database(path: &PathBuf) -> Result<()> {
    let conn = Connection::open(path)?;
    init_connection(&conn)
}

fn init_connection(conn: &Connection) -> Result<()> {
    conn.pragma_update(None, "journal_mode", "WAL")?;
    conn.pragma_update(None, "foreign_keys", "ON")?;
    init_migration_schema(conn)?;
    init_document_schema(conn)?;
    Ok(())
}

fn init_migration_schema(conn: &Connection) -> Result<()> {
    conn.execute_batch(
        r#"
        CREATE TABLE IF NOT EXISTS schema_migrations (
            name TEXT PRIMARY KEY,
            applied_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
        );
        "#,
    )?;
    Ok(())
}

fn init_document_schema(conn: &Connection) -> Result<()> {
    conn.execute_batch(
        r#"
        CREATE TABLE IF NOT EXISTS collections (
            namespace TEXT PRIMARY KEY,
            db TEXT NOT NULL,
            name TEXT NOT NULL,
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            options_bson BLOB
        );

        CREATE INDEX IF NOT EXISTS idx_collections_db_name
            ON collections(db, name);

        CREATE TABLE IF NOT EXISTS indexes (
            namespace TEXT NOT NULL,
            name TEXT NOT NULL,
            key_bson BLOB NOT NULL,
            unique_index INTEGER NOT NULL DEFAULT 0,
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (namespace, name)
        );

        CREATE INDEX IF NOT EXISTS idx_indexes_namespace
            ON indexes(namespace);

        CREATE TABLE IF NOT EXISTS index_entries (
            namespace TEXT NOT NULL,
            index_name TEXT NOT NULL,
            key_value TEXT NOT NULL,
            id_key TEXT NOT NULL,
            PRIMARY KEY (namespace, index_name, key_value, id_key)
        );

        CREATE INDEX IF NOT EXISTS idx_index_entries_lookup
            ON index_entries(namespace, index_name, key_value);

        CREATE TABLE IF NOT EXISTS documents (
            namespace TEXT NOT NULL,
            id_key TEXT NOT NULL,
            bson BLOB NOT NULL,
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (namespace, id_key)
        );

        CREATE INDEX IF NOT EXISTS idx_documents_namespace_created
            ON documents(namespace, created_at);
        "#,
    )?;
    Ok(())
}

async fn serve_client(mut stream: TcpStream, config: Config) -> Result<()> {
    let conn = Connection::open(&config.sqlite_path)?;
    init_connection(&conn)?;
    let mut client_state = ClientState::default();

    while let Some(message) = read_wire_message(&mut stream).await? {
        let response = handle_wire_message(&conn, &mut client_state, message)?;
        stream.write_all(&response).await?;
    }

    Ok(())
}

async fn read_wire_message(stream: &mut TcpStream) -> Result<Option<WireMessage>> {
    let mut header = [0_u8; 16];
    match stream.read_exact(&mut header).await {
        Ok(_) => {}
        Err(err) if err.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(err) => return Err(err.into()),
    }

    let message_len = read_i32(&header[0..4])? as usize;
    if !(16..=MAX_MESSAGE_BYTES).contains(&message_len) {
        return Err(MongolinoError::Protocol(format!(
            "invalid message length: {message_len}"
        )));
    }

    let request_id = read_i32(&header[4..8])?;
    let _response_to = read_i32(&header[8..12])?;
    let opcode = read_i32(&header[12..16])?;
    let mut payload = vec![0_u8; message_len - 16];
    stream.read_exact(&mut payload).await?;

    Ok(Some(WireMessage {
        request_id,
        opcode,
        payload,
    }))
}

fn handle_wire_message(
    conn: &Connection,
    client_state: &mut ClientState,
    message: WireMessage,
) -> Result<Vec<u8>> {
    match message.opcode {
        OP_MSG => handle_op_msg(conn, client_state, message),
        OP_QUERY => handle_op_query(conn, client_state, message),
        opcode => build_op_msg_response(
            message.request_id,
            command_error(59, &format!("unsupported opcode {opcode}")),
        ),
    }
}

fn handle_op_msg(
    conn: &Connection,
    client_state: &mut ClientState,
    message: WireMessage,
) -> Result<Vec<u8>> {
    let command = parse_op_msg_document(&message.payload)?;
    let response = handle_command_with_state(conn, client_state, &command)?;
    build_op_msg_response(message.request_id, response)
}

fn handle_op_query(
    conn: &Connection,
    client_state: &mut ClientState,
    message: WireMessage,
) -> Result<Vec<u8>> {
    let (full_collection_name, query) = parse_op_query(&message.payload)?;
    let db_name = full_collection_name
        .split_once('.')
        .map(|(db, _)| db)
        .unwrap_or("admin");
    let mut command = query;
    command.insert("$db", db_name);
    let response = handle_command_with_state(conn, client_state, &command)?;
    build_op_reply_response(message.request_id, response)
}

fn parse_op_msg_document(payload: &[u8]) -> Result<Document> {
    if payload.len() < 5 {
        return Err(MongolinoError::Protocol(
            "OP_MSG payload is too short".to_string(),
        ));
    }

    let _flags = read_i32(&payload[0..4])?;
    let mut offset = 4;
    let mut body = None;
    let mut sequences: Vec<(String, Vec<Document>)> = Vec::new();

    while offset < payload.len() {
        let section_kind = payload[offset];
        offset += 1;

        match section_kind {
            0 => {
                if body.is_some() {
                    return Err(MongolinoError::Protocol(
                        "OP_MSG payload contains multiple body sections".to_string(),
                    ));
                }
                let (doc, consumed) = read_document_at(&payload[offset..])?;
                body = Some(doc);
                offset += consumed;
            }
            1 => {
                if offset + 4 > payload.len() {
                    return Err(MongolinoError::Protocol(
                        "document sequence is missing size".to_string(),
                    ));
                }
                let size = read_i32(&payload[offset..offset + 4])? as usize;
                if size < 4 || offset + size > payload.len() {
                    return Err(MongolinoError::Protocol(
                        "invalid document sequence size".to_string(),
                    ));
                }
                let section_end = offset + size;
                let identifier_start = offset + 4;
                let identifier_len = payload[identifier_start..section_end]
                    .iter()
                    .position(|byte| *byte == 0)
                    .ok_or_else(|| {
                        MongolinoError::Protocol(
                            "document sequence identifier missing terminator".to_string(),
                        )
                    })?;
                let identifier = String::from_utf8_lossy(
                    &payload[identifier_start..identifier_start + identifier_len],
                )
                .to_string();
                let mut doc_offset = identifier_start + identifier_len + 1;
                let mut docs = Vec::new();
                while doc_offset < section_end {
                    let (doc, consumed) = read_document_at(&payload[doc_offset..section_end])?;
                    docs.push(doc);
                    doc_offset += consumed;
                }
                sequences.push((identifier, docs));
                offset = section_end;
            }
            other => {
                return Err(MongolinoError::Protocol(format!(
                    "unsupported OP_MSG section kind {other}"
                )));
            }
        }
    }

    let mut body =
        body.ok_or_else(|| MongolinoError::Protocol("OP_MSG body section missing".to_string()))?;
    for (identifier, docs) in sequences {
        if body.contains_key(&identifier) {
            return Err(MongolinoError::Protocol(format!(
                "OP_MSG document sequence duplicates body field {identifier}"
            )));
        }
        body.insert(
            identifier,
            Bson::Array(docs.into_iter().map(Bson::Document).collect()),
        );
    }
    Ok(body)
}

fn parse_op_query(payload: &[u8]) -> Result<(String, Document)> {
    let mut offset = 4;
    if payload.len() < offset + 8 {
        return Err(MongolinoError::Protocol(
            "OP_QUERY payload is too short".to_string(),
        ));
    }

    let collection_end = payload[offset..]
        .iter()
        .position(|byte| *byte == 0)
        .ok_or_else(|| MongolinoError::Protocol("OP_QUERY collection name missing".to_string()))?;
    let full_collection_name =
        String::from_utf8_lossy(&payload[offset..offset + collection_end]).to_string();
    offset += collection_end + 1;

    if payload.len() < offset + 8 {
        return Err(MongolinoError::Protocol(
            "OP_QUERY number fields missing".to_string(),
        ));
    }
    offset += 8;

    let (query, _consumed) = read_document_at(&payload[offset..])?;
    Ok((full_collection_name, query))
}

fn read_document_at(bytes: &[u8]) -> Result<(Document, usize)> {
    if bytes.len() < 5 {
        return Err(MongolinoError::Protocol(
            "BSON document is too short".to_string(),
        ));
    }

    let len = read_i32(&bytes[0..4])? as usize;
    if len < 5 || len > bytes.len() {
        return Err(MongolinoError::Protocol(format!(
            "invalid BSON document length: {len}"
        )));
    }

    let doc = Document::from_reader(&mut Cursor::new(&bytes[..len]))?;
    Ok((doc, len))
}

#[cfg(test)]
fn handle_command(conn: &Connection, command: &Document) -> Result<Document> {
    let mut client_state = ClientState::default();
    handle_command_with_state(conn, &mut client_state, command)
}

fn handle_command_with_state(
    conn: &Connection,
    client_state: &mut ClientState,
    command: &Document,
) -> Result<Document> {
    let Some(command_name) = command_name(command) else {
        return Ok(command_error(59, "empty command document"));
    };

    match command_name.as_str() {
        "hello" | "isMaster" | "ismaster" => Ok(hello_response()),
        "ping" => Ok(doc! { "ok": 1.0 }),
        "buildInfo" | "buildinfo" => Ok(doc! {
            "version": env!("CARGO_PKG_VERSION"),
            "gitVersion": "mongolino",
            "modules": Bson::Array(vec![]),
            "allocator": "sqlite",
            "javascriptEngine": "",
            "bits": 64_i32,
            "debug": cfg!(debug_assertions),
            "maxBsonObjectSize": 16_777_216_i32,
            "ok": 1.0,
        }),
        "listDatabases" => list_databases(conn),
        "endSessions" => Ok(doc! { "ok": 1.0 }),
        "create" => create_collection(conn, command),
        "listCollections" => list_collections(conn, command),
        "drop" => drop_collection(conn, command),
        "dropDatabase" => drop_database(conn, command),
        "count" => count_documents_command(conn, command),
        "distinct" => distinct_command(conn, command),
        "aggregate" => aggregate_command(conn, command),
        "findAndModify" | "findandmodify" => find_and_modify(conn, command_name.as_str(), command),
        "createIndexes" => create_indexes(conn, command),
        "listIndexes" => list_indexes(conn, command),
        "dropIndexes" => drop_indexes(conn, command),
        "insert" => insert_documents(conn, command),
        "find" => find_documents_with_state(conn, client_state, command),
        "getMore" => get_more(client_state, command),
        "killCursors" => kill_cursors(client_state, command),
        "update" => update_documents(conn, command),
        "delete" => delete_documents(conn, command),
        other => Ok(command_error(
            59,
            &format!("command '{other}' is not supported yet"),
        )),
    }
}

fn command_name(command: &Document) -> Option<String> {
    command.keys().next().map(|key| key.to_string())
}

fn hello_response() -> Document {
    doc! {
        "isWritablePrimary": true,
        "ismaster": true,
        "helloOk": true,
        "maxBsonObjectSize": 16_777_216_i32,
        "maxMessageSizeBytes": MAX_MESSAGE_BYTES as i32,
        "maxWriteBatchSize": 100_000_i32,
        "localTime": bson::DateTime::now(),
        "logicalSessionTimeoutMinutes": 30_i32,
        "connectionId": 1_i32,
        "minWireVersion": 0_i32,
        "maxWireVersion": 17_i32,
        "readOnly": false,
        "ok": 1.0,
    }
}

fn list_databases(conn: &Connection) -> Result<Document> {
    let mut stmt = conn.prepare(
        r#"
        SELECT db FROM collections
        UNION
        SELECT DISTINCT substr(namespace, 1, instr(namespace, '.') - 1) FROM documents
        ORDER BY 1
        "#,
    )?;
    let db_names = stmt
        .query_map([], |row| row.get::<_, String>(0))?
        .collect::<std::result::Result<Vec<_>, _>>()?;
    let databases = db_names
        .into_iter()
        .filter(|name| !name.is_empty())
        .map(|name| Bson::Document(doc! { "name": name, "sizeOnDisk": 0_i64, "empty": false }))
        .collect::<Vec<_>>();

    Ok(doc! {
        "databases": databases,
        "totalSize": 0_i64,
        "ok": 1.0,
    })
}

fn create_collection(conn: &Connection, command: &Document) -> Result<Document> {
    let db = command.get_str("$db").unwrap_or("test");
    let collection = match command.get_str("create") {
        Ok(collection) if !collection.is_empty() => collection,
        _ => {
            return Ok(command_error(
                9,
                "create command requires a collection name",
            ));
        }
    };
    if let Some(errmsg) = reject_unsupported_command_keys(command, &["create", "$db", "lsid"]) {
        return Ok(command_error(72, &errmsg));
    }

    let ns = namespace(db, collection);
    match insert_collection_catalog(conn, db, collection) {
        Ok(()) => Ok(doc! { "ok": 1.0 }),
        Err(err) if is_sqlite_constraint(&err) || collection_exists(conn, &ns)? => {
            Ok(command_error(48, "collection already exists"))
        }
        Err(err) => Err(err.into()),
    }
}

fn list_collections(conn: &Connection, command: &Document) -> Result<Document> {
    let db = command.get_str("$db").unwrap_or("test");
    if let Some(errmsg) = reject_unsupported_command_keys(
        command,
        &[
            "listCollections",
            "nameOnly",
            "authorizedCollections",
            "filter",
            "cursor",
            "$db",
            "lsid",
        ],
    ) {
        return Ok(command_error(72, &errmsg));
    }
    let name_only = match optional_bool(command, "nameOnly") {
        Ok(value) => value.unwrap_or(false),
        Err(errmsg) => return Ok(command_error(9, &errmsg)),
    };
    if let Err(errmsg) = optional_bool(command, "authorizedCollections") {
        return Ok(command_error(9, &errmsg));
    }
    match command.get("cursor") {
        None => {}
        Some(Bson::Document(cursor)) if cursor.is_empty() => {}
        Some(_) => {
            return Ok(command_error(
                9,
                "listCollections cursor must be an empty document",
            ));
        }
    }
    let filter_name = match command.get("filter") {
        None => None,
        Some(Bson::Document(filter)) if filter.is_empty() => None,
        Some(Bson::Document(filter)) if filter.len() == 1 => match filter.get("name") {
            Some(Bson::String(name)) => Some(name.clone()),
            _ => {
                return Ok(command_error(
                    2,
                    "listCollections filter only supports name equality",
                ));
            }
        },
        Some(Bson::Document(_)) => {
            return Ok(command_error(
                2,
                "listCollections filter only supports name equality",
            ));
        }
        Some(_) => {
            return Ok(command_error(
                9,
                "listCollections filter must be a document",
            ));
        }
    };

    let collections = collection_names_for_db(conn, db)?;
    let documents = collections
        .into_iter()
        .filter(|name| filter_name.as_ref().is_none_or(|filter| filter == name))
        .map(|name| {
            let mut doc = doc! {
                "name": name.clone(),
                "type": "collection",
            };
            if !name_only {
                doc.insert("options", Bson::Document(Document::new()));
                doc.insert("info", Bson::Document(doc! { "readOnly": false }));
                doc.insert(
                    "idIndex",
                    Bson::Document(doc! { "v": 2_i32, "key": { "_id": 1_i32 }, "name": "_id_" }),
                );
            }
            Bson::Document(doc)
        })
        .collect::<Vec<_>>();

    Ok(doc! {
        "cursor": {
            "id": 0_i64,
            "ns": namespace(db, "$cmd.listCollections"),
            "firstBatch": documents,
        },
        "ok": 1.0,
    })
}

fn drop_collection(conn: &Connection, command: &Document) -> Result<Document> {
    let db = command.get_str("$db").unwrap_or("test");
    let collection = match command.get_str("drop") {
        Ok(collection) if !collection.is_empty() => collection,
        _ => return Ok(command_error(9, "drop command requires a collection name")),
    };
    if let Some(errmsg) = reject_unsupported_command_keys(command, &["drop", "$db", "lsid"]) {
        return Ok(command_error(72, &errmsg));
    }

    let ns = namespace(db, collection);
    let tx = conn.unchecked_transaction()?;
    tx.execute("DELETE FROM documents WHERE namespace = ?1", params![ns])?;
    tx.execute("DELETE FROM collections WHERE namespace = ?1", params![ns])?;
    tx.execute("DELETE FROM indexes WHERE namespace = ?1", params![ns])?;
    tx.execute(
        "DELETE FROM index_entries WHERE namespace = ?1",
        params![ns],
    )?;
    tx.commit()?;

    Ok(doc! {
        "ns": ns,
        "nIndexesWas": 1_i32,
        "ok": 1.0,
    })
}

fn drop_database(conn: &Connection, command: &Document) -> Result<Document> {
    let db = command.get_str("$db").unwrap_or("test");
    if let Some(errmsg) =
        reject_unsupported_command_keys(command, &["dropDatabase", "comment", "$db", "lsid"])
    {
        return Ok(command_error(72, &errmsg));
    }

    let prefix = format!("{db}.%");
    let tx = conn.unchecked_transaction()?;
    tx.execute(
        "DELETE FROM documents WHERE namespace LIKE ?1",
        params![prefix],
    )?;
    tx.execute("DELETE FROM collections WHERE db = ?1", params![db])?;
    tx.execute(
        "DELETE FROM indexes WHERE namespace LIKE ?1",
        params![prefix],
    )?;
    tx.execute(
        "DELETE FROM index_entries WHERE namespace LIKE ?1",
        params![prefix],
    )?;
    tx.commit()?;

    Ok(doc! {
        "dropped": db,
        "ok": 1.0,
    })
}

fn count_documents_command(conn: &Connection, command: &Document) -> Result<Document> {
    let db = command.get_str("$db").unwrap_or("test");
    let collection = match command.get_str("count") {
        Ok(collection) if !collection.is_empty() => collection,
        _ => return Ok(command_error(9, "count command requires a collection name")),
    };
    if let Some(errmsg) = reject_unsupported_command_keys(
        command,
        &["count", "query", "skip", "limit", "$db", "lsid"],
    ) {
        return Ok(command_error(72, &errmsg));
    }
    let filter = match command.get("query") {
        None => Document::new(),
        Some(Bson::Document(filter)) => filter.clone(),
        Some(_) => return Ok(command_error(9, "count query must be a document")),
    };
    let skip = match optional_i64(command, "skip") {
        Ok(Some(value)) if value < 0 => return Ok(command_error(9, "skip must be non-negative")),
        Ok(Some(value)) => value as usize,
        Ok(None) => 0,
        Err(errmsg) => return Ok(command_error(9, &errmsg)),
    };
    let limit = match optional_i64(command, "limit") {
        Ok(Some(value)) if value < 0 => return Ok(command_error(9, "limit must be non-negative")),
        Ok(Some(0)) | Ok(None) => None,
        Ok(Some(value)) => Some(value as usize),
        Err(errmsg) => return Ok(command_error(9, &errmsg)),
    };

    let documents = documents_for_namespace(conn, &namespace(db, collection))?;
    let count = match count_matching_documents(documents, &filter, skip, limit) {
        Ok(count) => count,
        Err(err) => return Ok(command_error(err.code, &err.errmsg)),
    };
    Ok(doc! { "n": count, "ok": 1.0 })
}

fn distinct_command(conn: &Connection, command: &Document) -> Result<Document> {
    let db = command.get_str("$db").unwrap_or("test");
    let collection = match command.get_str("distinct") {
        Ok(collection) if !collection.is_empty() => collection,
        _ => {
            return Ok(command_error(
                9,
                "distinct command requires a collection name",
            ));
        }
    };
    let key = match command.get_str("key") {
        Ok(key) if !key.is_empty() => key,
        _ => return Ok(command_error(9, "distinct requires a non-empty key")),
    };
    if let Some(errmsg) =
        reject_unsupported_command_keys(command, &["distinct", "key", "query", "$db", "lsid"])
    {
        return Ok(command_error(72, &errmsg));
    }
    let filter = match command.get("query") {
        None => Document::new(),
        Some(Bson::Document(filter)) => filter.clone(),
        Some(_) => return Ok(command_error(9, "distinct query must be a document")),
    };

    let mut values = Vec::<Bson>::new();
    for document in documents_for_namespace(conn, &namespace(db, collection))? {
        match matches_filter(&document, &filter) {
            Ok(true) => {
                for value in distinct_values_at_path(&document, key) {
                    if !values
                        .iter()
                        .any(|existing| bson_values_equal(existing, &value))
                    {
                        values.push(value);
                    }
                }
            }
            Ok(false) => {}
            Err(err) => return Ok(command_error(err.code, &err.errmsg)),
        }
    }
    values.sort_by(compare_bson_order);
    Ok(doc! { "values": values, "ok": 1.0 })
}

fn aggregate_command(conn: &Connection, command: &Document) -> Result<Document> {
    let db = command.get_str("$db").unwrap_or("test");
    let collection = match command.get_str("aggregate") {
        Ok(collection) if !collection.is_empty() => collection,
        _ => {
            return Ok(command_error(
                9,
                "aggregate command requires a collection name",
            ));
        }
    };
    if let Some(errmsg) = reject_unsupported_command_keys(
        command,
        &["aggregate", "pipeline", "cursor", "$db", "lsid"],
    ) {
        return Ok(command_error(72, &errmsg));
    }
    match command.get("cursor") {
        Some(Bson::Document(_)) => {}
        _ => return Ok(command_error(9, "aggregate requires a cursor document")),
    }
    let pipeline = match command.get_array("pipeline") {
        Ok(pipeline) => pipeline,
        Err(_) => return Ok(command_error(9, "aggregate requires a pipeline array")),
    };
    let (filter, skip, limit) = match parse_count_documents_pipeline(pipeline) {
        Ok(parsed) => parsed,
        Err(response) => return Ok(response),
    };
    let documents = documents_for_namespace(conn, &namespace(db, collection))?;
    let count = match count_matching_documents(documents, &filter, skip, limit) {
        Ok(count) => count,
        Err(err) => return Ok(command_error(err.code, &err.errmsg)),
    };

    Ok(doc! {
        "cursor": {
            "id": 0_i64,
            "ns": namespace(db, collection),
            "firstBatch": [{ "_id": 1_i32, "n": count }],
        },
        "ok": 1.0,
    })
}

fn parse_count_documents_pipeline(
    pipeline: &[Bson],
) -> std::result::Result<(Document, usize, Option<usize>), Document> {
    let mut filter = Document::new();
    let mut skip = 0_usize;
    let mut limit = None;
    let mut saw_group = false;

    for stage in pipeline {
        let Bson::Document(stage) = stage else {
            return Err(command_error(
                9,
                "aggregate pipeline stages must be documents",
            ));
        };
        if stage.len() != 1 {
            return Err(command_error(
                72,
                "aggregate stages must contain one operator",
            ));
        }
        if let Some(Bson::Document(match_filter)) = stage.get("$match") {
            if saw_group {
                return Err(command_error(
                    72,
                    "$match after count group is not supported",
                ));
            }
            filter = match_filter.clone();
        } else if let Some(skip_value) = stage.get("$skip") {
            if saw_group {
                return Err(command_error(
                    72,
                    "$skip after count group is not supported",
                ));
            }
            skip = match non_negative_stage_usize(skip_value, "$skip") {
                Ok(value) => value,
                Err(err) => return Err(err),
            };
        } else if let Some(limit_value) = stage.get("$limit") {
            if saw_group {
                return Err(command_error(
                    72,
                    "$limit after count group is not supported",
                ));
            }
            limit = Some(match non_negative_stage_usize(limit_value, "$limit") {
                Ok(value) => value,
                Err(err) => return Err(err),
            });
        } else if let Some(Bson::Document(group)) = stage.get("$group") {
            if is_count_documents_group(group) {
                saw_group = true;
            } else {
                return Err(command_error(
                    72,
                    "aggregate only supports PyMongo count_documents group shape",
                ));
            }
        } else {
            return Err(command_error(
                72,
                "aggregate only supports count_documents pipeline stages",
            ));
        }
    }

    if !saw_group {
        return Err(command_error(
            72,
            "aggregate only supports count_documents pipeline",
        ));
    }

    Ok((filter, skip, limit))
}

fn find_and_modify(conn: &Connection, command_key: &str, command: &Document) -> Result<Document> {
    let db = command.get_str("$db").unwrap_or("test");
    let collection = match command.get_str(command_key) {
        Ok(collection) if !collection.is_empty() => collection,
        _ => {
            return Ok(command_error(
                9,
                "findAndModify command requires a collection name",
            ));
        }
    };
    if let Some(errmsg) = reject_unsupported_command_keys(
        command,
        &[
            "findAndModify",
            "findandmodify",
            "query",
            "sort",
            "remove",
            "update",
            "new",
            "upsert",
            "fields",
            "projection",
            "$db",
            "lsid",
        ],
    ) {
        return Ok(command_error(72, &errmsg));
    }

    let query = match optional_document(command, "query") {
        Ok(Some(query)) => query,
        Ok(None) => Document::new(),
        Err(errmsg) => return Ok(command_error(9, &errmsg)),
    };
    let sort = match parse_sort(command) {
        Ok(sort) => sort,
        Err(errmsg) => return Ok(command_error(2, &errmsg)),
    };
    let projection = match parse_find_and_modify_projection(command) {
        Ok(projection) => projection,
        Err(errmsg) => return Ok(command_error(2, &errmsg)),
    };
    let remove = match optional_bool(command, "remove") {
        Ok(value) => value.unwrap_or(false),
        Err(errmsg) => return Ok(command_error(9, &errmsg)),
    };
    let return_new = match optional_bool(command, "new") {
        Ok(value) => value.unwrap_or(false),
        Err(errmsg) => return Ok(command_error(9, &errmsg)),
    };
    let upsert = match optional_bool(command, "upsert") {
        Ok(value) => value.unwrap_or(false),
        Err(errmsg) => return Ok(command_error(9, &errmsg)),
    };

    if remove && command.contains_key("update") {
        return Ok(command_error(
            9,
            "findAndModify cannot combine remove and update",
        ));
    }
    if remove && upsert {
        return Ok(command_error(
            9,
            "findAndModify cannot combine remove and upsert",
        ));
    }
    if !remove && !command.contains_key("update") {
        return Ok(command_error(9, "findAndModify requires remove or update"));
    }

    let update = if remove {
        None
    } else {
        match command.get("update") {
            Some(Bson::Document(update)) => match classify_update(update) {
                Ok(update) => Some(update),
                Err(errmsg) => return Ok(command_error(update_error_code(&errmsg), &errmsg)),
            },
            Some(Bson::Array(_)) => {
                return Ok(command_error(
                    72,
                    "findAndModify pipeline updates are not supported",
                ));
            }
            Some(_) => return Ok(command_error(9, "findAndModify update must be a document")),
            None => unreachable!("update presence checked above"),
        }
    };

    let namespace = namespace(db, collection);
    let tx = conn.unchecked_transaction()?;
    ensure_collection_catalog_tx(&tx, &namespace)?;
    let outcome = if remove {
        apply_find_and_modify_remove(&tx, &namespace, &query, sort.as_deref())?
    } else {
        apply_find_and_modify_update(
            &tx,
            &namespace,
            &query,
            sort.as_deref(),
            update.as_ref().expect("update parsed above"),
            upsert,
            return_new,
        )?
    };
    let outcome = match outcome {
        Ok(outcome) => outcome,
        Err(response) => return Ok(response),
    };
    tx.commit()?;

    let value = outcome
        .value
        .map(|document| {
            projection
                .as_ref()
                .map(|projection| apply_projection(&document, projection))
                .unwrap_or(document)
        })
        .map(Bson::Document)
        .unwrap_or(Bson::Null);
    let mut last_error = doc! {
        "n": outcome.n,
    };
    if let Some(updated_existing) = outcome.updated_existing {
        last_error.insert("updatedExisting", updated_existing);
    }
    if let Some(upserted) = outcome.upserted {
        last_error.insert("upserted", upserted);
    }

    Ok(doc! {
        "value": value,
        "lastErrorObject": last_error,
        "ok": 1.0,
    })
}

struct FindAndModifyOutcome {
    value: Option<Document>,
    n: i32,
    updated_existing: Option<bool>,
    upserted: Option<Bson>,
}

fn apply_find_and_modify_remove(
    tx: &rusqlite::Transaction<'_>,
    namespace: &str,
    query: &Document,
    sort: Option<&[(String, i32)]>,
) -> Result<std::result::Result<FindAndModifyOutcome, Document>> {
    let Some(target) = (match find_and_modify_target_tx(tx, namespace, query, sort)? {
        Ok(target) => target,
        Err(response) => return Ok(Err(response)),
    }) else {
        return Ok(Ok(FindAndModifyOutcome {
            value: None,
            n: 0,
            updated_existing: None,
            upserted: None,
        }));
    };

    delete_index_entries_for_document_tx(tx, namespace, &target.id_key)?;
    tx.execute(
        "DELETE FROM documents WHERE namespace = ?1 AND id_key = ?2",
        params![namespace, target.id_key],
    )?;
    Ok(Ok(FindAndModifyOutcome {
        value: Some(target.document),
        n: 1,
        updated_existing: None,
        upserted: None,
    }))
}

fn apply_find_and_modify_update(
    tx: &rusqlite::Transaction<'_>,
    namespace: &str,
    query: &Document,
    sort: Option<&[(String, i32)]>,
    update: &UpdateSpec,
    upsert: bool,
    return_new: bool,
) -> Result<std::result::Result<FindAndModifyOutcome, Document>> {
    let Some(target) = (match find_and_modify_target_tx(tx, namespace, query, sort)? {
        Ok(target) => target,
        Err(response) => return Ok(Err(response)),
    }) else {
        if !upsert {
            return Ok(Ok(FindAndModifyOutcome {
                value: None,
                n: 0,
                updated_existing: Some(false),
                upserted: None,
            }));
        }

        let mut new_document = match build_upsert_document(query, update) {
            Ok(document) => document,
            Err(errmsg) => return Ok(Err(command_error(update_error_code(&errmsg), &errmsg))),
        };
        ensure_document_id(&mut new_document);
        let upserted_id = new_document.get("_id").cloned().ok_or_else(|| {
            MongolinoError::Protocol("upsert document is missing _id".to_string())
        })?;
        if let Err(errmsg) = ensure_unique_constraints_tx(tx, namespace, &new_document, None) {
            return Ok(Err(command_error(update_error_code(&errmsg), &errmsg)));
        }
        if let Err(err) = insert_stored_document_tx(tx, namespace, &new_document) {
            let errmsg = duplicate_or_sql_error(namespace, &new_document, err);
            return Ok(Err(command_error(update_error_code(&errmsg), &errmsg)));
        }
        refresh_index_entries_for_document_tx(
            tx,
            namespace,
            &id_key_from_bson(&upserted_id),
            &new_document,
        )?;
        return Ok(Ok(FindAndModifyOutcome {
            value: return_new.then_some(new_document),
            n: 1,
            updated_existing: Some(false),
            upserted: Some(upserted_id),
        }));
    };

    let new_document = match apply_update_to_document(&target.document, update) {
        Ok(document) => document,
        Err(errmsg) => {
            return Ok(Err(command_error(update_error_code(&errmsg), &errmsg)));
        }
    };
    let new_id_key = match id_key(&new_document) {
        Ok(id_key) => id_key,
        Err(err) => return Err(err),
    };
    if new_id_key != target.id_key {
        let errmsg = "update cannot change _id";
        return Ok(Err(command_error(update_error_code(errmsg), errmsg)));
    }
    if new_document != target.document {
        if let Err(errmsg) =
            ensure_unique_constraints_tx(tx, namespace, &new_document, Some(&target.id_key))
        {
            return Ok(Err(command_error(update_error_code(&errmsg), &errmsg)));
        }
        if let Err(err) = update_stored_document_tx(tx, namespace, &target.id_key, &new_document) {
            let errmsg = duplicate_or_sql_error(namespace, &new_document, err);
            return Ok(Err(command_error(update_error_code(&errmsg), &errmsg)));
        }
        refresh_index_entries_for_document_tx(tx, namespace, &target.id_key, &new_document)?;
    }

    Ok(Ok(FindAndModifyOutcome {
        value: Some(if return_new {
            new_document
        } else {
            target.document
        }),
        n: 1,
        updated_existing: Some(true),
        upserted: None,
    }))
}

fn find_and_modify_target_tx(
    tx: &rusqlite::Transaction<'_>,
    namespace: &str,
    query: &Document,
    sort: Option<&[(String, i32)]>,
) -> Result<std::result::Result<Option<StoredDocument>, Document>> {
    let mut matches = Vec::new();
    for stored in stored_documents_for_namespace_tx(tx, namespace)? {
        match matches_filter(&stored.document, query) {
            Ok(true) => matches.push(stored),
            Ok(false) => {}
            Err(err) => return Ok(Err(command_error(err.code, &err.errmsg))),
        }
    }
    if let Some(sort) = sort {
        sort_stored_documents(&mut matches, sort);
    }
    Ok(Ok(matches.into_iter().next()))
}

fn sort_stored_documents(documents: &mut [StoredDocument], sort: &[(String, i32)]) {
    documents
        .sort_by(|left, right| compare_documents_for_sort(&left.document, &right.document, sort));
}

fn non_negative_stage_usize(value: &Bson, operator: &str) -> std::result::Result<usize, Document> {
    match value {
        Bson::Int32(value) if *value >= 0 => Ok(*value as usize),
        Bson::Int64(value) if *value >= 0 => Ok(*value as usize),
        _ => Err(command_error(
            9,
            &format!("{operator} requires a non-negative integer"),
        )),
    }
}

fn is_count_documents_group(group: &Document) -> bool {
    matches!(group.get("_id"), Some(Bson::Int32(1) | Bson::Int64(1)))
        && matches!(
            group.get("n"),
            Some(Bson::Document(sum)) if matches!(sum.get("$sum"), Some(Bson::Int32(1) | Bson::Int64(1)))
        )
}

fn count_matching_documents(
    documents: Vec<Document>,
    filter: &Document,
    skip: usize,
    limit: Option<usize>,
) -> MatchResult<i64> {
    let mut matched = 0_i64;
    let mut skipped = 0_usize;
    for document in documents {
        match matches_filter(&document, filter) {
            Ok(true) if skipped < skip => skipped += 1,
            Ok(true) => {
                matched += 1;
                if limit.is_some_and(|limit| matched as usize >= limit) {
                    break;
                }
            }
            Ok(false) => {}
            Err(err) => return Err(err),
        }
    }
    Ok(matched)
}

fn distinct_values_at_path(document: &Document, path: &str) -> Vec<Bson> {
    values_at_path(document, path)
        .into_iter()
        .flat_map(|value| match value {
            Bson::Array(values) => values.clone(),
            value => vec![value.clone()],
        })
        .collect()
}

#[derive(Clone, Debug)]
struct IndexSpec {
    name: String,
    key: Document,
    unique: bool,
}

fn create_indexes(conn: &Connection, command: &Document) -> Result<Document> {
    let db = command.get_str("$db").unwrap_or("test");
    let collection = match command.get_str("createIndexes") {
        Ok(collection) if !collection.is_empty() => collection,
        _ => return Ok(command_error(9, "createIndexes requires a collection name")),
    };
    let index_values = match command.get_array("indexes") {
        Ok(values) if !values.is_empty() => values,
        Ok(_) => {
            return Ok(command_error(
                9,
                "createIndexes requires a non-empty indexes array",
            ));
        }
        Err(_) => return Ok(command_error(9, "createIndexes requires an indexes array")),
    };
    if let Some(errmsg) =
        reject_unsupported_command_keys(command, &["createIndexes", "indexes", "$db", "lsid"])
    {
        return Ok(command_error(72, &errmsg));
    }

    let namespace = namespace(db, collection);
    let mut specs = Vec::new();
    for value in index_values {
        let Bson::Document(index) = value else {
            return Ok(command_error(9, "index specs must be documents"));
        };
        match parse_index_spec(index) {
            Ok(spec) => specs.push(spec),
            Err(response) => return Ok(response),
        }
    }

    let tx = conn.unchecked_transaction()?;
    ensure_collection_catalog_tx(&tx, &namespace)?;
    let before = index_count_tx(&tx, &namespace)? + 1;
    for spec in &specs {
        if spec.name == "_id_" {
            return Ok(command_error(67, "cannot create explicit _id_ index"));
        }
        if let Some(existing) = index_by_name_tx(&tx, &namespace, &spec.name)? {
            if existing.key == spec.key && existing.unique == spec.unique {
                continue;
            }
            return Ok(command_error(
                85,
                "index already exists with a different specification",
            ));
        }
        if spec.unique
            && let Err(errmsg) = validate_unique_index_tx(&tx, &namespace, spec)
        {
            let code = if errmsg.starts_with("duplicate key error") {
                11000
            } else {
                72
            };
            return Ok(command_error(code, &errmsg));
        }
        insert_index_tx(&tx, &namespace, spec)?;
        rebuild_index_entries_tx(&tx, &namespace, spec)?;
    }
    let after = index_count_tx(&tx, &namespace)? + 1;
    tx.commit()?;

    Ok(doc! {
        "numIndexesBefore": before,
        "numIndexesAfter": after,
        "createdCollectionAutomatically": false,
        "ok": 1.0,
    })
}

fn list_indexes(conn: &Connection, command: &Document) -> Result<Document> {
    let db = command.get_str("$db").unwrap_or("test");
    let collection = match command.get_str("listIndexes") {
        Ok(collection) if !collection.is_empty() => collection,
        _ => return Ok(command_error(9, "listIndexes requires a collection name")),
    };
    if let Some(errmsg) =
        reject_unsupported_command_keys(command, &["listIndexes", "cursor", "$db", "lsid"])
    {
        return Ok(command_error(72, &errmsg));
    }
    match command.get("cursor") {
        None => {}
        Some(Bson::Document(_)) => {}
        Some(_) => return Ok(command_error(9, "listIndexes cursor must be a document")),
    }

    let ns = namespace(db, collection);
    let mut batch = vec![Bson::Document(doc! {
        "v": 2_i32,
        "key": { "_id": 1_i32 },
        "name": "_id_",
    })];
    for spec in indexes_for_namespace(conn, &ns)? {
        let mut document = doc! {
            "v": 2_i32,
            "key": spec.key,
            "name": spec.name,
        };
        if spec.unique {
            document.insert("unique", true);
        }
        batch.push(Bson::Document(document));
    }

    Ok(doc! {
        "cursor": {
            "id": 0_i64,
            "ns": namespace(db, collection),
            "firstBatch": batch,
        },
        "ok": 1.0,
    })
}

fn drop_indexes(conn: &Connection, command: &Document) -> Result<Document> {
    let db = command.get_str("$db").unwrap_or("test");
    let collection = match command.get_str("dropIndexes") {
        Ok(collection) if !collection.is_empty() => collection,
        _ => return Ok(command_error(9, "dropIndexes requires a collection name")),
    };
    if let Some(errmsg) =
        reject_unsupported_command_keys(command, &["dropIndexes", "index", "$db", "lsid"])
    {
        return Ok(command_error(72, &errmsg));
    }
    let index = match command.get("index") {
        Some(Bson::String(name)) if !name.is_empty() => name.clone(),
        Some(Bson::Document(key)) => generated_index_name(key),
        _ => {
            return Ok(command_error(
                9,
                "dropIndexes requires an index name or key document",
            ));
        }
    };
    if index == "_id_" {
        return Ok(command_error(67, "cannot drop _id_ index"));
    }

    let ns = namespace(db, collection);
    let tx = conn.unchecked_transaction()?;
    let before = index_count_tx(&tx, &ns)? + 1;
    let removed = if index == "*" {
        tx.execute(
            "DELETE FROM index_entries WHERE namespace = ?1",
            params![ns],
        )?;
        tx.execute("DELETE FROM indexes WHERE namespace = ?1", params![ns])?
    } else {
        tx.execute(
            "DELETE FROM index_entries WHERE namespace = ?1 AND index_name = ?2",
            params![ns, index],
        )?;
        tx.execute(
            "DELETE FROM indexes WHERE namespace = ?1 AND name = ?2",
            params![ns, index],
        )?
    };
    if removed == 0 && index != "*" {
        return Ok(command_error(27, "index not found"));
    }
    let after = index_count_tx(&tx, &ns)? + 1;
    tx.commit()?;

    Ok(doc! {
        "nIndexesWas": before,
        "numIndexesBefore": before,
        "numIndexesAfter": after,
        "ok": 1.0,
    })
}

fn parse_index_spec(index: &Document) -> std::result::Result<IndexSpec, Document> {
    if let Some(errmsg) = reject_unsupported_command_keys(index, &["key", "name", "unique", "v"]) {
        return Err(command_error(72, &errmsg));
    }
    let key = match index.get_document("key") {
        Ok(key) if !key.is_empty() => key.clone(),
        _ => {
            return Err(command_error(
                9,
                "index spec requires a non-empty key document",
            ));
        }
    };
    validate_index_key(&key)?;
    let name = match index.get_str("name") {
        Ok(name) if !name.is_empty() => name.to_string(),
        Ok(_) => return Err(command_error(9, "index name must not be empty")),
        Err(_) => generated_index_name(&key),
    };
    let unique = match index.get("unique") {
        None => false,
        Some(Bson::Boolean(value)) => *value,
        Some(_) => return Err(command_error(9, "unique must be a boolean")),
    };
    match index.get("v") {
        None | Some(Bson::Int32(2) | Bson::Int64(2)) => {}
        Some(_) => return Err(command_error(72, "only index version 2 is supported")),
    }

    Ok(IndexSpec { name, key, unique })
}

fn validate_index_key(key: &Document) -> std::result::Result<(), Document> {
    for (field, direction) in key {
        if field.is_empty()
            || field.starts_with('$')
            || field.split('.').any(|part| part.is_empty())
        {
            return Err(command_error(
                9,
                "index field names must be non-empty paths",
            ));
        }
        match direction {
            Bson::Int32(1) | Bson::Int64(1) | Bson::Int32(-1) | Bson::Int64(-1) => {}
            Bson::String(kind) => {
                return Err(command_error(
                    72,
                    &format!("{kind} indexes are not supported"),
                ));
            }
            _ => return Err(command_error(72, "index directions must be 1 or -1")),
        }
    }
    Ok(())
}

fn generated_index_name(key: &Document) -> String {
    key.iter()
        .map(|(field, direction)| {
            let direction = match direction {
                Bson::Int32(value) => *value as i64,
                Bson::Int64(value) => *value,
                _ => 1,
            };
            format!("{field}_{direction}")
        })
        .collect::<Vec<_>>()
        .join("_")
}

fn insert_index_tx(
    tx: &rusqlite::Transaction<'_>,
    namespace: &str,
    spec: &IndexSpec,
) -> std::result::Result<(), rusqlite::Error> {
    tx.execute(
        "INSERT INTO indexes(namespace, name, key_bson, unique_index) VALUES (?1, ?2, ?3, ?4)",
        params![
            namespace,
            spec.name,
            encode_document(&spec.key)?,
            if spec.unique { 1_i32 } else { 0_i32 }
        ],
    )?;
    Ok(())
}

fn index_by_name_tx(
    tx: &rusqlite::Transaction<'_>,
    namespace: &str,
    name: &str,
) -> Result<Option<IndexSpec>> {
    tx.query_row(
        "SELECT name, key_bson, unique_index FROM indexes WHERE namespace = ?1 AND name = ?2",
        params![namespace, name],
        |row| {
            let name = row.get::<_, String>(0)?;
            let key = decode_document_sql(row.get::<_, Vec<u8>>(1)?)?;
            let unique = row.get::<_, i32>(2)? != 0;
            Ok(IndexSpec { name, key, unique })
        },
    )
    .optional()
    .map_err(Into::into)
}

fn indexes_for_namespace(conn: &Connection, namespace: &str) -> Result<Vec<IndexSpec>> {
    let mut stmt = conn.prepare(
        "SELECT name, key_bson, unique_index FROM indexes WHERE namespace = ?1 ORDER BY name",
    )?;
    stmt.query_map(params![namespace], |row| {
        let name = row.get::<_, String>(0)?;
        let key = decode_document_sql(row.get::<_, Vec<u8>>(1)?)?;
        let unique = row.get::<_, i32>(2)? != 0;
        Ok(IndexSpec { name, key, unique })
    })?
    .collect::<std::result::Result<Vec<_>, _>>()
    .map_err(Into::into)
}

fn index_count_tx(tx: &rusqlite::Transaction<'_>, namespace: &str) -> Result<i32> {
    Ok(tx.query_row(
        "SELECT COUNT(*) FROM indexes WHERE namespace = ?1",
        params![namespace],
        |row| row.get::<_, i32>(0),
    )?)
}

fn unique_indexes_for_namespace_tx(
    tx: &rusqlite::Transaction<'_>,
    namespace: &str,
) -> Result<Vec<IndexSpec>> {
    let mut stmt = tx.prepare(
        "SELECT name, key_bson, unique_index FROM indexes WHERE namespace = ?1 AND unique_index = 1 ORDER BY name",
    )?;
    stmt.query_map(params![namespace], |row| {
        let name = row.get::<_, String>(0)?;
        let key = decode_document_sql(row.get::<_, Vec<u8>>(1)?)?;
        let unique = row.get::<_, i32>(2)? != 0;
        Ok(IndexSpec { name, key, unique })
    })?
    .collect::<std::result::Result<Vec<_>, _>>()
    .map_err(Into::into)
}

fn validate_unique_index_tx(
    tx: &rusqlite::Transaction<'_>,
    namespace: &str,
    spec: &IndexSpec,
) -> std::result::Result<(), String> {
    let mut seen = HashMap::new();
    for stored in stored_documents_for_namespace_tx(tx, namespace).map_err(|err| err.to_string())? {
        let key = unique_key_for_document(spec, &stored.document)?;
        if let Some(existing_id) = seen.insert(key.clone(), stored.id_key.clone()) {
            return Err(format!(
                "duplicate key error collection: {namespace} index: {} dup key: {key} existing _id: {existing_id}",
                spec.name
            ));
        }
    }
    Ok(())
}

fn ensure_unique_constraints_tx(
    tx: &rusqlite::Transaction<'_>,
    namespace: &str,
    document: &Document,
    excluding_id_key: Option<&str>,
) -> std::result::Result<(), String> {
    let indexes = unique_indexes_for_namespace_tx(tx, namespace).map_err(|err| err.to_string())?;
    if indexes.is_empty() {
        return Ok(());
    }

    for index in indexes {
        let wanted_key = unique_key_for_document(&index, document)?;
        for stored in
            stored_documents_for_namespace_tx(tx, namespace).map_err(|err| err.to_string())?
        {
            if excluding_id_key.is_some_and(|id_key| id_key == stored.id_key) {
                continue;
            }
            let existing_key = unique_key_for_document(&index, &stored.document)?;
            if existing_key == wanted_key {
                return Err(format!(
                    "duplicate key error collection: {namespace} index: {} dup key: {wanted_key}",
                    index.name
                ));
            }
        }
    }
    Ok(())
}

fn unique_key_for_document(
    index: &IndexSpec,
    document: &Document,
) -> std::result::Result<String, String> {
    let mut parts = Vec::new();
    for field in index.key.keys() {
        let values = values_at_path(document, field);
        let value = match values.as_slice() {
            [] => Bson::Null,
            [value] if matches!(value, Bson::Array(_)) => {
                return Err(format!(
                    "unique index {} does not support array value at {field}",
                    index.name
                ));
            }
            [value] => (*value).clone(),
            _ => {
                return Err(format!(
                    "unique index {} does not support multikey path {field}",
                    index.name
                ));
            }
        };
        parts.push(format!("{field}:{}", id_key_from_bson(&value)));
    }
    Ok(parts.join("|"))
}

fn rebuild_index_entries_tx(
    tx: &rusqlite::Transaction<'_>,
    namespace: &str,
    spec: &IndexSpec,
) -> std::result::Result<(), rusqlite::Error> {
    tx.execute(
        "DELETE FROM index_entries WHERE namespace = ?1 AND index_name = ?2",
        params![namespace, spec.name],
    )?;
    for stored in stored_documents_for_namespace_tx(tx, namespace).map_err(sql_string_error)? {
        insert_index_entry_for_document_tx(tx, namespace, spec, &stored.id_key, &stored.document)?;
    }
    Ok(())
}

fn refresh_index_entries_for_document_tx(
    tx: &rusqlite::Transaction<'_>,
    namespace: &str,
    id_key: &str,
    document: &Document,
) -> std::result::Result<(), rusqlite::Error> {
    tx.execute(
        "DELETE FROM index_entries WHERE namespace = ?1 AND id_key = ?2",
        params![namespace, id_key],
    )?;
    let indexes = indexes_for_namespace_tx(tx, namespace).map_err(sql_string_error)?;
    for spec in indexes {
        insert_index_entry_for_document_tx(tx, namespace, &spec, id_key, document)?;
    }
    Ok(())
}

fn delete_index_entries_for_document_tx(
    tx: &rusqlite::Transaction<'_>,
    namespace: &str,
    id_key: &str,
) -> std::result::Result<(), rusqlite::Error> {
    tx.execute(
        "DELETE FROM index_entries WHERE namespace = ?1 AND id_key = ?2",
        params![namespace, id_key],
    )?;
    Ok(())
}

fn insert_index_entry_for_document_tx(
    tx: &rusqlite::Transaction<'_>,
    namespace: &str,
    spec: &IndexSpec,
    id_key: &str,
    document: &Document,
) -> std::result::Result<(), rusqlite::Error> {
    let Some(key_value) = planner_key_for_document(spec, document) else {
        return Ok(());
    };
    tx.execute(
        "INSERT OR IGNORE INTO index_entries(namespace, index_name, key_value, id_key) VALUES (?1, ?2, ?3, ?4)",
        params![namespace, spec.name, key_value, id_key],
    )?;
    Ok(())
}

fn planner_key_for_document(spec: &IndexSpec, document: &Document) -> Option<String> {
    let field = single_field_index_name(spec)?;
    let value = get_document_path(document, field)?;
    if matches!(value, Bson::Array(_)) {
        return None;
    }
    Some(id_key_from_bson(value))
}

fn single_field_index_name(spec: &IndexSpec) -> Option<&str> {
    if spec.key.len() == 1 {
        spec.key.keys().next().map(String::as_str)
    } else {
        None
    }
}

fn indexes_for_namespace_tx(
    tx: &rusqlite::Transaction<'_>,
    namespace: &str,
) -> Result<Vec<IndexSpec>> {
    let mut stmt = tx.prepare(
        "SELECT name, key_bson, unique_index FROM indexes WHERE namespace = ?1 ORDER BY name",
    )?;
    stmt.query_map(params![namespace], |row| {
        let name = row.get::<_, String>(0)?;
        let key = decode_document_sql(row.get::<_, Vec<u8>>(1)?)?;
        let unique = row.get::<_, i32>(2)? != 0;
        Ok(IndexSpec { name, key, unique })
    })?
    .collect::<std::result::Result<Vec<_>, _>>()
    .map_err(Into::into)
}

fn sql_string_error(err: MongolinoError) -> rusqlite::Error {
    rusqlite::Error::ToSqlConversionFailure(Box::new(std::io::Error::other(err.to_string())))
}

fn insert_collection_catalog(
    conn: &Connection,
    db: &str,
    collection: &str,
) -> std::result::Result<(), rusqlite::Error> {
    let ns = namespace(db, collection);
    conn.execute(
        "INSERT INTO collections(namespace, db, name) VALUES (?1, ?2, ?3)",
        params![ns, db, collection],
    )?;
    Ok(())
}

fn ensure_collection_catalog_tx(
    tx: &rusqlite::Transaction<'_>,
    namespace: &str,
) -> std::result::Result<(), rusqlite::Error> {
    let Some((db, collection)) = namespace.split_once('.') else {
        return Ok(());
    };
    tx.execute(
        "INSERT OR IGNORE INTO collections(namespace, db, name) VALUES (?1, ?2, ?3)",
        params![namespace, db, collection],
    )?;
    Ok(())
}

fn collection_exists(conn: &Connection, namespace: &str) -> Result<bool> {
    let exists = conn
        .query_row(
            "SELECT 1 FROM collections WHERE namespace = ?1",
            params![namespace],
            |_| Ok(()),
        )
        .optional()?
        .is_some();
    Ok(exists)
}

fn collection_names_for_db(conn: &Connection, db: &str) -> Result<Vec<String>> {
    let mut stmt = conn.prepare(
        r#"
        SELECT name FROM collections WHERE db = ?1
        UNION
        SELECT DISTINCT substr(namespace, length(?1) + 2)
          FROM documents
         WHERE namespace LIKE ?2
        ORDER BY 1
        "#,
    )?;
    let prefix = format!("{db}.%");
    let names = stmt
        .query_map(params![db, prefix], |row| row.get::<_, String>(0))?
        .collect::<std::result::Result<Vec<_>, _>>()?;
    Ok(names)
}

fn insert_documents(conn: &Connection, command: &Document) -> Result<Document> {
    let db = command.get_str("$db").unwrap_or("test");
    let collection = match command.get_str("insert") {
        Ok(collection) if !collection.is_empty() => collection,
        _ => {
            return Ok(command_error(
                9,
                "insert command requires a collection name",
            ));
        }
    };
    let documents = match command.get_array("documents") {
        Ok(documents) if !documents.is_empty() => documents,
        Ok(_) => {
            return Ok(command_error(
                9,
                "insert command requires a non-empty documents array",
            ));
        }
        Err(_) => {
            return Ok(command_error(
                9,
                "insert command requires a documents array",
            ));
        }
    };
    let ordered = match optional_bool(command, "ordered") {
        Ok(value) => value.unwrap_or(true),
        Err(errmsg) => return Ok(command_error(9, &errmsg)),
    };
    if let Some(errmsg) =
        reject_unsupported_command_keys(command, &["insert", "documents", "ordered", "$db", "lsid"])
    {
        return Ok(command_error(72, &errmsg));
    }
    let namespace = namespace(db, collection);
    let mut prepared = Vec::with_capacity(documents.len());

    for value in documents {
        let Bson::Document(original) = value else {
            return Ok(command_error(2, "insert documents must be BSON documents"));
        };
        let mut document = original.clone();
        ensure_document_id(&mut document);
        let id_key = id_key(&document)?;
        let mut encoded = Vec::new();
        document.to_writer(&mut encoded)?;
        prepared.push((id_key, encoded, document));
    }

    let tx = conn.unchecked_transaction()?;
    let mut inserted = 0_i32;
    let mut write_errors = Vec::new();
    ensure_collection_catalog_tx(&tx, &namespace)?;

    {
        let mut stmt = tx.prepare(
            "INSERT INTO documents(namespace, id_key, bson, updated_at)
             VALUES (?1, ?2, ?3, CURRENT_TIMESTAMP)",
        )?;

        for (index, (id_key, encoded, document)) in prepared.iter().enumerate() {
            if let Err(errmsg) = ensure_unique_constraints_tx(&tx, &namespace, document, None) {
                write_errors.push(write_error(
                    index as i32,
                    unique_write_error_code(&errmsg),
                    &errmsg,
                ));
                if ordered {
                    break;
                }
                continue;
            }
            match stmt.execute(params![namespace, id_key, encoded]) {
                Ok(_) => {
                    refresh_index_entries_for_document_tx(&tx, &namespace, id_key, document)?;
                    inserted += 1;
                }
                Err(err) if is_sqlite_constraint(&err) => {
                    write_errors.push(write_error(
                        index as i32,
                        11000,
                        &format!("duplicate key error collection: {namespace} _id: {id_key}"),
                    ));
                    if ordered {
                        break;
                    }
                }
                Err(err) => return Err(err.into()),
            }
        }
    }

    tx.commit()?;
    let mut response = doc! {
        "n": inserted,
        "ok": 1.0,
    };
    if !write_errors.is_empty() {
        response.insert("writeErrors", write_errors);
    }
    Ok(response)
}

fn optional_bool(command: &Document, key: &str) -> std::result::Result<Option<bool>, String> {
    match command.get(key) {
        None => Ok(None),
        Some(Bson::Boolean(value)) => Ok(Some(*value)),
        Some(_) => Err(format!("{key} must be a boolean")),
    }
}

fn reject_unsupported_command_keys(command: &Document, allowed: &[&str]) -> Option<String> {
    command
        .keys()
        .find(|key| !allowed.contains(&key.as_str()))
        .map(|key| format!("{key} is not supported for this command"))
}

fn is_sqlite_constraint(err: &rusqlite::Error) -> bool {
    matches!(
        err,
        rusqlite::Error::SqliteFailure(sqlite_err, _)
            if sqlite_err.code == rusqlite::ErrorCode::ConstraintViolation
    )
}

fn write_error(index: i32, code: i32, errmsg: &str) -> Bson {
    Bson::Document(doc! {
        "index": index,
        "code": code,
        "errmsg": errmsg,
    })
}

fn update_documents(conn: &Connection, command: &Document) -> Result<Document> {
    let db = command.get_str("$db").unwrap_or("test");
    let collection = match command.get_str("update") {
        Ok(collection) if !collection.is_empty() => collection,
        _ => {
            return Ok(command_error(
                9,
                "update command requires a collection name",
            ));
        }
    };
    let updates = match command.get_array("updates") {
        Ok(updates) if !updates.is_empty() => updates,
        Ok(_) => {
            return Ok(command_error(
                9,
                "update command requires a non-empty updates array",
            ));
        }
        Err(_) => return Ok(command_error(9, "update command requires an updates array")),
    };
    let ordered = match optional_bool(command, "ordered") {
        Ok(value) => value.unwrap_or(true),
        Err(errmsg) => return Ok(command_error(9, &errmsg)),
    };
    if let Some(errmsg) =
        reject_unsupported_command_keys(command, &["update", "updates", "ordered", "$db", "lsid"])
    {
        return Ok(command_error(72, &errmsg));
    }

    let namespace = namespace(db, collection);
    let tx = conn.unchecked_transaction()?;
    let mut matched_count = 0_i32;
    let mut modified_count = 0_i32;
    let mut upserted_count = 0_i32;
    let mut upserted = Vec::new();
    let mut write_errors = Vec::new();
    ensure_collection_catalog_tx(&tx, &namespace)?;

    for (index, entry) in updates.iter().enumerate() {
        let result = apply_update_entry(&tx, &namespace, entry);
        match result {
            Ok(outcome) => {
                matched_count += outcome.matched;
                modified_count += outcome.modified;
                if let Some(id) = outcome.upserted_id {
                    upserted_count += 1;
                    upserted.push(Bson::Document(doc! {
                        "index": index as i32,
                        "_id": id,
                    }));
                    matched_count += 1;
                }
            }
            Err(errmsg) => {
                write_errors.push(write_error(
                    index as i32,
                    update_error_code(&errmsg),
                    &errmsg,
                ));
                if ordered {
                    break;
                }
            }
        }
    }

    tx.commit()?;
    let mut response = doc! {
        "n": matched_count,
        "nModified": modified_count,
        "ok": 1.0,
    };
    if upserted_count > 0 {
        response.insert("nUpserted", upserted_count);
        response.insert("upserted", upserted);
    }
    if !write_errors.is_empty() {
        response.insert("writeErrors", write_errors);
    }
    Ok(response)
}

#[derive(Debug)]
struct UpdateOutcome {
    matched: i32,
    modified: i32,
    upserted_id: Option<Bson>,
}

fn apply_update_entry(
    tx: &rusqlite::Transaction<'_>,
    namespace: &str,
    entry: &Bson,
) -> std::result::Result<UpdateOutcome, String> {
    let Bson::Document(entry) = entry else {
        return Err("update entries must be documents".to_string());
    };
    reject_unsupported_entry_keys(entry, &["q", "u", "upsert", "multi"])?;
    let query = entry
        .get_document("q")
        .map_err(|_| "update entry requires q document".to_string())?;
    let update = entry
        .get_document("u")
        .map_err(|_| "update entry requires u document".to_string())?;
    let upsert = optional_bool_doc(entry, "upsert")?.unwrap_or(false);
    let multi = optional_bool_doc(entry, "multi")?.unwrap_or(false);
    let update = classify_update(update)?;

    let mut matches = Vec::new();
    for stored in stored_documents_for_namespace_tx(tx, namespace).map_err(|err| err.to_string())? {
        match matches_filter(&stored.document, query) {
            Ok(true) => matches.push(stored),
            Ok(false) => {}
            Err(err) => return Err(err.errmsg),
        }
        if !multi && !matches.is_empty() {
            break;
        }
    }

    if matches.is_empty() {
        if !upsert {
            return Ok(UpdateOutcome {
                matched: 0,
                modified: 0,
                upserted_id: None,
            });
        }

        let mut new_document = build_upsert_document(query, &update)?;
        ensure_document_id(&mut new_document);
        let upserted_id = new_document
            .get("_id")
            .cloned()
            .ok_or_else(|| "upsert document is missing _id".to_string())?;
        ensure_unique_constraints_tx(tx, namespace, &new_document, None)?;
        insert_stored_document_tx(tx, namespace, &new_document)
            .map_err(|err| duplicate_or_sql_error(namespace, &new_document, err))?;
        refresh_index_entries_for_document_tx(
            tx,
            namespace,
            &id_key_from_bson(&upserted_id),
            &new_document,
        )
        .map_err(|err| err.to_string())?;
        return Ok(UpdateOutcome {
            matched: 0,
            modified: 0,
            upserted_id: Some(upserted_id),
        });
    }

    let matched = matches.len() as i32;
    let mut modified = 0_i32;
    for stored in matches {
        let new_document = apply_update_to_document(&stored.document, &update)?;
        let new_id_key = id_key(&new_document).map_err(|err| err.to_string())?;
        if new_id_key != stored.id_key {
            return Err("update cannot change _id".to_string());
        }
        if new_document != stored.document {
            ensure_unique_constraints_tx(tx, namespace, &new_document, Some(&stored.id_key))?;
            update_stored_document_tx(tx, namespace, &stored.id_key, &new_document)
                .map_err(|err| duplicate_or_sql_error(namespace, &new_document, err))?;
            refresh_index_entries_for_document_tx(tx, namespace, &stored.id_key, &new_document)
                .map_err(|err| err.to_string())?;
            modified += 1;
        }
    }

    Ok(UpdateOutcome {
        matched,
        modified,
        upserted_id: None,
    })
}

#[derive(Clone, Debug)]
struct StoredDocument {
    id_key: String,
    document: Document,
}

fn stored_documents_for_namespace_tx(
    tx: &rusqlite::Transaction<'_>,
    namespace: &str,
) -> Result<Vec<StoredDocument>> {
    let mut stmt =
        tx.prepare("SELECT id_key, bson FROM documents WHERE namespace = ?1 ORDER BY created_at")?;
    stmt.query_map(params![namespace], |row| {
        Ok((row.get::<_, String>(0)?, row.get::<_, Vec<u8>>(1)?))
    })?
    .map(|row| {
        let (id_key, bytes) = row?;
        Ok(StoredDocument {
            id_key,
            document: decode_document(bytes)?,
        })
    })
    .collect::<Result<Vec<_>>>()
}

fn insert_stored_document_tx(
    tx: &rusqlite::Transaction<'_>,
    namespace: &str,
    document: &Document,
) -> std::result::Result<(), rusqlite::Error> {
    let id_key = id_key(document).map_err(|err| {
        rusqlite::Error::ToSqlConversionFailure(Box::new(std::io::Error::other(err.to_string())))
    })?;
    let mut encoded = Vec::new();
    document.to_writer(&mut encoded).map_err(|err| {
        rusqlite::Error::ToSqlConversionFailure(Box::new(std::io::Error::other(err.to_string())))
    })?;
    tx.execute(
        "INSERT INTO documents(namespace, id_key, bson, updated_at)
         VALUES (?1, ?2, ?3, CURRENT_TIMESTAMP)",
        params![namespace, id_key, encoded],
    )?;
    Ok(())
}

fn update_stored_document_tx(
    tx: &rusqlite::Transaction<'_>,
    namespace: &str,
    id_key: &str,
    document: &Document,
) -> std::result::Result<(), rusqlite::Error> {
    let mut encoded = Vec::new();
    document.to_writer(&mut encoded).map_err(|err| {
        rusqlite::Error::ToSqlConversionFailure(Box::new(std::io::Error::other(err.to_string())))
    })?;
    tx.execute(
        "UPDATE documents SET bson = ?1, updated_at = CURRENT_TIMESTAMP
         WHERE namespace = ?2 AND id_key = ?3",
        params![encoded, namespace, id_key],
    )?;
    Ok(())
}

fn duplicate_or_sql_error(namespace: &str, document: &Document, err: rusqlite::Error) -> String {
    if is_sqlite_constraint(&err) {
        let key = id_key(document).unwrap_or_else(|_| "<unknown>".to_string());
        format!("duplicate key error collection: {namespace} _id: {key}")
    } else {
        err.to_string()
    }
}

fn update_error_code(errmsg: &str) -> i32 {
    if errmsg.starts_with("duplicate key error") {
        11000
    } else {
        2
    }
}

fn unique_write_error_code(errmsg: &str) -> i32 {
    if errmsg.starts_with("duplicate key error") {
        11000
    } else {
        2
    }
}

fn reject_unsupported_entry_keys(
    entry: &Document,
    allowed: &[&str],
) -> std::result::Result<(), String> {
    if let Some(key) = entry.keys().find(|key| !allowed.contains(&key.as_str())) {
        Err(format!("{key} is not supported for this command entry"))
    } else {
        Ok(())
    }
}

fn optional_bool_doc(document: &Document, key: &str) -> std::result::Result<Option<bool>, String> {
    match document.get(key) {
        None => Ok(None),
        Some(Bson::Boolean(value)) => Ok(Some(*value)),
        Some(_) => Err(format!("{key} must be a boolean")),
    }
}

#[derive(Clone, Debug)]
enum UpdateSpec {
    Replacement(Document),
    Modifier {
        set: Document,
        unset: Document,
        inc: Document,
    },
}

fn classify_update(update: &Document) -> std::result::Result<UpdateSpec, String> {
    if update.is_empty() {
        return Err("update document must not be empty".to_string());
    }
    let has_operator = update.keys().any(|key| key.starts_with('$'));
    let has_replacement = update.keys().any(|key| !key.starts_with('$'));
    if has_operator && has_replacement {
        return Err("update document cannot mix replacement fields and operators".to_string());
    }
    if !has_operator {
        return Ok(UpdateSpec::Replacement(update.clone()));
    }

    let mut set = Document::new();
    let mut unset = Document::new();
    let mut inc = Document::new();
    let mut paths = Vec::new();
    for (operator, operand) in update {
        let Bson::Document(operand) = operand else {
            return Err(format!("{operator} requires a document operand"));
        };
        match operator.as_str() {
            "$set" => {
                append_update_paths(operator, operand, &mut paths)?;
                set = operand.clone();
            }
            "$unset" => {
                append_update_paths(operator, operand, &mut paths)?;
                unset = operand.clone();
            }
            "$inc" => {
                append_update_paths(operator, operand, &mut paths)?;
                inc = operand.clone();
            }
            _ => return Err(format!("unsupported update operator {operator}")),
        }
    }
    if paths.is_empty() {
        return Err("modifier update must contain at least one path".to_string());
    }
    reject_path_collisions(&paths, "update")?;
    Ok(UpdateSpec::Modifier { set, unset, inc })
}

fn append_update_paths(
    operator: &str,
    document: &Document,
    paths: &mut Vec<String>,
) -> std::result::Result<(), String> {
    for key in document.keys() {
        if key.is_empty() || key.starts_with('$') || key.split('.').any(|part| part.is_empty()) {
            return Err(format!("{operator} contains unsupported path {key}"));
        }
        if key == "_id" || key.starts_with("_id.") {
            return Err("update cannot change _id".to_string());
        }
        paths.push(key.to_string());
    }
    Ok(())
}

fn apply_update_to_document(
    original: &Document,
    update: &UpdateSpec,
) -> std::result::Result<Document, String> {
    match update {
        UpdateSpec::Replacement(replacement) => {
            let mut document = replacement.clone();
            match (original.get("_id"), document.get("_id")) {
                (Some(original_id), Some(new_id)) if !bson_values_equal(original_id, new_id) => {
                    return Err("replacement update cannot change _id".to_string());
                }
                (Some(original_id), None) => {
                    document.insert("_id", original_id.clone());
                }
                _ => {}
            }
            Ok(document)
        }
        UpdateSpec::Modifier { set, unset, inc } => {
            let mut document = original.clone();
            for (path, value) in set {
                set_update_path(&mut document, path, value.clone())?;
            }
            for path in unset.keys() {
                unset_update_path(&mut document, path)?;
            }
            for (path, operand) in inc {
                inc_update_path(&mut document, path, operand)?;
            }
            Ok(document)
        }
    }
}

fn build_upsert_document(
    query: &Document,
    update: &UpdateSpec,
) -> std::result::Result<Document, String> {
    match update {
        UpdateSpec::Replacement(replacement) => {
            let mut document = replacement.clone();
            if !document.contains_key("_id")
                && let Some(id) = equality_id_from_filter(query)
            {
                document.insert("_id", id.clone());
            }
            Ok(document)
        }
        UpdateSpec::Modifier { .. } => {
            let mut document = equality_document_from_filter(query)?;
            document = apply_update_to_document(&document, update)?;
            Ok(document)
        }
    }
}

fn equality_id_from_filter(query: &Document) -> Option<&Bson> {
    match query.get("_id") {
        Some(value) if !is_operator_document(value) => Some(value),
        Some(Bson::Document(document)) if document.len() == 1 => document.get("$eq"),
        _ => None,
    }
}

fn equality_document_from_filter(query: &Document) -> std::result::Result<Document, String> {
    let mut document = Document::new();
    for (field, value) in query {
        if field.starts_with('$') || field.contains('$') {
            continue;
        }
        let value = if !is_operator_document(value) {
            Some(value)
        } else if let Bson::Document(operator) = value {
            if operator.len() == 1 {
                operator.get("$eq")
            } else {
                None
            }
        } else {
            None
        };
        if let Some(value) = value {
            set_update_path(&mut document, field, value.clone())?;
        }
    }
    Ok(document)
}

fn set_update_path(
    document: &mut Document,
    path: &str,
    value: Bson,
) -> std::result::Result<(), String> {
    let mut parts = path.split('.').collect::<Vec<_>>();
    let Some(last) = parts.pop() else {
        return Err("update path must not be empty".to_string());
    };
    let mut current = document;
    for part in parts {
        match current.get(part) {
            Some(Bson::Document(_)) => {}
            Some(_) => return Err(format!("cannot traverse scalar parent {part}")),
            None => {
                current.insert(part, Document::new());
            }
        }
        current = current
            .get_document_mut(part)
            .map_err(|_| format!("cannot traverse scalar parent {part}"))?;
    }
    current.insert(last, value);
    Ok(())
}

fn unset_update_path(document: &mut Document, path: &str) -> std::result::Result<(), String> {
    let mut parts = path.split('.').collect::<Vec<_>>();
    let Some(last) = parts.pop() else {
        return Err("update path must not be empty".to_string());
    };
    let mut current = document;
    for part in parts {
        match current.get(part) {
            Some(Bson::Document(_)) => {
                current = current
                    .get_document_mut(part)
                    .map_err(|_| format!("cannot traverse scalar parent {part}"))?;
            }
            Some(_) => return Err(format!("cannot traverse scalar parent {part}")),
            None => return Ok(()),
        }
    }
    current.remove(last);
    Ok(())
}

fn inc_update_path(
    document: &mut Document,
    path: &str,
    operand: &Bson,
) -> std::result::Result<(), String> {
    if numeric_value(operand).is_none() {
        return Err("$inc operands must be numeric".to_string());
    }
    let existing = get_document_path(document, path).cloned();
    match existing {
        None => set_update_path(document, path, operand.clone()),
        Some(current) if numeric_value(&current).is_some() => {
            let updated = add_numeric_bson(&current, operand)?;
            set_update_path(document, path, updated)
        }
        Some(_) => Err("$inc can only apply to numeric fields".to_string()),
    }
}

fn add_numeric_bson(left: &Bson, right: &Bson) -> std::result::Result<Bson, String> {
    match (left, right) {
        (Bson::Double(left), _) | (_, Bson::Double(left)) if left.is_nan() => {
            Err("$inc does not support NaN".to_string())
        }
        (Bson::Double(_), _) | (_, Bson::Double(_)) => Ok(Bson::Double(
            numeric_value(left).unwrap() + numeric_value(right).unwrap(),
        )),
        (Bson::Int32(left), Bson::Int32(right)) => {
            let sum = (*left as i64) + (*right as i64);
            if (i32::MIN as i64..=i32::MAX as i64).contains(&sum) {
                Ok(Bson::Int32(sum as i32))
            } else {
                Ok(Bson::Int64(sum))
            }
        }
        (Bson::Int64(left), Bson::Int64(right)) => left
            .checked_add(*right)
            .map(Bson::Int64)
            .ok_or_else(|| "$inc overflowed int64".to_string()),
        (Bson::Int64(left), Bson::Int32(right)) => left
            .checked_add(*right as i64)
            .map(Bson::Int64)
            .ok_or_else(|| "$inc overflowed int64".to_string()),
        (Bson::Int32(left), Bson::Int64(right)) => (*left as i64)
            .checked_add(*right)
            .map(Bson::Int64)
            .ok_or_else(|| "$inc overflowed int64".to_string()),
        _ => unreachable!("non-numeric $inc operands should be rejected before addition"),
    }
}

fn delete_documents(conn: &Connection, command: &Document) -> Result<Document> {
    let db = command.get_str("$db").unwrap_or("test");
    let collection = match command.get_str("delete") {
        Ok(collection) if !collection.is_empty() => collection,
        _ => {
            return Ok(command_error(
                9,
                "delete command requires a collection name",
            ));
        }
    };
    let deletes = match command.get_array("deletes") {
        Ok(deletes) if !deletes.is_empty() => deletes,
        Ok(_) => {
            return Ok(command_error(
                9,
                "delete command requires a non-empty deletes array",
            ));
        }
        Err(_) => return Ok(command_error(9, "delete command requires a deletes array")),
    };
    let ordered = match optional_bool(command, "ordered") {
        Ok(value) => value.unwrap_or(true),
        Err(errmsg) => return Ok(command_error(9, &errmsg)),
    };
    if let Some(errmsg) =
        reject_unsupported_command_keys(command, &["delete", "deletes", "ordered", "$db", "lsid"])
    {
        return Ok(command_error(72, &errmsg));
    }

    let namespace = namespace(db, collection);
    let tx = conn.unchecked_transaction()?;
    let mut removed = 0_i32;
    let mut write_errors = Vec::new();

    for (index, entry) in deletes.iter().enumerate() {
        match apply_delete_entry(&tx, &namespace, entry) {
            Ok(count) => removed += count,
            Err(errmsg) => {
                write_errors.push(write_error(index as i32, 2, &errmsg));
                if ordered {
                    break;
                }
            }
        }
    }

    tx.commit()?;
    let mut response = doc! {
        "n": removed,
        "ok": 1.0,
    };
    if !write_errors.is_empty() {
        response.insert("writeErrors", write_errors);
    }
    Ok(response)
}

fn apply_delete_entry(
    tx: &rusqlite::Transaction<'_>,
    namespace: &str,
    entry: &Bson,
) -> std::result::Result<i32, String> {
    let Bson::Document(entry) = entry else {
        return Err("delete entries must be documents".to_string());
    };
    reject_unsupported_entry_keys(entry, &["q", "limit"])?;
    let query = entry
        .get_document("q")
        .map_err(|_| "delete entry requires q document".to_string())?;
    let limit = match entry.get("limit") {
        Some(Bson::Int32(0)) | Some(Bson::Int64(0)) => 0,
        Some(Bson::Int32(1)) | Some(Bson::Int64(1)) => 1,
        Some(_) => return Err("delete limit must be 0 or 1".to_string()),
        None => return Err("delete entry requires limit".to_string()),
    };

    let mut targets = Vec::new();
    for stored in stored_documents_for_namespace_tx(tx, namespace).map_err(|err| err.to_string())? {
        match matches_filter(&stored.document, query) {
            Ok(true) => targets.push(stored.id_key),
            Ok(false) => {}
            Err(err) => return Err(err.errmsg),
        }
        if limit == 1 && !targets.is_empty() {
            break;
        }
    }

    let mut removed = 0_i32;
    for id_key in targets {
        delete_index_entries_for_document_tx(tx, namespace, &id_key)
            .map_err(|err| err.to_string())?;
        removed += tx
            .execute(
                "DELETE FROM documents WHERE namespace = ?1 AND id_key = ?2",
                params![namespace, id_key],
            )
            .map_err(|err| err.to_string())? as i32;
    }
    Ok(removed)
}

#[cfg(test)]
fn find_documents(conn: &Connection, command: &Document) -> Result<Document> {
    let mut client_state = ClientState::default();
    find_documents_with_state(conn, &mut client_state, command)
}

fn find_documents_with_state(
    conn: &Connection,
    client_state: &mut ClientState,
    command: &Document,
) -> Result<Document> {
    let db = command.get_str("$db").unwrap_or("test");
    let collection = match command.get_str("find") {
        Ok(collection) if !collection.is_empty() => collection,
        _ => return Ok(command_error(9, "find command requires a collection")),
    };
    let filter = match command.get("filter") {
        None => Document::new(),
        Some(Bson::Document(filter)) => filter.clone(),
        Some(_) => return Ok(command_error(9, "find filter must be a document")),
    };
    if let Some(errmsg) = reject_unsupported_command_keys(
        command,
        &[
            "find",
            "filter",
            "batchSize",
            "projection",
            "sort",
            "skip",
            "limit",
            "singleBatch",
            "$db",
            "lsid",
        ],
    ) {
        return Ok(command_error(72, &errmsg));
    }
    if let Err(errmsg) = optional_bool(command, "singleBatch") {
        return Ok(command_error(9, &errmsg));
    }
    let single_batch = command.get_bool("singleBatch").unwrap_or(false);
    let batch_size = match optional_i64(command, "batchSize") {
        Ok(Some(value)) if value < 0 => {
            return Ok(command_error(9, "batchSize must be non-negative"));
        }
        Ok(Some(value)) => value.min(1000) as usize,
        Ok(None) => 101,
        Err(errmsg) => return Ok(command_error(9, &errmsg)),
    };
    let skip = match optional_i64(command, "skip") {
        Ok(Some(value)) if value < 0 => return Ok(command_error(9, "skip must be non-negative")),
        Ok(Some(value)) => value as usize,
        Ok(None) => 0,
        Err(errmsg) => return Ok(command_error(9, &errmsg)),
    };
    let limit = match optional_i64(command, "limit") {
        Ok(Some(value)) if value < 0 => return Ok(command_error(9, "limit must be non-negative")),
        Ok(Some(0)) | Ok(None) => None,
        Ok(Some(value)) => Some(value as usize),
        Err(errmsg) => return Ok(command_error(9, &errmsg)),
    };
    let projection = match parse_projection(command) {
        Ok(value) => value,
        Err(errmsg) => return Ok(command_error(2, &errmsg)),
    };
    let sort = match parse_sort(command) {
        Ok(value) => value,
        Err(errmsg) => return Ok(command_error(2, &errmsg)),
    };
    let ns = namespace(db, collection);

    if sort.is_none()
        && skip == 0
        && limit.is_none()
        && projection.is_none()
        && batch_size > 0
        && let Some(id_filter) = simple_id_equality_filter(&filter)
    {
        let wanted_id = id_key_from_bson(id_filter);
        if let Some(document) = conn
            .query_row(
                "SELECT bson FROM documents WHERE namespace = ?1 AND id_key = ?2",
                params![ns, wanted_id],
                |row| row.get::<_, Vec<u8>>(0),
            )
            .optional()?
            .map(decode_document)
            .transpose()?
        {
            return Ok(cursor_response(
                db,
                collection,
                0,
                "firstBatch",
                vec![document],
            ));
        }
        return Ok(cursor_response(db, collection, 0, "firstBatch", vec![]));
    }

    let docs = match query_documents(
        conn,
        &ns,
        &filter,
        sort.as_deref(),
        skip,
        limit,
        projection.as_ref(),
    ) {
        Ok(docs) => docs,
        Err(err) => return Ok(command_error(err.code, &err.errmsg)),
    };

    Ok(cursor_response_for_documents(
        client_state,
        db,
        collection,
        &ns,
        docs,
        batch_size,
        single_batch,
    ))
}

fn get_more(client_state: &mut ClientState, command: &Document) -> Result<Document> {
    let db = command.get_str("$db").unwrap_or("test");
    let cursor_id = match command.get("getMore") {
        Some(Bson::Int64(value)) if *value > 0 => *value,
        Some(Bson::Int32(value)) if *value > 0 => *value as i64,
        Some(Bson::Int64(_)) | Some(Bson::Int32(_)) => {
            return Ok(command_error(9, "getMore cursor id must be positive"));
        }
        _ => return Ok(command_error(9, "getMore requires an integer cursor id")),
    };
    let collection = match command.get_str("collection") {
        Ok(collection) if !collection.is_empty() => collection,
        _ => return Ok(command_error(9, "getMore requires a collection name")),
    };
    let batch_size = match optional_i64(command, "batchSize") {
        Ok(Some(value)) if value <= 0 => {
            return Ok(command_error(9, "batchSize must be positive"));
        }
        Ok(Some(value)) => value.min(1000) as usize,
        Ok(None) => 101,
        Err(errmsg) => return Ok(command_error(9, &errmsg)),
    };
    if let Some(errmsg) = reject_unsupported_command_keys(
        command,
        &["getMore", "collection", "batchSize", "$db", "lsid"],
    ) {
        return Ok(command_error(72, &errmsg));
    }

    let ns = namespace(db, collection);
    let Some(cursor) = client_state.cursors.get_mut(&cursor_id) else {
        return Ok(command_error(43, "cursor not found"));
    };
    if cursor.namespace != ns {
        return Ok(command_error(
            43,
            "cursor namespace does not match getMore collection",
        ));
    }

    let mut batch = Vec::new();
    for _ in 0..batch_size {
        let Some(document) = cursor.remaining.pop_front() else {
            break;
        };
        batch.push(document);
    }
    let response_cursor_id = if cursor.remaining.is_empty() {
        client_state.cursors.remove(&cursor_id);
        0
    } else {
        cursor_id
    };

    Ok(cursor_response(
        db,
        collection,
        response_cursor_id,
        "nextBatch",
        batch,
    ))
}

fn kill_cursors(client_state: &mut ClientState, command: &Document) -> Result<Document> {
    let db = command.get_str("$db").unwrap_or("test");
    let collection = match command.get_str("killCursors") {
        Ok(collection) if !collection.is_empty() => collection,
        _ => return Ok(command_error(9, "killCursors requires a collection name")),
    };
    let cursor_ids = match command.get_array("cursors") {
        Ok(cursors) => cursors,
        Err(_) => return Ok(command_error(9, "killCursors requires a cursors array")),
    };
    if let Some(errmsg) =
        reject_unsupported_command_keys(command, &["killCursors", "cursors", "$db", "lsid"])
    {
        return Ok(command_error(72, &errmsg));
    }

    let ns = namespace(db, collection);
    let mut cursors_killed = Vec::new();
    let mut cursors_not_found = Vec::new();

    for value in cursor_ids {
        let cursor_id = match value {
            Bson::Int64(value) if *value > 0 => *value,
            Bson::Int32(value) if *value > 0 => *value as i64,
            Bson::Int64(_) | Bson::Int32(_) => {
                return Ok(command_error(9, "killCursors cursor ids must be positive"));
            }
            _ => return Ok(command_error(9, "killCursors cursor ids must be integers")),
        };

        if client_state
            .cursors
            .get(&cursor_id)
            .is_some_and(|cursor| cursor.namespace == ns)
        {
            client_state.cursors.remove(&cursor_id);
            cursors_killed.push(Bson::Int64(cursor_id));
        } else {
            cursors_not_found.push(Bson::Int64(cursor_id));
        }
    }

    Ok(doc! {
        "cursorsKilled": cursors_killed,
        "cursorsNotFound": cursors_not_found,
        "cursorsAlive": Bson::Array(vec![]),
        "cursorsUnknown": Bson::Array(vec![]),
        "ok": 1.0,
    })
}

fn split_batch(documents: Vec<Document>, batch_size: usize) -> (Vec<Document>, Vec<Document>) {
    let mut first_batch = Vec::new();
    let mut remaining = Vec::new();
    for (index, document) in documents.into_iter().enumerate() {
        if index < batch_size {
            first_batch.push(document);
        } else {
            remaining.push(document);
        }
    }
    (first_batch, remaining)
}

fn cursor_response_for_documents(
    client_state: &mut ClientState,
    db: &str,
    collection: &str,
    namespace: &str,
    documents: Vec<Document>,
    batch_size: usize,
    single_batch: bool,
) -> Document {
    let (first_batch, remaining) = split_batch(documents, batch_size);
    let cursor_id = if !single_batch && !remaining.is_empty() {
        client_state.insert_cursor(namespace.to_string(), remaining)
    } else {
        0
    };

    cursor_response(db, collection, cursor_id, "firstBatch", first_batch)
}

fn optional_i64(command: &Document, key: &str) -> std::result::Result<Option<i64>, String> {
    match command.get(key) {
        None => Ok(None),
        Some(Bson::Int32(value)) => Ok(Some(*value as i64)),
        Some(Bson::Int64(value)) => Ok(Some(*value)),
        Some(_) => Err(format!("{key} must be an integer")),
    }
}

fn optional_document(
    command: &Document,
    key: &str,
) -> std::result::Result<Option<Document>, String> {
    match command.get(key) {
        None => Ok(None),
        Some(Bson::Document(document)) => Ok(Some(document.clone())),
        Some(_) => Err(format!("{key} must be a document")),
    }
}

#[derive(Clone, Debug)]
enum ProjectionMode {
    Include,
    Exclude,
}

#[derive(Clone, Debug)]
struct ProjectionSpec {
    mode: ProjectionMode,
    fields: Vec<String>,
    include_id: bool,
}

fn parse_projection(command: &Document) -> std::result::Result<Option<ProjectionSpec>, String> {
    let Some(value) = command.get("projection") else {
        return Ok(None);
    };
    let Bson::Document(projection) = value else {
        return Err("projection must be a document".to_string());
    };
    parse_projection_document(projection)
}

fn parse_find_and_modify_projection(
    command: &Document,
) -> std::result::Result<Option<ProjectionSpec>, String> {
    let projection = optional_document(command, "projection")?;
    let fields = optional_document(command, "fields")?;
    let projection = match (projection, fields) {
        (Some(projection), Some(fields)) if projection != fields => {
            return Err("fields and projection cannot conflict".to_string());
        }
        (Some(projection), _) => Some(projection),
        (None, Some(fields)) => Some(fields),
        (None, None) => None,
    };
    match projection {
        Some(projection) => parse_projection_document(&projection),
        None => Ok(None),
    }
}

fn parse_projection_document(
    projection: &Document,
) -> std::result::Result<Option<ProjectionSpec>, String> {
    let mut mode = None;
    let mut fields = Vec::new();
    let mut include_id = true;
    let mut saw_id = false;

    for (field, value) in projection {
        if field.starts_with('$') {
            return Err("projection field names starting with $ are not supported".to_string());
        }
        let include = projection_value(value)?;
        if field == "_id" {
            saw_id = true;
            include_id = include;
            continue;
        }

        let field_mode = if include {
            ProjectionMode::Include
        } else {
            ProjectionMode::Exclude
        };
        match (&mode, &field_mode) {
            (None, _) => mode = Some(field_mode),
            (Some(ProjectionMode::Include), ProjectionMode::Include)
            | (Some(ProjectionMode::Exclude), ProjectionMode::Exclude) => {}
            _ => {
                return Err(
                    "projection cannot mix inclusion and exclusion fields except _id".to_string(),
                );
            }
        }
        fields.push(field.to_string());
    }

    reject_path_collisions(&fields, "projection")?;
    Ok(mode
        .or_else(|| {
            saw_id.then_some(if include_id {
                ProjectionMode::Include
            } else {
                ProjectionMode::Exclude
            })
        })
        .map(|mode| ProjectionSpec {
            mode,
            fields,
            include_id,
        }))
}

fn projection_value(value: &Bson) -> std::result::Result<bool, String> {
    match value {
        Bson::Int32(0) | Bson::Int64(0) | Bson::Boolean(false) => Ok(false),
        Bson::Int32(1) | Bson::Int64(1) | Bson::Boolean(true) => Ok(true),
        _ => Err("projection values must be 0, 1, true, or false".to_string()),
    }
}

fn reject_path_collisions(paths: &[String], context: &str) -> std::result::Result<(), String> {
    for (index, left) in paths.iter().enumerate() {
        for right in paths.iter().skip(index + 1) {
            if left == right
                || left
                    .strip_prefix(right)
                    .is_some_and(|suffix| suffix.starts_with('.'))
                || right
                    .strip_prefix(left)
                    .is_some_and(|suffix| suffix.starts_with('.'))
            {
                return Err(format!(
                    "{context} contains conflicting paths {left} and {right}"
                ));
            }
        }
    }
    Ok(())
}

fn apply_projection(document: &Document, projection: &ProjectionSpec) -> Document {
    match projection.mode {
        ProjectionMode::Include => {
            let mut out = Document::new();
            if projection.include_id
                && let Some(id) = document.get("_id")
            {
                out.insert("_id", id.clone());
            }
            for field in &projection.fields {
                if let Some(value) = get_document_path(document, field) {
                    set_document_path(&mut out, field, value.clone());
                }
            }
            out
        }
        ProjectionMode::Exclude => {
            let mut out = document.clone();
            if !projection.include_id {
                out.remove("_id");
            }
            for field in &projection.fields {
                unset_document_path(&mut out, field);
            }
            out
        }
    }
}

fn get_document_path<'a>(document: &'a Document, path: &str) -> Option<&'a Bson> {
    let mut parts = path.split('.');
    let first = parts.next()?;
    let mut current = document.get(first)?;
    for part in parts {
        let Bson::Document(document) = current else {
            return None;
        };
        current = document.get(part)?;
    }
    Some(current)
}

fn set_document_path(document: &mut Document, path: &str, value: Bson) {
    let mut parts = path.split('.').collect::<Vec<_>>();
    let Some(last) = parts.pop() else {
        return;
    };
    let mut current = document;
    for part in parts {
        let needs_document = !matches!(current.get(part), Some(Bson::Document(_)));
        if needs_document {
            current.insert(part, Document::new());
        }
        current = current
            .get_document_mut(part)
            .expect("document inserted above");
    }
    current.insert(last, value);
}

fn unset_document_path(document: &mut Document, path: &str) {
    let mut parts = path.split('.').collect::<Vec<_>>();
    let Some(last) = parts.pop() else {
        return;
    };
    let mut current = document;
    for part in parts {
        let Ok(next) = current.get_document_mut(part) else {
            return;
        };
        current = next;
    }
    current.remove(last);
}

fn parse_sort(command: &Document) -> std::result::Result<Option<Vec<(String, i32)>>, String> {
    let Some(value) = command.get("sort") else {
        return Ok(None);
    };
    let Bson::Document(sort) = value else {
        return Err("sort must be a document".to_string());
    };
    let mut spec = Vec::new();
    for (field, direction) in sort {
        if field.starts_with('$') {
            return Err("sort field names starting with $ are not supported".to_string());
        }
        let direction = match direction {
            Bson::Int32(1) | Bson::Int64(1) => 1,
            Bson::Int32(-1) | Bson::Int64(-1) => -1,
            _ => return Err("sort directions must be 1 or -1".to_string()),
        };
        spec.push((field.to_string(), direction));
    }
    Ok(Some(spec))
}

fn sort_documents(documents: &mut [Document], sort: &[(String, i32)]) {
    documents.sort_by(|left, right| compare_documents_for_sort(left, right, sort));
}

fn compare_documents_for_sort(
    left: &Document,
    right: &Document,
    sort: &[(String, i32)],
) -> std::cmp::Ordering {
    for (field, direction) in sort {
        let ordering = compare_optional_bson(
            get_document_path(left, field),
            get_document_path(right, field),
        );
        if !ordering.is_eq() {
            return if *direction == 1 {
                ordering
            } else {
                ordering.reverse()
            };
        }
    }
    compare_optional_bson(left.get("_id"), right.get("_id"))
}

fn compare_optional_bson(left: Option<&Bson>, right: Option<&Bson>) -> std::cmp::Ordering {
    match (left, right) {
        (None, None) => std::cmp::Ordering::Equal,
        (None, Some(_)) => std::cmp::Ordering::Less,
        (Some(_), None) => std::cmp::Ordering::Greater,
        (Some(left), Some(right)) => compare_bson_order(left, right),
    }
}

fn compare_bson_order(left: &Bson, right: &Bson) -> std::cmp::Ordering {
    match (numeric_value(left), numeric_value(right)) {
        (Some(left), Some(right)) => {
            return left
                .partial_cmp(&right)
                .unwrap_or(std::cmp::Ordering::Equal);
        }
        _ => {}
    }

    let left_rank = bson_type_rank(left);
    let right_rank = bson_type_rank(right);
    left_rank
        .cmp(&right_rank)
        .then_with(|| format!("{left:?}").cmp(&format!("{right:?}")))
}

fn bson_type_rank(value: &Bson) -> i32 {
    match value {
        Bson::Null => 1,
        Bson::Boolean(_) => 2,
        Bson::Int32(_) | Bson::Int64(_) | Bson::Double(_) => 3,
        Bson::String(_) => 4,
        Bson::ObjectId(_) => 5,
        Bson::Array(_) => 6,
        Bson::Document(_) => 7,
        _ => 100,
    }
}

fn simple_id_equality_filter(filter: &Document) -> Option<&Bson> {
    if filter.len() == 1 {
        filter
            .get("_id")
            .filter(|value| !is_operator_document(value))
    } else {
        None
    }
}

fn indexed_candidate_documents(
    conn: &Connection,
    namespace: &str,
    filter: &Document,
) -> Result<Option<Vec<Document>>> {
    let Some((field, value)) = simple_equality_filter_field(filter) else {
        return Ok(None);
    };
    if matches!(value, Bson::Array(_)) {
        return Ok(None);
    }
    let Some(index) = indexes_for_namespace(conn, namespace)?
        .into_iter()
        .find(|index| single_field_index_name(index).is_some_and(|indexed| indexed == field))
    else {
        return Ok(None);
    };
    let key_value = id_key_from_bson(value);
    let mut stmt = conn.prepare(
        r#"
        SELECT d.bson
          FROM index_entries e
          JOIN documents d
            ON d.namespace = e.namespace
           AND d.id_key = e.id_key
         WHERE e.namespace = ?1
           AND e.index_name = ?2
           AND e.key_value = ?3
         ORDER BY d.created_at
        "#,
    )?;
    let documents = stmt
        .query_map(params![namespace, index.name, key_value], |row| {
            row.get::<_, Vec<u8>>(0)
        })?
        .map(|row| decode_document(row?))
        .collect::<Result<Vec<_>>>()?;
    Ok(Some(documents))
}

fn candidate_documents(
    conn: &Connection,
    namespace: &str,
    filter: &Document,
) -> Result<Vec<Document>> {
    match indexed_candidate_documents(conn, namespace, filter)? {
        Some(documents) => Ok(documents),
        None => documents_for_namespace(conn, namespace),
    }
}

fn query_documents(
    conn: &Connection,
    namespace: &str,
    filter: &Document,
    sort: Option<&[(String, i32)]>,
    skip: usize,
    limit: Option<usize>,
    projection: Option<&ProjectionSpec>,
) -> std::result::Result<Vec<Document>, MatchError> {
    let source_documents = candidate_documents(conn, namespace, filter)
        .map_err(|err| match_error(8, err.to_string()))?;
    shape_documents(source_documents, filter, sort, skip, limit, projection)
}

fn shape_documents(
    source_documents: Vec<Document>,
    filter: &Document,
    sort: Option<&[(String, i32)]>,
    skip: usize,
    limit: Option<usize>,
    projection: Option<&ProjectionSpec>,
) -> MatchResult<Vec<Document>> {
    let mut docs = Vec::new();
    for document in source_documents {
        if matches_filter(&document, filter)? {
            docs.push(document);
        }
    }

    if let Some(sort) = sort {
        sort_documents(&mut docs, sort);
    }
    if skip > 0 {
        docs = docs.into_iter().skip(skip).collect();
    }
    if let Some(limit) = limit {
        docs.truncate(limit);
    }
    if let Some(projection) = projection {
        docs = docs
            .into_iter()
            .map(|document| apply_projection(&document, projection))
            .collect();
    }

    Ok(docs)
}

fn simple_equality_filter_field(filter: &Document) -> Option<(&str, &Bson)> {
    for (field, value) in filter {
        if field.starts_with('$') {
            continue;
        }
        if !is_operator_document(value) {
            return Some((field.as_str(), value));
        }
        if let Bson::Document(operators) = value
            && operators.len() == 1
            && let Some(eq) = operators.get("$eq")
        {
            return Some((field.as_str(), eq));
        }
    }
    None
}

#[derive(Debug)]
struct MatchError {
    code: i32,
    errmsg: String,
}

type MatchResult<T> = std::result::Result<T, MatchError>;

fn match_error(code: i32, errmsg: impl Into<String>) -> MatchError {
    MatchError {
        code,
        errmsg: errmsg.into(),
    }
}

fn matches_filter(document: &Document, filter: &Document) -> MatchResult<bool> {
    for (key, condition) in filter {
        if key.starts_with('$') {
            if !matches_logical_operator(document, key, condition)? {
                return Ok(false);
            }
        } else if !matches_field_condition(document, key, condition)? {
            return Ok(false);
        }
    }
    Ok(true)
}

fn matches_logical_operator(
    document: &Document,
    operator: &str,
    operand: &Bson,
) -> MatchResult<bool> {
    if !matches!(operator, "$and" | "$or" | "$nor") {
        return Err(match_error(
            2,
            format!("unsupported top-level query operator {operator}"),
        ));
    }

    let clauses = match operand {
        Bson::Array(clauses) if !clauses.is_empty() => clauses,
        Bson::Array(_) => {
            return Err(match_error(
                2,
                format!("{operator} requires a non-empty array"),
            ));
        }
        _ => return Err(match_error(2, format!("{operator} requires an array"))),
    };

    let mut results = Vec::with_capacity(clauses.len());
    for clause in clauses {
        let Bson::Document(clause) = clause else {
            return Err(match_error(
                2,
                format!("{operator} entries must be documents"),
            ));
        };
        results.push(matches_filter(document, clause)?);
    }

    match operator {
        "$and" => Ok(results.into_iter().all(|matched| matched)),
        "$or" => Ok(results.into_iter().any(|matched| matched)),
        "$nor" => Ok(!results.into_iter().any(|matched| matched)),
        _ => unreachable!("unsupported logical operator checked above"),
    }
}

fn matches_field_condition(
    document: &Document,
    field: &str,
    condition: &Bson,
) -> MatchResult<bool> {
    let values = values_at_path(document, field);
    if is_operator_document(condition) {
        let Bson::Document(operators) = condition else {
            unreachable!("operator document checked above");
        };
        return matches_operator_document(&values, operators);
    }

    Ok(values
        .iter()
        .any(|candidate| bson_values_equal(candidate, condition)))
}

fn matches_operator_document(values: &[&Bson], operators: &Document) -> MatchResult<bool> {
    if operators.keys().any(|key| !key.starts_with('$')) {
        return Err(match_error(
            2,
            "field predicate cannot mix operators and literal document fields",
        ));
    }

    for (operator, operand) in operators {
        let matched = match operator.as_str() {
            "$eq" => values
                .iter()
                .any(|candidate| bson_values_equal(candidate, operand)),
            "$ne" => values
                .iter()
                .all(|candidate| !bson_values_equal(candidate, operand)),
            "$gt" => values
                .iter()
                .any(|candidate| compare_bson(candidate, operand, |ordering| ordering.is_gt())),
            "$gte" => values
                .iter()
                .any(|candidate| compare_bson(candidate, operand, |ordering| !ordering.is_lt())),
            "$lt" => values
                .iter()
                .any(|candidate| compare_bson(candidate, operand, |ordering| ordering.is_lt())),
            "$lte" => values
                .iter()
                .any(|candidate| compare_bson(candidate, operand, |ordering| !ordering.is_gt())),
            "$in" => {
                let Bson::Array(needles) = operand else {
                    return Err(match_error(2, "$in requires an array"));
                };
                values.iter().any(|candidate| {
                    needles
                        .iter()
                        .any(|needle| bson_values_equal(candidate, needle))
                })
            }
            "$nin" => {
                let Bson::Array(needles) = operand else {
                    return Err(match_error(2, "$nin requires an array"));
                };
                values.iter().all(|candidate| {
                    needles
                        .iter()
                        .all(|needle| !bson_values_equal(candidate, needle))
                })
            }
            "$exists" => {
                let Bson::Boolean(should_exist) = operand else {
                    return Err(match_error(2, "$exists requires a boolean"));
                };
                !values.is_empty() == *should_exist
            }
            "$not" => {
                let Bson::Document(nested) = operand else {
                    return Err(match_error(2, "$not requires a document"));
                };
                !matches_operator_document(values, nested)?
            }
            _ => {
                return Err(match_error(
                    2,
                    format!("unsupported query operator {operator}"),
                ));
            }
        };

        if !matched {
            return Ok(false);
        }
    }
    Ok(true)
}

fn is_operator_document(value: &Bson) -> bool {
    matches!(value, Bson::Document(document) if document.keys().any(|key| key.starts_with('$')))
}

fn values_at_path<'a>(document: &'a Document, path: &str) -> Vec<&'a Bson> {
    let mut parts = path.split('.');
    let Some(first) = parts.next() else {
        return Vec::new();
    };
    let rest = parts.collect::<Vec<_>>();
    document
        .get(first)
        .map(|value| values_at_path_parts(value, &rest))
        .unwrap_or_default()
}

fn values_at_path_parts<'a>(value: &'a Bson, parts: &[&str]) -> Vec<&'a Bson> {
    if parts.is_empty() {
        return vec![value];
    }

    match value {
        Bson::Document(document) => document
            .get(parts[0])
            .map(|next| values_at_path_parts(next, &parts[1..]))
            .unwrap_or_default(),
        Bson::Array(values) => values
            .iter()
            .flat_map(|next| values_at_path_parts(next, parts))
            .collect(),
        _ => Vec::new(),
    }
}

fn bson_values_equal(candidate: &Bson, expected: &Bson) -> bool {
    if let Bson::Array(values) = candidate {
        if !matches!(expected, Bson::Array(_)) {
            return values
                .iter()
                .any(|value| bson_values_equal(value, expected));
        }
    }

    match (numeric_value(candidate), numeric_value(expected)) {
        (Some(left), Some(right)) => left == right,
        _ => candidate == expected,
    }
}

fn compare_bson(
    candidate: &Bson,
    expected: &Bson,
    predicate: impl Fn(std::cmp::Ordering) -> bool,
) -> bool {
    let Some(left) = numeric_value(candidate) else {
        return false;
    };
    let Some(right) = numeric_value(expected) else {
        return false;
    };
    left.partial_cmp(&right).is_some_and(predicate)
}

fn numeric_value(value: &Bson) -> Option<f64> {
    match value {
        Bson::Int32(value) => Some(*value as f64),
        Bson::Int64(value) => Some(*value as f64),
        Bson::Double(value) => Some(*value),
        _ => None,
    }
}

fn documents_for_namespace(conn: &Connection, namespace: &str) -> Result<Vec<Document>> {
    let mut stmt =
        conn.prepare("SELECT bson FROM documents WHERE namespace = ?1 ORDER BY created_at")?;
    stmt.query_map(params![namespace], |row| row.get::<_, Vec<u8>>(0))?
        .map(|row| decode_document(row?))
        .collect::<Result<Vec<_>>>()
}

fn cursor_response(
    db: &str,
    collection: &str,
    cursor_id: i64,
    batch_field: &str,
    documents: Vec<Document>,
) -> Document {
    let mut cursor = doc! {
        "id": cursor_id,
        "ns": namespace(db, collection),
    };
    cursor.insert(
        batch_field,
        documents
            .into_iter()
            .map(Bson::Document)
            .collect::<Vec<_>>(),
    );

    doc! {
        "cursor": cursor,
        "ok": 1.0,
    }
}

fn decode_document(bytes: Vec<u8>) -> Result<Document> {
    Ok(Document::from_reader(&mut Cursor::new(bytes))?)
}

fn encode_document(document: &Document) -> std::result::Result<Vec<u8>, rusqlite::Error> {
    let mut encoded = Vec::new();
    document.to_writer(&mut encoded).map_err(|err| {
        rusqlite::Error::ToSqlConversionFailure(Box::new(std::io::Error::other(err.to_string())))
    })?;
    Ok(encoded)
}

fn decode_document_sql(bytes: Vec<u8>) -> std::result::Result<Document, rusqlite::Error> {
    Document::from_reader(&mut Cursor::new(bytes)).map_err(|err| {
        rusqlite::Error::FromSqlConversionFailure(0, rusqlite::types::Type::Blob, Box::new(err))
    })
}

fn ensure_document_id(document: &mut Document) {
    if !document.contains_key("_id") {
        document.insert("_id", ObjectId::new());
    }
}

fn id_key(document: &Document) -> Result<String> {
    document
        .get("_id")
        .map(id_key_from_bson)
        .ok_or_else(|| MongolinoError::Protocol("document is missing _id".to_string()))
}

fn id_key_from_bson(value: &Bson) -> String {
    match value {
        Bson::ObjectId(value) => format!("oid:{value}"),
        Bson::String(value) => format!("str:{value}"),
        Bson::Int32(value) => format!("i32:{value}"),
        Bson::Int64(value) => format!("i64:{value}"),
        Bson::Double(value) => format!("f64:{value}"),
        Bson::Boolean(value) => format!("bool:{value}"),
        other => format!("{other:?}"),
    }
}

fn namespace(db: &str, collection: &str) -> String {
    format!("{db}.{collection}")
}

fn command_error(code: i32, errmsg: &str) -> Document {
    doc! {
        "ok": 0.0,
        "code": code,
        "errmsg": errmsg,
    }
}

fn build_op_msg_response(response_to: i32, body: Document) -> Result<Vec<u8>> {
    let mut payload = Vec::new();
    payload.extend_from_slice(&0_i32.to_le_bytes());
    payload.push(0);
    body.to_writer(&mut payload)?;
    build_message(OP_MSG, next_request_id(), response_to, &payload)
}

fn build_op_reply_response(response_to: i32, body: Document) -> Result<Vec<u8>> {
    let mut payload = Vec::new();
    payload.extend_from_slice(&0_i32.to_le_bytes());
    payload.extend_from_slice(&0_i64.to_le_bytes());
    payload.extend_from_slice(&0_i32.to_le_bytes());
    payload.extend_from_slice(&1_i32.to_le_bytes());
    body.to_writer(&mut payload)?;
    build_message(OP_REPLY, next_request_id(), response_to, &payload)
}

fn build_message(
    opcode: i32,
    request_id: i32,
    response_to: i32,
    payload: &[u8],
) -> Result<Vec<u8>> {
    let len = 16_usize
        .checked_add(payload.len())
        .ok_or_else(|| MongolinoError::Protocol("message length overflow".to_string()))?;
    if len > i32::MAX as usize {
        return Err(MongolinoError::Protocol("message is too large".to_string()));
    }

    let mut message = Vec::with_capacity(len);
    message.extend_from_slice(&(len as i32).to_le_bytes());
    message.extend_from_slice(&request_id.to_le_bytes());
    message.extend_from_slice(&response_to.to_le_bytes());
    message.extend_from_slice(&opcode.to_le_bytes());
    message.extend_from_slice(payload);
    Ok(message)
}

fn next_request_id() -> i32 {
    NEXT_REQUEST_ID.fetch_add(1, Ordering::Relaxed)
}

fn read_i32(bytes: &[u8]) -> Result<i32> {
    let bytes: [u8; 4] = bytes
        .try_into()
        .map_err(|_| MongolinoError::Protocol("expected 4 byte integer".to_string()))?;
    Ok(i32::from_le_bytes(bytes))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_conn() -> Connection {
        let conn = Connection::open_in_memory().unwrap();
        init_connection(&conn).unwrap();
        conn
    }

    fn first_batch(response: &Document) -> Vec<Document> {
        batch(response, "firstBatch")
    }

    fn next_batch(response: &Document) -> Vec<Document> {
        batch(response, "nextBatch")
    }

    fn batch(response: &Document, field: &str) -> Vec<Document> {
        response
            .get_document("cursor")
            .unwrap()
            .get_array(field)
            .unwrap()
            .iter()
            .map(|value| value.as_document().unwrap().clone())
            .collect()
    }

    fn cursor_id(response: &Document) -> i64 {
        response
            .get_document("cursor")
            .unwrap()
            .get_i64("id")
            .unwrap()
    }

    fn bson_bytes(document: &Document) -> Vec<u8> {
        let mut bytes = Vec::new();
        document.to_writer(&mut bytes).unwrap();
        bytes
    }

    fn assert_command_error(response: &Document) {
        assert_eq!(response.get_f64("ok").unwrap(), 0.0);
        assert!(response.contains_key("code"));
        assert!(response.contains_key("errmsg"));
    }

    fn write_errors(response: &Document) -> Vec<Document> {
        response
            .get_array("writeErrors")
            .unwrap()
            .iter()
            .map(|value| value.as_document().unwrap().clone())
            .collect()
    }

    #[test]
    fn generated_ids_are_persistable_keys() {
        let mut document = doc! { "name": "Ada" };
        ensure_document_id(&mut document);

        assert!(document.contains_key("_id"));
        assert!(id_key(&document).unwrap().starts_with("oid:"));
    }

    #[test]
    fn op_msg_roundtrip_parses_body_document() {
        let body = doc! { "ping": 1_i32, "$db": "admin" };
        let mut payload = Vec::new();
        payload.extend_from_slice(&0_i32.to_le_bytes());
        payload.push(0);
        body.to_writer(&mut payload).unwrap();

        let parsed = parse_op_msg_document(&payload).unwrap();
        assert_eq!(parsed.get_i32("ping").unwrap(), 1);
        assert_eq!(parsed.get_str("$db").unwrap(), "admin");
    }

    #[test]
    fn op_msg_rejects_multiple_body_sections() {
        let mut payload = Vec::new();
        payload.extend_from_slice(&0_i32.to_le_bytes());
        payload.push(0);
        doc! { "ping": 1_i32, "$db": "admin" }
            .to_writer(&mut payload)
            .unwrap();
        payload.push(0);
        doc! { "ping": 2_i32, "$db": "admin" }
            .to_writer(&mut payload)
            .unwrap();

        let err = parse_op_msg_document(&payload).unwrap_err();
        assert!(err.to_string().contains("multiple body sections"));
    }

    #[test]
    fn op_msg_document_sequence_is_exposed_as_command_array() {
        let body = doc! { "insert": "users", "$db": "app" };
        let docs = vec![doc! { "_id": "u1" }, doc! { "_id": "u2" }];
        let mut payload = Vec::new();
        payload.extend_from_slice(&0_i32.to_le_bytes());
        payload.push(0);
        body.to_writer(&mut payload).unwrap();
        payload.push(1);

        let mut sequence = Vec::new();
        sequence.extend_from_slice(&0_i32.to_le_bytes());
        sequence.extend_from_slice(b"documents\0");
        for document in docs {
            document.to_writer(&mut sequence).unwrap();
        }
        let sequence_size = sequence.len() as i32;
        sequence[0..4].copy_from_slice(&sequence_size.to_le_bytes());
        payload.extend_from_slice(&sequence);

        let parsed = parse_op_msg_document(&payload).unwrap();
        assert_eq!(parsed.get_str("insert").unwrap(), "users");
        assert_eq!(parsed.get_array("documents").unwrap().len(), 2);
    }

    #[test]
    fn op_msg_rejects_unsupported_section_kind() {
        let payload = vec![0, 0, 0, 0, 9];
        let err = parse_op_msg_document(&payload).unwrap_err();
        assert!(
            err.to_string()
                .contains("unsupported OP_MSG section kind 9")
        );
    }

    #[test]
    fn insert_and_find_roundtrip_through_sqlite() {
        let conn = test_conn();
        let command = doc! {
            "insert": "users",
            "$db": "app",
            "documents": [
                { "_id": "u1", "name": "Ada" },
                { "_id": "u2", "name": "Grace" },
            ],
        };

        let insert_response = insert_documents(&conn, &command).unwrap();
        assert_eq!(insert_response.get_i32("n").unwrap(), 2);

        let find_response = find_documents(
            &conn,
            &doc! { "find": "users", "$db": "app", "filter": { "_id": "u2" } },
        )
        .unwrap();
        let batch = first_batch(&find_response);
        assert_eq!(batch.len(), 1);
        assert_eq!(batch[0].get_str("name").unwrap(), "Grace");
    }

    #[test]
    fn collection_scan_and_batch_size_are_covered() {
        let conn = test_conn();
        insert_documents(
            &conn,
            &doc! {
                "insert": "users",
                "$db": "app",
                "documents": [
                    { "_id": "u1", "name": "Ada" },
                    { "_id": "u2", "name": "Grace" },
                ],
            },
        )
        .unwrap();

        let response = find_documents(
            &conn,
            &doc! { "find": "users", "$db": "app", "batchSize": 1_i32 },
        )
        .unwrap();
        assert_eq!(first_batch(&response).len(), 1);
    }

    #[test]
    fn find_batch_size_returns_live_cursor_with_remainder() {
        let conn = test_conn();
        seed_find_documents(&conn);
        let mut client_state = ClientState::default();

        let response = find_documents_with_state(
            &conn,
            &mut client_state,
            &doc! {
                "find": "users",
                "$db": "app",
                "sort": { "_id": 1_i32 },
                "batchSize": 1_i32,
            },
        )
        .unwrap();
        let cursor = response.get_document("cursor").unwrap();

        assert!(cursor.get_i64("id").unwrap() > 0);
        let batch = first_batch(&response);
        assert_eq!(batch.len(), 1);
        assert_eq!(batch[0].get_str("_id").unwrap(), "u1");
        assert_eq!(client_state.cursors.len(), 1);
    }

    #[test]
    fn get_more_returns_next_batches_and_closes_on_exhaustion() {
        let conn = test_conn();
        seed_find_documents(&conn);
        let mut client_state = ClientState::default();

        let response = find_documents_with_state(
            &conn,
            &mut client_state,
            &doc! {
                "find": "users",
                "$db": "app",
                "sort": { "_id": 1_i32 },
                "batchSize": 1_i32,
            },
        )
        .unwrap();
        let id = cursor_id(&response);
        assert!(id > 0);

        let next = get_more(
            &mut client_state,
            &doc! {
                "getMore": id,
                "collection": "users",
                "$db": "app",
                "batchSize": 1_i32,
            },
        )
        .unwrap();
        assert_eq!(cursor_id(&next), id);
        assert_eq!(next_batch(&next)[0].get_str("_id").unwrap(), "u2");

        let final_batch = get_more(
            &mut client_state,
            &doc! {
                "getMore": id,
                "collection": "users",
                "$db": "app",
                "batchSize": 10_i32,
            },
        )
        .unwrap();
        assert_eq!(cursor_id(&final_batch), 0);
        assert_eq!(next_batch(&final_batch)[0].get_str("_id").unwrap(), "u3");
        assert!(client_state.cursors.is_empty());
    }

    #[test]
    fn get_more_rejects_zero_batch_size_without_consuming_cursor() {
        let conn = test_conn();
        seed_find_documents(&conn);
        let mut client_state = ClientState::default();

        let response = find_documents_with_state(
            &conn,
            &mut client_state,
            &doc! { "find": "users", "$db": "app", "sort": { "_id": 1_i32 }, "batchSize": 1_i32 },
        )
        .unwrap();
        let id = cursor_id(&response);
        assert!(id > 0);

        let zero_batch = get_more(
            &mut client_state,
            &doc! { "getMore": id, "collection": "users", "$db": "app", "batchSize": 0_i32 },
        )
        .unwrap();
        assert_command_error(&zero_batch);
        assert_eq!(zero_batch.get_i32("code").unwrap(), 9);
        assert!(client_state.cursors.contains_key(&id));

        let final_batch = get_more(
            &mut client_state,
            &doc! { "getMore": id, "collection": "users", "$db": "app", "batchSize": 10_i32 },
        )
        .unwrap();
        let ids = next_batch(&final_batch)
            .iter()
            .map(|document| document.get_str("_id").unwrap().to_string())
            .collect::<Vec<_>>();
        assert_eq!(ids, vec!["u2", "u3"]);
        assert_eq!(cursor_id(&final_batch), 0);
        assert!(client_state.cursors.is_empty());
    }

    #[test]
    fn get_more_rejects_malformed_requests() {
        let mut client_state = ClientState::default();

        for command in [
            doc! { "getMore": "bad", "collection": "users", "$db": "app" },
            doc! { "getMore": -1_i64, "collection": "users", "$db": "app" },
            doc! { "getMore": 1_i64, "$db": "app" },
            doc! { "getMore": 1_i64, "collection": "users", "$db": "app", "batchSize": -1_i32 },
            doc! { "getMore": 1_i64, "collection": "users", "$db": "app", "batchSize": 0_i32 },
            doc! { "getMore": 1_i64, "collection": "users", "$db": "app", "batchSize": 1.5 },
            doc! { "getMore": 1_i64, "collection": "users", "$db": "app", "comment": "nope" },
            doc! { "getMore": 999_i64, "collection": "users", "$db": "app" },
        ] {
            let response = get_more(&mut client_state, &command).unwrap();
            assert_command_error(&response);
        }
    }

    #[test]
    fn kill_cursors_removes_live_cursor_and_reports_repeated_kill_not_found() {
        let conn = test_conn();
        seed_find_documents(&conn);
        let mut client_state = ClientState::default();
        let response = find_documents_with_state(
            &conn,
            &mut client_state,
            &doc! { "find": "users", "$db": "app", "sort": { "_id": 1_i32 }, "batchSize": 1_i32 },
        )
        .unwrap();
        let id = cursor_id(&response);

        let killed = kill_cursors(
            &mut client_state,
            &doc! { "killCursors": "users", "$db": "app", "cursors": [id] },
        )
        .unwrap();
        assert_eq!(
            killed.get_array("cursorsKilled").unwrap(),
            &vec![Bson::Int64(id)]
        );
        assert!(client_state.cursors.is_empty());

        let repeated = kill_cursors(
            &mut client_state,
            &doc! { "killCursors": "users", "$db": "app", "cursors": [id] },
        )
        .unwrap();
        assert_eq!(
            repeated.get_array("cursorsNotFound").unwrap(),
            &vec![Bson::Int64(id)]
        );
    }

    #[test]
    fn kill_cursors_namespace_mismatch_does_not_remove_live_cursor() {
        let conn = test_conn();
        seed_find_documents(&conn);
        let mut client_state = ClientState::default();
        let response = find_documents_with_state(
            &conn,
            &mut client_state,
            &doc! { "find": "users", "$db": "app", "sort": { "_id": 1_i32 }, "batchSize": 1_i32 },
        )
        .unwrap();
        let id = cursor_id(&response);

        let mismatch = kill_cursors(
            &mut client_state,
            &doc! { "killCursors": "other", "$db": "app", "cursors": [id] },
        )
        .unwrap();
        assert_eq!(
            mismatch.get_array("cursorsNotFound").unwrap(),
            &vec![Bson::Int64(id)]
        );
        assert!(client_state.cursors.contains_key(&id));
    }

    #[test]
    fn get_more_after_kill_or_exhaustion_is_explicit_error() {
        let conn = test_conn();
        seed_find_documents(&conn);
        let mut client_state = ClientState::default();
        let response = find_documents_with_state(
            &conn,
            &mut client_state,
            &doc! { "find": "users", "$db": "app", "sort": { "_id": 1_i32 }, "batchSize": 1_i32 },
        )
        .unwrap();
        let killed_id = cursor_id(&response);
        kill_cursors(
            &mut client_state,
            &doc! { "killCursors": "users", "$db": "app", "cursors": [killed_id] },
        )
        .unwrap();
        let after_kill = get_more(
            &mut client_state,
            &doc! { "getMore": killed_id, "collection": "users", "$db": "app" },
        )
        .unwrap();
        assert_command_error(&after_kill);
        assert_eq!(after_kill.get_i32("code").unwrap(), 43);

        let response = find_documents_with_state(
            &conn,
            &mut client_state,
            &doc! { "find": "users", "$db": "app", "sort": { "_id": 1_i32 }, "batchSize": 2_i32 },
        )
        .unwrap();
        let exhausted_id = cursor_id(&response);
        assert!(exhausted_id > 0);
        let final_batch = get_more(
            &mut client_state,
            &doc! { "getMore": exhausted_id, "collection": "users", "$db": "app", "batchSize": 10_i32 },
        )
        .unwrap();
        assert_eq!(cursor_id(&final_batch), 0);
        let after_exhaustion = get_more(
            &mut client_state,
            &doc! { "getMore": exhausted_id, "collection": "users", "$db": "app" },
        )
        .unwrap();
        assert_command_error(&after_exhaustion);
        assert_eq!(after_exhaustion.get_i32("code").unwrap(), 43);
    }

    #[test]
    fn kill_cursors_rejects_malformed_requests() {
        let mut client_state = ClientState::default();

        for command in [
            doc! { "killCursors": "", "$db": "app", "cursors": [1_i64] },
            doc! { "killCursors": "users", "$db": "app" },
            doc! { "killCursors": "users", "$db": "app", "cursors": ["bad"] },
            doc! { "killCursors": "users", "$db": "app", "cursors": [-1_i64] },
            doc! { "killCursors": "users", "$db": "app", "cursors": [1_i64], "comment": "nope" },
        ] {
            let response = kill_cursors(&mut client_state, &command).unwrap();
            assert_command_error(&response);
        }
    }

    #[test]
    fn init_connection_installs_migration_scaffolding() {
        let conn = test_conn();

        let migrations_table: String = conn
            .query_row(
                "SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'schema_migrations'",
                [],
                |row| row.get(0),
            )
            .unwrap();

        assert_eq!(migrations_table, "schema_migrations");
    }

    #[test]
    fn list_databases_reports_namespaces_with_documents() {
        let conn = test_conn();
        insert_documents(
            &conn,
            &doc! {
                "insert": "users",
                "$db": "app",
                "documents": [{ "_id": "u1" }],
            },
        )
        .unwrap();

        let response = list_databases(&conn).unwrap();
        assert_eq!(response.get_f64("ok").unwrap(), 1.0);
        let databases = response.get_array("databases").unwrap();
        assert_eq!(
            databases[0].as_document().unwrap().get_str("name").unwrap(),
            "app"
        );
    }

    #[test]
    fn catalog_create_and_list_collections_tracks_empty_collections() {
        let conn = test_conn();

        let create = create_collection(&conn, &doc! { "create": "empty", "$db": "app" }).unwrap();
        assert_eq!(create.get_f64("ok").unwrap(), 1.0);

        let list =
            list_collections(&conn, &doc! { "listCollections": 1_i32, "$db": "app" }).unwrap();
        let names = first_batch(&list)
            .into_iter()
            .map(|doc| doc.get_str("name").unwrap().to_string())
            .collect::<Vec<_>>();
        assert_eq!(names, vec!["empty"]);

        let dbs = list_databases(&conn).unwrap();
        let names = dbs
            .get_array("databases")
            .unwrap()
            .iter()
            .map(|db| {
                db.as_document()
                    .unwrap()
                    .get_str("name")
                    .unwrap()
                    .to_string()
            })
            .collect::<Vec<_>>();
        assert!(names.contains(&"app".to_string()));
    }

    #[test]
    fn catalog_surfaces_document_only_namespaces_and_write_creates_catalog() {
        let conn = test_conn();
        conn.execute(
            "INSERT INTO documents(namespace, id_key, bson) VALUES (?1, ?2, ?3)",
            params!["legacy.users", "str:u1", bson_bytes(&doc! { "_id": "u1" })],
        )
        .unwrap();

        let legacy =
            list_collections(&conn, &doc! { "listCollections": 1_i32, "$db": "legacy" }).unwrap();
        assert_eq!(first_batch(&legacy)[0].get_str("name").unwrap(), "users");

        insert_documents(
            &conn,
            &doc! { "insert": "users", "$db": "app", "documents": [{ "_id": "u1" }] },
        )
        .unwrap();
        assert!(collection_exists(&conn, "app.users").unwrap());
    }

    #[test]
    fn drop_collection_removes_documents_catalog_and_index_state() {
        let conn = test_conn();
        insert_documents(
            &conn,
            &doc! { "insert": "users", "$db": "app", "documents": [{ "_id": "u1", "name": "Ada" }] },
        )
        .unwrap();
        create_indexes(
            &conn,
            &doc! {
                "createIndexes": "users",
                "$db": "app",
                "indexes": [{ "key": { "name": 1_i32 }, "name": "name_1" }],
            },
        )
        .unwrap();
        let entries_before_drop: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM index_entries WHERE namespace = 'app.users'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(entries_before_drop, 1);

        let response = drop_collection(&conn, &doc! { "drop": "users", "$db": "app" }).unwrap();
        assert_eq!(response.get_f64("ok").unwrap(), 1.0);
        assert!(
            documents_for_namespace(&conn, "app.users")
                .unwrap()
                .is_empty()
        );
        assert!(
            first_batch(
                &list_collections(&conn, &doc! { "listCollections": 1_i32, "$db": "app" }).unwrap()
            )
            .is_empty()
        );
        let indexes_after_drop: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM indexes WHERE namespace = 'app.users'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        let entries_after_drop: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM index_entries WHERE namespace = 'app.users'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(indexes_after_drop, 0);
        assert_eq!(entries_after_drop, 0);
    }

    #[test]
    fn drop_database_removes_only_that_database() {
        let conn = test_conn();
        insert_documents(
            &conn,
            &doc! { "insert": "users", "$db": "app", "documents": [{ "_id": "u1" }] },
        )
        .unwrap();
        insert_documents(
            &conn,
            &doc! { "insert": "users", "$db": "other", "documents": [{ "_id": "u2" }] },
        )
        .unwrap();

        let response = drop_database(&conn, &doc! { "dropDatabase": 1_i32, "$db": "app" }).unwrap();
        assert_eq!(response.get_str("dropped").unwrap(), "app");
        assert!(
            documents_for_namespace(&conn, "app.users")
                .unwrap()
                .is_empty()
        );
        assert_eq!(
            documents_for_namespace(&conn, "other.users").unwrap().len(),
            1
        );
    }

    #[test]
    fn lifecycle_commands_reject_unsupported_options() {
        let conn = test_conn();

        for response in [
            create_collection(
                &conn,
                &doc! { "create": "users", "$db": "app", "validator": {} },
            )
            .unwrap(),
            list_collections(
                &conn,
                &doc! { "listCollections": 1_i32, "$db": "app", "filter": { "type": "collection" } },
            )
            .unwrap(),
            drop_collection(&conn, &doc! { "drop": "users", "$db": "app", "comment": "nope" })
                .unwrap(),
            drop_database(
                &conn,
                &doc! { "dropDatabase": 1_i32, "$db": "app", "writeConcern": { "w": 1_i32 } },
            )
            .unwrap(),
        ] {
            assert_command_error(&response);
        }
    }

    #[test]
    fn count_command_respects_filter_skip_and_limit() {
        let conn = test_conn();
        seed_find_documents(&conn);

        let response = count_documents_command(
            &conn,
            &doc! {
                "count": "users",
                "$db": "app",
                "query": { "active": true },
                "skip": 1_i32,
                "limit": 10_i32,
            },
        )
        .unwrap();

        assert_eq!(response.get_i64("n").unwrap(), 1);
    }

    #[test]
    fn aggregate_count_documents_shape_is_supported() {
        let conn = test_conn();
        seed_find_documents(&conn);

        let response = aggregate_command(
            &conn,
            &doc! {
                "aggregate": "users",
                "$db": "app",
                "pipeline": [
                    { "$match": { "profile.city": "Rome" } },
                    { "$skip": 1_i32 },
                    { "$limit": 10_i32 },
                    { "$group": { "_id": 1_i32, "n": { "$sum": 1_i32 } } },
                ],
                "cursor": {},
            },
        )
        .unwrap();
        let batch = first_batch(&response);
        assert_eq!(batch[0].get_i64("n").unwrap(), 1);
    }

    #[test]
    fn distinct_command_supports_scalar_dotted_and_array_values() {
        let conn = test_conn();
        seed_find_documents(&conn);

        let names = distinct_command(
            &conn,
            &doc! { "distinct": "users", "$db": "app", "key": "profile.city" },
        )
        .unwrap();
        assert_eq!(
            names.get_array("values").unwrap(),
            &vec![
                Bson::String("London".to_string()),
                Bson::String("Rome".to_string())
            ]
        );

        let tags = distinct_command(
            &conn,
            &doc! { "distinct": "users", "$db": "app", "key": "tags", "query": { "active": true } },
        )
        .unwrap();
        assert_eq!(
            tags.get_array("values").unwrap(),
            &vec![
                Bson::String("logic".to_string()),
                Bson::String("math".to_string())
            ]
        );
    }

    #[test]
    fn count_distinct_and_aggregate_reject_unsupported_options() {
        let conn = test_conn();
        seed_find_documents(&conn);

        for response in [
            count_documents_command(
                &conn,
                &doc! { "count": "users", "$db": "app", "query": [], },
            )
            .unwrap(),
            count_documents_command(
                &conn,
                &doc! { "count": "users", "$db": "app", "hint": "_id_" },
            )
            .unwrap(),
            distinct_command(
                &conn,
                &doc! { "distinct": "users", "$db": "app", "key": "name", "collation": {} },
            )
            .unwrap(),
            aggregate_command(
                &conn,
                &doc! {
                    "aggregate": "users",
                    "$db": "app",
                    "pipeline": [{ "$project": { "name": 1_i32 } }],
                    "cursor": {},
                },
            )
            .unwrap(),
        ] {
            assert_command_error(&response);
        }
    }

    #[test]
    fn index_commands_create_list_and_drop_metadata() {
        let conn = test_conn();

        let created = create_indexes(
            &conn,
            &doc! {
                "createIndexes": "users",
                "$db": "app",
                "indexes": [
                    { "key": { "email": 1_i32 }, "name": "email_1", "unique": true },
                    { "key": { "profile.city": -1_i32 } },
                ],
            },
        )
        .unwrap();
        assert_eq!(created.get_i32("numIndexesBefore").unwrap(), 1);
        assert_eq!(created.get_i32("numIndexesAfter").unwrap(), 3);

        let listed = list_indexes(
            &conn,
            &doc! { "listIndexes": "users", "$db": "app", "cursor": {} },
        )
        .unwrap();
        let names = first_batch(&listed)
            .into_iter()
            .map(|index| index.get_str("name").unwrap().to_string())
            .collect::<Vec<_>>();
        assert_eq!(names, vec!["_id_", "email_1", "profile.city_-1"]);

        let dropped = drop_indexes(
            &conn,
            &doc! { "dropIndexes": "users", "$db": "app", "index": "email_1" },
        )
        .unwrap();
        assert_eq!(dropped.get_i32("numIndexesAfter").unwrap(), 2);
        let names = first_batch(
            &list_indexes(&conn, &doc! { "listIndexes": "users", "$db": "app" }).unwrap(),
        )
        .into_iter()
        .map(|index| index.get_str("name").unwrap().to_string())
        .collect::<Vec<_>>();
        assert_eq!(names, vec!["_id_", "profile.city_-1"]);
    }

    #[test]
    fn index_duplicate_create_is_idempotent_but_conflicts_error() {
        let conn = test_conn();
        let command = doc! {
            "createIndexes": "users",
            "$db": "app",
            "indexes": [{ "key": { "email": 1_i32 }, "name": "email_1" }],
        };
        create_indexes(&conn, &command).unwrap();
        let repeated = create_indexes(&conn, &command).unwrap();
        assert_eq!(repeated.get_f64("ok").unwrap(), 1.0);

        let conflict = create_indexes(
            &conn,
            &doc! {
                "createIndexes": "users",
                "$db": "app",
                "indexes": [{ "key": { "email": -1_i32 }, "name": "email_1" }],
            },
        )
        .unwrap();
        assert_command_error(&conflict);
        assert_eq!(conflict.get_i32("code").unwrap(), 85);
    }

    #[test]
    fn index_commands_reject_unsupported_shapes() {
        let conn = test_conn();

        for response in [
            create_indexes(
                &conn,
                &doc! { "createIndexes": "users", "$db": "app", "indexes": [] },
            )
            .unwrap(),
            create_indexes(
                &conn,
                &doc! {
                    "createIndexes": "users",
                    "$db": "app",
                    "indexes": [{ "key": { "name": "text" }, "name": "name_text" }],
                },
            )
            .unwrap(),
            create_indexes(
                &conn,
                &doc! {
                    "createIndexes": "users",
                    "$db": "app",
                    "indexes": [{ "key": { "name": 1_i32 }, "partialFilterExpression": { "active": true } }],
                },
            )
            .unwrap(),
            drop_indexes(&conn, &doc! { "dropIndexes": "users", "$db": "app", "index": "_id_" })
                .unwrap(),
            list_indexes(
                &conn,
                &doc! { "listIndexes": "users", "$db": "app", "cursor": "bad" },
            )
            .unwrap(),
        ] {
            assert_command_error(&response);
        }
    }

    #[test]
    fn unique_index_creation_rejects_existing_duplicates() {
        let conn = test_conn();
        insert_documents(
            &conn,
            &doc! {
                "insert": "users",
                "$db": "app",
                "documents": [
                    { "_id": "u1", "email": "same@example.test" },
                    { "_id": "u2", "email": "same@example.test" },
                ],
            },
        )
        .unwrap();

        let response = create_indexes(
            &conn,
            &doc! {
                "createIndexes": "users",
                "$db": "app",
                "indexes": [{ "key": { "email": 1_i32 }, "name": "email_1", "unique": true }],
            },
        )
        .unwrap();

        assert_command_error(&response);
        assert_eq!(response.get_i32("code").unwrap(), 11000);
    }

    #[test]
    fn unique_index_enforces_insert_update_and_upsert() {
        let conn = test_conn();
        insert_documents(
            &conn,
            &doc! {
                "insert": "users",
                "$db": "app",
                "documents": [
                    { "_id": "u1", "email": "ada@example.test" },
                    { "_id": "u2", "email": "grace@example.test" },
                ],
            },
        )
        .unwrap();
        create_indexes(
            &conn,
            &doc! {
                "createIndexes": "users",
                "$db": "app",
                "indexes": [{ "key": { "email": 1_i32 }, "name": "email_1", "unique": true }],
            },
        )
        .unwrap();

        let duplicate_insert = insert_documents(
            &conn,
            &doc! {
                "insert": "users",
                "$db": "app",
                "documents": [{ "_id": "u3", "email": "ada@example.test" }],
            },
        )
        .unwrap();
        assert_eq!(
            write_errors(&duplicate_insert)[0].get_i32("code").unwrap(),
            11000
        );

        let duplicate_update = update_documents(
            &conn,
            &doc! {
                "update": "users",
                "$db": "app",
                "updates": [{ "q": { "_id": "u2" }, "u": { "$set": { "email": "ada@example.test" } } }],
            },
        )
        .unwrap();
        assert_eq!(
            write_errors(&duplicate_update)[0].get_i32("code").unwrap(),
            11000
        );
        assert_eq!(
            first_batch(
                &find_documents(
                    &conn,
                    &doc! { "find": "users", "$db": "app", "filter": { "_id": "u2" } },
                )
                .unwrap()
            )[0]
            .get_str("email")
            .unwrap(),
            "grace@example.test"
        );

        let duplicate_upsert = update_documents(
            &conn,
            &doc! {
                "update": "users",
                "$db": "app",
                "updates": [
                    {
                        "q": { "_id": "u4" },
                        "u": { "$set": { "email": "ada@example.test" } },
                        "upsert": true,
                    }
                ],
            },
        )
        .unwrap();
        assert_eq!(
            write_errors(&duplicate_upsert)[0].get_i32("code").unwrap(),
            11000
        );
    }

    #[test]
    fn unique_unordered_bulk_continues_and_drop_index_disables_enforcement() {
        let conn = test_conn();
        create_indexes(
            &conn,
            &doc! {
                "createIndexes": "users",
                "$db": "app",
                "indexes": [{ "key": { "email": 1_i32 }, "name": "email_1", "unique": true }],
            },
        )
        .unwrap();

        let response = insert_documents(
            &conn,
            &doc! {
                "insert": "users",
                "$db": "app",
                "ordered": false,
                "documents": [
                    { "_id": "u1", "email": "same@example.test" },
                    { "_id": "u2", "email": "same@example.test" },
                    { "_id": "u3", "email": "other@example.test" },
                ],
            },
        )
        .unwrap();
        assert_eq!(response.get_i32("n").unwrap(), 2);
        assert_eq!(write_errors(&response)[0].get_i32("index").unwrap(), 1);

        drop_indexes(
            &conn,
            &doc! { "dropIndexes": "users", "$db": "app", "index": "email_1" },
        )
        .unwrap();
        let allowed = insert_documents(
            &conn,
            &doc! {
                "insert": "users",
                "$db": "app",
                "documents": [{ "_id": "u4", "email": "same@example.test" }],
            },
        )
        .unwrap();
        assert_eq!(allowed.get_i32("n").unwrap(), 1);
    }

    #[test]
    fn unique_index_rejects_array_values() {
        let conn = test_conn();
        insert_documents(
            &conn,
            &doc! {
                "insert": "users",
                "$db": "app",
                "documents": [{ "_id": "u1", "emails": ["a@example.test"] }],
            },
        )
        .unwrap();

        let response = create_indexes(
            &conn,
            &doc! {
                "createIndexes": "users",
                "$db": "app",
                "indexes": [{ "key": { "emails": 1_i32 }, "name": "emails_1", "unique": true }],
            },
        )
        .unwrap();
        assert_command_error(&response);
        assert_eq!(response.get_i32("code").unwrap(), 72);
    }

    #[test]
    fn find_and_modify_update_returns_pre_and_post_images_with_projection() {
        let conn = test_conn();
        seed_find_documents(&conn);

        let pre_image = handle_command(
            &conn,
            &doc! {
                "findAndModify": "users",
                "$db": "app",
                "query": { "_id": "u1" },
                "update": { "$inc": { "age": 1_i32 }, "$set": { "status": "seen" } },
                "fields": { "name": 1_i32, "age": 1_i32, "_id": 0_i32 },
            },
        )
        .unwrap();
        assert_eq!(pre_image.get_f64("ok").unwrap(), 1.0);
        let value = pre_image.get_document("value").unwrap();
        assert_eq!(value.get_str("name").unwrap(), "Ada");
        assert_eq!(value.get_i32("age").unwrap(), 37);
        assert!(!value.contains_key("_id"));
        let leo = pre_image.get_document("lastErrorObject").unwrap();
        assert_eq!(leo.get_i32("n").unwrap(), 1);
        assert!(leo.get_bool("updatedExisting").unwrap());

        let post_image = handle_command(
            &conn,
            &doc! {
                "findandmodify": "users",
                "$db": "app",
                "query": { "_id": "u1" },
                "update": { "$inc": { "age": 1_i32 } },
                "new": true,
                "projection": { "age": 1_i32, "status": 1_i32, "_id": 0_i32 },
            },
        )
        .unwrap();
        let value = post_image.get_document("value").unwrap();
        assert_eq!(value.get_i32("age").unwrap(), 39);
        assert_eq!(value.get_str("status").unwrap(), "seen");
    }

    #[test]
    fn find_and_modify_sorted_replace_and_delete_use_expected_target() {
        let conn = test_conn();
        seed_find_documents(&conn);

        let replaced = find_and_modify(
            &conn,
            "findAndModify",
            &doc! {
                "findAndModify": "users",
                "$db": "app",
                "query": { "profile.city": "Rome" },
                "sort": { "age": -1_i32 },
                "update": { "name": "Katherine Johnson", "age": 42_i32 },
                "new": true,
            },
        )
        .unwrap();
        let value = replaced.get_document("value").unwrap();
        assert_eq!(value.get_str("_id").unwrap(), "u3");
        assert_eq!(value.get_str("name").unwrap(), "Katherine Johnson");

        let removed = find_and_modify(
            &conn,
            "findAndModify",
            &doc! {
                "findAndModify": "users",
                "$db": "app",
                "query": { "profile.city": "Rome" },
                "sort": { "age": 1_i32 },
                "remove": true,
            },
        )
        .unwrap();
        assert_eq!(
            removed
                .get_document("value")
                .unwrap()
                .get_str("_id")
                .unwrap(),
            "u1"
        );
        assert_eq!(find_ids(&conn, doc! { "_id": "u1" }), Vec::<String>::new());
        assert_eq!(find_ids(&conn, doc! { "_id": "u3" }), vec!["u3"]);
    }

    #[test]
    fn find_and_modify_upsert_reports_inserted_document_and_last_error() {
        let conn = test_conn();

        let response = find_and_modify(
            &conn,
            "findAndModify",
            &doc! {
                "findAndModify": "users",
                "$db": "app",
                "query": { "_id": "u4", "email": "new@example.test" },
                "update": { "$set": { "name": "New" }, "$inc": { "count": 1_i32 } },
                "upsert": true,
                "new": true,
            },
        )
        .unwrap();

        let value = response.get_document("value").unwrap();
        assert_eq!(value.get_str("_id").unwrap(), "u4");
        assert_eq!(value.get_str("email").unwrap(), "new@example.test");
        assert_eq!(value.get_i32("count").unwrap(), 1);
        let leo = response.get_document("lastErrorObject").unwrap();
        assert_eq!(leo.get_i32("n").unwrap(), 1);
        assert!(!leo.get_bool("updatedExisting").unwrap());
        assert_eq!(leo.get_str("upserted").unwrap(), "u4");
    }

    #[test]
    fn find_and_modify_duplicate_key_and_id_immutability_are_command_errors() {
        let conn = test_conn();
        insert_documents(
            &conn,
            &doc! {
                "insert": "users",
                "$db": "app",
                "documents": [
                    { "_id": "u1", "email": "ada@example.test" },
                    { "_id": "u2", "email": "grace@example.test" },
                ],
            },
        )
        .unwrap();
        create_indexes(
            &conn,
            &doc! {
                "createIndexes": "users",
                "$db": "app",
                "indexes": [{ "key": { "email": 1_i32 }, "name": "email_1", "unique": true }],
            },
        )
        .unwrap();

        let duplicate = find_and_modify(
            &conn,
            "findAndModify",
            &doc! {
                "findAndModify": "users",
                "$db": "app",
                "query": { "_id": "u2" },
                "update": { "$set": { "email": "ada@example.test" } },
                "new": true,
            },
        )
        .unwrap();
        assert_command_error(&duplicate);
        assert_eq!(duplicate.get_i32("code").unwrap(), 11000);
        assert_eq!(
            first_batch(
                &find_documents(
                    &conn,
                    &doc! { "find": "users", "$db": "app", "filter": { "_id": "u2" } },
                )
                .unwrap(),
            )[0]
            .get_str("email")
            .unwrap(),
            "grace@example.test"
        );

        let id_change = find_and_modify(
            &conn,
            "findAndModify",
            &doc! {
                "findAndModify": "users",
                "$db": "app",
                "query": { "_id": "u1" },
                "update": { "_id": "changed", "email": "other@example.test" },
            },
        )
        .unwrap();
        assert_command_error(&id_change);
        assert!(id_change.get_str("errmsg").unwrap().contains("_id"));
    }

    #[test]
    fn find_and_modify_refreshes_index_entries_after_update_and_delete() {
        let conn = test_conn();
        seed_find_documents(&conn);
        create_indexes(
            &conn,
            &doc! {
                "createIndexes": "users",
                "$db": "app",
                "indexes": [{ "key": { "name": 1_i32 }, "name": "name_1" }],
            },
        )
        .unwrap();

        find_and_modify(
            &conn,
            "findAndModify",
            &doc! {
                "findAndModify": "users",
                "$db": "app",
                "query": { "_id": "u1" },
                "update": { "$set": { "name": "Ada Lovelace" } },
            },
        )
        .unwrap();
        assert_eq!(
            find_ids(&conn, doc! { "name": "Ada" }),
            Vec::<String>::new()
        );
        assert_eq!(find_ids(&conn, doc! { "name": "Ada Lovelace" }), vec!["u1"]);

        find_and_modify(
            &conn,
            "findAndModify",
            &doc! {
                "findAndModify": "users",
                "$db": "app",
                "query": { "_id": "u1" },
                "remove": true,
            },
        )
        .unwrap();
        assert_eq!(
            find_ids(&conn, doc! { "name": "Ada Lovelace" }),
            Vec::<String>::new()
        );
    }

    #[test]
    fn find_and_modify_rejects_malformed_and_unsupported_shapes() {
        let conn = test_conn();
        seed_find_documents(&conn);

        for command in [
            doc! { "findAndModify": "", "$db": "app", "query": {} },
            doc! { "findAndModify": "users", "$db": "app", "query": "bad", "remove": true },
            doc! { "findAndModify": "users", "$db": "app", "sort": "bad", "remove": true },
            doc! { "findAndModify": "users", "$db": "app", "projection": "bad", "remove": true },
            doc! { "findAndModify": "users", "$db": "app", "fields": { "name": 1_i32 }, "projection": { "age": 1_i32 }, "remove": true },
            doc! { "findAndModify": "users", "$db": "app", "remove": true, "update": { "$set": { "name": "x" } } },
            doc! { "findAndModify": "users", "$db": "app", "remove": false },
            doc! { "findAndModify": "users", "$db": "app" },
            doc! { "findAndModify": "users", "$db": "app", "update": [{ "$set": { "name": "x" } }] },
            doc! { "findAndModify": "users", "$db": "app", "update": "bad" },
            doc! { "findAndModify": "users", "$db": "app", "arrayFilters": [], "update": { "$set": { "name": "x" } } },
            doc! { "findAndModify": "users", "$db": "app", "collation": {}, "update": { "$set": { "name": "x" } } },
            doc! { "findAndModify": "users", "$db": "app", "hint": "_id_", "update": { "$set": { "name": "x" } } },
            doc! { "findAndModify": "users", "$db": "app", "writeConcern": { "w": 1_i32 }, "update": { "$set": { "name": "x" } } },
            doc! { "findAndModify": "users", "$db": "app", "bypassDocumentValidation": true, "update": { "$set": { "name": "x" } } },
            doc! { "findAndModify": "users", "$db": "app", "maxTimeMS": 1_i32, "update": { "$set": { "name": "x" } } },
            doc! { "findAndModify": "users", "$db": "app", "let": {}, "update": { "$set": { "name": "x" } } },
            doc! { "findAndModify": "users", "$db": "app", "remove": true, "upsert": true },
            doc! { "findAndModify": "users", "$db": "app", "query": { "$where": "bad" }, "remove": true },
            doc! { "findAndModify": "users", "$db": "app", "projection": { "name": { "$literal": 1_i32 } }, "remove": true },
        ] {
            let response = handle_command(&conn, &command).unwrap();
            assert_command_error(&response);
        }
    }

    #[test]
    fn planner_uses_index_entries_for_simple_equality() {
        let conn = test_conn();
        seed_find_documents(&conn);
        create_indexes(
            &conn,
            &doc! {
                "createIndexes": "users",
                "$db": "app",
                "indexes": [{ "key": { "profile.city": 1_i32 }, "name": "city_1" }],
            },
        )
        .unwrap();

        let candidates =
            indexed_candidate_documents(&conn, "app.users", &doc! { "profile.city": "Rome" })
                .unwrap()
                .unwrap();
        let candidate_ids = candidates
            .iter()
            .map(|doc| doc.get_str("_id").unwrap().to_string())
            .collect::<Vec<_>>();
        assert_eq!(candidate_ids, vec!["u1", "u3"]);

        assert_eq!(
            find_ids(&conn, doc! { "profile.city": "Rome" }),
            vec!["u1", "u3"]
        );
    }

    #[test]
    fn planner_entries_stay_fresh_after_update_delete_and_drop() {
        let conn = test_conn();
        seed_find_documents(&conn);
        create_indexes(
            &conn,
            &doc! {
                "createIndexes": "users",
                "$db": "app",
                "indexes": [{ "key": { "name": 1_i32 }, "name": "name_1" }],
            },
        )
        .unwrap();

        update_documents(
            &conn,
            &doc! {
                "update": "users",
                "$db": "app",
                "updates": [{ "q": { "_id": "u1" }, "u": { "$set": { "name": "Ada Lovelace" } } }],
            },
        )
        .unwrap();
        assert_eq!(
            find_ids(&conn, doc! { "name": "Ada" }),
            Vec::<String>::new()
        );
        assert_eq!(find_ids(&conn, doc! { "name": "Ada Lovelace" }), vec!["u1"]);

        delete_documents(
            &conn,
            &doc! { "delete": "users", "$db": "app", "deletes": [{ "q": { "_id": "u1" }, "limit": 1_i32 }] },
        )
        .unwrap();
        assert_eq!(
            find_ids(&conn, doc! { "name": "Ada Lovelace" }),
            Vec::<String>::new()
        );

        drop_indexes(
            &conn,
            &doc! { "dropIndexes": "users", "$db": "app", "index": "name_1" },
        )
        .unwrap();
        let count: i32 = conn
            .query_row(
                "SELECT COUNT(*) FROM index_entries WHERE namespace = 'app.users'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(count, 0);
    }

    #[test]
    fn empty_and_unknown_commands_are_command_errors() {
        let conn = test_conn();

        let empty = handle_command(&conn, &doc! {}).unwrap();
        assert_command_error(&empty);
        assert!(empty.get_str("errmsg").unwrap().contains("empty command"));

        let unknown = handle_command(&conn, &doc! { "collStats": "users", "$db": "app" }).unwrap();
        assert_command_error(&unknown);
        assert!(unknown.get_str("errmsg").unwrap().contains("not supported"));
    }

    #[test]
    fn malformed_insert_without_documents_is_rejected() {
        let conn = test_conn();
        let response = insert_documents(&conn, &doc! { "insert": "users", "$db": "app" }).unwrap();
        assert_command_error(&response);
        assert!(
            response
                .get_str("errmsg")
                .unwrap()
                .contains("documents array")
        );
    }

    #[test]
    fn malformed_find_without_collection_name_is_rejected() {
        let conn = test_conn();
        let response = find_documents(&conn, &doc! { "find": 1_i32, "$db": "app" }).unwrap();
        assert_command_error(&response);
        assert!(
            response
                .get_str("errmsg")
                .unwrap()
                .contains("requires a collection")
        );
    }

    #[test]
    fn insert_duplicate_id_reports_write_error_and_preserves_original() {
        let conn = test_conn();
        insert_documents(
            &conn,
            &doc! {
                "insert": "users",
                "$db": "app",
                "documents": [{ "_id": "u1", "name": "Ada" }],
            },
        )
        .unwrap();

        let duplicate = insert_documents(
            &conn,
            &doc! {
                "insert": "users",
                "$db": "app",
                "documents": [{ "_id": "u1", "name": "Grace" }],
            },
        )
        .unwrap();

        assert_eq!(duplicate.get_f64("ok").unwrap(), 1.0);
        assert_eq!(duplicate.get_i32("n").unwrap(), 0);
        assert_eq!(write_errors(&duplicate)[0].get_i32("code").unwrap(), 11000);

        let stored = first_batch(
            &find_documents(
                &conn,
                &doc! { "find": "users", "$db": "app", "filter": { "_id": "u1" } },
            )
            .unwrap(),
        );
        assert_eq!(stored[0].get_str("name").unwrap(), "Ada");
    }

    #[test]
    fn insert_ordered_duplicate_stops_before_later_documents() {
        let conn = test_conn();
        let response = insert_documents(
            &conn,
            &doc! {
                "insert": "users",
                "$db": "app",
                "ordered": true,
                "documents": [
                    { "_id": "u1", "name": "Ada" },
                    { "_id": "u1", "name": "Duplicate" },
                    { "_id": "u2", "name": "Grace" },
                ],
            },
        )
        .unwrap();

        assert_eq!(response.get_i32("n").unwrap(), 1);
        assert_eq!(write_errors(&response)[0].get_i32("index").unwrap(), 1);
        assert!(
            first_batch(&find_documents(&conn, &doc! { "find": "users", "$db": "app" }).unwrap())
                .iter()
                .all(|doc| doc.get_str("_id").unwrap() != "u2")
        );
    }

    #[test]
    fn insert_unordered_duplicate_continues_after_errors() {
        let conn = test_conn();
        insert_documents(
            &conn,
            &doc! {
                "insert": "users",
                "$db": "app",
                "documents": [{ "_id": "u1", "name": "Ada" }],
            },
        )
        .unwrap();

        let response = insert_documents(
            &conn,
            &doc! {
                "insert": "users",
                "$db": "app",
                "ordered": false,
                "documents": [
                    { "_id": "u2", "name": "Grace" },
                    { "_id": "u1", "name": "Duplicate" },
                    { "_id": "u3", "name": "Katherine" },
                    { "_id": "u2", "name": "Duplicate again" },
                ],
            },
        )
        .unwrap();

        assert_eq!(response.get_i32("n").unwrap(), 2);
        let errors = write_errors(&response);
        assert_eq!(errors.len(), 2);
        assert_eq!(errors[0].get_i32("index").unwrap(), 1);
        assert_eq!(errors[1].get_i32("index").unwrap(), 3);
        assert_eq!(
            first_batch(&find_documents(&conn, &doc! { "find": "users", "$db": "app" }).unwrap())
                .len(),
            3
        );
    }

    #[test]
    fn insert_generates_id_and_stores_operator_shaped_field_names_as_data() {
        let conn = test_conn();
        let response = insert_documents(
            &conn,
            &doc! {
                "insert": "events",
                "$db": "app",
                "documents": [{ "$set": "literal", "a.$bad": true }],
            },
        )
        .unwrap();

        assert_eq!(response.get_i32("n").unwrap(), 1);
        let docs =
            first_batch(&find_documents(&conn, &doc! { "find": "events", "$db": "app" }).unwrap());
        assert!(docs[0].contains_key("_id"));
        assert_eq!(docs[0].get_str("$set").unwrap(), "literal");
        assert!(docs[0].get_bool("a.$bad").unwrap());
    }

    #[test]
    fn insert_rejects_malformed_shapes_and_unsupported_options() {
        let conn = test_conn();

        for command in [
            doc! { "insert": 1_i32, "$db": "app", "documents": [] },
            doc! { "insert": "", "$db": "app", "documents": [] },
            doc! { "insert": "users", "$db": "app", "documents": [] },
            doc! { "insert": "users", "$db": "app", "documents": [1_i32] },
            doc! { "insert": "users", "$db": "app", "ordered": "yes", "documents": [{ "_id": "u1" }] },
            doc! { "insert": "users", "$db": "app", "writeConcern": { "w": 1_i32 }, "documents": [{ "_id": "u1" }] },
        ] {
            let response = insert_documents(&conn, &command).unwrap();
            assert_command_error(&response);
        }
    }

    fn seed_find_documents(conn: &Connection) {
        insert_documents(
            conn,
            &doc! {
                "insert": "users",
                "$db": "app",
                "documents": [
                    {
                        "_id": "u1",
                        "name": "Ada",
                        "age": 37_i32,
                        "score": 7_i64,
                        "active": true,
                        "profile": { "city": "Rome", "$set": "literal" },
                        "tags": ["math", "logic"],
                        "nested": [{ "kind": "first" }, { "kind": "second" }],
                        "nothing": Bson::Null,
                    },
                    {
                        "_id": "u2",
                        "name": "Grace",
                        "age": 39_i64,
                        "score": 9.5,
                        "active": false,
                        "profile": { "city": "London" },
                        "tags": ["systems"],
                    },
                    {
                        "_id": "u3",
                        "name": "Katherine",
                        "age": 41.0,
                        "active": true,
                        "profile": { "city": "Rome" },
                        "tags": [],
                    },
                ],
            },
        )
        .unwrap();
    }

    fn find_ids(conn: &Connection, filter: Document) -> Vec<String> {
        first_batch(
            &find_documents(
                &conn,
                &doc! { "find": "users", "$db": "app", "filter": filter },
            )
            .unwrap(),
        )
        .into_iter()
        .map(|doc| doc.get_str("_id").unwrap().to_string())
        .collect()
    }

    #[test]
    fn find_matcher_supports_field_equality_and_dotted_paths() {
        let conn = test_conn();
        seed_find_documents(&conn);

        assert_eq!(find_ids(&conn, doc! { "name": "Ada" }), vec!["u1"]);
        assert_eq!(
            find_ids(&conn, doc! { "profile.city": "Rome" }),
            vec!["u1", "u3"]
        );
        assert_eq!(find_ids(&conn, doc! { "active": false }), vec!["u2"]);
        assert_eq!(find_ids(&conn, doc! { "nothing": Bson::Null }), vec!["u1"]);
        assert_eq!(find_ids(&conn, doc! { "tags": "logic" }), vec!["u1"]);
        assert_eq!(
            find_ids(&conn, doc! { "nested.kind": "second" }),
            vec!["u1"]
        );
        assert_eq!(
            find_ids(&conn, doc! { "profile.$set": "literal" }),
            vec!["u1"]
        );
    }

    #[test]
    fn matcher_pure_functions_cover_documents_arrays_and_missing_paths() {
        let document = doc! {
            "a": { "b": 2_i32 },
            "arr": [1_i32, 2_i32],
            "typed": 2_i64,
        };

        assert!(matches_filter(&document, &doc! { "a": { "b": 2_i32 } }).unwrap());
        assert!(matches_filter(&document, &doc! { "arr": [1_i32, 2_i32] }).unwrap());
        assert!(matches_filter(&document, &doc! { "arr": 2_i64 }).unwrap());
        assert!(matches_filter(&document, &doc! { "typed": 2.0 }).unwrap());
        assert!(!matches_filter(&document, &doc! { "a.b.c": 1_i32 }).unwrap());
        assert!(!matches_filter(&document, &doc! { "a.b.c.d.e": 1_i32 }).unwrap());
    }

    #[test]
    fn find_matcher_supports_field_operators_and_mixed_numeric_types() {
        let conn = test_conn();
        seed_find_documents(&conn);

        assert_eq!(
            find_ids(&conn, doc! { "age": { "$eq": 37_i64 } }),
            vec!["u1"]
        );
        assert_eq!(
            find_ids(&conn, doc! { "age": { "$ne": 39_i32 } }),
            vec!["u1", "u3"]
        );
        assert_eq!(
            find_ids(&conn, doc! { "age": { "$gte": 39_i32, "$lte": 41_i32 } }),
            vec!["u2", "u3"]
        );
        assert_eq!(
            find_ids(&conn, doc! { "score": { "$gt": 7_i32 } }),
            vec!["u2"]
        );
        assert_eq!(
            find_ids(&conn, doc! { "name": { "$in": ["Ada", "Katherine"] } }),
            vec!["u1", "u3"]
        );
        assert_eq!(
            find_ids(&conn, doc! { "name": { "$nin": ["Ada", "Grace"] } }),
            vec!["u3"]
        );
        assert_eq!(
            find_ids(&conn, doc! { "score": { "$exists": false } }),
            vec!["u3"]
        );
        assert_eq!(
            find_ids(&conn, doc! { "age": { "$not": { "$lt": 39_i32 } } }),
            vec!["u2", "u3"]
        );
    }

    #[test]
    fn find_matcher_supports_logical_operators() {
        let conn = test_conn();
        seed_find_documents(&conn);

        assert_eq!(
            find_ids(
                &conn,
                doc! { "$and": [{ "profile.city": "Rome" }, { "active": true }] }
            ),
            vec!["u1", "u3"]
        );
        assert_eq!(
            find_ids(
                &conn,
                doc! { "$or": [{ "name": "Ada" }, { "name": "Grace" }] }
            ),
            vec!["u1", "u2"]
        );
        assert_eq!(
            find_ids(&conn, doc! { "$nor": [{ "profile.city": "Rome" }] }),
            vec!["u2"]
        );
    }

    #[test]
    fn find_rejects_unsupported_and_malformed_query_operators() {
        let conn = test_conn();
        seed_find_documents(&conn);

        for filter in [
            doc! { "$where": "this.age > 1" },
            doc! { "name": { "$regex": "A" } },
            doc! { "tags": { "$elemMatch": { "$eq": "math" } } },
            doc! { "tags": { "$all": ["math"] } },
            doc! { "age": { "$in": "Ada" } },
            doc! { "age": { "$nin": "Ada" } },
            doc! { "age": { "$exists": 1_i32 } },
            doc! { "$and": [] },
            doc! { "$or": [1_i32] },
            doc! { "age": { "$not": 5_i32 } },
            doc! { "age": { "$gte": 1_i32, "literal": 2_i32 } },
        ] {
            let response = find_documents(
                &conn,
                &doc! { "find": "users", "$db": "app", "filter": filter },
            )
            .unwrap();
            assert_command_error(&response);
        }
    }

    #[test]
    fn find_rejects_non_document_filter() {
        let conn = test_conn();
        let response = find_documents(
            &conn,
            &doc! { "find": "users", "$db": "app", "filter": 1_i32 },
        )
        .unwrap();
        assert_command_error(&response);
    }

    #[test]
    fn find_projection_supports_inclusion_exclusion_and_id_override() {
        let conn = test_conn();
        seed_find_documents(&conn);

        let included = first_batch(
            &find_documents(
                &conn,
                &doc! {
                    "find": "users",
                    "$db": "app",
                    "filter": { "_id": "u1" },
                    "projection": { "name": 1_i32, "profile.city": 1_i32, "_id": 0_i32 },
                },
            )
            .unwrap(),
        );
        assert!(!included[0].contains_key("_id"));
        assert_eq!(included[0].get_str("name").unwrap(), "Ada");
        assert_eq!(
            included[0]
                .get_document("profile")
                .unwrap()
                .get_str("city")
                .unwrap(),
            "Rome"
        );
        assert!(!included[0].contains_key("age"));

        let excluded = first_batch(
            &find_documents(
                &conn,
                &doc! {
                    "find": "users",
                    "$db": "app",
                    "filter": { "_id": "u1" },
                    "projection": { "profile.city": 0_i32, "tags": false },
                },
            )
            .unwrap(),
        );
        assert!(excluded[0].contains_key("_id"));
        assert!(
            !excluded[0]
                .get_document("profile")
                .unwrap()
                .contains_key("city")
        );
        assert!(!excluded[0].contains_key("tags"));
    }

    #[test]
    fn find_projection_supports_id_only_specs() {
        let conn = test_conn();
        seed_find_documents(&conn);

        let excluded_id = first_batch(
            &find_documents(
                &conn,
                &doc! {
                    "find": "users",
                    "$db": "app",
                    "filter": { "_id": "u1" },
                    "projection": { "_id": 0_i32 },
                },
            )
            .unwrap(),
        );
        assert!(!excluded_id[0].contains_key("_id"));
        assert_eq!(excluded_id[0].get_str("name").unwrap(), "Ada");
        assert_eq!(excluded_id[0].get_i32("age").unwrap(), 37);

        let included_id = first_batch(
            &find_documents(
                &conn,
                &doc! {
                    "find": "users",
                    "$db": "app",
                    "filter": { "_id": "u1" },
                    "projection": { "_id": 1_i32 },
                },
            )
            .unwrap(),
        );
        assert_eq!(included_id[0].len(), 1);
        assert_eq!(included_id[0].get_str("_id").unwrap(), "u1");

        let included_without_id = first_batch(
            &find_documents(
                &conn,
                &doc! {
                    "find": "users",
                    "$db": "app",
                    "filter": { "_id": "u1" },
                    "projection": { "name": 1_i32, "_id": 0_i32 },
                },
            )
            .unwrap(),
        );
        assert_eq!(included_without_id[0].len(), 1);
        assert_eq!(included_without_id[0].get_str("name").unwrap(), "Ada");
        assert!(!included_without_id[0].contains_key("_id"));
    }

    #[test]
    fn find_sort_skip_limit_and_batch_size_shape_results() {
        let conn = test_conn();
        seed_find_documents(&conn);

        let sorted = first_batch(
            &find_documents(
                &conn,
                &doc! {
                    "find": "users",
                    "$db": "app",
                    "sort": { "age": -1_i32 },
                    "skip": 1_i32,
                    "limit": 1_i32,
                },
            )
            .unwrap(),
        );
        assert_eq!(sorted.len(), 1);
        assert_eq!(sorted[0].get_str("_id").unwrap(), "u2");

        let limited_batch = first_batch(
            &find_documents(
                &conn,
                &doc! {
                    "find": "users",
                    "$db": "app",
                    "sort": { "profile.city": 1_i32, "_id": 1_i32 },
                    "batchSize": 2_i32,
                    "limit": 0_i32,
                },
            )
            .unwrap(),
        );
        assert_eq!(limited_batch.len(), 2);
        assert_eq!(limited_batch[0].get_str("_id").unwrap(), "u2");
    }

    #[test]
    fn find_sort_handles_missing_and_mixed_bson_types_deterministically() {
        let conn = test_conn();
        insert_documents(
            &conn,
            &doc! {
                "insert": "mixed",
                "$db": "app",
                "documents": [
                    { "_id": "a", "value": "string" },
                    { "_id": "b", "value": 3_i32 },
                    { "_id": "c" },
                    { "_id": "d", "value": false },
                ],
            },
        )
        .unwrap();

        let ids = first_batch(
            &find_documents(
                &conn,
                &doc! { "find": "mixed", "$db": "app", "sort": { "value": 1_i32 } },
            )
            .unwrap(),
        )
        .into_iter()
        .map(|doc| doc.get_str("_id").unwrap().to_string())
        .collect::<Vec<_>>();
        assert_eq!(ids, vec!["c", "d", "b", "a"]);
    }

    #[test]
    fn find_rejects_invalid_projection_sort_and_bounds() {
        let conn = test_conn();
        seed_find_documents(&conn);

        for command in [
            doc! { "find": "users", "$db": "app", "projection": { "name": 1_i32, "age": 0_i32 } },
            doc! { "find": "users", "$db": "app", "projection": { "name": 2_i32 } },
            doc! { "find": "users", "$db": "app", "projection": { "$bad": 1_i32 } },
            doc! { "find": "users", "$db": "app", "projection": { "profile": 1_i32, "profile.city": 1_i32 } },
            doc! { "find": "users", "$db": "app", "sort": { "age": 2_i32 } },
            doc! { "find": "users", "$db": "app", "sort": { "$bad": 1_i32 } },
            doc! { "find": "users", "$db": "app", "skip": -1_i32 },
            doc! { "find": "users", "$db": "app", "limit": -1_i32 },
            doc! { "find": "users", "$db": "app", "batchSize": -1_i32 },
            doc! { "find": "users", "$db": "app", "batchSize": 1.5 },
        ] {
            let response = find_documents(&conn, &command).unwrap();
            assert_command_error(&response);
        }
    }

    #[test]
    fn find_zero_batch_size_returns_empty_closed_batch_and_large_skip_is_empty() {
        let conn = test_conn();
        seed_find_documents(&conn);

        let zero_batch = first_batch(
            &find_documents(
                &conn,
                &doc! { "find": "users", "$db": "app", "batchSize": 0_i32 },
            )
            .unwrap(),
        );
        assert!(zero_batch.is_empty());

        let skipped = first_batch(
            &find_documents(
                &conn,
                &doc! { "find": "users", "$db": "app", "skip": 10_000_i64 },
            )
            .unwrap(),
        );
        assert!(skipped.is_empty());
    }

    #[test]
    fn update_replacement_preserves_id_and_counts_modified_documents() {
        let conn = test_conn();
        seed_find_documents(&conn);

        let response = update_documents(
            &conn,
            &doc! {
                "update": "users",
                "$db": "app",
                "updates": [
                    { "q": { "_id": "u1" }, "u": { "name": "Ada Lovelace", "age": 38_i32 } }
                ],
            },
        )
        .unwrap();

        assert_eq!(response.get_i32("n").unwrap(), 1);
        assert_eq!(response.get_i32("nModified").unwrap(), 1);
        let docs = first_batch(
            &find_documents(
                &conn,
                &doc! { "find": "users", "$db": "app", "filter": { "_id": "u1" } },
            )
            .unwrap(),
        );
        assert_eq!(docs[0].get_str("_id").unwrap(), "u1");
        assert_eq!(docs[0].get_str("name").unwrap(), "Ada Lovelace");
        assert!(!docs[0].contains_key("profile"));
    }

    #[test]
    fn update_modifiers_support_set_unset_inc_single_and_multi() {
        let conn = test_conn();
        seed_find_documents(&conn);

        let single = update_documents(
            &conn,
            &doc! {
                "update": "users",
                "$db": "app",
                "updates": [
                    {
                        "q": { "_id": "u1" },
                        "u": {
                            "$set": { "profile.city": "Milan", "profile.country": "IT" },
                            "$unset": { "tags": "" },
                            "$inc": { "age": 1_i32, "newCounter": 2_i32 },
                        },
                    }
                ],
            },
        )
        .unwrap();
        assert_eq!(single.get_i32("n").unwrap(), 1);
        assert_eq!(single.get_i32("nModified").unwrap(), 1);

        let multi = update_documents(
            &conn,
            &doc! {
                "update": "users",
                "$db": "app",
                "updates": [
                    { "q": { "active": true }, "u": { "$inc": { "age": 1_i32 } }, "multi": true }
                ],
            },
        )
        .unwrap();
        assert_eq!(multi.get_i32("n").unwrap(), 2);
        assert_eq!(multi.get_i32("nModified").unwrap(), 2);

        let docs = first_batch(
            &find_documents(
                &conn,
                &doc! { "find": "users", "$db": "app", "sort": { "_id": 1_i32 } },
            )
            .unwrap(),
        );
        assert_eq!(
            docs[0]
                .get_document("profile")
                .unwrap()
                .get_str("city")
                .unwrap(),
            "Milan"
        );
        assert_eq!(docs[0].get_i32("age").unwrap(), 39);
        assert_eq!(docs[0].get_i32("newCounter").unwrap(), 2);
        assert!(!docs[0].contains_key("tags"));
        assert_eq!(docs[2].get_f64("age").unwrap(), 42.0);
    }

    #[test]
    fn update_inc_handles_mixed_integer_precision_and_overflow() {
        let conn = test_conn();
        insert_documents(
            &conn,
            &doc! {
                "insert": "numbers",
                "$db": "app",
                "documents": [
                    { "_id": "overflow", "value": Bson::Int64(i64::MAX) },
                    { "_id": "exact", "value": Bson::Int64(i64::MAX - 1) },
                    { "_id": "promote", "value": Bson::Int32(i32::MAX) },
                ],
            },
        )
        .unwrap();

        let overflow = update_documents(
            &conn,
            &doc! {
                "update": "numbers",
                "$db": "app",
                "updates": [
                    { "q": { "_id": "overflow" }, "u": { "$inc": { "value": 1_i32 } } }
                ],
            },
        )
        .unwrap();
        assert_eq!(overflow.get_f64("ok").unwrap(), 1.0);
        assert_eq!(write_errors(&overflow)[0].get_i32("index").unwrap(), 0);
        let overflow_docs = first_batch(
            &find_documents(
                &conn,
                &doc! { "find": "numbers", "$db": "app", "filter": { "_id": "overflow" } },
            )
            .unwrap(),
        );
        assert_eq!(overflow_docs[0].get_i64("value").unwrap(), i64::MAX);

        let exact = update_documents(
            &conn,
            &doc! {
                "update": "numbers",
                "$db": "app",
                "updates": [
                    { "q": { "_id": "exact" }, "u": { "$inc": { "value": 1_i32 } } }
                ],
            },
        )
        .unwrap();
        assert_eq!(exact.get_i32("n").unwrap(), 1);
        assert_eq!(exact.get_i32("nModified").unwrap(), 1);
        let exact_docs = first_batch(
            &find_documents(
                &conn,
                &doc! { "find": "numbers", "$db": "app", "filter": { "_id": "exact" } },
            )
            .unwrap(),
        );
        assert_eq!(exact_docs[0].get_i64("value").unwrap(), i64::MAX);

        let promoted = update_documents(
            &conn,
            &doc! {
                "update": "numbers",
                "$db": "app",
                "updates": [
                    { "q": { "_id": "promote" }, "u": { "$inc": { "value": 1_i32 } } }
                ],
            },
        )
        .unwrap();
        assert_eq!(promoted.get_i32("n").unwrap(), 1);
        assert_eq!(promoted.get_i32("nModified").unwrap(), 1);
        let promoted_docs = first_batch(
            &find_documents(
                &conn,
                &doc! { "find": "numbers", "$db": "app", "filter": { "_id": "promote" } },
            )
            .unwrap(),
        );
        assert_eq!(
            promoted_docs[0].get_i64("value").unwrap(),
            i32::MAX as i64 + 1
        );
    }

    #[test]
    fn update_upsert_supports_replacement_and_modifier_updates() {
        let conn = test_conn();
        seed_find_documents(&conn);

        let replacement = update_documents(
            &conn,
            &doc! {
                "update": "users",
                "$db": "app",
                "updates": [
                    {
                        "q": { "_id": "u4" },
                        "u": { "name": "Dorothy", "age": 44_i32 },
                        "upsert": true,
                    }
                ],
            },
        )
        .unwrap();
        assert_eq!(replacement.get_i32("n").unwrap(), 1);
        assert_eq!(replacement.get_i32("nModified").unwrap(), 0);
        assert_eq!(replacement.get_i32("nUpserted").unwrap(), 1);

        let modifier = update_documents(
            &conn,
            &doc! {
                "update": "users",
                "$db": "app",
                "updates": [
                    {
                        "q": { "name": "Mary", "profile.city": { "$eq": "Arlington" } },
                        "u": { "$set": { "active": true }, "$inc": { "score": 3_i32 } },
                        "upsert": true,
                    }
                ],
            },
        )
        .unwrap();
        assert_eq!(modifier.get_i32("n").unwrap(), 1);
        assert_eq!(modifier.get_i32("nUpserted").unwrap(), 1);

        assert_eq!(find_ids(&conn, doc! { "_id": "u4" }), vec!["u4"]);
        let mary = first_batch(
            &find_documents(
                &conn,
                &doc! { "find": "users", "$db": "app", "filter": { "name": "Mary" } },
            )
            .unwrap(),
        );
        assert_eq!(
            mary[0]
                .get_document("profile")
                .unwrap()
                .get_str("city")
                .unwrap(),
            "Arlington"
        );
        assert!(mary[0].get_bool("active").unwrap());
        assert_eq!(mary[0].get_i32("score").unwrap(), 3);
    }

    #[test]
    fn update_duplicate_key_upsert_returns_write_error_and_preserves_existing_document() {
        let conn = test_conn();
        seed_find_documents(&conn);

        let response = update_documents(
            &conn,
            &doc! {
                "update": "users",
                "$db": "app",
                "updates": [
                    {
                        "q": { "name": "Nobody" },
                        "u": { "_id": "u1", "name": "Replacement" },
                        "upsert": true,
                    }
                ],
            },
        )
        .unwrap();

        assert_eq!(response.get_f64("ok").unwrap(), 1.0);
        assert_eq!(write_errors(&response)[0].get_i32("code").unwrap(), 11000);
        assert_eq!(find_ids(&conn, doc! { "name": "Ada" }), vec!["u1"]);
    }

    #[test]
    fn update_ordered_and_unordered_batches_handle_partial_failures() {
        let conn = test_conn();
        seed_find_documents(&conn);

        let ordered = update_documents(
            &conn,
            &doc! {
                "update": "users",
                "$db": "app",
                "ordered": true,
                "updates": [
                    { "q": { "_id": "u1" }, "u": { "$inc": { "name": 1_i32 } } },
                    { "q": { "_id": "u2" }, "u": { "$set": { "name": "Changed" } } },
                ],
            },
        )
        .unwrap();
        assert_eq!(ordered.get_i32("n").unwrap(), 0);
        assert_eq!(write_errors(&ordered)[0].get_i32("index").unwrap(), 0);
        assert_eq!(
            find_ids(&conn, doc! { "name": "Changed" }),
            Vec::<String>::new()
        );

        let unordered = update_documents(
            &conn,
            &doc! {
                "update": "users",
                "$db": "app",
                "ordered": false,
                "updates": [
                    { "q": { "_id": "u1" }, "u": { "$inc": { "name": 1_i32 } } },
                    { "q": { "_id": "u2" }, "u": { "$set": { "name": "Changed" } } },
                ],
            },
        )
        .unwrap();
        assert_eq!(unordered.get_i32("n").unwrap(), 1);
        assert_eq!(unordered.get_i32("nModified").unwrap(), 1);
        assert_eq!(write_errors(&unordered)[0].get_i32("index").unwrap(), 0);
        assert_eq!(find_ids(&conn, doc! { "name": "Changed" }), vec!["u2"]);
    }

    #[test]
    fn update_rejects_malformed_and_adversarial_shapes() {
        let conn = test_conn();
        seed_find_documents(&conn);

        for command in [
            doc! { "update": "users", "$db": "app" },
            doc! { "update": "users", "$db": "app", "updates": "bad" },
            doc! { "update": "users", "$db": "app", "updates": [] },
            doc! { "update": "", "$db": "app", "updates": [] },
            doc! { "update": "users", "$db": "app", "ordered": "yes", "updates": [] },
            doc! { "update": "users", "$db": "app", "writeConcern": { "w": 1_i32 }, "updates": [] },
        ] {
            let response = update_documents(&conn, &command).unwrap();
            assert_command_error(&response);
        }

        for update in [
            doc! { "u": { "$set": { "name": "x" } } },
            doc! { "q": { "_id": "u1" } },
            doc! { "q": 1_i32, "u": { "$set": { "name": "x" } } },
            doc! { "q": { "_id": "u1" }, "u": 1_i32 },
            doc! { "q": { "_id": "u1" }, "u": {} },
            doc! { "q": { "_id": "u1" }, "u": { "$set": { "name": "x" }, "plain": true } },
            doc! { "q": { "_id": "u1" }, "u": { "$rename": { "name": "n" } } },
            doc! { "q": { "_id": "u1" }, "u": { "$push": { "tags": "x" } } },
            doc! { "q": { "_id": "u1" }, "u": { "$pull": { "tags": "x" } } },
            doc! { "q": { "_id": "u1" }, "u": { "_id": "other", "name": "x" } },
            doc! { "q": { "_id": "u1" }, "u": { "$inc": { "name": 1_i32 } } },
            doc! { "q": { "_id": "u1" }, "u": { "$inc": { "age": "x" } } },
            doc! { "q": { "_id": "u1" }, "u": { "$set": { "age.value": 1_i32 } } },
            doc! { "q": { "_id": "u1" }, "u": { "$set": { "profile": 1_i32, "profile.city": "x" } } },
            doc! { "q": { "$where": "bad" }, "u": { "$set": { "name": "x" } } },
            doc! { "q": { "_id": "u1" }, "u": { "$set": { "_id": "x" } } },
            doc! { "q": { "_id": "u1" }, "u": { "$unset": { "_id": "" } } },
        ] {
            let response = update_documents(
                &conn,
                &doc! { "update": "users", "$db": "app", "updates": [update] },
            )
            .unwrap();
            assert_eq!(response.get_f64("ok").unwrap(), 1.0);
            assert!(!write_errors(&response).is_empty());
        }
    }

    #[test]
    fn delete_one_and_delete_many_remove_matched_documents() {
        let conn = test_conn();
        seed_find_documents(&conn);

        let one = delete_documents(
            &conn,
            &doc! {
                "delete": "users",
                "$db": "app",
                "deletes": [{ "q": { "profile.city": "Rome" }, "limit": 1_i32 }],
            },
        )
        .unwrap();
        assert_eq!(one.get_i32("n").unwrap(), 1);
        assert_eq!(find_ids(&conn, doc! { "profile.city": "Rome" }), vec!["u3"]);

        let many = delete_documents(
            &conn,
            &doc! {
                "delete": "users",
                "$db": "app",
                "deletes": [{ "q": { "active": false }, "limit": 0_i32 }],
            },
        )
        .unwrap();
        assert_eq!(many.get_i32("n").unwrap(), 1);
        assert_eq!(find_ids(&conn, doc! {}), vec!["u3"]);
    }

    #[test]
    fn delete_empty_and_repeated_delete_are_noops_with_zero_count() {
        let conn = test_conn();

        let empty = delete_documents(
            &conn,
            &doc! {
                "delete": "users",
                "$db": "app",
                "deletes": [{ "q": { "_id": "u1" }, "limit": 1_i32 }],
            },
        )
        .unwrap();
        assert_eq!(empty.get_i32("n").unwrap(), 0);

        seed_find_documents(&conn);
        let first = delete_documents(
            &conn,
            &doc! {
                "delete": "users",
                "$db": "app",
                "deletes": [{ "q": { "_id": "u1" }, "limit": 1_i32 }],
            },
        )
        .unwrap();
        let second = delete_documents(
            &conn,
            &doc! {
                "delete": "users",
                "$db": "app",
                "deletes": [{ "q": { "_id": "u1" }, "limit": 1_i32 }],
            },
        )
        .unwrap();
        assert_eq!(first.get_i32("n").unwrap(), 1);
        assert_eq!(second.get_i32("n").unwrap(), 0);
    }

    #[test]
    fn delete_ordered_and_unordered_batches_handle_partial_failures() {
        let conn = test_conn();
        seed_find_documents(&conn);

        let ordered = delete_documents(
            &conn,
            &doc! {
                "delete": "users",
                "$db": "app",
                "ordered": true,
                "deletes": [
                    { "q": { "$where": "bad" }, "limit": 1_i32 },
                    { "q": { "_id": "u1" }, "limit": 1_i32 },
                ],
            },
        )
        .unwrap();
        assert_eq!(ordered.get_i32("n").unwrap(), 0);
        assert_eq!(write_errors(&ordered)[0].get_i32("index").unwrap(), 0);
        assert_eq!(find_ids(&conn, doc! { "_id": "u1" }), vec!["u1"]);

        let unordered = delete_documents(
            &conn,
            &doc! {
                "delete": "users",
                "$db": "app",
                "ordered": false,
                "deletes": [
                    { "q": { "$where": "bad" }, "limit": 1_i32 },
                    { "q": { "_id": "u1" }, "limit": 1_i32 },
                ],
            },
        )
        .unwrap();
        assert_eq!(unordered.get_i32("n").unwrap(), 1);
        assert_eq!(write_errors(&unordered)[0].get_i32("index").unwrap(), 0);
        assert!(find_ids(&conn, doc! { "_id": "u1" }).is_empty());
    }

    #[test]
    fn delete_rejects_malformed_and_adversarial_shapes() {
        let conn = test_conn();
        seed_find_documents(&conn);

        for command in [
            doc! { "delete": "users", "$db": "app" },
            doc! { "delete": "users", "$db": "app", "deletes": "bad" },
            doc! { "delete": "users", "$db": "app", "deletes": [] },
            doc! { "delete": "", "$db": "app", "deletes": [] },
            doc! { "delete": "users", "$db": "app", "ordered": "yes", "deletes": [] },
            doc! { "delete": "users", "$db": "app", "writeConcern": { "w": 1_i32 }, "deletes": [] },
        ] {
            let response = delete_documents(&conn, &command).unwrap();
            assert_command_error(&response);
        }

        for delete in [
            doc! { "limit": 1_i32 },
            doc! { "q": 1_i32, "limit": 1_i32 },
            doc! { "q": { "_id": "u1" } },
            doc! { "q": { "_id": "u1" }, "limit": 2_i32 },
            doc! { "q": { "_id": "u1" }, "limit": -1_i32 },
            doc! { "q": { "_id": "u1" }, "limit": "1" },
            doc! { "q": { "$where": "bad" }, "limit": 1_i32 },
            doc! { "q": { "_id": "u1" }, "limit": 1_i32, "hint": { "_id": 1_i32 } },
        ] {
            let response = delete_documents(
                &conn,
                &doc! { "delete": "users", "$db": "app", "deletes": [delete] },
            )
            .unwrap();
            assert_eq!(response.get_f64("ok").unwrap(), 1.0);
            assert!(!write_errors(&response).is_empty());
        }
    }
}
