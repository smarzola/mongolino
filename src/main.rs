use std::collections::{HashMap, HashSet, VecDeque};
use std::env;
use std::io::Cursor;
use std::path::PathBuf;
use std::sync::atomic::{AtomicI32, Ordering};

use bson::{Bson, Document, doc, oid::ObjectId};
use regex::RegexBuilder;
use rusqlite::{Connection, OptionalExtension, params};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

const OP_REPLY: i32 = 1;
const OP_QUERY: i32 = 2004;
const OP_MSG: i32 = 2013;
const MAX_MESSAGE_BYTES: usize = 48 * 1024 * 1024;
const DOCUMENT_VALIDATION_ERROR_CODE: i32 = 121;
const RETRYABLE_WRITE_CACHE_LIMIT: usize = 128;

static NEXT_REQUEST_ID: AtomicI32 = AtomicI32::new(1);

pub(crate) type Result<T> = std::result::Result<T, MongolinoError>;

#[derive(Debug)]
pub(crate) enum MongolinoError {
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
pub(crate) struct ClientState {
    cursors: HashMap<i64, CursorState>,
    next_cursor_id: i64,
    retryable_writes: VecDeque<RetryableWriteEntry>,
}

impl Default for ClientState {
    fn default() -> Self {
        Self {
            cursors: HashMap::new(),
            next_cursor_id: 1,
            retryable_writes: VecDeque::new(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct RetryableWriteKey {
    session_id: String,
    txn_number: i64,
}

#[derive(Clone, Debug)]
struct RetryableWriteEntry {
    key: RetryableWriteKey,
    command_name: String,
    command_body: Vec<u8>,
    response: Document,
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

    fn retryable_write_response(
        &self,
        key: &RetryableWriteKey,
        command_name: &str,
        command_body: &[u8],
    ) -> std::result::Result<Option<Document>, Document> {
        let Some(entry) = self.retryable_writes.iter().find(|entry| &entry.key == key) else {
            return Ok(None);
        };
        if entry.command_name == command_name && entry.command_body == command_body {
            Ok(Some(entry.response.clone()))
        } else {
            Err(command_error(
                72,
                "retryable write txnNumber was already used for a different command",
            ))
        }
    }

    fn record_retryable_write(
        &mut self,
        key: RetryableWriteKey,
        command_name: String,
        command_body: Vec<u8>,
        response: Document,
    ) {
        if self
            .retryable_writes
            .iter()
            .any(|entry| entry.key == key && entry.command_name == command_name)
        {
            return;
        }
        self.retryable_writes.push_back(RetryableWriteEntry {
            key,
            command_name,
            command_body,
            response,
        });
        while self.retryable_writes.len() > RETRYABLE_WRITE_CACHE_LIMIT {
            self.retryable_writes.pop_front();
        }
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

pub(crate) fn init_connection(conn: &Connection) -> Result<()> {
    conn.pragma_update(None, "journal_mode", "WAL")?;
    conn.pragma_update(None, "synchronous", "NORMAL")?;
    conn.pragma_update(None, "foreign_keys", "ON")?;
    init_migration_schema(conn)?;
    init_document_schema(conn)?;
    ensure_index_metadata_columns(conn)?;
    Ok(())
}

const SQLITE_SYNCHRONOUS_FULL: i64 = 2;

fn sqlite_synchronous(conn: &Connection) -> Result<i64> {
    Ok(conn.query_row("PRAGMA synchronous", [], |row| row.get(0))?)
}

fn with_sqlite_synchronous_full<T>(
    conn: &Connection,
    operation: impl FnOnce(&Connection) -> Result<T>,
) -> Result<T> {
    let previous = sqlite_synchronous(conn)?;
    if previous != SQLITE_SYNCHRONOUS_FULL {
        conn.pragma_update(None, "synchronous", "FULL")?;
    }

    let result = operation(conn);
    let restore = if previous != SQLITE_SYNCHRONOUS_FULL {
        conn.pragma_update(None, "synchronous", previous)
    } else {
        Ok(())
    };

    match (result, restore) {
        (Ok(value), Ok(())) => Ok(value),
        (Err(err), _) => Err(err),
        (Ok(_), Err(err)) => Err(err.into()),
    }
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

fn ensure_index_metadata_columns(conn: &Connection) -> Result<()> {
    let columns = conn
        .prepare("PRAGMA table_info(indexes)")?
        .query_map([], |row| row.get::<_, String>(1))?
        .collect::<std::result::Result<Vec<_>, _>>()?;
    if !columns.iter().any(|column| column == "sparse_index") {
        conn.execute(
            "ALTER TABLE indexes ADD COLUMN sparse_index INTEGER NOT NULL DEFAULT 0",
            [],
        )?;
    }
    if !columns.iter().any(|column| column == "partial_filter_bson") {
        conn.execute(
            "ALTER TABLE indexes ADD COLUMN partial_filter_bson BLOB",
            [],
        )?;
    }
    if !columns
        .iter()
        .any(|column| column == "expire_after_seconds")
    {
        conn.execute(
            "ALTER TABLE indexes ADD COLUMN expire_after_seconds INTEGER",
            [],
        )?;
    }
    if !columns.iter().any(|column| column == "collation_bson") {
        conn.execute("ALTER TABLE indexes ADD COLUMN collation_bson BLOB", [])?;
    }
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

        CREATE TABLE IF NOT EXISTS index_multikey_omissions (
            namespace TEXT NOT NULL,
            index_name TEXT NOT NULL,
            id_key TEXT NOT NULL,
            PRIMARY KEY (namespace, index_name, id_key)
        );

        CREATE INDEX IF NOT EXISTS idx_index_multikey_omissions_lookup
            ON index_multikey_omissions(namespace, index_name);

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
    migrate_collection_options_column(conn)?;
    Ok(())
}

fn migrate_collection_options_column(conn: &Connection) -> Result<()> {
    let has_options_bson = conn
        .prepare("PRAGMA table_info(collections)")?
        .query_map([], |row| row.get::<_, String>(1))?
        .collect::<std::result::Result<Vec<_>, _>>()?
        .into_iter()
        .any(|name| name == "options_bson");

    if !has_options_bson {
        conn.execute("ALTER TABLE collections ADD COLUMN options_bson BLOB", [])?;
    }
    conn.execute(
        "INSERT OR IGNORE INTO schema_migrations(name) VALUES (?1)",
        params!["collection_options_bson"],
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

pub(crate) fn handle_command_with_state(
    conn: &Connection,
    client_state: &mut ClientState,
    command: &Document,
) -> Result<Document> {
    let Some(command_name) = command_name(command) else {
        return Ok(command_error(59, "empty command document"));
    };

    let workflow = match parse_driver_workflow_options(command_name.as_str(), command) {
        Ok(workflow) => workflow,
        Err(response) => return Ok(response),
    };
    let retryable_body = match workflow.retryable_key.as_ref() {
        Some(key) => {
            let command_body = encode_document(command)?;
            match client_state.retryable_write_response(key, &command_name, &command_body) {
                Ok(Some(response)) => return Ok(response),
                Ok(None) => Some(command_body),
                Err(response) => return Ok(response),
            }
        }
        None => None,
    };

    let mut dispatch = |conn: &Connection| match command_name.as_str() {
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
        "endSessions" => end_sessions(command),
        "create" => create_collection(conn, command),
        "listCollections" => list_collections(conn, command),
        "collMod" => coll_mod(conn, command),
        "drop" => drop_collection(conn, command),
        "dropDatabase" => drop_database(conn, command),
        "count" => count_documents_command(conn, command),
        "distinct" => distinct_command(conn, command),
        "aggregate" => aggregate_command_with_state(conn, client_state, command),
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
        "commitTransaction" | "abortTransaction" | "prepareTransaction" => Ok(command_error(
            263,
            "transactions are not supported by mongolino",
        )),
        other => Ok(command_error(
            59,
            &format!("command '{other}' is not supported yet"),
        )),
    };

    let response = if workflow.write_concern.journaled {
        with_sqlite_synchronous_full(conn, dispatch)?
    } else {
        dispatch(conn)?
    };

    if let (Some(key), Some(command_body)) = (workflow.retryable_key, retryable_body) {
        client_state.record_retryable_write(key, command_name, command_body, response.clone());
    }

    Ok(response)
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

#[derive(Clone, Debug)]
struct DriverWorkflowOptions {
    retryable_key: Option<RetryableWriteKey>,
    write_concern: WriteConcernOptions,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct WriteConcernOptions {
    journaled: bool,
}

fn parse_driver_workflow_options(
    command_name: &str,
    command: &Document,
) -> std::result::Result<DriverWorkflowOptions, Document> {
    let write_concern = validate_driver_workflow_options(command_name, command)
        .map_err(|errmsg| command_error(72, &errmsg))?;

    let session_id = match command.get("lsid") {
        Some(value) => Some(validate_lsid(value).map_err(|errmsg| command_error(72, &errmsg))?),
        None => None,
    };
    let txn_number = match command.get("txnNumber") {
        Some(value) => Some(parse_txn_number(value).map_err(|errmsg| command_error(72, &errmsg))?),
        None => None,
    };

    if let Some(txn_number) = txn_number {
        let Some(session_id) = session_id else {
            return Err(command_error(72, "txnNumber requires a valid lsid"));
        };
        if !supports_retryable_write(command_name) {
            return Err(command_error(
                72,
                "txnNumber is only supported for retryable write commands",
            ));
        }
        return Ok(DriverWorkflowOptions {
            retryable_key: Some(RetryableWriteKey {
                session_id,
                txn_number,
            }),
            write_concern,
        });
    }

    Ok(DriverWorkflowOptions {
        retryable_key: None,
        write_concern,
    })
}

fn validate_driver_workflow_options(
    command_name: &str,
    command: &Document,
) -> std::result::Result<WriteConcernOptions, String> {
    if transaction_command_name(command_name) {
        return Err("transactions are not supported by mongolino".to_string());
    }
    for key in ["startTransaction", "autocommit"] {
        if command.contains_key(key) {
            return Err(format!(
                "{key} is not supported; transactions are not supported"
            ));
        }
    }
    if command_name == "endSessions" {
        validate_end_sessions_command(command)?;
        return Ok(WriteConcernOptions::default());
    }
    if let Some(value) = command.get("readConcern") {
        if !supports_read_concern(command_name) {
            return Err(format!("readConcern is not supported for {command_name}"));
        }
        validate_read_concern(value)?;
    }
    let write_concern = if let Some(value) = command.get("writeConcern") {
        if !supports_write_concern(command_name) {
            return Err(format!("writeConcern is not supported for {command_name}"));
        }
        parse_write_concern(value)?
    } else {
        WriteConcernOptions::default()
    };
    Ok(write_concern)
}

fn supports_read_concern(command_name: &str) -> bool {
    matches!(
        command_name,
        "find" | "aggregate" | "count" | "distinct" | "listCollections" | "listDatabases"
    )
}

fn supports_write_concern(command_name: &str) -> bool {
    matches!(
        command_name,
        "insert"
            | "update"
            | "delete"
            | "findAndModify"
            | "findandmodify"
            | "create"
            | "collMod"
            | "drop"
            | "dropDatabase"
            | "createIndexes"
            | "dropIndexes"
    )
}

fn supports_retryable_write(command_name: &str) -> bool {
    matches!(
        command_name,
        "insert" | "update" | "delete" | "findAndModify" | "findandmodify"
    )
}

fn transaction_command_name(command_name: &str) -> bool {
    matches!(
        command_name,
        "commitTransaction" | "abortTransaction" | "prepareTransaction"
    )
}

fn validate_lsid(value: &Bson) -> std::result::Result<String, String> {
    let Bson::Document(session) = value else {
        return Err("lsid must be a document".to_string());
    };
    if session.len() != 1 || !session.contains_key("id") {
        return Err("lsid must contain only an id field".to_string());
    }
    let Some(Bson::Binary(binary)) = session.get("id") else {
        return Err("lsid.id must be BSON binary UUID data".to_string());
    };
    if binary.bytes.len() != 16 {
        return Err("lsid.id UUID data must be 16 bytes".to_string());
    }
    Ok(hex_bytes(&binary.bytes))
}

fn hex_bytes(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push(HEX[(byte >> 4) as usize] as char);
        out.push(HEX[(byte & 0x0f) as usize] as char);
    }
    out
}

fn parse_txn_number(value: &Bson) -> std::result::Result<i64, String> {
    let txn_number = match value {
        Bson::Int64(value) => *value,
        Bson::Int32(value) => *value as i64,
        _ => return Err("txnNumber must be an integer".to_string()),
    };
    if txn_number < 0 {
        return Err("txnNumber must be non-negative".to_string());
    }
    Ok(txn_number)
}

fn validate_read_concern(value: &Bson) -> std::result::Result<(), String> {
    let Bson::Document(read_concern) = value else {
        return Err("readConcern must be a document".to_string());
    };
    for key in read_concern.keys() {
        if key != "level" {
            return Err(format!("readConcern field {key} is not supported"));
        }
    }
    match read_concern.get("level") {
        None => Ok(()),
        Some(Bson::String(level)) if matches!(level.as_str(), "local" | "available") => Ok(()),
        Some(Bson::String(level)) => Err(format!("readConcern level {level} is not supported")),
        Some(_) => Err("readConcern level must be a string".to_string()),
    }
}

fn parse_write_concern(value: &Bson) -> std::result::Result<WriteConcernOptions, String> {
    let Bson::Document(write_concern) = value else {
        return Err("writeConcern must be a document".to_string());
    };
    for key in write_concern.keys() {
        if !matches!(key.as_str(), "w" | "j" | "wtimeout" | "wtimeoutMS") {
            return Err(format!("writeConcern field {key} is not supported"));
        }
    }
    match write_concern.get("w") {
        None => {}
        Some(Bson::Int32(1)) | Some(Bson::Int64(1)) => {}
        Some(Bson::String(value)) if value == "majority" => {}
        Some(Bson::Int32(0)) | Some(Bson::Int64(0)) => {
            return Err("writeConcern w:0 is not supported".to_string());
        }
        Some(Bson::Int32(_)) | Some(Bson::Int64(_)) | Some(Bson::String(_)) => {
            return Err("writeConcern w value is not supported".to_string());
        }
        Some(_) => return Err("writeConcern w must be an integer or string".to_string()),
    }
    let journaled = match write_concern.get("j") {
        None => false,
        Some(Bson::Boolean(value)) => *value,
        Some(_) => return Err("writeConcern j must be a boolean".to_string()),
    };
    if write_concern.contains_key("wtimeout") && write_concern.contains_key("wtimeoutMS") {
        return Err("writeConcern cannot include both wtimeout and wtimeoutMS".to_string());
    }
    for key in ["wtimeout", "wtimeoutMS"] {
        match write_concern.get(key) {
            None => {}
            Some(Bson::Int32(value)) if *value >= 0 => {}
            Some(Bson::Int64(value)) if *value >= 0 => {}
            Some(Bson::Int32(_)) | Some(Bson::Int64(_)) => {
                return Err(format!("writeConcern {key} must be non-negative"));
            }
            Some(_) => return Err(format!("writeConcern {key} must be an integer")),
        }
    }
    Ok(WriteConcernOptions { journaled })
}

fn validate_end_sessions_command(command: &Document) -> std::result::Result<(), String> {
    for key in command.keys() {
        if !matches!(key.as_str(), "endSessions" | "$db" | "lsid") {
            return Err(format!("{key} is not supported for endSessions"));
        }
    }
    let sessions = command
        .get_array("endSessions")
        .map_err(|_| "endSessions must be an array".to_string())?;
    for session in sessions {
        validate_lsid(session)?;
    }
    Ok(())
}

fn end_sessions(command: &Document) -> Result<Document> {
    if let Err(errmsg) = validate_end_sessions_command(command) {
        return Ok(command_error(72, &errmsg));
    }
    Ok(doc! { "ok": 1.0 })
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
    if let Some(errmsg) = reject_unsupported_command_keys(
        command,
        &[
            "create",
            "validator",
            "validationLevel",
            "validationAction",
            "index",
            "writeConcern",
            "$db",
            "lsid",
        ],
    ) {
        return Ok(command_error(72, &errmsg));
    }
    let options = match collection_options_from_command(command) {
        Ok(options) => options,
        Err(errmsg) => return Ok(command_error(72, &errmsg)),
    };

    let ns = namespace(db, collection);
    match insert_collection_catalog_with_options(conn, db, collection, &options.document) {
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
            "readConcern",
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
                let options =
                    collection_options_document(conn, &namespace(db, &name)).unwrap_or_default();
                doc.insert("options", Bson::Document(options));
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

fn coll_mod(conn: &Connection, command: &Document) -> Result<Document> {
    let db = command.get_str("$db").unwrap_or("test");
    let collection = match command.get_str("collMod") {
        Ok(collection) if !collection.is_empty() => collection,
        _ => {
            return Ok(command_error(
                9,
                "collMod command requires a collection name",
            ));
        }
    };
    if let Some(errmsg) = reject_unsupported_command_keys(
        command,
        &[
            "collMod",
            "validator",
            "validationLevel",
            "validationAction",
            "index",
            "writeConcern",
            "$db",
            "lsid",
        ],
    ) {
        return Ok(command_error(72, &errmsg));
    }
    if !command.contains_key("validator")
        && !command.contains_key("validationLevel")
        && !command.contains_key("validationAction")
        && !command.contains_key("index")
    {
        return Ok(command_error(
            9,
            "collMod requires validator, validationLevel, validationAction, or index",
        ));
    }

    let ns = namespace(db, collection);
    let tx = conn.unchecked_transaction()?;
    if !collection_exists_tx(&tx, &ns)? {
        return Ok(command_error(26, "collection does not exist"));
    }
    let mut options = collection_options_tx(&tx, &ns)?.document;
    if let Some(value) = command.get("validator") {
        match value {
            Bson::Document(validator) if validator.is_empty() => {
                options.remove("validator");
                options.remove("validationLevel");
                options.remove("validationAction");
            }
            Bson::Document(validator) => {
                options.insert("validator", Bson::Document(validator.clone()));
            }
            _ => return Ok(command_error(72, "validator must be a document")),
        }
    }
    if let Some(value) = command.get("validationLevel") {
        options.insert("validationLevel", value.clone());
    }
    if let Some(value) = command.get("validationAction") {
        options.insert("validationAction", value.clone());
    }
    if let Err(errmsg) = parse_collection_options(options.clone()) {
        return Ok(command_error(72, &errmsg));
    }
    let ttl_update = match command.get("index") {
        Some(value) => match parse_coll_mod_ttl_index_update(&tx, &ns, value) {
            Ok(update) => Some(update),
            Err(response) => return Ok(response),
        },
        None => None,
    };
    set_collection_options_tx(&tx, &ns, &options)?;
    let ttl_response = if let Some(update) = ttl_update {
        update_index_expire_after_seconds_tx(&tx, &ns, &update.name, update.new_expire_after)?;
        Some(doc! {
            "expireAfterSeconds_old": update.old_expire_after,
            "expireAfterSeconds_new": update.new_expire_after,
        })
    } else {
        None
    };
    tx.commit()?;
    let mut response = doc! { "ok": 1.0 };
    if let Some(ttl_response) = ttl_response {
        response.extend(ttl_response);
    }
    Ok(response)
}

struct CollModTtlIndexUpdate {
    name: String,
    old_expire_after: i64,
    new_expire_after: i64,
}

fn parse_coll_mod_ttl_index_update(
    tx: &rusqlite::Transaction<'_>,
    namespace: &str,
    value: &Bson,
) -> std::result::Result<CollModTtlIndexUpdate, Document> {
    let Bson::Document(index) = value else {
        return Err(command_error(72, "collMod index must be a document"));
    };
    if let Some(key) = index
        .keys()
        .find(|key| !matches!(key.as_str(), "name" | "expireAfterSeconds"))
    {
        return Err(command_error(
            72,
            &format!("collMod index option {key} is not supported"),
        ));
    }
    let name = match index.get_str("name") {
        Ok(name) if !name.is_empty() => name.to_string(),
        Ok(_) => return Err(command_error(9, "collMod index name must not be empty")),
        Err(_) => return Err(command_error(9, "collMod index requires name")),
    };
    if name == "_id_" {
        return Err(command_error(72, "collMod cannot update _id_ index TTL"));
    }
    let new_expire_after = match index.get("expireAfterSeconds") {
        Some(value) => parse_expire_after_seconds(value)?,
        None => {
            return Err(command_error(
                9,
                "collMod index requires expireAfterSeconds",
            ));
        }
    };
    let existing = match index_by_name_tx(tx, namespace, &name) {
        Ok(Some(existing)) => existing,
        Ok(None) => return Err(command_error(27, "index not found")),
        Err(err) => return Err(command_error(2, &err.to_string())),
    };
    let Some(old_expire_after) = existing.expire_after_seconds else {
        return Err(command_error(
            72,
            "collMod TTL updates require an existing TTL index",
        ));
    };
    validate_ttl_index_spec(
        &existing.key,
        &existing.name,
        existing.sparse,
        existing.partial_filter.as_ref(),
    )?;
    Ok(CollModTtlIndexUpdate {
        name,
        old_expire_after,
        new_expire_after,
    })
}

fn drop_collection(conn: &Connection, command: &Document) -> Result<Document> {
    let db = command.get_str("$db").unwrap_or("test");
    let collection = match command.get_str("drop") {
        Ok(collection) if !collection.is_empty() => collection,
        _ => return Ok(command_error(9, "drop command requires a collection name")),
    };
    if let Some(errmsg) =
        reject_unsupported_command_keys(command, &["drop", "writeConcern", "$db", "lsid"])
    {
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
    tx.execute(
        "DELETE FROM index_multikey_omissions WHERE namespace = ?1",
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
    if let Some(errmsg) = reject_unsupported_command_keys(
        command,
        &["dropDatabase", "comment", "writeConcern", "$db", "lsid"],
    ) {
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
    tx.execute(
        "DELETE FROM index_multikey_omissions WHERE namespace LIKE ?1",
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
        &[
            "count",
            "query",
            "skip",
            "limit",
            "hint",
            "explain",
            "readConcern",
            "$db",
            "lsid",
            "collation",
        ],
    ) {
        return Ok(command_error(72, &errmsg));
    }
    let collation = match Collation::parse_optional(command, "collation") {
        Ok(collation) => collation,
        Err(errmsg) => return Ok(command_error(72, &errmsg)),
    };
    let filter = match command.get("query") {
        None => Document::new(),
        Some(Bson::Document(filter)) => filter.clone(),
        Some(_) => return Ok(command_error(9, "count query must be a document")),
    };
    if let Err(err) = validate_filter_shape(&filter) {
        return Ok(command_error(err.code, &err.errmsg));
    }
    if let Err(err) = validate_filter_for_collation(&filter, &collation) {
        return Ok(command_error(err.code, &err.errmsg));
    }
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
    let explain = match optional_bool(command, "explain") {
        Ok(value) => value.unwrap_or(false),
        Err(errmsg) => return Ok(command_error(9, &errmsg)),
    };

    let ns = namespace(db, collection);
    let hint = match parse_optional_hint(command) {
        Ok(Some(hint)) => match resolve_hint(indexes_for_namespace(conn, &ns)?, hint) {
            Ok(hint) => {
                if let Err(errmsg) = validate_hint_collation(&hint, &collation, &filter) {
                    return Ok(command_error(2, &errmsg));
                }
                Some(hint)
            }
            Err(errmsg) => return Ok(command_error(2, &errmsg)),
        },
        Ok(None) => None,
        Err(errmsg) => return Ok(command_error(2, &errmsg)),
    };
    if explain {
        return match planner_v2_plan_for_count(conn, &ns, &filter, hint.as_ref(), &collation) {
            Ok(plan) => Ok(explain_response(
                "count",
                &ns,
                &filter,
                hint.is_some(),
                &plan,
            )),
            Err(errmsg) => Ok(command_error(2, &errmsg)),
        };
    }
    sweep_ttl_namespace(conn, &ns)?;
    let count = if let Some(hint) = hint.as_ref() {
        let documents =
            match candidate_documents_with_hint(conn, &ns, &filter, Some(hint), &collation) {
                Ok(documents) => documents,
                Err(errmsg) => return Ok(command_error(2, &errmsg)),
            };
        match count_matching_documents(documents, &filter, skip, limit, &collation) {
            Ok(count) => count,
            Err(err) => return Ok(command_error(err.code, &err.errmsg)),
        }
    } else {
        match pushed_down_count_with_collation(conn, &ns, &filter, skip, limit, &collation)? {
            Some(count) => count,
            None => {
                let documents = documents_for_namespace(conn, &ns)?;
                match count_matching_documents(documents, &filter, skip, limit, &collation) {
                    Ok(count) => count,
                    Err(err) => return Ok(command_error(err.code, &err.errmsg)),
                }
            }
        }
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
    if let Some(errmsg) = reject_unsupported_command_keys(
        command,
        &[
            "distinct",
            "key",
            "query",
            "collation",
            "readConcern",
            "$db",
            "lsid",
        ],
    ) {
        return Ok(command_error(72, &errmsg));
    }
    let collation = match Collation::parse_optional(command, "collation") {
        Ok(collation) => collation,
        Err(errmsg) => return Ok(command_error(72, &errmsg)),
    };
    let filter = match command.get("query") {
        None => Document::new(),
        Some(Bson::Document(filter)) => filter.clone(),
        Some(_) => return Ok(command_error(9, "distinct query must be a document")),
    };
    if let Err(err) = validate_filter_shape(&filter) {
        return Ok(command_error(err.code, &err.errmsg));
    }
    if let Err(err) = validate_filter_for_collation(&filter, &collation) {
        return Ok(command_error(err.code, &err.errmsg));
    }

    let mut values = Vec::<Bson>::new();
    let ns = namespace(db, collection);
    sweep_ttl_namespace(conn, &ns)?;
    for document in documents_for_namespace(conn, &ns)? {
        match matches_filter_with_collation(&document, &filter, &collation) {
            Ok(true) => {
                for value in distinct_values_at_path(&document, key) {
                    if !values.iter().any(|existing| {
                        bson_values_equal_with_collation(existing, &value, &collation)
                    }) {
                        values.push(value);
                    }
                }
            }
            Ok(false) => {}
            Err(err) => return Ok(command_error(err.code, &err.errmsg)),
        }
    }
    values.sort_by(|left, right| compare_bson_order_with_collation(left, right, &collation));
    Ok(doc! { "values": values, "ok": 1.0 })
}

#[cfg(test)]
fn aggregate_command(conn: &Connection, command: &Document) -> Result<Document> {
    let mut client_state = ClientState::default();
    aggregate_command_with_state(conn, &mut client_state, command)
}

fn aggregate_command_with_state(
    conn: &Connection,
    client_state: &mut ClientState,
    command: &Document,
) -> Result<Document> {
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
        &[
            "aggregate",
            "pipeline",
            "cursor",
            "collation",
            "readConcern",
            "$db",
            "lsid",
        ],
    ) {
        return Ok(command_error(72, &errmsg));
    }
    let collation = match Collation::parse_optional(command, "collation") {
        Ok(collation) => collation,
        Err(errmsg) => return Ok(command_error(72, &errmsg)),
    };
    let batch_size = match parse_aggregate_cursor(command) {
        Ok(batch_size) => batch_size,
        Err(response) => return Ok(response),
    };
    let pipeline = match command.get_array("pipeline") {
        Ok(pipeline) => pipeline,
        Err(_) => return Ok(command_error(9, "aggregate requires a pipeline array")),
    };
    if let Err(response) = validate_aggregate_pipeline_shape(pipeline) {
        return Ok(response);
    }
    if let Err(response) = validate_aggregate_pipeline_for_collation(pipeline, &collation) {
        return Ok(response);
    }
    let ns = namespace(db, collection);
    sweep_ttl_namespace(conn, &ns)?;
    if let Some(documents) = aggregate_match_count_pushdown(conn, &ns, pipeline, &collation)? {
        let documents = match documents {
            Ok(documents) => documents,
            Err(response) => return Ok(response),
        };
        return Ok(cursor_response_for_documents(
            client_state,
            db,
            collection,
            &ns,
            documents,
            batch_size,
            false,
        ));
    }
    let documents = match aggregate_pipeline_documents(conn, db, &ns, pipeline, &collation)? {
        Ok(documents) => documents,
        Err(response) => return Ok(response),
    };

    Ok(cursor_response_for_documents(
        client_state,
        db,
        collection,
        &ns,
        documents,
        batch_size,
        false,
    ))
}

fn parse_aggregate_cursor(command: &Document) -> std::result::Result<usize, Document> {
    let cursor = match command.get("cursor") {
        Some(Bson::Document(cursor)) => cursor,
        _ => return Err(command_error(9, "aggregate requires a cursor document")),
    };
    if let Some(key) = cursor.keys().find(|key| key.as_str() != "batchSize") {
        return Err(command_error(
            72,
            &format!("aggregate cursor option {key} is not supported"),
        ));
    }
    match cursor.get("batchSize") {
        None => Ok(101),
        Some(Bson::Int32(value)) if *value > 0 && *value <= 1000 => Ok(*value as usize),
        Some(Bson::Int64(value)) if *value > 0 && *value <= 1000 => Ok(*value as usize),
        Some(Bson::Int32(_)) | Some(Bson::Int64(_)) => Err(command_error(
            9,
            "aggregate cursor batchSize must be between 1 and 1000",
        )),
        Some(_) => Err(command_error(
            9,
            "aggregate cursor batchSize must be an integer",
        )),
    }
}

fn aggregate_match_count_pushdown(
    conn: &Connection,
    namespace: &str,
    pipeline: &[Bson],
    collation: &Collation,
) -> Result<Option<std::result::Result<Vec<Document>, Document>>> {
    let Some((filter, field)) = parse_match_count_pipeline(pipeline) else {
        return Ok(None);
    };
    let Some(count) =
        pushed_down_count_with_collation(conn, namespace, filter, 0, None, collation)?
    else {
        return Ok(None);
    };
    let documents = if count == 0 {
        Vec::new()
    } else {
        vec![doc! { field: count }]
    };
    Ok(Some(Ok(documents)))
}

fn parse_match_count_pipeline<'a>(pipeline: &'a [Bson]) -> Option<(&'a Document, &'a str)> {
    if pipeline.len() != 2 {
        return None;
    }
    let Bson::Document(match_stage) = &pipeline[0] else {
        return None;
    };
    if match_stage.len() != 1 {
        return None;
    }
    let (match_operator, match_operand) = match_stage.iter().next()?;
    if match_operator != "$match" {
        return None;
    }
    let Bson::Document(filter) = match_operand else {
        return None;
    };

    let Bson::Document(count_stage) = &pipeline[1] else {
        return None;
    };
    if count_stage.len() != 1 {
        return None;
    }
    let (count_operator, count_operand) = count_stage.iter().next()?;
    if count_operator != "$count" {
        return None;
    }
    let Bson::String(field) = count_operand else {
        return None;
    };
    if field.is_empty() {
        return None;
    }
    Some((filter, field.as_str()))
}

fn aggregate_pipeline_documents(
    conn: &Connection,
    db: &str,
    namespace: &str,
    pipeline: &[Bson],
    collation: &Collation,
) -> Result<std::result::Result<Vec<Document>, Document>> {
    let mut documents = documents_for_namespace(conn, namespace)?;

    for stage in pipeline {
        let Bson::Document(stage) = stage else {
            return Ok(Err(command_error(
                9,
                "aggregate pipeline stages must be documents",
            )));
        };
        if stage.len() != 1 {
            return Ok(Err(command_error(
                72,
                "aggregate stages must contain one operator",
            )));
        }
        let (operator, operand) = stage.iter().next().expect("stage len checked above");
        match operator.as_str() {
            "$match" => {
                let Bson::Document(filter) = operand else {
                    return Ok(Err(command_error(9, "$match requires a document")));
                };
                documents = match shape_documents(documents, filter, None, 0, None, None, collation)
                {
                    Ok(documents) => documents,
                    Err(err) => return Ok(Err(command_error(err.code, &err.errmsg))),
                };
            }
            "$sort" => {
                let Bson::Document(sort) = operand else {
                    return Ok(Err(command_error(9, "$sort requires a document")));
                };
                let sort = match parse_sort_document(sort) {
                    Ok(sort) => sort,
                    Err(errmsg) => return Ok(Err(command_error(2, &errmsg))),
                };
                sort_documents_with_collation(&mut documents, &sort, collation);
            }
            "$skip" => {
                let skip = match non_negative_stage_usize(operand, "$skip") {
                    Ok(skip) => skip,
                    Err(response) => return Ok(Err(response)),
                };
                documents = documents.into_iter().skip(skip).collect();
            }
            "$limit" => {
                let limit = match non_negative_stage_usize(operand, "$limit") {
                    Ok(limit) => limit,
                    Err(response) => return Ok(Err(response)),
                };
                documents.truncate(limit);
            }
            "$project" => {
                let Bson::Document(projection) = operand else {
                    return Ok(Err(command_error(9, "$project requires a document")));
                };
                let projection = match parse_aggregate_project_stage(projection) {
                    Ok(Some(projection)) => projection,
                    Ok(None) => continue,
                    Err(response) => return Ok(Err(response)),
                };
                let mut projected = Vec::new();
                for document in documents {
                    match apply_aggregate_project_stage(&document, &projection, collation) {
                        Ok(document) => projected.push(document),
                        Err(response) => return Ok(Err(response)),
                    }
                }
                documents = projected;
            }
            "$addFields" | "$set" => {
                let Bson::Document(spec) = operand else {
                    return Ok(Err(command_error(
                        9,
                        &format!("{operator} requires a document"),
                    )));
                };
                let spec = match parse_aggregate_add_fields_stage(spec, operator) {
                    Ok(spec) => spec,
                    Err(response) => return Ok(Err(response)),
                };
                let mut shaped = Vec::new();
                for document in documents {
                    match apply_aggregate_add_fields_stage(document, &spec, collation) {
                        Ok(document) => shaped.push(document),
                        Err(response) => return Ok(Err(response)),
                    }
                }
                documents = shaped;
            }
            "$unset" => {
                let unset = match parse_aggregate_unset_stage(operand) {
                    Ok(unset) => unset,
                    Err(response) => return Ok(Err(response)),
                };
                documents = documents
                    .into_iter()
                    .map(|mut document| {
                        for path in &unset.paths {
                            unset_document_path(&mut document, path);
                        }
                        document
                    })
                    .collect();
            }
            "$replaceRoot" | "$replaceWith" => {
                let replacement = match parse_aggregate_replace_root_stage(operator, operand) {
                    Ok(replacement) => replacement,
                    Err(response) => return Ok(Err(response)),
                };
                let mut replaced = Vec::new();
                for document in documents {
                    match apply_aggregate_replace_root_stage(&document, &replacement, collation) {
                        Ok(document) => replaced.push(document),
                        Err(response) => return Ok(Err(response)),
                    }
                }
                documents = replaced;
            }
            "$lookup" => {
                let lookup = match parse_aggregate_lookup_stage(operand) {
                    Ok(lookup) => lookup,
                    Err(response) => return Ok(Err(response)),
                };
                documents =
                    match apply_aggregate_lookup_stage(conn, db, documents, &lookup, collation)? {
                        Ok(documents) => documents,
                        Err(response) => return Ok(Err(response)),
                    };
            }
            "$count" => {
                let Bson::String(field) = operand else {
                    return Ok(Err(command_error(
                        9,
                        "$count requires a non-empty string field name",
                    )));
                };
                if field.is_empty() {
                    return Ok(Err(command_error(
                        9,
                        "$count requires a non-empty string field name",
                    )));
                }
                documents = if documents.is_empty() {
                    Vec::new()
                } else {
                    vec![doc! { field: documents.len() as i64 }]
                };
            }
            "$unwind" => {
                let unwind = match parse_unwind_stage(operand) {
                    Ok(unwind) => unwind,
                    Err(response) => return Ok(Err(response)),
                };
                documents = apply_unwind_stage(documents, &unwind);
            }
            "$group" => {
                let Bson::Document(group) = operand else {
                    return Ok(Err(command_error(9, "$group requires a document")));
                };
                let preserve_empty_count = is_count_documents_group(group);
                let group = match parse_group_stage(group) {
                    Ok(group) => group,
                    Err(response) => return Ok(Err(response)),
                };
                documents =
                    match apply_group_stage(documents, &group, preserve_empty_count, collation) {
                        Ok(documents) => documents,
                        Err(response) => return Ok(Err(response)),
                    };
            }
            _ => {
                return Ok(Err(command_error(
                    72,
                    &format!("aggregate stage {operator} is not supported"),
                )));
            }
        }
    }

    Ok(Ok(documents))
}

fn validate_aggregate_pipeline_shape(pipeline: &[Bson]) -> std::result::Result<(), Document> {
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
        let (operator, operand) = stage.iter().next().expect("stage len checked above");
        match operator.as_str() {
            "$match" => {
                let Bson::Document(filter) = operand else {
                    return Err(command_error(9, "$match requires a document"));
                };
                if let Err(err) = validate_filter_shape(filter) {
                    return Err(command_error(err.code, &err.errmsg));
                }
            }
            "$sort" => {
                let Bson::Document(sort) = operand else {
                    return Err(command_error(9, "$sort requires a document"));
                };
                if let Err(errmsg) = parse_sort_document(sort) {
                    return Err(command_error(2, &errmsg));
                }
            }
            "$skip" => {
                non_negative_stage_usize(operand, "$skip")?;
            }
            "$limit" => {
                non_negative_stage_usize(operand, "$limit")?;
            }
            "$project" => {
                let Bson::Document(projection) = operand else {
                    return Err(command_error(9, "$project requires a document"));
                };
                if let Some(projection) = parse_aggregate_project_stage(projection)? {
                    validate_aggregate_project_static_expressions(&projection)?;
                }
            }
            "$addFields" | "$set" => {
                let Bson::Document(spec) = operand else {
                    return Err(command_error(9, &format!("{operator} requires a document")));
                };
                let fields = parse_aggregate_add_fields_stage(spec, operator)?;
                validate_aggregation_computed_fields_static(&fields)?;
            }
            "$unset" => {
                parse_aggregate_unset_stage(operand)?;
            }
            "$replaceRoot" | "$replaceWith" => {
                let replacement = parse_aggregate_replace_root_stage(operator, operand)?;
                validate_aggregate_replace_root_static_expression(&replacement)?;
            }
            "$lookup" => {
                parse_aggregate_lookup_stage(operand)?;
            }
            "$count" => {
                let Bson::String(field) = operand else {
                    return Err(command_error(
                        9,
                        "$count requires a non-empty string field name",
                    ));
                };
                if field.is_empty() {
                    return Err(command_error(
                        9,
                        "$count requires a non-empty string field name",
                    ));
                }
            }
            "$unwind" => {
                parse_unwind_stage(operand)?;
            }
            "$group" => {
                let Bson::Document(group) = operand else {
                    return Err(command_error(9, "$group requires a document"));
                };
                let group = parse_group_stage(group)?;
                validate_group_static_expressions(&group)?;
            }
            _ => {
                return Err(command_error(
                    72,
                    &format!("aggregate stage {operator} is not supported"),
                ));
            }
        }
    }

    Ok(())
}

fn validate_aggregate_project_static_expressions(
    projection: &AggregateProjectSpec,
) -> std::result::Result<(), Document> {
    if let AggregateProjectSpec::Include { computed, .. } = projection {
        validate_aggregation_computed_fields_static(computed)?;
    }
    Ok(())
}

fn validate_aggregation_computed_fields_static(
    fields: &[AggregationComputedField],
) -> std::result::Result<(), Document> {
    for field in fields {
        validate_static_aggregation_expression(&field.expression)?;
    }
    Ok(())
}

fn validate_aggregate_replace_root_static_expression(
    replacement: &AggregateReplaceRootSpec,
) -> std::result::Result<(), Document> {
    validate_static_aggregation_expression(&replacement.expression)?;
    let Some(value) = evaluate_constant_aggregation_expression(&replacement.expression)? else {
        return Ok(());
    };
    if matches!(value, Bson::Document(_)) {
        return Ok(());
    }
    Err(command_error(
        9,
        "$replaceRoot/$replaceWith expression must evaluate to a document",
    ))
}

fn validate_group_static_expressions(group: &GroupSpec) -> std::result::Result<(), Document> {
    validate_static_aggregation_expression(&group.id)?;
    for accumulator in &group.accumulators {
        validate_static_aggregation_expression(&accumulator.expression)?;
    }
    Ok(())
}

fn validate_static_aggregation_expression(
    expression: &AggregationExpression,
) -> std::result::Result<(), Document> {
    if evaluate_constant_aggregation_expression(expression)?.is_some() {
        return Ok(());
    }
    Ok(())
}

fn validate_aggregate_pipeline_for_collation(
    pipeline: &[Bson],
    collation: &Collation,
) -> std::result::Result<(), Document> {
    for stage in pipeline {
        if let Bson::Document(stage) = stage
            && let Some(Bson::Document(filter)) = stage.get("$match")
            && let Err(err) = validate_filter_for_collation(filter, collation)
        {
            return Err(command_error(err.code, &err.errmsg));
        }
    }
    Ok(())
}

#[derive(Clone, Debug, PartialEq)]
enum AggregateProjectSpec {
    Include {
        fields: Vec<String>,
        computed: Vec<AggregationComputedField>,
        include_id: bool,
    },
    Exclude {
        fields: Vec<String>,
        include_id: bool,
    },
}

#[derive(Clone, Debug, PartialEq)]
struct AggregationComputedField {
    path: String,
    expression: AggregationExpression,
}

#[derive(Clone, Debug, PartialEq)]
struct AggregateUnsetSpec {
    paths: Vec<String>,
}

#[derive(Clone, Debug, PartialEq)]
struct AggregateReplaceRootSpec {
    expression: AggregationExpression,
}

#[derive(Clone, Debug, PartialEq)]
struct AggregateLookupSpec {
    from: String,
    local_field: String,
    foreign_field: String,
    as_field: String,
}

#[derive(Clone, Copy, Debug, PartialEq)]
enum AggregateProjectMode {
    Include,
    Exclude,
}

fn parse_aggregate_project_stage(
    projection: &Document,
) -> std::result::Result<Option<AggregateProjectSpec>, Document> {
    let mut mode = None;
    let mut fields = Vec::new();
    let mut computed = Vec::new();
    let mut include_id = true;
    let mut saw_id = false;

    for (field, value) in projection {
        validate_aggregation_output_path(field, "$project")?;
        let projection_mode = aggregate_project_mode_value(value);
        if field == "_id" {
            saw_id = true;
            match projection_mode {
                Some(AggregateProjectMode::Include) => include_id = true,
                Some(AggregateProjectMode::Exclude) => include_id = false,
                None => {
                    include_id = false;
                    computed.push(AggregationComputedField {
                        path: field.to_string(),
                        expression: parse_aggregation_expression(value, "$project", true)?,
                    });
                }
            }
            continue;
        }

        match projection_mode {
            Some(field_mode) => {
                match (mode, field_mode) {
                    (None, mode_value) => mode = Some(mode_value),
                    (Some(AggregateProjectMode::Include), AggregateProjectMode::Include)
                    | (Some(AggregateProjectMode::Exclude), AggregateProjectMode::Exclude) => {}
                    _ => {
                        return Err(command_error(
                            9,
                            "$project cannot mix inclusion and exclusion fields except _id",
                        ));
                    }
                }
                fields.push(field.to_string());
            }
            None => {
                if matches!(mode, Some(AggregateProjectMode::Exclude)) {
                    return Err(command_error(
                        9,
                        "$project cannot mix computed fields with exclusion projection",
                    ));
                }
                mode = Some(AggregateProjectMode::Include);
                computed.push(AggregationComputedField {
                    path: field.to_string(),
                    expression: parse_aggregation_expression(value, "$project", true)?,
                });
            }
        }
    }

    if projection.is_empty() {
        return Ok(None);
    }
    let collision_paths = fields
        .iter()
        .cloned()
        .chain(computed.iter().map(|field| field.path.clone()))
        .collect::<Vec<_>>();
    reject_path_collisions(&collision_paths, "$project")
        .map_err(|errmsg| command_error(9, &errmsg))?;

    match mode {
        Some(AggregateProjectMode::Exclude) => {
            Ok(Some(AggregateProjectSpec::Exclude { fields, include_id }))
        }
        Some(AggregateProjectMode::Include) => Ok(Some(AggregateProjectSpec::Include {
            fields,
            computed,
            include_id,
        })),
        None if saw_id => Ok(Some(if include_id {
            AggregateProjectSpec::Include {
                fields,
                computed,
                include_id,
            }
        } else {
            AggregateProjectSpec::Exclude { fields, include_id }
        })),
        None => Ok(None),
    }
}

fn aggregate_project_mode_value(value: &Bson) -> Option<AggregateProjectMode> {
    match value {
        Bson::Int32(0) | Bson::Int64(0) | Bson::Boolean(false) => {
            Some(AggregateProjectMode::Exclude)
        }
        Bson::Int32(1) | Bson::Int64(1) | Bson::Boolean(true) => {
            Some(AggregateProjectMode::Include)
        }
        _ => None,
    }
}

fn apply_aggregate_project_stage(
    document: &Document,
    projection: &AggregateProjectSpec,
    collation: &Collation,
) -> std::result::Result<Document, Document> {
    match projection {
        AggregateProjectSpec::Include {
            fields,
            computed,
            include_id,
        } => {
            let mut out = Document::new();
            if *include_id && let Some(id) = document.get("_id") {
                out.insert("_id", id.clone());
            }
            for field in fields {
                if field == "_id" {
                    continue;
                }
                if let Some(value) = get_document_path(document, field) {
                    set_document_path(&mut out, field, value.clone());
                }
            }
            let context = AggregationExpressionContext::new(document, collation);
            for computed_field in computed {
                let value = computed_field
                    .expression
                    .evaluate(&context)?
                    .unwrap_or(Bson::Null);
                set_document_path(&mut out, &computed_field.path, value);
            }
            Ok(out)
        }
        AggregateProjectSpec::Exclude { fields, include_id } => {
            let mut out = document.clone();
            if !include_id {
                out.remove("_id");
            }
            for field in fields {
                unset_document_path(&mut out, field);
            }
            Ok(out)
        }
    }
}

fn parse_aggregate_add_fields_stage(
    spec: &Document,
    context: &str,
) -> std::result::Result<Vec<AggregationComputedField>, Document> {
    let mut fields = Vec::new();
    let mut paths = Vec::new();
    for (path, value) in spec {
        validate_aggregation_output_path(path, context)?;
        paths.push(path.to_string());
        fields.push(AggregationComputedField {
            path: path.to_string(),
            expression: parse_aggregation_expression(value, context, true)?,
        });
    }
    reject_path_collisions(&paths, context).map_err(|errmsg| command_error(9, &errmsg))?;
    Ok(fields)
}

fn apply_aggregate_add_fields_stage(
    mut document: Document,
    fields: &[AggregationComputedField],
    collation: &Collation,
) -> std::result::Result<Document, Document> {
    let source = document.clone();
    let context = AggregationExpressionContext::new(&source, collation);
    for field in fields {
        let value = field.expression.evaluate(&context)?.unwrap_or(Bson::Null);
        set_document_path(&mut document, &field.path, value);
    }
    Ok(document)
}

fn parse_aggregate_unset_stage(value: &Bson) -> std::result::Result<AggregateUnsetSpec, Document> {
    let paths = match value {
        Bson::String(path) => vec![path.to_string()],
        Bson::Array(values) => {
            let mut paths = Vec::new();
            for value in values {
                let Bson::String(path) = value else {
                    return Err(command_error(9, "$unset array entries must be strings"));
                };
                paths.push(path.to_string());
            }
            paths
        }
        _ => {
            return Err(command_error(
                9,
                "$unset requires a string path or array of string paths",
            ));
        }
    };
    for path in &paths {
        validate_aggregation_output_path(path, "$unset")?;
    }
    reject_path_collisions(&paths, "$unset").map_err(|errmsg| command_error(9, &errmsg))?;
    Ok(AggregateUnsetSpec { paths })
}

fn validate_aggregation_output_path(
    path: &str,
    context: &str,
) -> std::result::Result<(), Document> {
    if path.starts_with('$') {
        return Err(command_error(
            9,
            &format!("{context} output field path cannot start with $"),
        ));
    }
    validate_aggregation_path(path, context, true)
}

fn parse_aggregate_replace_root_stage(
    operator: &str,
    operand: &Bson,
) -> std::result::Result<AggregateReplaceRootSpec, Document> {
    let expression = match operator {
        "$replaceRoot" => {
            let Bson::Document(spec) = operand else {
                return Err(command_error(
                    9,
                    "$replaceRoot requires a document with newRoot",
                ));
            };
            for key in spec.keys() {
                if key != "newRoot" {
                    return Err(command_error(
                        72,
                        &format!("$replaceRoot option {key} is not supported"),
                    ));
                }
            }
            let Some(new_root) = spec.get("newRoot") else {
                return Err(command_error(9, "$replaceRoot requires newRoot"));
            };
            parse_aggregation_expression(new_root, "$replaceRoot", true)?
        }
        "$replaceWith" => parse_aggregation_expression(operand, "$replaceWith", true)?,
        _ => unreachable!("replace root parser called for checked operators"),
    };
    Ok(AggregateReplaceRootSpec { expression })
}

fn apply_aggregate_replace_root_stage(
    document: &Document,
    spec: &AggregateReplaceRootSpec,
    collation: &Collation,
) -> std::result::Result<Document, Document> {
    let context = AggregationExpressionContext::new(document, collation);
    match spec.expression.evaluate(&context)?.unwrap_or(Bson::Null) {
        Bson::Document(document) => Ok(document),
        _ => Err(command_error(
            9,
            "$replaceRoot/$replaceWith expression must evaluate to a document",
        )),
    }
}

fn parse_aggregate_lookup_stage(
    value: &Bson,
) -> std::result::Result<AggregateLookupSpec, Document> {
    let Bson::Document(spec) = value else {
        return Err(command_error(9, "$lookup requires a document"));
    };
    for key in spec.keys() {
        if !matches!(key.as_str(), "from" | "localField" | "foreignField" | "as") {
            let code = if matches!(key.as_str(), "pipeline" | "let") {
                72
            } else {
                72
            };
            return Err(command_error(
                code,
                &format!("$lookup option {key} is not supported"),
            ));
        }
    }
    let from = required_lookup_string(spec, "from")?;
    validate_lookup_collection_name(&from)?;
    let local_field = required_lookup_string(spec, "localField")?;
    validate_aggregation_path(&local_field, "$lookup localField", true)?;
    let foreign_field = required_lookup_string(spec, "foreignField")?;
    validate_aggregation_path(&foreign_field, "$lookup foreignField", true)?;
    let as_field = required_lookup_string(spec, "as")?;
    validate_aggregation_output_path(&as_field, "$lookup as")?;
    reject_path_collisions(std::slice::from_ref(&as_field), "$lookup")
        .map_err(|errmsg| command_error(9, &errmsg))?;

    Ok(AggregateLookupSpec {
        from,
        local_field,
        foreign_field,
        as_field,
    })
}

fn required_lookup_string(spec: &Document, key: &str) -> std::result::Result<String, Document> {
    match spec.get(key) {
        Some(Bson::String(value)) if !value.is_empty() => Ok(value.clone()),
        Some(Bson::String(_)) => Err(command_error(
            9,
            &format!("$lookup {key} must not be empty"),
        )),
        Some(_) => Err(command_error(9, &format!("$lookup {key} must be a string"))),
        None => Err(command_error(9, &format!("$lookup requires {key}"))),
    }
}

fn validate_lookup_collection_name(collection: &str) -> std::result::Result<(), Document> {
    if collection.contains('.') {
        return Err(command_error(
            72,
            "$lookup cross-database namespaces are not supported",
        ));
    }
    if collection.starts_with('$') || collection.contains('\0') {
        return Err(command_error(9, "$lookup from must be a collection name"));
    }
    Ok(())
}

fn apply_aggregate_lookup_stage(
    conn: &Connection,
    db: &str,
    documents: Vec<Document>,
    spec: &AggregateLookupSpec,
    collation: &Collation,
) -> Result<std::result::Result<Vec<Document>, Document>> {
    let foreign_namespace = namespace(db, &spec.from);
    sweep_ttl_namespace(conn, &foreign_namespace)?;
    let foreign_documents = documents_for_namespace(conn, &foreign_namespace)?;
    let mut out = Vec::with_capacity(documents.len());

    for mut document in documents {
        if lookup_as_path_collides(&document, &spec.as_field) {
            return Ok(Err(command_error(
                9,
                "$lookup as path collides with an existing non-document field",
            )));
        }
        let local_values = lookup_values_at_path(&document, &spec.local_field);
        let mut matches = Vec::new();
        for foreign in &foreign_documents {
            let foreign_values = lookup_values_at_path(foreign, &spec.foreign_field);
            if lookup_values_match(&local_values, &foreign_values, collation) {
                matches.push(Bson::Document(foreign.clone()));
            }
        }
        set_document_path(&mut document, &spec.as_field, Bson::Array(matches));
        out.push(document);
    }

    Ok(Ok(out))
}

fn lookup_values_at_path(document: &Document, path: &str) -> Vec<Bson> {
    let values = values_at_path(document, path);
    if values.is_empty() {
        return vec![Bson::Null];
    }
    values
        .into_iter()
        .flat_map(|value| match value {
            Bson::Array(values) => {
                if values.is_empty() {
                    vec![Bson::Null]
                } else {
                    values.clone()
                }
            }
            value => vec![value.clone()],
        })
        .collect()
}

fn lookup_values_match(local: &[Bson], foreign: &[Bson], collation: &Collation) -> bool {
    local.iter().any(|local| {
        foreign
            .iter()
            .any(|foreign| collation.values_equal(local, foreign))
    })
}

fn lookup_as_path_collides(document: &Document, path: &str) -> bool {
    let mut parts = path.split('.').peekable();
    let mut current = document;
    while let Some(part) = parts.next() {
        if parts.peek().is_none() {
            return false;
        }
        match current.get(part) {
            None => return false,
            Some(Bson::Document(next)) => current = next,
            Some(_) => return true,
        }
    }
    false
}

#[derive(Clone, Debug, PartialEq)]
struct UnwindSpec {
    path: String,
    preserve_null_and_empty_arrays: bool,
    include_array_index: Option<String>,
}

fn parse_unwind_stage(value: &Bson) -> std::result::Result<UnwindSpec, Document> {
    match value {
        Bson::String(path) => Ok(UnwindSpec {
            path: parse_aggregation_field_path(path, "$unwind path")?,
            preserve_null_and_empty_arrays: false,
            include_array_index: None,
        }),
        Bson::Document(spec) => parse_unwind_document(spec),
        _ => Err(command_error(
            9,
            "$unwind requires a field path string or document",
        )),
    }
}

fn parse_unwind_document(spec: &Document) -> std::result::Result<UnwindSpec, Document> {
    for key in spec.keys() {
        if !matches!(
            key.as_str(),
            "path" | "preserveNullAndEmptyArrays" | "includeArrayIndex"
        ) {
            return Err(command_error(
                72,
                &format!("$unwind option {key} is not supported"),
            ));
        }
    }

    let path = match spec.get_str("path") {
        Ok(path) => parse_aggregation_field_path(path, "$unwind path")?,
        Err(_) => {
            return Err(command_error(
                9,
                "$unwind document requires a field path string",
            ));
        }
    };
    let preserve_null_and_empty_arrays = match spec.get("preserveNullAndEmptyArrays") {
        None => false,
        Some(Bson::Boolean(value)) => *value,
        Some(_) => {
            return Err(command_error(
                9,
                "$unwind preserveNullAndEmptyArrays must be a boolean",
            ));
        }
    };
    let include_array_index = match spec.get("includeArrayIndex") {
        None => None,
        Some(Bson::String(index_path)) => {
            validate_aggregation_path(index_path, "$unwind includeArrayIndex", true)?;
            if index_path.starts_with('$') {
                return Err(command_error(
                    9,
                    "$unwind includeArrayIndex cannot start with $",
                ));
            }
            reject_path_collisions(&[path.clone(), index_path.to_string()], "$unwind")
                .map_err(|errmsg| command_error(9, &errmsg))?;
            Some(index_path.to_string())
        }
        Some(_) => {
            return Err(command_error(
                9,
                "$unwind includeArrayIndex must be a string",
            ));
        }
    };

    Ok(UnwindSpec {
        path,
        preserve_null_and_empty_arrays,
        include_array_index,
    })
}

fn apply_unwind_stage(documents: Vec<Document>, spec: &UnwindSpec) -> Vec<Document> {
    let mut out = Vec::new();
    for document in documents {
        match get_document_path(&document, &spec.path) {
            Some(Bson::Array(values)) if values.is_empty() => {
                if spec.preserve_null_and_empty_arrays {
                    let mut preserved = document;
                    unset_document_path(&mut preserved, &spec.path);
                    set_unwind_index(&mut preserved, spec, Bson::Null);
                    out.push(preserved);
                }
            }
            Some(Bson::Array(values)) => {
                for (index, value) in values.clone().into_iter().enumerate() {
                    let mut unwound = document.clone();
                    set_document_path(&mut unwound, &spec.path, value);
                    set_unwind_index(&mut unwound, spec, Bson::Int32(index as i32));
                    out.push(unwound);
                }
            }
            Some(Bson::Null) | None => {
                if spec.preserve_null_and_empty_arrays {
                    let mut preserved = document;
                    set_unwind_index(&mut preserved, spec, Bson::Null);
                    out.push(preserved);
                }
            }
            Some(_) => {
                let mut scalar = document;
                set_unwind_index(&mut scalar, spec, Bson::Null);
                out.push(scalar);
            }
        }
    }
    out
}

fn set_unwind_index(document: &mut Document, spec: &UnwindSpec, index: Bson) {
    if let Some(path) = &spec.include_array_index {
        set_document_path(document, path, index);
    }
}

#[derive(Clone, Debug, PartialEq)]
struct GroupSpec {
    id: AggregationExpression,
    accumulators: Vec<AccumulatorSpec>,
}

#[derive(Clone, Debug, PartialEq)]
struct AccumulatorSpec {
    field: String,
    op: AccumulatorOp,
    expression: AggregationExpression,
}

#[derive(Clone, Copy, Debug, PartialEq)]
enum AccumulatorOp {
    Sum,
    Avg,
    Min,
    Max,
    First,
    Last,
    Push,
    AddToSet,
}

#[derive(Clone, Debug)]
struct GroupState {
    id: Bson,
    accumulators: Vec<AccumulatorState>,
}

#[derive(Clone, Debug)]
enum AccumulatorState {
    Sum { total: f64, saw_double: bool },
    Avg { total: f64, count: i64 },
    Min(Option<Bson>),
    Max(Option<Bson>),
    First(Option<Bson>),
    Last(Option<Bson>),
    Push(Vec<Bson>),
    AddToSet(Vec<Bson>),
}

fn parse_group_stage(group: &Document) -> std::result::Result<GroupSpec, Document> {
    let Some(id) = group.get("_id") else {
        return Err(command_error(9, "$group requires an _id expression"));
    };
    let id = parse_aggregation_expression(id, "$group _id", true)?;
    let mut accumulators = Vec::new();
    for (field, value) in group {
        if field == "_id" {
            continue;
        }
        validate_group_key_field_name(field, "$group accumulator")?;
        let Bson::Document(accumulator) = value else {
            return Err(command_error(
                9,
                "$group accumulator fields require an accumulator document",
            ));
        };
        if accumulator.len() != 1 {
            return Err(command_error(
                9,
                "$group accumulator documents must contain one operator",
            ));
        }
        let (operator, operand) = accumulator
            .iter()
            .next()
            .expect("accumulator len checked above");
        let op = match operator.as_str() {
            "$sum" => AccumulatorOp::Sum,
            "$avg" => AccumulatorOp::Avg,
            "$min" => AccumulatorOp::Min,
            "$max" => AccumulatorOp::Max,
            "$first" => AccumulatorOp::First,
            "$last" => AccumulatorOp::Last,
            "$push" => AccumulatorOp::Push,
            "$addToSet" => AccumulatorOp::AddToSet,
            _ => {
                return Err(command_error(
                    72,
                    &format!("$group accumulator {operator} is not supported"),
                ));
            }
        };
        let expression = parse_accumulator_expression(op, operand)?;
        accumulators.push(AccumulatorSpec {
            field: field.to_string(),
            op,
            expression,
        });
    }
    Ok(GroupSpec { id, accumulators })
}

fn parse_accumulator_expression(
    op: AccumulatorOp,
    operand: &Bson,
) -> std::result::Result<AggregationExpression, Document> {
    match op {
        AccumulatorOp::Sum => match operand {
            Bson::Int32(_) | Bson::Int64(_) | Bson::Double(_) => {
                parse_aggregation_expression(operand, "$group $sum", false)
            }
            Bson::String(value) if value.starts_with('$') => {
                parse_aggregation_expression(operand, "$group $sum", false)
            }
            Bson::Document(_) | Bson::Array(_) => {
                parse_aggregation_expression(operand, "$group $sum", true)
            }
            _ => Err(command_error(
                72,
                "$group $sum supports numeric constants, field paths, and supported expressions",
            )),
        },
        AccumulatorOp::Avg => match operand {
            Bson::String(value) if value.starts_with('$') => {
                parse_aggregation_expression(operand, "$group $avg", false)
            }
            Bson::Document(_) | Bson::Array(_) => {
                parse_aggregation_expression(operand, "$group $avg", true)
            }
            _ => Err(command_error(
                72,
                "$group $avg supports field paths and supported expressions",
            )),
        },
        AccumulatorOp::Min
        | AccumulatorOp::Max
        | AccumulatorOp::First
        | AccumulatorOp::Last
        | AccumulatorOp::Push
        | AccumulatorOp::AddToSet => {
            parse_aggregation_expression(operand, "$group accumulator", true)
        }
    }
}

fn apply_group_stage(
    documents: Vec<Document>,
    spec: &GroupSpec,
    preserve_empty_count: bool,
    collation: &Collation,
) -> std::result::Result<Vec<Document>, Document> {
    if documents.is_empty() {
        return Ok(if preserve_empty_count {
            vec![doc! { "_id": 1_i32, "n": 0_i64 }]
        } else {
            Vec::new()
        });
    }

    let mut groups = Vec::<GroupState>::new();
    for document in &documents {
        let context = AggregationExpressionContext::new(document, collation);
        let id = spec.id.evaluate(&context)?.unwrap_or(Bson::Null);
        let group_index = groups
            .iter()
            .position(|group| aggregation_values_equal(&group.id, &id));
        let index = match group_index {
            Some(index) => index,
            None => {
                groups.push(GroupState {
                    id,
                    accumulators: spec
                        .accumulators
                        .iter()
                        .map(|accumulator| AccumulatorState::new(accumulator.op))
                        .collect(),
                });
                groups.len() - 1
            }
        };
        let group = &mut groups[index];
        for (state, accumulator) in group.accumulators.iter_mut().zip(&spec.accumulators) {
            state.accumulate(accumulator, document, collation)?;
        }
    }

    Ok(groups
        .into_iter()
        .map(|group| group.into_document(spec))
        .collect())
}

impl AccumulatorState {
    fn new(op: AccumulatorOp) -> Self {
        match op {
            AccumulatorOp::Sum => Self::Sum {
                total: 0.0,
                saw_double: false,
            },
            AccumulatorOp::Avg => Self::Avg {
                total: 0.0,
                count: 0,
            },
            AccumulatorOp::Min => Self::Min(None),
            AccumulatorOp::Max => Self::Max(None),
            AccumulatorOp::First => Self::First(None),
            AccumulatorOp::Last => Self::Last(None),
            AccumulatorOp::Push => Self::Push(Vec::new()),
            AccumulatorOp::AddToSet => Self::AddToSet(Vec::new()),
        }
    }

    fn accumulate(
        &mut self,
        spec: &AccumulatorSpec,
        document: &Document,
        collation: &Collation,
    ) -> std::result::Result<(), Document> {
        let context = AggregationExpressionContext::new(document, collation);
        let value = spec.expression.evaluate(&context)?;
        match self {
            Self::Sum { total, saw_double } => {
                if let Some(value) = value
                    && let Some((number, is_double)) = numeric_bson_value(&value)
                {
                    *total += number;
                    *saw_double |= is_double;
                }
            }
            Self::Avg { total, count } => {
                if let Some(value) = value
                    && let Some((number, _)) = numeric_bson_value(&value)
                {
                    *total += number;
                    *count += 1;
                }
            }
            Self::Min(current) => {
                let Some(value) = value else {
                    return Ok(());
                };
                if current
                    .as_ref()
                    .is_none_or(|current| compare_bson_order(&value, current).is_lt())
                {
                    *current = Some(value);
                }
            }
            Self::Max(current) => {
                let Some(value) = value else {
                    return Ok(());
                };
                if current
                    .as_ref()
                    .is_none_or(|current| compare_bson_order(&value, current).is_gt())
                {
                    *current = Some(value);
                }
            }
            Self::First(current) => {
                if current.is_none() {
                    *current = Some(value.unwrap_or(Bson::Null));
                }
            }
            Self::Last(current) => {
                *current = Some(value.unwrap_or(Bson::Null));
            }
            Self::Push(values) => {
                values.push(value.unwrap_or(Bson::Null));
            }
            Self::AddToSet(values) => {
                let value = value.unwrap_or(Bson::Null);
                if !values
                    .iter()
                    .any(|existing| aggregation_values_equal(existing, &value))
                {
                    values.push(value);
                }
            }
        }
        Ok(())
    }

    fn into_bson(self) -> Bson {
        match self {
            Self::Sum { total, saw_double } => numeric_total_to_bson(total, saw_double),
            Self::Avg { total, count } => {
                if count == 0 {
                    Bson::Null
                } else {
                    Bson::Double(total / count as f64)
                }
            }
            Self::Min(value) | Self::Max(value) | Self::First(value) | Self::Last(value) => {
                value.unwrap_or(Bson::Null)
            }
            Self::Push(values) | Self::AddToSet(values) => Bson::Array(values),
        }
    }
}

impl GroupState {
    fn into_document(self, spec: &GroupSpec) -> Document {
        let mut out = doc! { "_id": self.id };
        for (state, accumulator) in self.accumulators.into_iter().zip(&spec.accumulators) {
            out.insert(&accumulator.field, state.into_bson());
        }
        out
    }
}

fn numeric_bson_value(value: &Bson) -> Option<(f64, bool)> {
    match value {
        Bson::Int32(value) => Some((*value as f64, false)),
        Bson::Int64(value) => Some((*value as f64, false)),
        Bson::Double(value) => Some((*value, true)),
        _ => None,
    }
}

fn numeric_total_to_bson(total: f64, saw_double: bool) -> Bson {
    if !saw_double && total.fract() == 0.0 && total >= i64::MIN as f64 && total <= i64::MAX as f64 {
        Bson::Int64(total as i64)
    } else {
        Bson::Double(total)
    }
}

fn find_and_modify(conn: &Connection, command_key: &str, command: &Document) -> Result<Document> {
    if command.contains_key("findAndModify") && command.contains_key("findandmodify") {
        return Ok(command_error(
            9,
            "findAndModify command cannot include both command aliases",
        ));
    }

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
            "bypassDocumentValidation",
            "bypass_document_validation",
            "fields",
            "projection",
            "hint",
            "collation",
            "arrayFilters",
            "writeConcern",
            "txnNumber",
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
    let bypass_validation = match optional_document_validation_bypass(command) {
        Ok(value) => value.unwrap_or(false),
        Err(errmsg) => return Ok(command_error(9, &errmsg)),
    };
    if let Err(err) = validate_filter_shape(&query) {
        return Ok(command_error(err.code, &err.errmsg));
    }
    let collation = match Collation::parse_optional(command, "collation") {
        Ok(collation) => collation,
        Err(errmsg) => return Ok(command_error(72, &errmsg)),
    };
    if let Err(err) = validate_filter_for_collation(&query, &collation) {
        return Ok(command_error(err.code, &err.errmsg));
    }

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
            Some(update) => match parse_update_spec(update, command.get("arrayFilters")) {
                Ok(update) => Some(update),
                Err(errmsg) => return Ok(command_error(update_error_code(&errmsg), &errmsg)),
            },
            None => unreachable!("update presence checked above"),
        }
    };

    let namespace = namespace(db, collection);
    let hint = match parse_optional_hint(command) {
        Ok(Some(hint)) => match resolve_hint(indexes_for_namespace(conn, &namespace)?, hint) {
            Ok(hint) => {
                if let Err(errmsg) = validate_hint_collation(&hint, &collation, &query) {
                    return Ok(command_error(2, &errmsg));
                }
                Some(hint)
            }
            Err(errmsg) => return Ok(command_error(2, &errmsg)),
        },
        Ok(None) => None,
        Err(errmsg) => return Ok(command_error(2, &errmsg)),
    };
    let tx = conn.unchecked_transaction()?;
    ensure_collection_catalog_tx(&tx, &namespace)?;
    let options = if bypass_validation {
        CollectionOptions::empty()
    } else {
        collection_options_tx(&tx, &namespace)?
    };
    if !remove
        && let Some(response) = find_and_modify_update_preflight_error(
            &tx,
            &namespace,
            &query,
            sort.as_deref(),
            hint.as_ref(),
            &collation,
            update.as_ref().expect("update parsed above"),
            upsert,
            &options,
        )?
    {
        return Ok(response);
    }
    sweep_ttl_namespace_at_tx(&tx, &namespace, bson::DateTime::now())?;
    let outcome = if remove {
        apply_find_and_modify_remove(
            &tx,
            &namespace,
            &query,
            sort.as_deref(),
            hint.as_ref(),
            &collation,
        )?
    } else {
        apply_find_and_modify_update(
            &tx,
            &namespace,
            &query,
            sort.as_deref(),
            hint.as_ref(),
            &collation,
            update.as_ref().expect("update parsed above"),
            upsert,
            return_new,
            &options,
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
    hint: Option<&ResolvedHint>,
    collation: &Collation,
) -> Result<std::result::Result<FindAndModifyOutcome, Document>> {
    let Some(target) =
        (match find_and_modify_target_tx(tx, namespace, query, sort, hint, collation)? {
            Ok(target) => target,
            Err(response) => return Ok(Err(response)),
        })
    else {
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
    hint: Option<&ResolvedHint>,
    collation: &Collation,
    update: &ParsedUpdate,
    upsert: bool,
    return_new: bool,
    options: &CollectionOptions,
) -> Result<std::result::Result<FindAndModifyOutcome, Document>> {
    let Some(target) =
        (match find_and_modify_target_tx(tx, namespace, query, sort, hint, collation)? {
            Ok(target) => target,
            Err(response) => return Ok(Err(response)),
        })
    else {
        if !upsert {
            return Ok(Ok(FindAndModifyOutcome {
                value: None,
                n: 0,
                updated_existing: Some(false),
                upserted: None,
            }));
        }

        let mut new_document = match build_upsert_document(query, update, collation) {
            Ok(document) => document,
            Err(errmsg) => return Ok(Err(command_error(update_error_code(&errmsg), &errmsg))),
        };
        ensure_document_id(&mut new_document);
        let upserted_id = new_document.get("_id").cloned().ok_or_else(|| {
            MongolinoError::Protocol("upsert document is missing _id".to_string())
        })?;
        if let Err(errmsg) = validate_document_with_options(options, &new_document) {
            return Ok(Err(command_error(update_error_code(&errmsg), &errmsg)));
        }
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

    let new_document = match apply_update_to_document(&target.document, update, query, collation) {
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
        if let Err(errmsg) = validate_document_with_options(options, &new_document) {
            return Ok(Err(command_error(update_error_code(&errmsg), &errmsg)));
        }
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

fn find_and_modify_update_preflight_error(
    tx: &rusqlite::Transaction<'_>,
    namespace: &str,
    query: &Document,
    sort: Option<&[(String, i32)]>,
    hint: Option<&ResolvedHint>,
    collation: &Collation,
    update: &ParsedUpdate,
    upsert: bool,
    options: &CollectionOptions,
) -> Result<Option<Document>> {
    let Some(target) =
        (match find_and_modify_target_tx(tx, namespace, query, sort, hint, collation)? {
            Ok(target) => target,
            Err(response) => return Ok(Some(response)),
        })
    else {
        if !upsert {
            return Ok(None);
        }
        let mut new_document = match build_upsert_document(query, update, collation) {
            Ok(document) => document,
            Err(errmsg) => return Ok(Some(command_error(update_error_code(&errmsg), &errmsg))),
        };
        ensure_document_id(&mut new_document);
        if let Err(errmsg) = validate_document_with_options(options, &new_document) {
            return Ok(Some(command_error(update_error_code(&errmsg), &errmsg)));
        }
        return Ok(None);
    };

    let new_document = match apply_update_to_document(&target.document, update, query, collation) {
        Ok(document) => document,
        Err(errmsg) => return Ok(Some(command_error(update_error_code(&errmsg), &errmsg))),
    };
    let new_id_key = id_key(&new_document)?;
    if new_id_key != target.id_key {
        let errmsg = "update cannot change _id";
        return Ok(Some(command_error(update_error_code(errmsg), errmsg)));
    }
    if new_document != target.document
        && let Err(errmsg) = validate_document_with_options(options, &new_document)
    {
        return Ok(Some(command_error(update_error_code(&errmsg), &errmsg)));
    }
    Ok(None)
}

fn find_and_modify_target_tx(
    tx: &rusqlite::Transaction<'_>,
    namespace: &str,
    query: &Document,
    sort: Option<&[(String, i32)]>,
    hint: Option<&ResolvedHint>,
    collation: &Collation,
) -> Result<std::result::Result<Option<StoredDocument>, Document>> {
    let mut matches = Vec::new();
    let candidates =
        match transaction_candidate_documents_with_hint(tx, namespace, query, hint, collation) {
            Ok(candidates) => candidates,
            Err(errmsg) => return Ok(Err(command_error(2, &errmsg))),
        };
    for stored in candidates {
        match matches_filter_with_collation(&stored.document, query, collation) {
            Ok(true) => matches.push(stored),
            Ok(false) => {}
            Err(err) => return Ok(Err(command_error(err.code, &err.errmsg))),
        }
    }
    if let Some(sort) = sort {
        sort_stored_documents(&mut matches, sort, collation);
    }
    Ok(Ok(matches.into_iter().next()))
}

fn sort_stored_documents(
    documents: &mut [StoredDocument],
    sort: &[(String, i32)],
    collation: &Collation,
) {
    documents.sort_by(|left, right| {
        compare_documents_for_sort(&left.document, &right.document, sort, collation)
    });
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

#[derive(Clone, Debug, PartialEq)]
enum AggregationExpression {
    FieldPath(String),
    Variable(AggregationVariable, Option<String>),
    Literal(Bson),
    Array(Vec<AggregationExpression>),
    Document(Vec<(String, AggregationExpression)>),
    Operator(AggregationExpressionOperator),
}

#[derive(Clone, Copy, Debug, PartialEq)]
enum AggregationVariable {
    Root,
    Current,
}

#[derive(Clone, Debug, PartialEq)]
enum AggregationExpressionOperator {
    Literal(Bson),
    IfNull(Vec<AggregationExpression>),
    Concat(Vec<AggregationExpression>),
    ToString(Box<AggregationExpression>),
    ToLower(Box<AggregationExpression>),
    ToUpper(Box<AggregationExpression>),
    Eq(Box<AggregationExpression>, Box<AggregationExpression>),
    Ne(Box<AggregationExpression>, Box<AggregationExpression>),
    Gt(Box<AggregationExpression>, Box<AggregationExpression>),
    Gte(Box<AggregationExpression>, Box<AggregationExpression>),
    Lt(Box<AggregationExpression>, Box<AggregationExpression>),
    Lte(Box<AggregationExpression>, Box<AggregationExpression>),
    And(Vec<AggregationExpression>),
    Or(Vec<AggregationExpression>),
    Not(Box<AggregationExpression>),
    Cond {
        condition: Box<AggregationExpression>,
        then_expr: Box<AggregationExpression>,
        else_expr: Box<AggregationExpression>,
    },
    Add(Vec<AggregationExpression>),
    Subtract(Box<AggregationExpression>, Box<AggregationExpression>),
    Multiply(Vec<AggregationExpression>),
    Divide(Box<AggregationExpression>, Box<AggregationExpression>),
}

struct AggregationExpressionContext<'a> {
    root: &'a Document,
    current: &'a Document,
    collation: &'a Collation,
}

impl<'a> AggregationExpressionContext<'a> {
    fn new(document: &'a Document, collation: &'a Collation) -> Self {
        Self {
            root: document,
            current: document,
            collation,
        }
    }
}

impl AggregationExpression {
    fn evaluate(
        &self,
        context: &AggregationExpressionContext<'_>,
    ) -> std::result::Result<Option<Bson>, Document> {
        match self {
            Self::FieldPath(path) => Ok(get_document_path(context.current, path).cloned()),
            Self::Variable(variable, path) => {
                let document = match variable {
                    AggregationVariable::Root => context.root,
                    AggregationVariable::Current => context.current,
                };
                Ok(match path {
                    None => Some(Bson::Document(document.clone())),
                    Some(path) => get_document_path(document, path).cloned(),
                })
            }
            Self::Literal(value) => Ok(Some(value.clone())),
            Self::Array(expressions) => {
                let mut values = Vec::new();
                for expression in expressions {
                    values.push(expression.evaluate(context)?.unwrap_or(Bson::Null));
                }
                Ok(Some(Bson::Array(values)))
            }
            Self::Document(fields) => {
                let mut out = Document::new();
                for (field, expression) in fields {
                    out.insert(field, expression.evaluate(context)?.unwrap_or(Bson::Null));
                }
                Ok(Some(Bson::Document(out)))
            }
            Self::Operator(operator) => operator.evaluate(context),
        }
    }

    fn is_constant(&self) -> bool {
        match self {
            Self::FieldPath(_) | Self::Variable(_, _) => false,
            Self::Literal(_) => true,
            Self::Array(expressions) => expressions.iter().all(Self::is_constant),
            Self::Document(fields) => fields
                .iter()
                .all(|(_, expression)| expression.is_constant()),
            Self::Operator(operator) => operator.is_constant(),
        }
    }
}

impl AggregationExpressionOperator {
    fn evaluate(
        &self,
        context: &AggregationExpressionContext<'_>,
    ) -> std::result::Result<Option<Bson>, Document> {
        match self {
            Self::Literal(value) => Ok(Some(value.clone())),
            Self::IfNull(expressions) => {
                for expression in expressions.iter().take(expressions.len().saturating_sub(1)) {
                    match expression.evaluate(context)? {
                        Some(Bson::Null) | None => {}
                        Some(value) => return Ok(Some(value)),
                    }
                }
                Ok(Some(
                    expressions
                        .last()
                        .expect("$ifNull arity checked")
                        .evaluate(context)?
                        .unwrap_or(Bson::Null),
                ))
            }
            Self::Concat(expressions) => {
                let mut out = String::new();
                for expression in expressions {
                    match expression.evaluate(context)?.unwrap_or(Bson::Null) {
                        Bson::String(value) => out.push_str(&value),
                        Bson::Null => {}
                        _ => {
                            return Err(command_error(
                                9,
                                "$concat operands must evaluate to strings or null",
                            ));
                        }
                    }
                }
                Ok(Some(Bson::String(out)))
            }
            Self::ToString(expression) => Ok(Some(Bson::String(aggregation_to_string(
                &expression.evaluate(context)?.unwrap_or(Bson::Null),
            )?))),
            Self::ToLower(expression) => Ok(Some(Bson::String(
                aggregation_string_operand(expression, context, "$toLower")?.to_lowercase(),
            ))),
            Self::ToUpper(expression) => Ok(Some(Bson::String(
                aggregation_string_operand(expression, context, "$toUpper")?.to_uppercase(),
            ))),
            Self::Eq(left, right) => Ok(Some(Bson::Boolean(aggregation_operands_equal(
                left, right, context,
            )?))),
            Self::Ne(left, right) => Ok(Some(Bson::Boolean(!aggregation_operands_equal(
                left, right, context,
            )?))),
            Self::Gt(left, right) => Ok(Some(Bson::Boolean(aggregation_compare_operands(
                left,
                right,
                context,
                |ordering| ordering.is_gt(),
            )?))),
            Self::Gte(left, right) => Ok(Some(Bson::Boolean(aggregation_compare_operands(
                left,
                right,
                context,
                |ordering| !ordering.is_lt(),
            )?))),
            Self::Lt(left, right) => Ok(Some(Bson::Boolean(aggregation_compare_operands(
                left,
                right,
                context,
                |ordering| ordering.is_lt(),
            )?))),
            Self::Lte(left, right) => Ok(Some(Bson::Boolean(aggregation_compare_operands(
                left,
                right,
                context,
                |ordering| !ordering.is_gt(),
            )?))),
            Self::And(expressions) => {
                for expression in expressions {
                    if !aggregation_truthy(&expression.evaluate(context)?.unwrap_or(Bson::Null)) {
                        return Ok(Some(Bson::Boolean(false)));
                    }
                }
                Ok(Some(Bson::Boolean(true)))
            }
            Self::Or(expressions) => {
                for expression in expressions {
                    if aggregation_truthy(&expression.evaluate(context)?.unwrap_or(Bson::Null)) {
                        return Ok(Some(Bson::Boolean(true)));
                    }
                }
                Ok(Some(Bson::Boolean(false)))
            }
            Self::Not(expression) => Ok(Some(Bson::Boolean(!aggregation_truthy(
                &expression.evaluate(context)?.unwrap_or(Bson::Null),
            )))),
            Self::Cond {
                condition,
                then_expr,
                else_expr,
            } => {
                if aggregation_truthy(&condition.evaluate(context)?.unwrap_or(Bson::Null)) {
                    then_expr.evaluate(context)
                } else {
                    else_expr.evaluate(context)
                }
            }
            Self::Add(expressions) => {
                let mut total = 0.0;
                let mut saw_double = false;
                for expression in expressions {
                    let value = expression.evaluate(context)?.unwrap_or(Bson::Null);
                    let Some((number, is_double)) = numeric_bson_value(&value) else {
                        return Err(command_error(9, "$add operands must be numeric"));
                    };
                    total += number;
                    saw_double |= is_double;
                }
                Ok(Some(numeric_total_to_bson(total, saw_double)))
            }
            Self::Subtract(left, right) => {
                let (left, left_double) = aggregation_numeric_operand(left, context, "$subtract")?;
                let (right, right_double) =
                    aggregation_numeric_operand(right, context, "$subtract")?;
                Ok(Some(numeric_total_to_bson(
                    left - right,
                    left_double || right_double,
                )))
            }
            Self::Multiply(expressions) => {
                let mut total = 1.0;
                let mut saw_double = false;
                for expression in expressions {
                    let (number, is_double) =
                        aggregation_numeric_operand(expression, context, "$multiply")?;
                    total *= number;
                    saw_double |= is_double;
                }
                Ok(Some(numeric_total_to_bson(total, saw_double)))
            }
            Self::Divide(left, right) => {
                let (left, _) = aggregation_numeric_operand(left, context, "$divide")?;
                let (right, _) = aggregation_numeric_operand(right, context, "$divide")?;
                if right == 0.0 {
                    return Err(command_error(9, "$divide cannot divide by zero"));
                }
                Ok(Some(Bson::Double(left / right)))
            }
        }
    }

    fn is_constant(&self) -> bool {
        match self {
            Self::Literal(_) => true,
            Self::IfNull(expressions)
            | Self::Concat(expressions)
            | Self::And(expressions)
            | Self::Or(expressions)
            | Self::Add(expressions)
            | Self::Multiply(expressions) => {
                expressions.iter().all(AggregationExpression::is_constant)
            }
            Self::ToString(expression)
            | Self::ToLower(expression)
            | Self::ToUpper(expression)
            | Self::Not(expression) => expression.is_constant(),
            Self::Eq(left, right)
            | Self::Ne(left, right)
            | Self::Gt(left, right)
            | Self::Gte(left, right)
            | Self::Lt(left, right)
            | Self::Lte(left, right)
            | Self::Subtract(left, right)
            | Self::Divide(left, right) => left.is_constant() && right.is_constant(),
            Self::Cond {
                condition,
                then_expr,
                else_expr,
            } => condition.is_constant() && then_expr.is_constant() && else_expr.is_constant(),
        }
    }
}

fn evaluate_constant_aggregation_expression(
    expression: &AggregationExpression,
) -> std::result::Result<Option<Bson>, Document> {
    if !expression.is_constant() {
        return Ok(None);
    }
    let document = Document::new();
    let context = AggregationExpressionContext::new(&document, &Collation::Simple);
    expression.evaluate(&context)
}

fn aggregation_operands_equal(
    left: &AggregationExpression,
    right: &AggregationExpression,
    context: &AggregationExpressionContext<'_>,
) -> std::result::Result<bool, Document> {
    let left = left.evaluate(context)?.unwrap_or(Bson::Null);
    let right = right.evaluate(context)?.unwrap_or(Bson::Null);
    Ok(context.collation.values_equal(&left, &right))
}

fn aggregation_compare_operands(
    left: &AggregationExpression,
    right: &AggregationExpression,
    context: &AggregationExpressionContext<'_>,
    predicate: impl Fn(std::cmp::Ordering) -> bool,
) -> std::result::Result<bool, Document> {
    let left = left.evaluate(context)?.unwrap_or(Bson::Null);
    let right = right.evaluate(context)?.unwrap_or(Bson::Null);
    Ok(predicate(context.collation.compare_order(&left, &right)))
}

fn aggregation_numeric_operand(
    expression: &AggregationExpression,
    context: &AggregationExpressionContext<'_>,
    operator: &str,
) -> std::result::Result<(f64, bool), Document> {
    let value = expression.evaluate(context)?.unwrap_or(Bson::Null);
    numeric_bson_value(&value)
        .ok_or_else(|| command_error(9, &format!("{operator} operands must be numeric")))
}

fn aggregation_string_operand(
    expression: &AggregationExpression,
    context: &AggregationExpressionContext<'_>,
    operator: &str,
) -> std::result::Result<String, Document> {
    match expression.evaluate(context)?.unwrap_or(Bson::Null) {
        Bson::String(value) => Ok(value),
        Bson::Null => Ok(String::new()),
        _ => Err(command_error(
            9,
            &format!("{operator} operand must evaluate to a string or null"),
        )),
    }
}

fn aggregation_to_string(value: &Bson) -> std::result::Result<String, Document> {
    match value {
        Bson::Null => Ok(String::new()),
        Bson::String(value) => Ok(value.clone()),
        Bson::Boolean(value) => Ok(value.to_string()),
        Bson::Int32(value) => Ok(value.to_string()),
        Bson::Int64(value) => Ok(value.to_string()),
        Bson::Double(value) => Ok(value.to_string()),
        Bson::ObjectId(value) => Ok(value.to_hex()),
        Bson::DateTime(value) => Ok(value.to_string()),
        _ => Err(command_error(
            9,
            "$toString does not support this operand type",
        )),
    }
}

fn aggregation_truthy(value: &Bson) -> bool {
    match value {
        Bson::Null => false,
        Bson::Boolean(value) => *value,
        Bson::Int32(value) => *value != 0,
        Bson::Int64(value) => *value != 0,
        Bson::Double(value) => *value != 0.0,
        _ => true,
    }
}

fn parse_aggregation_expression(
    value: &Bson,
    context: &str,
    allow_document_key_spec: bool,
) -> std::result::Result<AggregationExpression, Document> {
    parse_aggregation_expression_inner(value, context, allow_document_key_spec, true)
}

fn parse_aggregation_expression_inner(
    value: &Bson,
    context: &str,
    allow_document_expression: bool,
    allow_array_expression: bool,
) -> std::result::Result<AggregationExpression, Document> {
    match value {
        Bson::String(value) if value.starts_with("$$") => {
            let (variable, path) = parse_aggregation_variable(value, context)?;
            Ok(AggregationExpression::Variable(variable, path))
        }
        Bson::String(value) if value.starts_with('$') => {
            let path = parse_aggregation_field_path(value, context)?;
            Ok(AggregationExpression::FieldPath(path))
        }
        Bson::Null
        | Bson::Boolean(_)
        | Bson::String(_)
        | Bson::Int32(_)
        | Bson::Int64(_)
        | Bson::Double(_)
        | Bson::ObjectId(_)
        | Bson::DateTime(_) => Ok(AggregationExpression::Literal(value.clone())),
        Bson::Array(values) if allow_array_expression => values
            .iter()
            .map(|value| parse_aggregation_expression_inner(value, context, true, true))
            .collect::<std::result::Result<Vec<_>, _>>()
            .map(AggregationExpression::Array),
        Bson::Array(_) => Err(command_error(
            72,
            &format!("{context} does not support array expressions"),
        )),
        Bson::Document(document) => {
            parse_aggregation_document_expression(document, context, allow_document_expression)
        }
        _ => Err(command_error(
            72,
            &format!("{context} expression type is not supported"),
        )),
    }
}

fn parse_aggregation_document_expression(
    document: &Document,
    context: &str,
    allow_document_expression: bool,
) -> std::result::Result<AggregationExpression, Document> {
    let dollar_keys = document
        .keys()
        .filter(|key| key.starts_with('$'))
        .collect::<Vec<_>>();
    if !dollar_keys.is_empty() {
        if document.len() != 1 {
            return Err(command_error(
                9,
                &format!("{context} expression documents must contain one operator"),
            ));
        }
        let (operator, operand) = document.iter().next().expect("document len checked above");
        return parse_aggregation_expression_operator(operator, operand, context)
            .map(AggregationExpression::Operator);
    }
    if !allow_document_expression {
        return Err(command_error(
            72,
            &format!("{context} does not support document expressions"),
        ));
    }

    let mut fields = Vec::new();
    for (field, nested) in document {
        validate_group_key_field_name(field, context)?;
        fields.push((
            field.to_string(),
            parse_aggregation_expression_inner(nested, context, true, true)?,
        ));
    }
    Ok(AggregationExpression::Document(fields))
}

fn parse_aggregation_expression_operator(
    operator: &str,
    operand: &Bson,
    context: &str,
) -> std::result::Result<AggregationExpressionOperator, Document> {
    match operator {
        "$literal" => Ok(AggregationExpressionOperator::Literal(operand.clone())),
        "$ifNull" => {
            let expressions = parse_expression_array_args(operand, context, "$ifNull")?;
            if expressions.len() < 2 {
                return Err(command_error(9, "$ifNull requires at least two operands"));
            }
            Ok(AggregationExpressionOperator::IfNull(expressions))
        }
        "$concat" => Ok(AggregationExpressionOperator::Concat(
            parse_expression_array_args(operand, context, "$concat")?,
        )),
        "$toString" => Ok(AggregationExpressionOperator::ToString(Box::new(
            parse_single_expression_arg(operand, context, "$toString")?,
        ))),
        "$toLower" => Ok(AggregationExpressionOperator::ToLower(Box::new(
            parse_single_expression_arg(operand, context, "$toLower")?,
        ))),
        "$toUpper" => Ok(AggregationExpressionOperator::ToUpper(Box::new(
            parse_single_expression_arg(operand, context, "$toUpper")?,
        ))),
        "$eq" => parse_binary_expression_args(operand, context, "$eq").map(|(left, right)| {
            AggregationExpressionOperator::Eq(Box::new(left), Box::new(right))
        }),
        "$ne" => parse_binary_expression_args(operand, context, "$ne").map(|(left, right)| {
            AggregationExpressionOperator::Ne(Box::new(left), Box::new(right))
        }),
        "$gt" => parse_binary_expression_args(operand, context, "$gt").map(|(left, right)| {
            AggregationExpressionOperator::Gt(Box::new(left), Box::new(right))
        }),
        "$gte" => parse_binary_expression_args(operand, context, "$gte").map(|(left, right)| {
            AggregationExpressionOperator::Gte(Box::new(left), Box::new(right))
        }),
        "$lt" => parse_binary_expression_args(operand, context, "$lt").map(|(left, right)| {
            AggregationExpressionOperator::Lt(Box::new(left), Box::new(right))
        }),
        "$lte" => parse_binary_expression_args(operand, context, "$lte").map(|(left, right)| {
            AggregationExpressionOperator::Lte(Box::new(left), Box::new(right))
        }),
        "$and" => Ok(AggregationExpressionOperator::And(
            parse_expression_array_args(operand, context, "$and")?,
        )),
        "$or" => Ok(AggregationExpressionOperator::Or(
            parse_expression_array_args(operand, context, "$or")?,
        )),
        "$not" => Ok(AggregationExpressionOperator::Not(Box::new(
            parse_single_expression_array_arg(operand, context, "$not")?,
        ))),
        "$cond" => parse_cond_expression(operand, context),
        "$add" => Ok(AggregationExpressionOperator::Add(
            parse_expression_array_args(operand, context, "$add")?,
        )),
        "$subtract" => {
            parse_binary_expression_args(operand, context, "$subtract").map(|(left, right)| {
                AggregationExpressionOperator::Subtract(Box::new(left), Box::new(right))
            })
        }
        "$multiply" => Ok(AggregationExpressionOperator::Multiply(
            parse_expression_array_args(operand, context, "$multiply")?,
        )),
        "$divide" => {
            parse_binary_expression_args(operand, context, "$divide").map(|(left, right)| {
                AggregationExpressionOperator::Divide(Box::new(left), Box::new(right))
            })
        }
        _ => Err(command_error(
            72,
            &format!("{context} expression operator {operator} is not supported"),
        )),
    }
}

fn parse_expression_array_args(
    operand: &Bson,
    context: &str,
    operator: &str,
) -> std::result::Result<Vec<AggregationExpression>, Document> {
    let Bson::Array(values) = operand else {
        return Err(command_error(
            9,
            &format!("{operator} requires an array of operands"),
        ));
    };
    values
        .iter()
        .map(|value| parse_aggregation_expression_inner(value, context, true, true))
        .collect()
}

fn parse_single_expression_arg(
    operand: &Bson,
    context: &str,
    operator: &str,
) -> std::result::Result<AggregationExpression, Document> {
    if let Bson::Array(values) = operand {
        if values.len() != 1 {
            return Err(command_error(
                9,
                &format!("{operator} requires one operand"),
            ));
        }
        parse_aggregation_expression_inner(&values[0], context, true, true)
    } else {
        parse_aggregation_expression_inner(operand, context, true, true)
    }
}

fn parse_single_expression_array_arg(
    operand: &Bson,
    context: &str,
    operator: &str,
) -> std::result::Result<AggregationExpression, Document> {
    let values = parse_expression_array_args(operand, context, operator)?;
    if values.len() != 1 {
        return Err(command_error(
            9,
            &format!("{operator} requires one operand"),
        ));
    }
    Ok(values.into_iter().next().expect("len checked above"))
}

fn parse_binary_expression_args(
    operand: &Bson,
    context: &str,
    operator: &str,
) -> std::result::Result<(AggregationExpression, AggregationExpression), Document> {
    let values = parse_expression_array_args(operand, context, operator)?;
    if values.len() != 2 {
        return Err(command_error(
            9,
            &format!("{operator} requires exactly two operands"),
        ));
    }
    let mut values = values.into_iter();
    Ok((
        values.next().expect("len checked above"),
        values.next().expect("len checked above"),
    ))
}

fn parse_cond_expression(
    operand: &Bson,
    context: &str,
) -> std::result::Result<AggregationExpressionOperator, Document> {
    match operand {
        Bson::Array(values) => {
            if values.len() != 3 {
                return Err(command_error(9, "$cond array form requires three operands"));
            }
            Ok(AggregationExpressionOperator::Cond {
                condition: Box::new(parse_aggregation_expression_inner(
                    &values[0], context, true, true,
                )?),
                then_expr: Box::new(parse_aggregation_expression_inner(
                    &values[1], context, true, true,
                )?),
                else_expr: Box::new(parse_aggregation_expression_inner(
                    &values[2], context, true, true,
                )?),
            })
        }
        Bson::Document(document) => {
            for key in document.keys() {
                if !matches!(key.as_str(), "if" | "then" | "else") {
                    return Err(command_error(
                        72,
                        &format!("$cond option {key} is not supported"),
                    ));
                }
            }
            let Some(condition) = document.get("if") else {
                return Err(command_error(9, "$cond document form requires if"));
            };
            let Some(then_expr) = document.get("then") else {
                return Err(command_error(9, "$cond document form requires then"));
            };
            let Some(else_expr) = document.get("else") else {
                return Err(command_error(9, "$cond document form requires else"));
            };
            Ok(AggregationExpressionOperator::Cond {
                condition: Box::new(parse_aggregation_expression_inner(
                    condition, context, true, true,
                )?),
                then_expr: Box::new(parse_aggregation_expression_inner(
                    then_expr, context, true, true,
                )?),
                else_expr: Box::new(parse_aggregation_expression_inner(
                    else_expr, context, true, true,
                )?),
            })
        }
        _ => Err(command_error(
            9,
            "$cond requires an array or document operand",
        )),
    }
}

fn parse_aggregation_field_path(
    value: &str,
    context: &str,
) -> std::result::Result<String, Document> {
    let Some(path) = value.strip_prefix('$') else {
        return Err(command_error(
            9,
            &format!("{context} requires a field path"),
        ));
    };
    validate_aggregation_path(path, context, true)?;
    Ok(path.to_string())
}

fn parse_aggregation_variable(
    value: &str,
    context: &str,
) -> std::result::Result<(AggregationVariable, Option<String>), Document> {
    let (variable, rest) = if let Some(rest) = value.strip_prefix("$$ROOT") {
        (AggregationVariable::Root, rest)
    } else if let Some(rest) = value.strip_prefix("$$CURRENT") {
        (AggregationVariable::Current, rest)
    } else {
        return Err(command_error(
            72,
            &format!("{context} variable {value} is not supported"),
        ));
    };
    if rest.is_empty() {
        return Ok((variable, None));
    }
    let Some(path) = rest.strip_prefix('.') else {
        return Err(command_error(
            9,
            &format!("{context} variable path is malformed"),
        ));
    };
    validate_aggregation_path(path, context, true)?;
    Ok((variable, Some(path.to_string())))
}

fn validate_aggregation_path(
    path: &str,
    context: &str,
    reject_dollar_segments: bool,
) -> std::result::Result<(), Document> {
    if path.is_empty() {
        return Err(command_error(
            9,
            &format!("{context} field path cannot be empty"),
        ));
    }
    for segment in path.split('.') {
        if segment.is_empty() {
            return Err(command_error(
                9,
                &format!("{context} field path contains an empty segment"),
            ));
        }
        if reject_dollar_segments && segment.contains('$') {
            return Err(command_error(
                9,
                &format!("{context} field path contains unsupported $ segment"),
            ));
        }
    }
    Ok(())
}

fn validate_group_key_field_name(field: &str, context: &str) -> std::result::Result<(), Document> {
    if field.is_empty() || field.starts_with('$') || field.contains('.') {
        return Err(command_error(
            9,
            &format!("{context} document key fields must be simple field names"),
        ));
    }
    Ok(())
}

fn aggregation_values_equal(left: &Bson, right: &Bson) -> bool {
    match (numeric_value(left), numeric_value(right)) {
        (Some(left), Some(right)) => return left == right,
        _ => {}
    }
    match (left, right) {
        (Bson::Document(left), Bson::Document(right)) => {
            left.len() == right.len()
                && left.iter().all(|(key, left_value)| {
                    right.get(key).is_some_and(|right_value| {
                        aggregation_values_equal(left_value, right_value)
                    })
                })
        }
        (Bson::Array(left), Bson::Array(right)) => {
            left.len() == right.len()
                && left.iter().zip(right).all(|(left_value, right_value)| {
                    aggregation_values_equal(left_value, right_value)
                })
        }
        _ => left == right,
    }
}

fn is_count_documents_group(group: &Document) -> bool {
    group.len() == 2
        && matches!(group.get("_id"), Some(Bson::Int32(1) | Bson::Int64(1)))
        && matches!(
            group.get("n"),
            Some(Bson::Document(sum))
                if sum.len() == 1
                    && matches!(sum.get("$sum"), Some(Bson::Int32(1) | Bson::Int64(1)))
        )
}

fn count_matching_documents(
    documents: Vec<Document>,
    filter: &Document,
    skip: usize,
    limit: Option<usize>,
    collation: &Collation,
) -> MatchResult<i64> {
    let mut matched = 0_i64;
    let mut skipped = 0_usize;
    for document in documents {
        match matches_filter_with_collation(&document, filter, collation) {
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

fn pushed_down_count(
    conn: &Connection,
    namespace: &str,
    filter: &Document,
    skip: usize,
    limit: Option<usize>,
) -> Result<Option<i64>> {
    pushed_down_count_with_collation(conn, namespace, filter, skip, limit, &Collation::Simple)
}

fn pushed_down_count_with_collation(
    conn: &Connection,
    namespace: &str,
    filter: &Document,
    skip: usize,
    limit: Option<usize>,
    collation: &Collation,
) -> Result<Option<i64>> {
    let total = match plan_count_with_collation(conn, namespace, filter, collation)? {
        CountPlan::Empty => sql_count_documents(conn, namespace)?,
        CountPlan::IdEquality(id_key) => sql_count_id_equality(conn, namespace, &id_key)?,
        CountPlan::IndexedEquality {
            index_name,
            key_value,
        } => sql_count_index_entries(conn, namespace, &index_name, &key_value)?,
        CountPlan::IndexedRange { index_name, range } => {
            sql_count_index_entries_by_range(conn, namespace, &index_name, &range)?
        }
        CountPlan::Fallback => return Ok(None),
    };
    Ok(Some(apply_count_skip_limit(total, skip, limit)))
}

fn sql_count_documents(conn: &Connection, namespace: &str) -> Result<i64> {
    Ok(conn.query_row(
        "SELECT COUNT(*) FROM documents WHERE namespace = ?1",
        params![namespace],
        |row| row.get(0),
    )?)
}

fn sql_count_id_equality(conn: &Connection, namespace: &str, id_key: &str) -> Result<i64> {
    Ok(conn.query_row(
        "SELECT COUNT(*) FROM documents WHERE namespace = ?1 AND id_key = ?2",
        params![namespace, id_key],
        |row| row.get(0),
    )?)
}

fn sql_count_index_entries(
    conn: &Connection,
    namespace: &str,
    index_name: &str,
    key_value: &str,
) -> Result<i64> {
    Ok(conn.query_row(
        "SELECT COUNT(DISTINCT id_key) FROM index_entries WHERE namespace = ?1 AND index_name = ?2 AND key_value = ?3",
        params![namespace, index_name, key_value],
        |row| row.get(0),
    )?)
}

fn sql_count_index_entries_by_range(
    conn: &Connection,
    namespace: &str,
    index_name: &str,
    range: &RangePlannerKey,
) -> Result<i64> {
    let bounds = range_sql_bounds(range);
    Ok(conn.query_row(
        &format!(
            "SELECT COUNT(DISTINCT id_key) FROM index_entries WHERE namespace = ?1 AND index_name = ?2 AND {}",
            bounds.predicate
        ),
        params![namespace, index_name, bounds.lower, bounds.upper],
        |row| row.get(0),
    )?)
}

struct RangeSqlBounds {
    predicate: String,
    lower: String,
    upper: String,
}

fn range_sql_bounds(range: &RangePlannerKey) -> RangeSqlBounds {
    let lower_fallback = range.key_prefix.clone();
    let upper_fallback = format!("{}\u{10ffff}", range.key_prefix);
    let (lower_operator, lower) = match &range.lower {
        Some(bound) if bound.inclusive => (">=", format!("{}{}", range.key_prefix, bound.key)),
        Some(bound) => (">", format!("{}{}", range.key_prefix, bound.key)),
        None => (">=", lower_fallback),
    };
    let (upper_operator, upper) = match &range.upper {
        Some(bound) if bound.inclusive => ("<=", format!("{}{}", range.key_prefix, bound.key)),
        Some(bound) => ("<", format!("{}{}", range.key_prefix, bound.key)),
        None => ("<", upper_fallback),
    };
    RangeSqlBounds {
        predicate: format!("key_value {lower_operator} ?3 AND key_value {upper_operator} ?4"),
        lower,
        upper,
    }
}

fn apply_count_skip_limit(total: i64, skip: usize, limit: Option<usize>) -> i64 {
    let after_skip = total.saturating_sub(skip as i64);
    match limit {
        Some(limit) => after_skip.min(limit as i64),
        None => after_skip,
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum Collation {
    Simple,
    EnglishCaseInsensitive,
}

impl Collation {
    fn parse_optional(command: &Document, key: &str) -> std::result::Result<Self, String> {
        match command.get(key) {
            None => Ok(Self::Simple),
            Some(value) => Self::parse_bson(value),
        }
    }

    fn parse_bson(value: &Bson) -> std::result::Result<Self, String> {
        let Bson::Document(document) = value else {
            return Err("collation must be a document".to_string());
        };
        Self::parse_document(document)
    }

    fn parse_document(document: &Document) -> std::result::Result<Self, String> {
        if document.is_empty() {
            return Err("collation requires locale".to_string());
        }
        for key in document.keys() {
            if !matches!(key.as_str(), "locale" | "strength") {
                return Err(format!("collation option {key} is not supported"));
            }
        }
        let locale = match document.get("locale") {
            Some(Bson::String(locale)) if !locale.is_empty() => locale.as_str(),
            Some(Bson::String(_)) => return Err("collation locale must not be empty".to_string()),
            Some(_) => return Err("collation locale must be a string".to_string()),
            None => return Err("collation requires locale".to_string()),
        };
        match locale {
            "simple" => {
                if document.contains_key("strength") {
                    return Err("simple collation does not support strength".to_string());
                }
                Ok(Self::Simple)
            }
            "en" | "en_US" => match document.get("strength") {
                Some(Bson::Int32(2) | Bson::Int64(2)) => Ok(Self::EnglishCaseInsensitive),
                Some(_) => Err("English collation requires strength 2".to_string()),
                None => Err("English collation requires strength 2".to_string()),
            },
            _ => Err(format!("collation locale {locale} is not supported")),
        }
    }

    fn to_document(&self) -> Document {
        match self {
            Self::Simple => doc! { "locale": "simple" },
            Self::EnglishCaseInsensitive => doc! { "locale": "en", "strength": 2_i32 },
        }
    }

    fn is_simple(&self) -> bool {
        matches!(self, Self::Simple)
    }

    fn string_key(&self, value: &str) -> String {
        match self {
            Self::Simple => value.to_string(),
            Self::EnglishCaseInsensitive => value.to_lowercase(),
        }
    }

    fn id_key_from_bson(&self, value: &Bson) -> String {
        match (self, value) {
            (Self::EnglishCaseInsensitive, Bson::String(value)) => {
                format!("str-ci:{}", self.string_key(value))
            }
            _ => id_key_from_bson(value),
        }
    }

    fn values_equal(&self, candidate: &Bson, expected: &Bson) -> bool {
        if let Bson::Array(values) = candidate {
            if !matches!(expected, Bson::Array(_)) {
                return values
                    .iter()
                    .any(|value| self.values_equal(value, expected));
            }
        }

        match (numeric_value(candidate), numeric_value(expected)) {
            (Some(left), Some(right)) => return left == right,
            _ => {}
        }
        match (candidate, expected) {
            (Bson::String(left), Bson::String(right)) => {
                self.string_key(left) == self.string_key(right)
            }
            _ => candidate == expected,
        }
    }

    fn compare_order(&self, left: &Bson, right: &Bson) -> std::cmp::Ordering {
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
            .then_with(|| match (left, right) {
                (Bson::String(left), Bson::String(right)) => {
                    self.string_key(left).cmp(&self.string_key(right))
                }
                _ => format!("{left:?}").cmp(&format!("{right:?}")),
            })
    }
}

fn validate_filter_for_collation(filter: &Document, collation: &Collation) -> MatchResult<()> {
    if collation.is_simple() {
        return Ok(());
    }
    validate_filter_for_non_simple_collation(filter)
}

fn validate_filter_for_non_simple_collation(filter: &Document) -> MatchResult<()> {
    for (key, condition) in filter {
        if key.starts_with('$') {
            let Bson::Array(clauses) = condition else {
                continue;
            };
            for clause in clauses {
                if let Bson::Document(clause) = clause {
                    validate_filter_for_non_simple_collation(clause)?;
                }
            }
        } else {
            validate_field_condition_for_non_simple_collation(condition)?;
        }
    }
    Ok(())
}

fn validate_field_condition_for_non_simple_collation(condition: &Bson) -> MatchResult<()> {
    if !is_operator_document(condition) {
        return Ok(());
    }
    let Bson::Document(operators) = condition else {
        return Ok(());
    };
    for (operator, operand) in operators {
        match operator.as_str() {
            "$gt" | "$gte" | "$lt" | "$lte" if matches!(operand, Bson::String(_)) => {
                return Err(match_error(
                    72,
                    "string range predicates with non-simple collation are not supported",
                ));
            }
            "$not" => validate_field_condition_for_non_simple_collation(operand)?,
            "$elemMatch" => {
                if let Bson::Document(predicate) = operand {
                    if predicate
                        .keys()
                        .all(|key| is_scalar_elem_match_operator(key))
                    {
                        validate_field_condition_for_non_simple_collation(operand)?;
                    } else {
                        validate_filter_for_non_simple_collation(predicate)?;
                    }
                }
            }
            _ => {}
        }
    }
    Ok(())
}

#[derive(Debug, PartialEq)]
enum CountPlan {
    Empty,
    IdEquality(String),
    IndexedEquality {
        index_name: String,
        key_value: String,
    },
    IndexedRange {
        index_name: String,
        range: RangePlannerKey,
    },
    Fallback,
}

#[derive(Clone, Debug, PartialEq)]
enum PlannerScanStrategy {
    CollectionScan,
    IdExact,
    IndexExactEquality,
    IndexEqualityPrefix,
    IndexRange,
    IndexSort,
}

#[derive(Clone, Debug, PartialEq)]
struct PlannerDiagnostic {
    scan_strategy: PlannerScanStrategy,
    index_name: Option<String>,
    index_key: Option<Document>,
    matcher_validation_required: bool,
    fallback_reason: Option<String>,
}

#[derive(Clone, Debug, PartialEq)]
enum PlannerV2Plan {
    CollectionScan {
        diagnostic: PlannerDiagnostic,
    },
    IdEquality {
        id_key: String,
        diagnostic: PlannerDiagnostic,
    },
    IndexExactEquality {
        index_name: String,
        index_key: Document,
        key_value: String,
        diagnostic: PlannerDiagnostic,
    },
    IndexEqualityPrefix {
        index_name: String,
        index_key: Document,
        prefix_len: usize,
        key_value: String,
        diagnostic: PlannerDiagnostic,
    },
    IndexRange {
        index_name: String,
        index_key: Document,
        range: RangePlannerKey,
        diagnostic: PlannerDiagnostic,
    },
    IndexSort {
        index_name: String,
        index_key: Document,
        diagnostic: PlannerDiagnostic,
    },
}

#[derive(Clone, Debug, PartialEq)]
struct RangePlannerKey {
    field: String,
    equality_prefix_len: usize,
    key_prefix: String,
    lower: Option<RangeBound>,
    upper: Option<RangeBound>,
}

#[derive(Clone, Debug, PartialEq)]
struct RangeBound {
    key: String,
    inclusive: bool,
}

#[derive(Clone, Debug, PartialEq)]
struct SortPushdownPlan {
    index: IndexSpec,
    key_prefix: String,
    descending: bool,
}

#[derive(Clone, Debug, PartialEq)]
enum ResolvedHint {
    Id,
    Index(IndexSpec),
}

#[derive(Clone, Debug, PartialEq)]
enum ParsedHint {
    Name(String),
    Key(Document),
}

fn parse_optional_hint(document: &Document) -> std::result::Result<Option<ParsedHint>, String> {
    match document.get("hint") {
        None => Ok(None),
        Some(Bson::String(name)) if !name.is_empty() => Ok(Some(ParsedHint::Name(name.clone()))),
        Some(Bson::String(_)) => Err("hint index name must not be empty".to_string()),
        Some(Bson::Document(key)) if !key.is_empty() => Ok(Some(ParsedHint::Key(key.clone()))),
        Some(Bson::Document(_)) => Err("hint key document must not be empty".to_string()),
        Some(_) => Err("hint must be an index name or key document".to_string()),
    }
}

fn resolve_hint(
    indexes: Vec<IndexSpec>,
    hint: ParsedHint,
) -> std::result::Result<ResolvedHint, String> {
    match hint {
        ParsedHint::Name(name) if name == "_id_" => Ok(ResolvedHint::Id),
        ParsedHint::Name(name) => indexes
            .into_iter()
            .find(|index| index.name == name)
            .map(ResolvedHint::Index)
            .ok_or_else(|| format!("hint index {name} was not found")),
        ParsedHint::Key(key) if key == doc! { "_id": 1_i32 } || key == doc! { "_id": 1_i64 } => {
            Ok(ResolvedHint::Id)
        }
        ParsedHint::Key(key) => {
            validate_index_key(&key).map_err(|response| {
                response
                    .get_str("errmsg")
                    .unwrap_or("hint key document is not supported")
                    .to_string()
            })?;
            let matches = indexes
                .into_iter()
                .filter(|index| index.key == key)
                .collect::<Vec<_>>();
            match matches.as_slice() {
                [index] => Ok(ResolvedHint::Index(index.clone())),
                [] => Err(format!(
                    "hint index with key {} was not found",
                    generated_index_name(&key)
                )),
                _ => Err(format!(
                    "hint key {} matches multiple indexes",
                    generated_index_name(&key)
                )),
            }
        }
    }
}

fn validate_hint_collation(
    hint: &ResolvedHint,
    collation: &Collation,
    filter: &Document,
) -> std::result::Result<(), String> {
    match hint {
        ResolvedHint::Id => {
            if let Some(value) = exact_equality_filter_value(filter, "_id")
                && !id_equality_safe_for_collation(value, collation)
            {
                return Err(
                    "hint _id_ is incompatible with non-simple string collation".to_string()
                );
            }
            Ok(())
        }
        ResolvedHint::Index(index) if index.collation == *collation => Ok(()),
        ResolvedHint::Index(index) => Err(format!(
            "hint index {} collation is incompatible with this query",
            index.name
        )),
    }
}

fn planner_diagnostic(
    scan_strategy: PlannerScanStrategy,
    index: Option<&IndexSpec>,
    matcher_validation_required: bool,
    fallback_reason: Option<String>,
) -> PlannerDiagnostic {
    PlannerDiagnostic {
        scan_strategy,
        index_name: index.map(|index| index.name.clone()),
        index_key: index.map(|index| index.key.clone()),
        matcher_validation_required,
        fallback_reason,
    }
}

fn collection_scan_plan(reason: impl Into<String>) -> PlannerV2Plan {
    PlannerV2Plan::CollectionScan {
        diagnostic: planner_diagnostic(
            PlannerScanStrategy::CollectionScan,
            None,
            true,
            Some(reason.into()),
        ),
    }
}

#[cfg(test)]
fn planner_v2_plan_for_filter(indexes: Vec<IndexSpec>, filter: &Document) -> PlannerV2Plan {
    if filter.is_empty() {
        return collection_scan_plan("empty filter");
    }
    if let Some(value) = exact_equality_filter_value(filter, "_id") {
        return PlannerV2Plan::IdEquality {
            id_key: id_key_from_bson(value),
            diagnostic: planner_diagnostic(PlannerScanStrategy::IdExact, None, true, None),
        };
    }
    if filter.keys().any(|key| key.starts_with('$')) {
        return collection_scan_plan("top-level logical filters are not index-planned");
    }

    let mut fallback_reason = "no compatible index".to_string();
    for index in planner_indexes(indexes) {
        if !filter_implies_index_membership(&index, filter) {
            fallback_reason = format!(
                "filter does not safely imply index {} membership",
                index.name
            );
            continue;
        }
        if let Some(key_value) = planner_key_for_filter(&index, filter) {
            return PlannerV2Plan::IndexExactEquality {
                index_name: index.name.clone(),
                index_key: index.key.clone(),
                key_value,
                diagnostic: planner_diagnostic(
                    PlannerScanStrategy::IndexExactEquality,
                    Some(&index),
                    true,
                    None,
                ),
            };
        }
        if let Some(range) = range_planner_key_for_filter(&index, filter) {
            return PlannerV2Plan::IndexRange {
                index_name: index.name.clone(),
                index_key: index.key.clone(),
                range,
                diagnostic: planner_diagnostic(
                    PlannerScanStrategy::IndexRange,
                    Some(&index),
                    true,
                    None,
                ),
            };
        }
        if let Some((prefix_len, key_value)) = prefix_planner_key_for_filter(&index, filter) {
            return PlannerV2Plan::IndexEqualityPrefix {
                index_name: index.name.clone(),
                index_key: index.key.clone(),
                prefix_len,
                key_value,
                diagnostic: planner_diagnostic(
                    PlannerScanStrategy::IndexEqualityPrefix,
                    Some(&index),
                    true,
                    None,
                ),
            };
        }
        fallback_reason = format!(
            "index {} does not match supported exact, prefix, or range shapes",
            index.name
        );
    }

    collection_scan_plan(fallback_reason)
}

fn planner_v2_plan_for_query(
    conn: &Connection,
    namespace: &str,
    filter: &Document,
    hint: Option<&ResolvedHint>,
    collation: &Collation,
) -> std::result::Result<PlannerV2Plan, String> {
    if let Some(hint) = hint {
        return hinted_planner_v2_plan(conn, namespace, filter, hint, collation);
    }
    if filter.is_empty() {
        return Ok(collection_scan_plan("empty filter"));
    }
    if let Some(value) = exact_equality_filter_value(filter, "_id") {
        if id_equality_safe_for_collation(value, collation) {
            return Ok(PlannerV2Plan::IdEquality {
                id_key: id_key_from_bson(value),
                diagnostic: planner_diagnostic(PlannerScanStrategy::IdExact, None, true, None),
            });
        }
    }
    if filter.keys().any(|key| key.starts_with('$')) {
        return Ok(collection_scan_plan(
            "top-level logical filters are not index-planned",
        ));
    }

    let mut fallback_reason = "no compatible index".to_string();
    for index in
        planner_indexes(indexes_for_namespace(conn, namespace).map_err(|err| err.to_string())?)
    {
        if index.collation != *collation {
            fallback_reason = format!("index {} has incompatible collation", index.name);
            continue;
        }
        if !filter_implies_index_membership(&index, filter) {
            fallback_reason = format!(
                "filter does not safely imply index {} membership",
                index.name
            );
            continue;
        }
        if !index_entries_safe_for_planner(conn, namespace, &index.name)
            .map_err(|err| err.to_string())?
        {
            fallback_reason = format!("index {} has unsupported multikey omissions", index.name);
            continue;
        }
        if let Some(plan) = planner_v2_plan_for_index_shape(&index, filter) {
            return Ok(plan);
        }
        fallback_reason = format!(
            "index {} does not match supported exact, prefix, or range shapes",
            index.name
        );
    }
    Ok(collection_scan_plan(fallback_reason))
}

fn planner_v2_plan_for_find(
    conn: &Connection,
    namespace: &str,
    filter: &Document,
    sort: Option<&[(String, i32)]>,
    hint: Option<&ResolvedHint>,
    collation: &Collation,
) -> std::result::Result<PlannerV2Plan, String> {
    if let Some(sort) = sort
        && let Some(sort_plan) = sort_pushdown_plan(conn, namespace, filter, sort, hint, collation)?
    {
        return Ok(PlannerV2Plan::IndexSort {
            index_name: sort_plan.index.name.clone(),
            index_key: sort_plan.index.key.clone(),
            diagnostic: planner_diagnostic(
                PlannerScanStrategy::IndexSort,
                Some(&sort_plan.index),
                true,
                None,
            ),
        });
    }
    planner_v2_plan_for_query(conn, namespace, filter, hint, collation)
}

fn planner_v2_plan_for_count(
    conn: &Connection,
    namespace: &str,
    filter: &Document,
    hint: Option<&ResolvedHint>,
    collation: &Collation,
) -> std::result::Result<PlannerV2Plan, String> {
    if hint.is_some() {
        return planner_v2_plan_for_query(conn, namespace, filter, hint, collation);
    }
    if filter.is_empty() {
        return Ok(collection_scan_plan("empty filter"));
    }
    if let Some(value) = exact_equality_filter_value(filter, "_id") {
        if id_equality_safe_for_collation(value, collation) {
            return Ok(PlannerV2Plan::IdEquality {
                id_key: id_key_from_bson(value),
                diagnostic: planner_diagnostic(PlannerScanStrategy::IdExact, None, true, None),
            });
        }
    }
    for index in
        planner_indexes(indexes_for_namespace(conn, namespace).map_err(|err| err.to_string())?)
    {
        if index.collation != *collation {
            continue;
        }
        let Some(key_value) = planner_key_for_filter(&index, filter) else {
            continue;
        };
        if !filter_implies_index_membership(&index, filter)
            || !count_filter_covered_by_index(&index, filter)
        {
            continue;
        }
        if !index_entries_safe_for_planner(conn, namespace, &index.name)
            .map_err(|err| err.to_string())?
        {
            continue;
        }
        return Ok(PlannerV2Plan::IndexExactEquality {
            index_name: index.name.clone(),
            index_key: index.key.clone(),
            key_value,
            diagnostic: planner_diagnostic(
                PlannerScanStrategy::IndexExactEquality,
                Some(&index),
                true,
                None,
            ),
        });
    }
    for index in
        planner_indexes(indexes_for_namespace(conn, namespace).map_err(|err| err.to_string())?)
    {
        if index.collation != *collation || !collation.is_simple() {
            continue;
        }
        let Some(range) = range_planner_key_for_filter(&index, filter) else {
            continue;
        };
        if !filter_implies_index_membership(&index, filter)
            || !count_filter_covered_by_range_index(&index, filter, &range)
        {
            continue;
        }
        if !index_entries_safe_for_planner(conn, namespace, &index.name)
            .map_err(|err| err.to_string())?
        {
            continue;
        }
        return Ok(PlannerV2Plan::IndexRange {
            index_name: index.name.clone(),
            index_key: index.key.clone(),
            range,
            diagnostic: planner_diagnostic(
                PlannerScanStrategy::IndexRange,
                Some(&index),
                true,
                None,
            ),
        });
    }
    Ok(collection_scan_plan(
        "count filter is not fully covered by a supported index",
    ))
}

fn hinted_planner_v2_plan(
    conn: &Connection,
    namespace: &str,
    filter: &Document,
    hint: &ResolvedHint,
    collation: &Collation,
) -> std::result::Result<PlannerV2Plan, String> {
    match hint {
        ResolvedHint::Id => {
            let Some(value) = exact_equality_filter_value(filter, "_id") else {
                return Err("hint _id_ is incompatible with this filter".to_string());
            };
            if !id_equality_safe_for_collation(value, collation) {
                return Err(
                    "hint _id_ is incompatible with non-simple string collation".to_string()
                );
            }
            Ok(PlannerV2Plan::IdEquality {
                id_key: id_key_from_bson(value),
                diagnostic: planner_diagnostic(PlannerScanStrategy::IdExact, None, true, None),
            })
        }
        ResolvedHint::Index(index) => {
            if index.collation != *collation {
                return Err(format!(
                    "hint index {} collation is incompatible with this query",
                    index.name
                ));
            }
            if !filter_implies_index_membership(index, filter) {
                return Err(format!(
                    "hint index {} is unsafe for this filter membership",
                    index.name
                ));
            }
            if !index_entries_safe_for_planner(conn, namespace, &index.name)
                .map_err(|err| err.to_string())?
            {
                return Err(format!(
                    "hint index {} has unsupported multikey omissions",
                    index.name
                ));
            }
            planner_v2_plan_for_index_shape(index, filter).ok_or_else(|| {
                format!("hint index {} is incompatible with this filter", index.name)
            })
        }
    }
}

fn planner_v2_plan_for_index_shape(index: &IndexSpec, filter: &Document) -> Option<PlannerV2Plan> {
    if let Some(key_value) = planner_key_for_filter(index, filter) {
        return Some(PlannerV2Plan::IndexExactEquality {
            index_name: index.name.clone(),
            index_key: index.key.clone(),
            key_value,
            diagnostic: planner_diagnostic(
                PlannerScanStrategy::IndexExactEquality,
                Some(index),
                true,
                None,
            ),
        });
    }
    if index.collation.is_simple()
        && let Some(range) = range_planner_key_for_filter(index, filter)
    {
        return Some(PlannerV2Plan::IndexRange {
            index_name: index.name.clone(),
            index_key: index.key.clone(),
            range,
            diagnostic: planner_diagnostic(
                PlannerScanStrategy::IndexRange,
                Some(index),
                true,
                None,
            ),
        });
    }
    if let Some((prefix_len, key_value)) = prefix_planner_key_for_filter(index, filter) {
        return Some(PlannerV2Plan::IndexEqualityPrefix {
            index_name: index.name.clone(),
            index_key: index.key.clone(),
            prefix_len,
            key_value,
            diagnostic: planner_diagnostic(
                PlannerScanStrategy::IndexEqualityPrefix,
                Some(index),
                true,
                None,
            ),
        });
    }
    None
}

fn explain_response(
    command_name: &str,
    namespace: &str,
    filter: &Document,
    hint_provided: bool,
    plan: &PlannerV2Plan,
) -> Document {
    let diagnostic = plan_diagnostic(plan);
    doc! {
        "queryPlanner": {
            "namespace": namespace,
            "command": command_name,
            "parsedFilter": filter.clone(),
            "hintProvided": hint_provided,
            "matcherValidationRequired": diagnostic.matcher_validation_required,
            "winningPlan": winning_plan_document(plan),
        },
        "ok": 1.0,
    }
}

fn plan_diagnostic(plan: &PlannerV2Plan) -> &PlannerDiagnostic {
    match plan {
        PlannerV2Plan::CollectionScan { diagnostic }
        | PlannerV2Plan::IdEquality { diagnostic, .. }
        | PlannerV2Plan::IndexExactEquality { diagnostic, .. }
        | PlannerV2Plan::IndexEqualityPrefix { diagnostic, .. }
        | PlannerV2Plan::IndexRange { diagnostic, .. }
        | PlannerV2Plan::IndexSort { diagnostic, .. } => diagnostic,
    }
}

fn winning_plan_document(plan: &PlannerV2Plan) -> Document {
    let diagnostic = plan_diagnostic(plan);
    let mut document = doc! {
        "stage": planner_stage(&diagnostic.scan_strategy),
        "scanStrategy": planner_scan_strategy_name(&diagnostic.scan_strategy),
    };
    if let Some(index_name) = &diagnostic.index_name {
        document.insert("indexName", index_name.clone());
    }
    if let Some(index_key) = &diagnostic.index_key {
        document.insert("keyPattern", Bson::Document(index_key.clone()));
    }
    if let Some(fallback_reason) = &diagnostic.fallback_reason {
        document.insert("fallbackReason", fallback_reason.clone());
    }
    match plan {
        PlannerV2Plan::IdEquality { id_key, .. } => {
            document.insert("idKey", id_key.clone());
        }
        PlannerV2Plan::IndexExactEquality { key_value, .. } => {
            document.insert("plannerKey", key_value.clone());
        }
        PlannerV2Plan::IndexEqualityPrefix {
            prefix_len,
            key_value,
            ..
        } => {
            document.insert("plannerKey", key_value.clone());
            document.insert("prefixLen", *prefix_len as i32);
        }
        PlannerV2Plan::IndexRange { range, .. } => {
            document.insert("rangeField", range.field.clone());
            document.insert("equalityPrefixLen", range.equality_prefix_len as i32);
            document.insert("keyPrefix", range.key_prefix.clone());
        }
        PlannerV2Plan::CollectionScan { .. } | PlannerV2Plan::IndexSort { .. } => {}
    }
    document
}

fn planner_stage(strategy: &PlannerScanStrategy) -> &'static str {
    match strategy {
        PlannerScanStrategy::CollectionScan => "COLLSCAN",
        PlannerScanStrategy::IdExact => "IDHACK",
        PlannerScanStrategy::IndexExactEquality
        | PlannerScanStrategy::IndexEqualityPrefix
        | PlannerScanStrategy::IndexRange
        | PlannerScanStrategy::IndexSort => "IXSCAN",
    }
}

fn planner_scan_strategy_name(strategy: &PlannerScanStrategy) -> &'static str {
    match strategy {
        PlannerScanStrategy::CollectionScan => "collectionScan",
        PlannerScanStrategy::IdExact => "idExact",
        PlannerScanStrategy::IndexExactEquality => "indexExactEquality",
        PlannerScanStrategy::IndexEqualityPrefix => "indexEqualityPrefix",
        PlannerScanStrategy::IndexRange => "indexRange",
        PlannerScanStrategy::IndexSort => "indexSort",
    }
}

fn plan_count(conn: &Connection, namespace: &str, filter: &Document) -> Result<CountPlan> {
    plan_count_with_collation(conn, namespace, filter, &Collation::Simple)
}

fn plan_count_with_collation(
    conn: &Connection,
    namespace: &str,
    filter: &Document,
    collation: &Collation,
) -> Result<CountPlan> {
    if filter.is_empty() {
        return Ok(CountPlan::Empty);
    }
    if let Some(value) = exact_equality_filter_value(filter, "_id") {
        if id_equality_safe_for_collation(value, collation) {
            return Ok(CountPlan::IdEquality(id_key_from_bson(value)));
        }
    }
    for index in planner_indexes(indexes_for_namespace(conn, namespace)?) {
        if index.collation != *collation {
            continue;
        }
        let Some(key_value) = planner_key_for_filter(&index, filter) else {
            continue;
        };
        if !filter_implies_index_membership(&index, filter)
            || !count_filter_covered_by_index(&index, filter)
        {
            continue;
        }
        if !index_entries_safe_for_planner(conn, namespace, &index.name)? {
            continue;
        }
        return Ok(CountPlan::IndexedEquality {
            index_name: index.name,
            key_value,
        });
    }
    for index in planner_indexes(indexes_for_namespace(conn, namespace)?) {
        if index.collation != *collation || !collation.is_simple() {
            continue;
        }
        let Some(range) = range_planner_key_for_filter(&index, filter) else {
            continue;
        };
        if !filter_implies_index_membership(&index, filter)
            || !count_filter_covered_by_range_index(&index, filter, &range)
        {
            continue;
        }
        if !index_entries_safe_for_planner(conn, namespace, &index.name)? {
            continue;
        }
        return Ok(CountPlan::IndexedRange {
            index_name: index.name,
            range,
        });
    }
    let Some((field, value)) = exact_single_equality_filter(filter) else {
        return Ok(CountPlan::Fallback);
    };
    if !is_count_pushdown_scalar(value) {
        return Ok(CountPlan::Fallback);
    }
    if numeric_value(value).is_some() {
        return Ok(CountPlan::Fallback);
    }
    let Some(index) = indexes_for_namespace(conn, namespace)?
        .into_iter()
        .find(|index| {
            index.collation == *collation
                && single_field_index_name(index).is_some_and(|indexed| indexed == field)
        })
    else {
        return Ok(CountPlan::Fallback);
    };
    if !index_entries_safe_for_planner(conn, namespace, &index.name)? {
        return Ok(CountPlan::Fallback);
    }
    if !filter_implies_index_membership(&index, filter)
        || !count_filter_covered_by_index(&index, filter)
    {
        return Ok(CountPlan::Fallback);
    }
    Ok(CountPlan::IndexedEquality {
        index_name: index.name,
        key_value: index.collation.id_key_from_bson(value),
    })
}

fn is_count_pushdown_scalar(value: &Bson) -> bool {
    matches!(
        value,
        Bson::Double(_)
            | Bson::String(_)
            | Bson::Boolean(_)
            | Bson::ObjectId(_)
            | Bson::Int32(_)
            | Bson::Int64(_)
    )
}

fn exact_single_equality_filter(filter: &Document) -> Option<(&str, &Bson)> {
    if filter.len() != 1 {
        return None;
    }
    let (field, value) = filter.iter().next()?;
    if field.starts_with('$') {
        return None;
    }
    exact_equality_value(value).map(|value| (field.as_str(), value))
}

fn exact_equality_filter_value<'a>(filter: &'a Document, field: &str) -> Option<&'a Bson> {
    if filter.len() != 1 {
        return None;
    }
    filter.get(field).and_then(exact_equality_value)
}

fn exact_equality_value(value: &Bson) -> Option<&Bson> {
    if !is_operator_document(value) {
        return Some(value);
    }
    let Bson::Document(operators) = value else {
        return None;
    };
    if operators.len() == 1 {
        operators.get("$eq")
    } else {
        None
    }
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

#[derive(Clone, Debug, PartialEq)]
struct IndexSpec {
    name: String,
    key: Document,
    unique: bool,
    sparse: bool,
    partial_filter: Option<Document>,
    expire_after_seconds: Option<i64>,
    collation: Collation,
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
    if let Some(errmsg) = reject_unsupported_command_keys(
        command,
        &["createIndexes", "indexes", "writeConcern", "$db", "lsid"],
    ) {
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
            if existing.key == spec.key
                && existing.unique == spec.unique
                && existing.sparse == spec.sparse
                && existing.partial_filter == spec.partial_filter
                && existing.expire_after_seconds == spec.expire_after_seconds
                && existing.collation == spec.collation
            {
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
        if spec.sparse {
            document.insert("sparse", true);
        }
        if let Some(partial_filter) = spec.partial_filter {
            document.insert("partialFilterExpression", partial_filter);
        }
        if let Some(expire_after_seconds) = spec.expire_after_seconds {
            document.insert("expireAfterSeconds", expire_after_seconds);
        }
        if !spec.collation.is_simple() {
            document.insert("collation", spec.collation.to_document());
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
    if let Some(errmsg) = reject_unsupported_command_keys(
        command,
        &["dropIndexes", "index", "writeConcern", "$db", "lsid"],
    ) {
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
        tx.execute(
            "DELETE FROM index_multikey_omissions WHERE namespace = ?1",
            params![ns],
        )?;
        tx.execute("DELETE FROM indexes WHERE namespace = ?1", params![ns])?
    } else {
        tx.execute(
            "DELETE FROM index_entries WHERE namespace = ?1 AND index_name = ?2",
            params![ns, index],
        )?;
        tx.execute(
            "DELETE FROM index_multikey_omissions WHERE namespace = ?1 AND index_name = ?2",
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
    if let Some(errmsg) = reject_unsupported_command_keys(
        index,
        &[
            "key",
            "name",
            "unique",
            "v",
            "sparse",
            "partialFilterExpression",
            "expireAfterSeconds",
            "collation",
        ],
    ) {
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
    let sparse = match index.get("sparse") {
        None => false,
        Some(Bson::Boolean(value)) => *value,
        Some(_) => return Err(command_error(9, "sparse must be a boolean")),
    };
    let partial_filter = match index.get("partialFilterExpression") {
        None => None,
        Some(Bson::Document(filter)) => {
            validate_partial_filter(filter)?;
            Some(filter.clone())
        }
        Some(_) => {
            return Err(command_error(
                9,
                "partialFilterExpression must be a document",
            ));
        }
    };
    let expire_after_seconds = match index.get("expireAfterSeconds") {
        None => None,
        Some(value) => Some(parse_expire_after_seconds(value)?),
    };
    let collation = Collation::parse_optional(index, "collation")
        .map_err(|errmsg| command_error(72, &errmsg))?;
    if !collation.is_simple() && partial_filter.is_some() {
        return Err(command_error(
            72,
            "collation indexes with partialFilterExpression are not supported",
        ));
    }
    if !collation.is_simple() && expire_after_seconds.is_some() {
        return Err(command_error(
            72,
            "TTL indexes with non-simple collation are not supported",
        ));
    }
    if expire_after_seconds.is_some() {
        validate_ttl_index_spec(&key, &name, sparse, partial_filter.as_ref())?;
    }
    match index.get("v") {
        None | Some(Bson::Int32(2) | Bson::Int64(2)) => {}
        Some(_) => return Err(command_error(72, "only index version 2 is supported")),
    }

    Ok(IndexSpec {
        name,
        key,
        unique,
        sparse,
        partial_filter,
        expire_after_seconds,
        collation,
    })
}

fn parse_expire_after_seconds(value: &Bson) -> std::result::Result<i64, Document> {
    match value {
        Bson::Int32(value) if *value >= 0 => Ok(*value as i64),
        Bson::Int32(_) => Err(command_error(
            72,
            "expireAfterSeconds must be a non-negative integer",
        )),
        Bson::Int64(value) if *value >= 0 => Ok(*value),
        Bson::Int64(_) => Err(command_error(
            72,
            "expireAfterSeconds must be a non-negative integer",
        )),
        _ => Err(command_error(
            72,
            "expireAfterSeconds must be a non-negative integer",
        )),
    }
}

fn validate_ttl_index_spec(
    key: &Document,
    name: &str,
    sparse: bool,
    partial_filter: Option<&Document>,
) -> std::result::Result<(), Document> {
    if key.len() != 1 {
        return Err(command_error(72, "TTL indexes require a single-field key"));
    }
    let (field, direction) = key.iter().next().expect("key length checked above");
    if field == "_id" || name == "_id_" {
        return Err(command_error(72, "TTL indexes on _id are not supported"));
    }
    match direction {
        Bson::Int32(1) | Bson::Int64(1) | Bson::Int32(-1) | Bson::Int64(-1) => {}
        _ => {
            return Err(command_error(
                72,
                "TTL indexes require an ascending or descending key",
            ));
        }
    }
    if sparse {
        return Err(command_error(
            72,
            "TTL indexes with sparse are not supported",
        ));
    }
    if partial_filter.is_some() {
        return Err(command_error(
            72,
            "TTL indexes with partialFilterExpression are not supported",
        ));
    }
    Ok(())
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

fn validate_partial_filter(filter: &Document) -> std::result::Result<(), Document> {
    if filter.is_empty() {
        return Err(command_error(
            72,
            "partialFilterExpression must not be empty",
        ));
    }
    for (field, condition) in filter {
        if field == "$and" {
            let Bson::Array(clauses) = condition else {
                return Err(command_error(
                    72,
                    "partialFilterExpression $and requires an array",
                ));
            };
            if clauses.is_empty() {
                return Err(command_error(
                    72,
                    "partialFilterExpression $and requires a non-empty array",
                ));
            }
            for clause in clauses {
                let Bson::Document(clause) = clause else {
                    return Err(command_error(
                        72,
                        "partialFilterExpression $and entries must be documents",
                    ));
                };
                validate_partial_filter(clause)?;
            }
            continue;
        }
        if field.starts_with('$') {
            return Err(command_error(
                72,
                "partialFilterExpression only supports field predicates and $and",
            ));
        }
        validate_partial_field_predicate(condition)?;
    }
    Ok(())
}

fn validate_partial_field_predicate(condition: &Bson) -> std::result::Result<(), Document> {
    if !is_operator_document(condition) {
        if numeric_value(condition).is_some()
            || matches!(condition, Bson::Array(_) | Bson::Document(_))
        {
            return Err(command_error(
                72,
                "partialFilterExpression equality supports only non-numeric scalar values",
            ));
        }
        return Ok(());
    }
    let Bson::Document(operators) = condition else {
        unreachable!("operator document checked above");
    };
    if operators.len() != 1 {
        return Err(command_error(
            72,
            "partialFilterExpression field predicates support one operator",
        ));
    }
    match operators.iter().next() {
        Some((operator, value)) if operator == "$eq" => {
            if numeric_value(value).is_some() || matches!(value, Bson::Array(_) | Bson::Document(_))
            {
                return Err(command_error(
                    72,
                    "partialFilterExpression $eq supports only non-numeric scalar values",
                ));
            }
            Ok(())
        }
        Some((operator, Bson::Boolean(true))) if operator == "$exists" => Ok(()),
        Some((operator, _)) if operator == "$exists" => Err(command_error(
            72,
            "partialFilterExpression only supports $exists: true",
        )),
        Some((operator, _)) => Err(command_error(
            72,
            &format!("partialFilterExpression operator {operator} is not supported"),
        )),
        None => unreachable!("operator length checked above"),
    }
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
        "INSERT INTO indexes(namespace, name, key_bson, unique_index, sparse_index, partial_filter_bson, expire_after_seconds, collation_bson) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
        params![
            namespace,
            spec.name,
            encode_document(&spec.key)?,
            if spec.unique { 1_i32 } else { 0_i32 },
            if spec.sparse { 1_i32 } else { 0_i32 },
            spec.partial_filter
                .as_ref()
                .map(encode_document)
                .transpose()?,
            spec.expire_after_seconds,
            (!spec.collation.is_simple())
                .then(|| encode_document(&spec.collation.to_document()))
                .transpose()?,
        ],
    )?;
    Ok(())
}

fn update_index_expire_after_seconds_tx(
    tx: &rusqlite::Transaction<'_>,
    namespace: &str,
    name: &str,
    expire_after_seconds: i64,
) -> std::result::Result<(), rusqlite::Error> {
    tx.execute(
        "UPDATE indexes SET expire_after_seconds = ?1 WHERE namespace = ?2 AND name = ?3",
        params![expire_after_seconds, namespace, name],
    )?;
    Ok(())
}

fn index_by_name_tx(
    tx: &rusqlite::Transaction<'_>,
    namespace: &str,
    name: &str,
) -> Result<Option<IndexSpec>> {
    tx.query_row(
        "SELECT name, key_bson, unique_index, sparse_index, partial_filter_bson, expire_after_seconds, collation_bson FROM indexes WHERE namespace = ?1 AND name = ?2",
        params![namespace, name],
        |row| {
            let name = row.get::<_, String>(0)?;
            let key = decode_document_sql(row.get::<_, Vec<u8>>(1)?)?;
            let unique = row.get::<_, i32>(2)? != 0;
            let sparse = row.get::<_, i32>(3)? != 0;
            let partial_filter = row
                .get::<_, Option<Vec<u8>>>(4)?
                .map(decode_document_sql)
                .transpose()?;
            let expire_after_seconds = row.get::<_, Option<i64>>(5)?;
            let collation = decode_collation_sql(row.get::<_, Option<Vec<u8>>>(6)?)?;
            Ok(IndexSpec {
                name,
                key,
                unique,
                sparse,
                partial_filter,
                expire_after_seconds,
                collation,
            })
        },
    )
    .optional()
    .map_err(Into::into)
}

fn indexes_for_namespace(conn: &Connection, namespace: &str) -> Result<Vec<IndexSpec>> {
    let mut stmt = conn.prepare(
        "SELECT name, key_bson, unique_index, sparse_index, partial_filter_bson, expire_after_seconds, collation_bson FROM indexes WHERE namespace = ?1 ORDER BY name",
    )?;
    stmt.query_map(params![namespace], |row| {
        let name = row.get::<_, String>(0)?;
        let key = decode_document_sql(row.get::<_, Vec<u8>>(1)?)?;
        let unique = row.get::<_, i32>(2)? != 0;
        let sparse = row.get::<_, i32>(3)? != 0;
        let partial_filter = row
            .get::<_, Option<Vec<u8>>>(4)?
            .map(decode_document_sql)
            .transpose()?;
        let expire_after_seconds = row.get::<_, Option<i64>>(5)?;
        let collation = decode_collation_sql(row.get::<_, Option<Vec<u8>>>(6)?)?;
        Ok(IndexSpec {
            name,
            key,
            unique,
            sparse,
            partial_filter,
            expire_after_seconds,
            collation,
        })
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
        "SELECT name, key_bson, unique_index, sparse_index, partial_filter_bson, expire_after_seconds, collation_bson FROM indexes WHERE namespace = ?1 AND unique_index = 1 ORDER BY name",
    )?;
    stmt.query_map(params![namespace], |row| {
        let name = row.get::<_, String>(0)?;
        let key = decode_document_sql(row.get::<_, Vec<u8>>(1)?)?;
        let unique = row.get::<_, i32>(2)? != 0;
        let sparse = row.get::<_, i32>(3)? != 0;
        let partial_filter = row
            .get::<_, Option<Vec<u8>>>(4)?
            .map(decode_document_sql)
            .transpose()?;
        let expire_after_seconds = row.get::<_, Option<i64>>(5)?;
        let collation = decode_collation_sql(row.get::<_, Option<Vec<u8>>>(6)?)?;
        Ok(IndexSpec {
            name,
            key,
            unique,
            sparse,
            partial_filter,
            expire_after_seconds,
            collation,
        })
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
        if !document_belongs_to_index(spec, &stored.document)? {
            continue;
        }
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
        if !document_belongs_to_index(&index, document)? {
            continue;
        }
        if unique_conflict_check_with_index_entries_tx(
            tx,
            namespace,
            &index,
            document,
            excluding_id_key,
        )? {
            continue;
        }
        let wanted_key = unique_key_for_document(&index, document)?;
        for stored in
            stored_documents_for_namespace_tx(tx, namespace).map_err(|err| err.to_string())?
        {
            if excluding_id_key.is_some_and(|id_key| id_key == stored.id_key) {
                continue;
            }
            if !document_belongs_to_index(&index, &stored.document)? {
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

fn ensure_unique_constraints_for_replacements_tx(
    tx: &rusqlite::Transaction<'_>,
    namespace: &str,
    replacements: &[(String, Document)],
) -> std::result::Result<(), String> {
    if replacements.len() < 2 {
        return Ok(());
    }
    let indexes = unique_indexes_for_namespace_tx(tx, namespace).map_err(|err| err.to_string())?;
    if indexes.is_empty() {
        return Ok(());
    }

    for index in indexes {
        let mut seen = HashMap::new();
        for (id_key, document) in replacements {
            if !document_belongs_to_index(&index, document)? {
                continue;
            }
            let key = unique_key_for_document(&index, document)?;
            if let Some(existing_id) = seen.insert(key.clone(), id_key.clone()) {
                return Err(format!(
                    "duplicate key error collection: {namespace} index: {} dup key: {key} existing _id: {existing_id}",
                    index.name
                ));
            }
        }
    }
    Ok(())
}

fn unique_conflict_check_with_index_entries_tx(
    tx: &rusqlite::Transaction<'_>,
    namespace: &str,
    index: &IndexSpec,
    document: &Document,
    excluding_id_key: Option<&str>,
) -> std::result::Result<bool, String> {
    if !document_belongs_to_index(index, document)? {
        return Ok(true);
    }
    let key_value = if let Some(field) = single_field_index_name(index) {
        let values = values_at_path(document, field);
        let value = match values.as_slice() {
            [value] if is_unique_pushdown_scalar(value) => *value,
            _ => return Ok(false),
        };
        index.collation.id_key_from_bson(value)
    } else {
        let Some(key_value) =
            compound_key_from_document(index, document, is_unique_pushdown_scalar)
        else {
            return Ok(false);
        };
        key_value
    };
    let conflict = tx
        .query_row(
            r#"
            SELECT id_key
              FROM index_entries
             WHERE namespace = ?1
               AND index_name = ?2
               AND key_value = ?3
               AND (?4 IS NULL OR id_key != ?4)
             ORDER BY id_key
             LIMIT 1
            "#,
            params![namespace, index.name, key_value, excluding_id_key],
            |row| row.get::<_, String>(0),
        )
        .optional()
        .map_err(|err| err.to_string())?;
    if conflict.is_some() {
        let wanted_key = unique_key_for_document(index, document)?;
        return Err(format!(
            "duplicate key error collection: {namespace} index: {} dup key: {wanted_key}",
            index.name
        ));
    }
    Ok(true)
}

fn document_belongs_to_index(
    spec: &IndexSpec,
    document: &Document,
) -> std::result::Result<bool, String> {
    if spec.sparse
        && spec
            .key
            .keys()
            .any(|field| values_at_path(document, field).is_empty())
    {
        return Ok(false);
    }
    if let Some(partial_filter) = &spec.partial_filter {
        return matches_filter(document, partial_filter)
            .map_err(|err| format!("partialFilterExpression failed: {}", err.errmsg));
    }
    Ok(true)
}

fn is_unique_pushdown_scalar(value: &Bson) -> bool {
    matches!(
        value,
        Bson::String(_) | Bson::Boolean(_) | Bson::ObjectId(_) | Bson::DateTime(_)
    )
}

fn unique_key_for_document(
    index: &IndexSpec,
    document: &Document,
) -> std::result::Result<String, String> {
    let mut parts = Vec::new();
    for field in index.key.keys() {
        if indexed_path_contains_array(document, field) {
            let direct_value = get_document_path(document, field);
            if matches!(direct_value, Some(Bson::Array(_))) {
                return Err(format!(
                    "unique index {} does not support array value at {field}",
                    index.name
                ));
            }
            return Err(format!(
                "unique index {} does not support multikey path {field}",
                index.name
            ));
        }
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
        parts.push(format!(
            "{field}:{}",
            unique_key_value_with_collation(&value, &index.collation)
        ));
    }
    Ok(parts.join("|"))
}

fn unique_key_value_with_collation(value: &Bson, collation: &Collation) -> String {
    if let Some(number) = numeric_value(value) {
        let normalized = if number == 0.0 { 0.0 } else { number };
        return format!("num:{:016x}", normalized.to_bits());
    }
    collation.id_key_from_bson(value)
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
    tx.execute(
        "DELETE FROM index_multikey_omissions WHERE namespace = ?1 AND index_name = ?2",
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
    tx.execute(
        "DELETE FROM index_multikey_omissions WHERE namespace = ?1 AND id_key = ?2",
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
    tx.execute(
        "DELETE FROM index_multikey_omissions WHERE namespace = ?1 AND id_key = ?2",
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
    if !document_belongs_to_index(spec, document).map_err(sql_string_error_from_string)? {
        return Ok(());
    }
    if index_has_multikey_omission(spec, document) {
        tx.execute(
            "INSERT OR IGNORE INTO index_multikey_omissions(namespace, index_name, id_key) VALUES (?1, ?2, ?3)",
            params![namespace, spec.name, id_key],
        )?;
    }
    for key_value in planner_keys_for_document(spec, document) {
        tx.execute(
            "INSERT OR IGNORE INTO index_entries(namespace, index_name, key_value, id_key) VALUES (?1, ?2, ?3, ?4)",
            params![namespace, spec.name, key_value, id_key],
        )?;
    }
    Ok(())
}

fn index_has_multikey_omission(spec: &IndexSpec, document: &Document) -> bool {
    if is_compound_index(spec) {
        return spec
            .key
            .keys()
            .any(|field| indexed_path_contains_array(document, field));
    }
    let Some(field) = single_field_index_name(spec) else {
        return false;
    };
    indexed_path_contains_array(document, field)
        && supported_single_field_multikey_values(document, field).is_none()
}

fn indexed_path_contains_array(document: &Document, path: &str) -> bool {
    let mut parts = path.split('.');
    let Some(first) = parts.next() else {
        return false;
    };
    let rest = parts.collect::<Vec<_>>();
    document
        .get(first)
        .is_some_and(|value| bson_path_contains_array(value, &rest))
}

fn bson_path_contains_array(value: &Bson, parts: &[&str]) -> bool {
    match value {
        Bson::Array(_) => true,
        Bson::Document(document) if !parts.is_empty() => document
            .get(parts[0])
            .is_some_and(|next| bson_path_contains_array(next, &parts[1..])),
        _ => false,
    }
}

fn index_entries_safe_for_planner(
    conn: &Connection,
    namespace: &str,
    index_name: &str,
) -> Result<bool> {
    Ok(conn.query_row(
        "SELECT NOT EXISTS(SELECT 1 FROM index_multikey_omissions WHERE namespace = ?1 AND index_name = ?2)",
        params![namespace, index_name],
        |row| row.get::<_, bool>(0),
    )?)
}

fn index_entries_safe_for_planner_tx(
    tx: &rusqlite::Transaction<'_>,
    namespace: &str,
    index_name: &str,
) -> Result<bool> {
    Ok(tx.query_row(
        "SELECT NOT EXISTS(SELECT 1 FROM index_multikey_omissions WHERE namespace = ?1 AND index_name = ?2)",
        params![namespace, index_name],
        |row| row.get::<_, bool>(0),
    )?)
}

#[cfg(test)]
fn planner_key_for_document(spec: &IndexSpec, document: &Document) -> Option<String> {
    if single_field_index_name(spec).is_some() {
        return planner_keys_for_document(spec, document).into_iter().next();
    }
    compound_key_from_document(spec, document, is_compound_planner_scalar)
}

fn planner_keys_for_document(spec: &IndexSpec, document: &Document) -> Vec<String> {
    if let Some(field) = single_field_index_name(spec) {
        if indexed_path_contains_array(document, field) {
            return supported_single_field_multikey_values(document, field)
                .unwrap_or_default()
                .into_iter()
                .map(|value| spec.collation.id_key_from_bson(&value))
                .collect();
        }
        return get_document_path(document, field)
            .map(|value| {
                let mut keys = vec![spec.collation.id_key_from_bson(value)];
                if let Some(range_key) =
                    range_planner_key_value_for_collation(&[], value, &spec.collation)
                {
                    keys.push(range_key);
                }
                keys
            })
            .unwrap_or_default();
    }
    compound_planner_keys_for_document(spec, document)
}

fn compound_planner_keys_for_document(spec: &IndexSpec, document: &Document) -> Vec<String> {
    let mut keys = Vec::new();
    if let Some(full_key) = compound_key_from_document(spec, document, is_compound_planner_scalar) {
        keys.push(full_key);
    }
    let mut prefix_parts = Vec::new();
    let key_len = spec.key.len();
    for (position, field) in spec.key.keys().enumerate() {
        let Some(value) = get_document_path(document, field) else {
            break;
        };
        if let Some(range_key) =
            range_planner_key_value_for_collation(&prefix_parts, value, &spec.collation)
        {
            keys.push(range_key);
        }
        if !is_compound_planner_scalar(value) {
            break;
        }
        if position + 1 < key_len {
            prefix_parts.push(spec.collation.id_key_from_bson(value));
            keys.push(encode_compound_prefix_planner_key(&prefix_parts));
        }
    }
    keys
}

fn supported_single_field_multikey_values(document: &Document, field: &str) -> Option<Vec<Bson>> {
    let mut seen = HashSet::new();
    let mut values = Vec::new();
    for value in values_at_path(document, field) {
        let candidates: Vec<&Bson> = match value {
            Bson::Array(items) => items.iter().collect(),
            value => vec![value],
        };
        for candidate in candidates {
            if !is_multikey_planner_scalar(candidate) {
                return None;
            }
            let key = id_key_from_bson(candidate);
            if seen.insert(key) {
                values.push(candidate.clone());
            }
        }
    }
    Some(values)
}

fn single_field_index_name(spec: &IndexSpec) -> Option<&str> {
    if spec.key.len() == 1 {
        spec.key.keys().next().map(String::as_str)
    } else {
        None
    }
}

fn is_compound_index(spec: &IndexSpec) -> bool {
    spec.key.len() > 1
}

fn planner_indexes(mut indexes: Vec<IndexSpec>) -> Vec<IndexSpec> {
    indexes.sort_by(|left, right| {
        right
            .key
            .len()
            .cmp(&left.key.len())
            .then_with(|| left.name.cmp(&right.name))
    });
    indexes
}

fn compound_key_from_document(
    spec: &IndexSpec,
    document: &Document,
    is_safe_value: fn(&Bson) -> bool,
) -> Option<String> {
    if !is_compound_index(spec) {
        return None;
    }
    let mut parts = Vec::with_capacity(spec.key.len());
    for field in spec.key.keys() {
        let value = get_document_path(document, field)?;
        if !is_safe_value(value) {
            return None;
        }
        parts.push(spec.collation.id_key_from_bson(value));
    }
    Some(encode_compound_planner_key(&parts))
}

fn compound_planner_key_for_filter(spec: &IndexSpec, filter: &Document) -> Option<String> {
    if !is_compound_index(spec) || filter.keys().any(|key| key.starts_with('$')) {
        return None;
    }
    let mut parts = Vec::with_capacity(spec.key.len());
    for field in spec.key.keys() {
        let value = exact_equality_filter_part(filter, field)?;
        if !is_compound_planner_scalar(value) {
            return None;
        }
        parts.push(spec.collation.id_key_from_bson(value));
    }
    Some(encode_compound_planner_key(&parts))
}

fn prefix_planner_key_for_filter(spec: &IndexSpec, filter: &Document) -> Option<(usize, String)> {
    if !is_compound_index(spec) || filter.keys().any(|key| key.starts_with('$')) {
        return None;
    }
    let mut parts = Vec::new();
    for field in spec.key.keys() {
        let Some(value) = exact_equality_filter_part(filter, field) else {
            break;
        };
        if !is_compound_planner_scalar(value) {
            return None;
        }
        parts.push(spec.collation.id_key_from_bson(value));
    }
    if parts.is_empty() || parts.len() >= spec.key.len() {
        return None;
    }
    Some((parts.len(), encode_compound_prefix_planner_key(&parts)))
}

fn range_planner_key_for_filter(spec: &IndexSpec, filter: &Document) -> Option<RangePlannerKey> {
    if filter.keys().any(|key| key.starts_with('$')) {
        return None;
    }

    let mut equality_parts = Vec::new();
    for field in spec.key.keys() {
        if let Some(value) = exact_equality_filter_part(filter, field) {
            if !is_compound_planner_scalar(value) {
                return None;
            }
            equality_parts.push(spec.collation.id_key_from_bson(value));
            continue;
        }

        let condition = filter.get(field)?;
        if !spec.collation.is_simple() && condition_has_string_range(condition) {
            return None;
        }
        let (lower, upper) = range_bounds_for_condition(condition)?;
        return Some(RangePlannerKey {
            field: field.to_string(),
            equality_prefix_len: equality_parts.len(),
            key_prefix: encode_range_planner_prefix(&equality_parts),
            lower,
            upper,
        });
    }
    None
}

fn condition_has_string_range(condition: &Bson) -> bool {
    let Bson::Document(operators) = condition else {
        return false;
    };
    operators.iter().any(|(operator, value)| {
        matches!(operator.as_str(), "$gt" | "$gte" | "$lt" | "$lte")
            && matches!(value, Bson::String(_))
    })
}

fn range_bounds_for_condition(
    condition: &Bson,
) -> Option<(Option<RangeBound>, Option<RangeBound>)> {
    let Bson::Document(operators) = condition else {
        return None;
    };
    if operators.is_empty()
        || operators
            .keys()
            .any(|operator| !matches!(operator.as_str(), "$gt" | "$gte" | "$lt" | "$lte"))
    {
        return None;
    }
    let mut lower = None;
    let mut upper = None;
    let mut range_type = None::<&'static str>;
    for (operator, value) in operators {
        let (value_type, key) = sortable_range_value_key(value)?;
        if let Some(existing) = range_type {
            if existing != value_type {
                return None;
            }
        } else {
            range_type = Some(value_type);
        }
        match operator.as_str() {
            "$gt" => {
                lower = Some(RangeBound {
                    key,
                    inclusive: false,
                })
            }
            "$gte" => {
                lower = Some(RangeBound {
                    key,
                    inclusive: true,
                })
            }
            "$lt" => {
                upper = Some(RangeBound {
                    key,
                    inclusive: false,
                })
            }
            "$lte" => {
                upper = Some(RangeBound {
                    key,
                    inclusive: true,
                })
            }
            _ => return None,
        }
    }
    if lower.is_none() && upper.is_none() {
        return None;
    }
    Some((lower, upper))
}

fn planner_key_for_filter(spec: &IndexSpec, filter: &Document) -> Option<String> {
    if let Some(field) = single_field_index_name(spec) {
        let value = exact_equality_filter_part(filter, field)?;
        if !is_compound_planner_scalar(value) {
            return None;
        }
        return Some(spec.collation.id_key_from_bson(value));
    }
    compound_planner_key_for_filter(spec, filter)
}

fn filter_implies_index_membership(spec: &IndexSpec, filter: &Document) -> bool {
    if spec.sparse
        && spec
            .key
            .keys()
            .any(|field| exact_equality_filter_part(filter, field).is_none())
    {
        return false;
    }
    spec.partial_filter
        .as_ref()
        .is_none_or(|partial| filter_implies_partial_filter(filter, partial))
}

fn filter_implies_partial_filter(filter: &Document, partial: &Document) -> bool {
    partial.iter().all(|(field, condition)| {
        if field == "$and" {
            let Bson::Array(clauses) = condition else {
                return false;
            };
            return clauses.iter().all(|clause| {
                let Bson::Document(clause) = clause else {
                    return false;
                };
                filter_implies_partial_filter(filter, clause)
            });
        }
        filter.get(field).is_some_and(|query_condition| {
            query_implies_partial_predicate(query_condition, condition)
        })
    })
}

fn query_implies_partial_predicate(query_condition: &Bson, partial_condition: &Bson) -> bool {
    if partial_is_exists_true(partial_condition) {
        return exact_equality_value(query_condition).is_some()
            || matches!(
                query_condition,
                Bson::Document(document)
                    if document.len() == 1
                        && matches!(document.get("$exists"), Some(Bson::Boolean(true)))
            );
    }
    let Some(query_value) = exact_equality_value(query_condition) else {
        return false;
    };
    let Some(partial_value) = exact_equality_value(partial_condition) else {
        return false;
    };
    bson_values_equal(query_value, partial_value)
}

fn partial_is_exists_true(condition: &Bson) -> bool {
    matches!(
        condition,
        Bson::Document(document)
            if document.len() == 1 && matches!(document.get("$exists"), Some(Bson::Boolean(true)))
    )
}

fn count_filter_covered_by_index(spec: &IndexSpec, filter: &Document) -> bool {
    if filter.keys().any(|key| key.starts_with('$')) {
        return false;
    }
    filter.iter().all(|(field, condition)| {
        if spec.key.contains_key(field) {
            exact_equality_value(condition).is_some()
        } else {
            spec.partial_filter.as_ref().is_some_and(|partial| {
                partial_filter_contains_implied_field_predicate(partial, field, condition)
            })
        }
    })
}

fn count_filter_covered_by_range_index(
    spec: &IndexSpec,
    filter: &Document,
    range: &RangePlannerKey,
) -> bool {
    if filter.keys().any(|key| key.starts_with('$')) {
        return false;
    }
    let indexed_fields = spec.key.keys().cloned().collect::<Vec<_>>();
    filter.iter().all(|(field, condition)| {
        if field == &range.field {
            return range_bounds_for_condition(condition).is_some();
        }
        if indexed_fields
            .iter()
            .take(range.equality_prefix_len)
            .any(|indexed| indexed == field)
        {
            return exact_equality_value(condition).is_some();
        }
        spec.partial_filter.as_ref().is_some_and(|partial| {
            partial_filter_contains_implied_field_predicate(partial, field, condition)
        })
    })
}

fn partial_filter_contains_implied_field_predicate(
    partial: &Document,
    field: &str,
    query_condition: &Bson,
) -> bool {
    partial.iter().any(|(partial_field, partial_condition)| {
        if partial_field == "$and" {
            let Bson::Array(clauses) = partial_condition else {
                return false;
            };
            return clauses.iter().any(|clause| {
                let Bson::Document(clause) = clause else {
                    return false;
                };
                partial_filter_contains_implied_field_predicate(clause, field, query_condition)
            });
        }
        partial_field == field
            && query_implies_partial_predicate(query_condition, partial_condition)
    })
}

fn exact_equality_filter_part<'a>(filter: &'a Document, field: &str) -> Option<&'a Bson> {
    if filter.keys().any(|key| key.starts_with('$')) {
        return None;
    }
    filter.get(field).and_then(exact_equality_value)
}

fn is_compound_planner_scalar(value: &Bson) -> bool {
    matches!(
        value,
        Bson::String(_) | Bson::Boolean(_) | Bson::ObjectId(_) | Bson::DateTime(_)
    )
}

fn is_multikey_planner_scalar(value: &Bson) -> bool {
    matches!(
        value,
        Bson::String(_) | Bson::Boolean(_) | Bson::ObjectId(_) | Bson::DateTime(_)
    )
}

fn encode_compound_planner_key(parts: &[String]) -> String {
    let mut key = format!("compound:{}", parts.len());
    for part in parts {
        key.push(':');
        key.push_str(&part.len().to_string());
        key.push(':');
        key.push_str(part);
    }
    key
}

fn encode_compound_prefix_planner_key(parts: &[String]) -> String {
    let mut key = format!("compound-prefix:{}", parts.len());
    for part in parts {
        key.push(':');
        key.push_str(&part.len().to_string());
        key.push(':');
        key.push_str(part);
    }
    key
}

fn encode_range_planner_prefix(equality_parts: &[String]) -> String {
    let mut key = format!("range:{}", equality_parts.len());
    for part in equality_parts {
        key.push(':');
        key.push_str(&part.len().to_string());
        key.push(':');
        key.push_str(part);
    }
    key.push(':');
    key
}

fn range_planner_key_value_for_collation(
    equality_parts: &[String],
    range_value: &Bson,
    collation: &Collation,
) -> Option<String> {
    if !collation.is_simple() && matches!(range_value, Bson::String(_)) {
        return None;
    }
    let (_, sortable) = sortable_range_value_key(range_value)?;
    Some(format!(
        "{}{}",
        encode_range_planner_prefix(equality_parts),
        sortable
    ))
}

fn sortable_range_value_key(value: &Bson) -> Option<(&'static str, String)> {
    match value {
        Bson::String(value) => Some(("str", format!("str:{value}"))),
        Bson::Boolean(false) => Some(("bool", "bool:0".to_string())),
        Bson::Boolean(true) => Some(("bool", "bool:1".to_string())),
        Bson::ObjectId(value) => Some(("oid", format!("oid:{value}"))),
        Bson::DateTime(value) => {
            let shifted = value.timestamp_millis() as i128 - i64::MIN as i128;
            Some(("date", format!("date:{shifted:020}")))
        }
        _ => None,
    }
}

fn indexes_for_namespace_tx(
    tx: &rusqlite::Transaction<'_>,
    namespace: &str,
) -> Result<Vec<IndexSpec>> {
    let mut stmt = tx.prepare(
        "SELECT name, key_bson, unique_index, sparse_index, partial_filter_bson, expire_after_seconds, collation_bson FROM indexes WHERE namespace = ?1 ORDER BY name",
    )?;
    stmt.query_map(params![namespace], |row| {
        let name = row.get::<_, String>(0)?;
        let key = decode_document_sql(row.get::<_, Vec<u8>>(1)?)?;
        let unique = row.get::<_, i32>(2)? != 0;
        let sparse = row.get::<_, i32>(3)? != 0;
        let partial_filter = row
            .get::<_, Option<Vec<u8>>>(4)?
            .map(decode_document_sql)
            .transpose()?;
        let expire_after_seconds = row.get::<_, Option<i64>>(5)?;
        let collation = decode_collation_sql(row.get::<_, Option<Vec<u8>>>(6)?)?;
        Ok(IndexSpec {
            name,
            key,
            unique,
            sparse,
            partial_filter,
            expire_after_seconds,
            collation,
        })
    })?
    .collect::<std::result::Result<Vec<_>, _>>()
    .map_err(Into::into)
}

fn ttl_indexes_for_namespace_tx(
    tx: &rusqlite::Transaction<'_>,
    namespace: &str,
) -> Result<Vec<IndexSpec>> {
    Ok(indexes_for_namespace_tx(tx, namespace)?
        .into_iter()
        .filter(|index| index.expire_after_seconds.is_some())
        .collect())
}

fn sweep_ttl_namespace(conn: &Connection, namespace: &str) -> Result<i32> {
    sweep_ttl_namespace_at(conn, namespace, bson::DateTime::now())
}

fn sweep_ttl_namespace_at(
    conn: &Connection,
    namespace: &str,
    sweep_time: bson::DateTime,
) -> Result<i32> {
    let tx = conn.unchecked_transaction()?;
    let removed = sweep_ttl_namespace_at_tx(&tx, namespace, sweep_time)?;
    tx.commit()?;
    Ok(removed)
}

fn sweep_ttl_namespace_at_tx(
    tx: &rusqlite::Transaction<'_>,
    namespace: &str,
    sweep_time: bson::DateTime,
) -> Result<i32> {
    let ttl_indexes = ttl_indexes_for_namespace_tx(tx, namespace)?;
    if ttl_indexes.is_empty() {
        return Ok(0);
    }

    let mut expired_ids = HashSet::new();
    for stored in stored_documents_for_namespace_tx(tx, namespace)? {
        if ttl_indexes
            .iter()
            .any(|index| ttl_index_expires_document(index, &stored.document, sweep_time))
        {
            expired_ids.insert(stored.id_key);
        }
    }

    let mut removed = 0_i32;
    for id_key in expired_ids {
        delete_index_entries_for_document_tx(tx, namespace, &id_key)?;
        removed += tx.execute(
            "DELETE FROM documents WHERE namespace = ?1 AND id_key = ?2",
            params![namespace, id_key],
        )? as i32;
    }
    Ok(removed)
}

fn ttl_index_expires_document(
    index: &IndexSpec,
    document: &Document,
    sweep_time: bson::DateTime,
) -> bool {
    let Some(expire_after_seconds) = index.expire_after_seconds else {
        return false;
    };
    let Some(field) = single_field_index_name(index) else {
        return false;
    };
    let cutoff = sweep_time
        .timestamp_millis()
        .saturating_sub(expire_after_seconds.saturating_mul(1000));
    matches!(
        get_document_path(document, field),
        Some(Bson::DateTime(value)) if value.timestamp_millis() <= cutoff
    )
}

fn sql_string_error(err: MongolinoError) -> rusqlite::Error {
    rusqlite::Error::ToSqlConversionFailure(Box::new(std::io::Error::other(err.to_string())))
}

fn sql_string_error_from_string(err: String) -> rusqlite::Error {
    rusqlite::Error::ToSqlConversionFailure(Box::new(std::io::Error::other(err)))
}

#[derive(Clone, Debug)]
struct CollectionOptions {
    document: Document,
    validator: Option<JsonSchemaValidator>,
}

impl CollectionOptions {
    fn empty() -> Self {
        Self {
            document: Document::new(),
            validator: None,
        }
    }
}

fn parse_collection_options(options: Document) -> std::result::Result<CollectionOptions, String> {
    let validator = match options.get("validator") {
        None => None,
        Some(Bson::Document(validator)) if validator.is_empty() => None,
        Some(Bson::Document(validator)) => Some(JsonSchemaValidator::parse(validator)?),
        Some(_) => return Err("validator must be a document".to_string()),
    };
    match options.get("validationLevel") {
        None => {}
        Some(Bson::String(value)) if value == "strict" => {}
        Some(Bson::String(value)) => {
            return Err(format!("validationLevel {value} is not supported"));
        }
        Some(_) => return Err("validationLevel must be a string".to_string()),
    }
    match options.get("validationAction") {
        None => {}
        Some(Bson::String(value)) if value == "error" => {}
        Some(Bson::String(value)) => {
            return Err(format!("validationAction {value} is not supported"));
        }
        Some(_) => return Err("validationAction must be a string".to_string()),
    }
    Ok(CollectionOptions {
        document: options,
        validator,
    })
}

fn collection_options_from_command(
    command: &Document,
) -> std::result::Result<CollectionOptions, String> {
    let mut options = Document::new();
    if let Some(value) = command.get("validator") {
        options.insert("validator", value.clone());
    }
    if let Some(value) = command.get("validationLevel") {
        options.insert("validationLevel", value.clone());
    }
    if let Some(value) = command.get("validationAction") {
        options.insert("validationAction", value.clone());
    }
    parse_collection_options(options)
}

fn collection_options(conn: &Connection, namespace: &str) -> Result<CollectionOptions> {
    let Some(bytes) = conn
        .query_row(
            "SELECT options_bson FROM collections WHERE namespace = ?1",
            params![namespace],
            |row| row.get::<_, Option<Vec<u8>>>(0),
        )
        .optional()?
        .flatten()
    else {
        return Ok(CollectionOptions::empty());
    };
    parse_collection_options(decode_document(bytes)?).map_err(MongolinoError::Protocol)
}

fn collection_options_tx(
    tx: &rusqlite::Transaction<'_>,
    namespace: &str,
) -> Result<CollectionOptions> {
    let Some(bytes) = tx
        .query_row(
            "SELECT options_bson FROM collections WHERE namespace = ?1",
            params![namespace],
            |row| row.get::<_, Option<Vec<u8>>>(0),
        )
        .optional()?
        .flatten()
    else {
        return Ok(CollectionOptions::empty());
    };
    parse_collection_options(decode_document(bytes)?).map_err(MongolinoError::Protocol)
}

fn collection_options_document(conn: &Connection, namespace: &str) -> Result<Document> {
    Ok(collection_options(conn, namespace)?.document)
}

fn set_collection_options_tx(
    tx: &rusqlite::Transaction<'_>,
    namespace: &str,
    options: &Document,
) -> std::result::Result<(), rusqlite::Error> {
    let encoded = if options.is_empty() {
        None
    } else {
        Some(encode_document(options)?)
    };
    tx.execute(
        "UPDATE collections SET options_bson = ?1 WHERE namespace = ?2",
        params![encoded, namespace],
    )?;
    Ok(())
}

#[derive(Clone, Debug)]
struct JsonSchemaValidator {
    root: SchemaNode,
}

impl JsonSchemaValidator {
    fn parse(validator: &Document) -> std::result::Result<Self, String> {
        if validator.len() != 1 {
            return Err("validator only supports a single $jsonSchema document".to_string());
        }
        let schema = validator
            .get_document("$jsonSchema")
            .map_err(|_| "validator requires a $jsonSchema document".to_string())?;
        let root = SchemaNode::parse(schema, "$jsonSchema", true)?;
        if !root.bson_types.contains(&BsonTypeName::Object) {
            return Err("$jsonSchema.bsonType must be object".to_string());
        }
        Ok(Self { root })
    }

    fn validate(&self, document: &Document) -> std::result::Result<(), String> {
        self.root.validate_document(document, "$root")
    }
}

#[derive(Clone, Debug)]
struct SchemaNode {
    bson_types: Vec<BsonTypeName>,
    required: Vec<String>,
    properties: HashMap<String, SchemaNode>,
}

impl SchemaNode {
    fn parse(schema: &Document, path: &str, root: bool) -> std::result::Result<Self, String> {
        if schema.is_empty() {
            return Err(format!("{path} schema must not be empty"));
        }
        for key in schema.keys() {
            if !["bsonType", "required", "properties"].contains(&key.as_str()) {
                return Err(format!("{path}.{key} is not supported"));
            }
        }
        let bson_types = match schema.get("bsonType") {
            Some(value) => parse_bson_type_set(value, &format!("{path}.bsonType"))?,
            None => return Err(format!("{path}.bsonType is required")),
        };
        if root && bson_types != vec![BsonTypeName::Object] {
            return Err("$jsonSchema.bsonType must be object".to_string());
        }
        let required = match schema.get("required") {
            None => Vec::new(),
            Some(value) => parse_required(value, &format!("{path}.required"))?,
        };
        let properties = match schema.get("properties") {
            None => HashMap::new(),
            Some(Bson::Document(properties)) => {
                let mut parsed = HashMap::new();
                for (field, property_schema) in properties {
                    if field.is_empty() || field.contains('.') {
                        return Err(format!(
                            "{path}.properties field names must be non-empty and must not contain dots"
                        ));
                    }
                    let Bson::Document(property_schema) = property_schema else {
                        return Err(format!("{path}.properties.{field} must be a document"));
                    };
                    parsed.insert(
                        field.to_string(),
                        SchemaNode::parse(
                            property_schema,
                            &format!("{path}.properties.{field}"),
                            false,
                        )?,
                    );
                }
                parsed
            }
            Some(_) => return Err(format!("{path}.properties must be a document")),
        };
        if (!required.is_empty() || !properties.is_empty())
            && !bson_types.contains(&BsonTypeName::Object)
        {
            return Err(format!(
                "{path} required/properties are only supported for object schemas"
            ));
        }
        Ok(Self {
            bson_types,
            required,
            properties,
        })
    }

    fn validate_bson(&self, value: &Bson, path: &str) -> std::result::Result<(), String> {
        if !self
            .bson_types
            .iter()
            .any(|expected| expected.matches(value))
        {
            return Err(format!(
                "Document failed validation: {path} must be {}",
                self.bson_types
                    .iter()
                    .map(BsonTypeName::as_str)
                    .collect::<Vec<_>>()
                    .join(" or ")
            ));
        }
        if let Bson::Document(document) = value {
            self.validate_document(document, path)?;
        }
        Ok(())
    }

    fn validate_document(
        &self,
        document: &Document,
        path: &str,
    ) -> std::result::Result<(), String> {
        for field in &self.required {
            if !document.contains_key(field) {
                return Err(format!(
                    "Document failed validation: {path}.{field} is required"
                ));
            }
        }
        for (field, schema) in &self.properties {
            if let Some(value) = document.get(field) {
                schema.validate_bson(value, &format!("{path}.{field}"))?;
            }
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum BsonTypeName {
    Object,
    Array,
    String,
    Int,
    Long,
    Double,
    Number,
    Bool,
    ObjectId,
    Date,
    Null,
}

impl BsonTypeName {
    fn parse(value: &str) -> Option<Self> {
        Some(match value {
            "object" => Self::Object,
            "array" => Self::Array,
            "string" => Self::String,
            "int" => Self::Int,
            "long" => Self::Long,
            "double" => Self::Double,
            "number" => Self::Number,
            "bool" => Self::Bool,
            "objectId" => Self::ObjectId,
            "date" => Self::Date,
            "null" => Self::Null,
            _ => return None,
        })
    }

    fn as_str(&self) -> &'static str {
        match self {
            Self::Object => "object",
            Self::Array => "array",
            Self::String => "string",
            Self::Int => "int",
            Self::Long => "long",
            Self::Double => "double",
            Self::Number => "number",
            Self::Bool => "bool",
            Self::ObjectId => "objectId",
            Self::Date => "date",
            Self::Null => "null",
        }
    }

    fn matches(&self, value: &Bson) -> bool {
        match self {
            Self::Object => matches!(value, Bson::Document(_)),
            Self::Array => matches!(value, Bson::Array(_)),
            Self::String => matches!(value, Bson::String(_)),
            Self::Int => matches!(value, Bson::Int32(_)),
            Self::Long => matches!(value, Bson::Int64(_)),
            Self::Double => matches!(value, Bson::Double(_)),
            Self::Number => matches!(value, Bson::Int32(_) | Bson::Int64(_) | Bson::Double(_)),
            Self::Bool => matches!(value, Bson::Boolean(_)),
            Self::ObjectId => matches!(value, Bson::ObjectId(_)),
            Self::Date => matches!(value, Bson::DateTime(_)),
            Self::Null => matches!(value, Bson::Null),
        }
    }
}

fn parse_bson_type_set(value: &Bson, path: &str) -> std::result::Result<Vec<BsonTypeName>, String> {
    let values = match value {
        Bson::String(value) => vec![value.as_str()],
        Bson::Array(values) if !values.is_empty() => values
            .iter()
            .map(|value| match value {
                Bson::String(value) => Ok(value.as_str()),
                _ => Err(format!("{path} array values must be strings")),
            })
            .collect::<std::result::Result<Vec<_>, _>>()?,
        Bson::Array(_) => return Err(format!("{path} array must not be empty")),
        _ => return Err(format!("{path} must be a string or array of strings")),
    };
    let mut parsed = Vec::new();
    for value in values {
        let Some(kind) = BsonTypeName::parse(value) else {
            return Err(format!("{path} value {value} is not supported"));
        };
        if !parsed.contains(&kind) {
            parsed.push(kind);
        }
    }
    Ok(parsed)
}

fn parse_required(value: &Bson, path: &str) -> std::result::Result<Vec<String>, String> {
    let Bson::Array(values) = value else {
        return Err(format!("{path} must be an array"));
    };
    let mut required = Vec::new();
    for value in values {
        let Bson::String(field) = value else {
            return Err(format!("{path} values must be strings"));
        };
        if field.is_empty() {
            return Err(format!("{path} values must be non-empty strings"));
        }
        if !required.contains(field) {
            required.push(field.to_string());
        }
    }
    Ok(required)
}

fn insert_collection_catalog_with_options(
    conn: &Connection,
    db: &str,
    collection: &str,
    options: &Document,
) -> std::result::Result<(), rusqlite::Error> {
    let ns = namespace(db, collection);
    let encoded = if options.is_empty() {
        None
    } else {
        Some(encode_document(options)?)
    };
    conn.execute(
        "INSERT INTO collections(namespace, db, name, options_bson) VALUES (?1, ?2, ?3, ?4)",
        params![ns, db, collection, encoded],
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

fn collection_exists_tx(tx: &rusqlite::Transaction<'_>, namespace: &str) -> Result<bool> {
    let exists = tx
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
    let bypass_validation = match optional_bool(command, "bypassDocumentValidation") {
        Ok(value) => value.unwrap_or(false),
        Err(errmsg) => return Ok(command_error(9, &errmsg)),
    };
    if let Some(errmsg) = reject_unsupported_command_keys(
        command,
        &[
            "insert",
            "documents",
            "ordered",
            "bypassDocumentValidation",
            "writeConcern",
            "txnNumber",
            "$db",
            "lsid",
        ],
    ) {
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

    let options = if bypass_validation {
        CollectionOptions::empty()
    } else {
        collection_options(conn, &namespace)?
    };
    let tx = conn.unchecked_transaction()?;
    let mut inserted = 0_i32;
    let mut write_errors = Vec::new();
    let mut swept = false;

    {
        let mut stmt = tx.prepare(
            "INSERT INTO documents(namespace, id_key, bson, updated_at)
             VALUES (?1, ?2, ?3, CURRENT_TIMESTAMP)",
        )?;

        for (index, (id_key, encoded, document)) in prepared.iter().enumerate() {
            if let Err(errmsg) = validate_document_with_options(&options, document) {
                write_errors.push(write_error(
                    index as i32,
                    update_error_code(&errmsg),
                    &errmsg,
                ));
                if ordered {
                    break;
                }
                continue;
            }
            if !swept {
                ensure_collection_catalog_tx(&tx, &namespace)?;
                sweep_ttl_namespace_at_tx(&tx, &namespace, bson::DateTime::now())?;
                swept = true;
            }
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

fn optional_document_validation_bypass(
    command: &Document,
) -> std::result::Result<Option<bool>, String> {
    let camel = optional_bool(command, "bypassDocumentValidation")?;
    let snake = optional_bool(command, "bypass_document_validation")?;
    match (camel, snake) {
        (Some(camel), Some(snake)) if camel != snake => Err(
            "bypassDocumentValidation and bypass_document_validation cannot conflict".to_string(),
        ),
        (Some(value), _) | (_, Some(value)) => Ok(Some(value)),
        (None, None) => Ok(None),
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
    let bypass_validation = match optional_bool(command, "bypassDocumentValidation") {
        Ok(value) => value.unwrap_or(false),
        Err(errmsg) => return Ok(command_error(9, &errmsg)),
    };
    if let Some(errmsg) = reject_unsupported_command_keys(
        command,
        &[
            "update",
            "updates",
            "ordered",
            "bypassDocumentValidation",
            "writeConcern",
            "txnNumber",
            "$db",
            "lsid",
        ],
    ) {
        return Ok(command_error(72, &errmsg));
    }

    let namespace = namespace(db, collection);
    let tx = conn.unchecked_transaction()?;
    let mut matched_count = 0_i32;
    let mut modified_count = 0_i32;
    let mut upserted_count = 0_i32;
    let mut upserted = Vec::new();
    let mut write_errors = Vec::new();
    let mut options = None;
    let mut swept = false;

    for (index, entry) in updates.iter().enumerate() {
        if let Err(errmsg) = validate_update_entry_shape_tx(&tx, &namespace, entry) {
            write_errors.push(write_error(
                index as i32,
                update_error_code(&errmsg),
                &errmsg,
            ));
            if ordered {
                break;
            }
            continue;
        }
        if !swept {
            ensure_collection_catalog_tx(&tx, &namespace)?;
            options = Some(if bypass_validation {
                CollectionOptions::empty()
            } else {
                collection_options_tx(&tx, &namespace)?
            });
            sweep_ttl_namespace_at_tx(&tx, &namespace, bson::DateTime::now())?;
            swept = true;
        }
        let result = apply_update_entry(
            &tx,
            &namespace,
            entry,
            options.as_ref().expect("options initialized before update"),
        );
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

fn validate_update_entry_shape_tx(
    tx: &rusqlite::Transaction<'_>,
    namespace: &str,
    entry: &Bson,
) -> std::result::Result<(), String> {
    let Bson::Document(entry) = entry else {
        return Err("update entries must be documents".to_string());
    };
    reject_unsupported_entry_keys(
        entry,
        &[
            "q",
            "u",
            "upsert",
            "multi",
            "hint",
            "collation",
            "arrayFilters",
        ],
    )?;
    let query = entry
        .get_document("q")
        .map_err(|_| "update entry requires q document".to_string())?;
    validate_filter_shape(query).map_err(|err| err.errmsg)?;
    let collation = Collation::parse_optional(entry, "collation")?;
    validate_filter_for_collation(query, &collation).map_err(|err| err.errmsg)?;
    let update = entry
        .get("u")
        .ok_or_else(|| "update entry requires u".to_string())?;
    optional_bool_doc(entry, "upsert")?;
    optional_bool_doc(entry, "multi")?;
    if let Some(hint) = parse_optional_hint(entry)? {
        let resolved = resolve_hint(
            indexes_for_namespace_tx(tx, namespace).map_err(|err| err.to_string())?,
            hint,
        )?;
        validate_hint_collation(&resolved, &collation, query)?;
    }
    parse_update_spec(update, entry.get("arrayFilters"))?;
    Ok(())
}

fn apply_update_entry(
    tx: &rusqlite::Transaction<'_>,
    namespace: &str,
    entry: &Bson,
    options: &CollectionOptions,
) -> std::result::Result<UpdateOutcome, String> {
    let Bson::Document(entry) = entry else {
        return Err("update entries must be documents".to_string());
    };
    reject_unsupported_entry_keys(
        entry,
        &[
            "q",
            "u",
            "upsert",
            "multi",
            "hint",
            "collation",
            "arrayFilters",
        ],
    )?;
    let query = entry
        .get_document("q")
        .map_err(|_| "update entry requires q document".to_string())?;
    let update = entry
        .get("u")
        .ok_or_else(|| "update entry requires u".to_string())?;
    let upsert = optional_bool_doc(entry, "upsert")?.unwrap_or(false);
    let multi = optional_bool_doc(entry, "multi")?.unwrap_or(false);
    let collation = Collation::parse_optional(entry, "collation")?;
    let hint = match parse_optional_hint(entry)? {
        Some(hint) => Some(resolve_hint(
            indexes_for_namespace_tx(tx, namespace).map_err(|err| err.to_string())?,
            hint,
        )?),
        None => None,
    };
    let update = parse_update_spec(update, entry.get("arrayFilters"))?;

    let mut matches = Vec::new();
    for stored in
        transaction_candidate_documents_with_hint(tx, namespace, query, hint.as_ref(), &collation)?
    {
        match matches_filter_with_collation(&stored.document, query, &collation) {
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

        let mut new_document = build_upsert_document(query, &update, &collation)?;
        ensure_document_id(&mut new_document);
        let upserted_id = new_document
            .get("_id")
            .cloned()
            .ok_or_else(|| "upsert document is missing _id".to_string())?;
        validate_document_with_options(options, &new_document)?;
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
    let mut replacements = Vec::new();
    for stored in matches {
        let new_document = apply_update_to_document(&stored.document, &update, query, &collation)?;
        let new_id_key = id_key(&new_document).map_err(|err| err.to_string())?;
        if new_id_key != stored.id_key {
            return Err("update cannot change _id".to_string());
        }
        if new_document != stored.document {
            validate_document_with_options(options, &new_document)?;
            ensure_unique_constraints_tx(tx, namespace, &new_document, Some(&stored.id_key))?;
            replacements.push((stored.id_key, new_document));
        }
    }
    ensure_unique_constraints_for_replacements_tx(tx, namespace, &replacements)?;

    let modified = replacements.len() as i32;
    for (id_key, new_document) in replacements {
        update_stored_document_tx(tx, namespace, &id_key, &new_document)
            .map_err(|err| duplicate_or_sql_error(namespace, &new_document, err))?;
        refresh_index_entries_for_document_tx(tx, namespace, &id_key, &new_document)
            .map_err(|err| err.to_string())?;
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

fn stored_document_by_id_key_tx(
    tx: &rusqlite::Transaction<'_>,
    namespace: &str,
    id_key: &str,
) -> Result<Option<StoredDocument>> {
    tx.query_row(
        "SELECT id_key, bson FROM documents WHERE namespace = ?1 AND id_key = ?2",
        params![namespace, id_key],
        |row| Ok((row.get::<_, String>(0)?, row.get::<_, Vec<u8>>(1)?)),
    )
    .optional()?
    .map(|(id_key, bytes)| {
        Ok(StoredDocument {
            id_key,
            document: decode_document(bytes)?,
        })
    })
    .transpose()
}

#[derive(Debug, PartialEq)]
enum TransactionCandidatePlan {
    IdEquality(String),
    IndexedEquality {
        index_name: String,
        key_value: String,
        unique: bool,
    },
    IndexedPrefix {
        index_name: String,
        key_value: String,
    },
    IndexedRange {
        index_name: String,
        range: RangePlannerKey,
    },
    Fallback,
}

fn plan_transaction_candidates(
    tx: &rusqlite::Transaction<'_>,
    namespace: &str,
    filter: &Document,
) -> Result<TransactionCandidatePlan> {
    plan_transaction_candidates_with_collation(tx, namespace, filter, &Collation::Simple)
}

fn plan_transaction_candidates_with_collation(
    tx: &rusqlite::Transaction<'_>,
    namespace: &str,
    filter: &Document,
    collation: &Collation,
) -> Result<TransactionCandidatePlan> {
    if let Some(value) = exact_equality_filter_value(filter, "_id") {
        if id_equality_safe_for_collation(value, collation) {
            return Ok(TransactionCandidatePlan::IdEquality(id_key_from_bson(
                value,
            )));
        }
    }
    for index in planner_indexes(indexes_for_namespace_tx(tx, namespace)?) {
        if index.collation != *collation {
            continue;
        }
        let Some(key_value) = planner_key_for_filter(&index, filter) else {
            continue;
        };
        if !filter_implies_index_membership(&index, filter) {
            continue;
        }
        if !index_entries_safe_for_planner_tx(tx, namespace, &index.name)? {
            continue;
        }
        return Ok(TransactionCandidatePlan::IndexedEquality {
            index_name: index.name,
            key_value,
            unique: index.unique,
        });
    }
    for index in planner_indexes(indexes_for_namespace_tx(tx, namespace)?) {
        if index.collation != *collation || !collation.is_simple() {
            continue;
        }
        let Some(range) = range_planner_key_for_filter(&index, filter) else {
            continue;
        };
        if !filter_implies_index_membership(&index, filter) {
            continue;
        }
        if !index_entries_safe_for_planner_tx(tx, namespace, &index.name)? {
            continue;
        }
        return Ok(TransactionCandidatePlan::IndexedRange {
            index_name: index.name,
            range,
        });
    }
    for index in planner_indexes(indexes_for_namespace_tx(tx, namespace)?) {
        if index.collation != *collation {
            continue;
        }
        let Some((_, key_value)) = prefix_planner_key_for_filter(&index, filter) else {
            continue;
        };
        if !filter_implies_index_membership(&index, filter) {
            continue;
        }
        if !index_entries_safe_for_planner_tx(tx, namespace, &index.name)? {
            continue;
        }
        return Ok(TransactionCandidatePlan::IndexedPrefix {
            index_name: index.name,
            key_value,
        });
    }
    Ok(TransactionCandidatePlan::Fallback)
}

fn transaction_candidate_documents(
    tx: &rusqlite::Transaction<'_>,
    namespace: &str,
    filter: &Document,
) -> Result<Vec<StoredDocument>> {
    transaction_candidate_documents_with_collation(tx, namespace, filter, &Collation::Simple)
}

fn transaction_candidate_documents_with_collation(
    tx: &rusqlite::Transaction<'_>,
    namespace: &str,
    filter: &Document,
    collation: &Collation,
) -> Result<Vec<StoredDocument>> {
    match plan_transaction_candidates_with_collation(tx, namespace, filter, collation)? {
        TransactionCandidatePlan::IdEquality(id_key) => {
            Ok(stored_document_by_id_key_tx(tx, namespace, &id_key)?
                .into_iter()
                .collect())
        }
        TransactionCandidatePlan::IndexedEquality {
            index_name,
            key_value,
            unique,
        } => {
            if unique {
                indexed_unique_candidate_document_tx(tx, namespace, &index_name, &key_value)
            } else {
                indexed_candidate_documents_tx(tx, namespace, &index_name, &key_value)
            }
        }
        TransactionCandidatePlan::IndexedPrefix {
            index_name,
            key_value,
        } => indexed_candidate_documents_tx(tx, namespace, &index_name, &key_value),
        TransactionCandidatePlan::IndexedRange { index_name, range } => {
            indexed_candidate_documents_tx_by_range(tx, namespace, &index_name, &range)
        }
        TransactionCandidatePlan::Fallback => stored_documents_for_namespace_tx(tx, namespace),
    }
}

fn transaction_candidate_documents_with_hint(
    tx: &rusqlite::Transaction<'_>,
    namespace: &str,
    filter: &Document,
    hint: Option<&ResolvedHint>,
    collation: &Collation,
) -> std::result::Result<Vec<StoredDocument>, String> {
    match hint {
        None => transaction_candidate_documents_with_collation(tx, namespace, filter, collation)
            .map_err(|err| err.to_string()),
        Some(ResolvedHint::Id) => {
            let Some(value) = exact_equality_filter_value(filter, "_id") else {
                return Err("hint _id_ is incompatible with this filter".to_string());
            };
            if !id_equality_safe_for_collation(value, collation) {
                return Err(
                    "hint _id_ is incompatible with non-simple string collation".to_string()
                );
            }
            stored_document_by_id_key_tx(tx, namespace, &id_key_from_bson(value))
                .map(|document| document.into_iter().collect())
                .map_err(|err| err.to_string())
        }
        Some(ResolvedHint::Index(index)) => hinted_transaction_candidate_documents_for_index(
            tx, namespace, filter, index, collation,
        ),
    }
}

fn hinted_transaction_candidate_documents_for_index(
    tx: &rusqlite::Transaction<'_>,
    namespace: &str,
    filter: &Document,
    index: &IndexSpec,
    collation: &Collation,
) -> std::result::Result<Vec<StoredDocument>, String> {
    if index.collation != *collation {
        return Err(format!(
            "hint index {} collation is incompatible with this query",
            index.name
        ));
    }
    if !filter_implies_index_membership(index, filter) {
        return Err(format!(
            "hint index {} is unsafe for this filter membership",
            index.name
        ));
    }
    if !index_entries_safe_for_planner_tx(tx, namespace, &index.name)
        .map_err(|err| err.to_string())?
    {
        return Err(format!(
            "hint index {} has unsupported multikey omissions",
            index.name
        ));
    }
    if let Some(key_value) = planner_key_for_filter(index, filter) {
        return indexed_candidate_documents_tx(tx, namespace, &index.name, &key_value)
            .map_err(|err| err.to_string());
    }
    if collation.is_simple()
        && let Some(range) = range_planner_key_for_filter(index, filter)
    {
        return indexed_candidate_documents_tx_by_range(tx, namespace, &index.name, &range)
            .map_err(|err| err.to_string());
    }
    if let Some((_, key_value)) = prefix_planner_key_for_filter(index, filter) {
        return indexed_candidate_documents_tx(tx, namespace, &index.name, &key_value)
            .map_err(|err| err.to_string());
    }
    Err(format!(
        "hint index {} is incompatible with this filter",
        index.name
    ))
}

fn indexed_unique_candidate_document_tx(
    tx: &rusqlite::Transaction<'_>,
    namespace: &str,
    index_name: &str,
    key_value: &str,
) -> Result<Vec<StoredDocument>> {
    let id_key = tx
        .query_row(
            r#"
            SELECT id_key
              FROM index_entries
             WHERE namespace = ?1
               AND index_name = ?2
               AND key_value = ?3
             LIMIT 1
            "#,
            params![namespace, index_name, key_value],
            |row| row.get::<_, String>(0),
        )
        .optional()?;
    match id_key {
        Some(id_key) => Ok(stored_document_by_id_key_tx(tx, namespace, &id_key)?
            .into_iter()
            .collect()),
        None => Ok(Vec::new()),
    }
}

fn indexed_candidate_documents_tx(
    tx: &rusqlite::Transaction<'_>,
    namespace: &str,
    index_name: &str,
    key_value: &str,
) -> Result<Vec<StoredDocument>> {
    let mut stmt = tx.prepare(
        r#"
        SELECT DISTINCT d.id_key, d.bson, d.created_at
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
    stmt.query_map(params![namespace, index_name, key_value], |row| {
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

fn indexed_candidate_documents_tx_by_range(
    tx: &rusqlite::Transaction<'_>,
    namespace: &str,
    index_name: &str,
    range: &RangePlannerKey,
) -> Result<Vec<StoredDocument>> {
    let bounds = range_sql_bounds(range);
    let mut stmt = tx.prepare(&format!(
        r#"
        SELECT DISTINCT d.id_key, d.bson, d.created_at
          FROM index_entries e
          JOIN documents d
            ON d.namespace = e.namespace
           AND d.id_key = e.id_key
         WHERE e.namespace = ?1
           AND e.index_name = ?2
           AND {}
         ORDER BY d.created_at
        "#,
        bounds.predicate
    ))?;
    stmt.query_map(
        params![namespace, index_name, bounds.lower, bounds.upper],
        |row| Ok((row.get::<_, String>(0)?, row.get::<_, Vec<u8>>(1)?)),
    )?
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
    } else if errmsg.starts_with("Document failed validation") {
        DOCUMENT_VALIDATION_ERROR_CODE
    } else if errmsg.starts_with("unique index") && errmsg.contains("does not support") {
        72
    } else {
        2
    }
}

fn unique_write_error_code(errmsg: &str) -> i32 {
    if errmsg.starts_with("duplicate key error") {
        11000
    } else if errmsg.starts_with("Document failed validation") {
        DOCUMENT_VALIDATION_ERROR_CODE
    } else if errmsg.starts_with("unique index") && errmsg.contains("does not support") {
        72
    } else {
        2
    }
}

fn validate_document_with_options(
    options: &CollectionOptions,
    document: &Document,
) -> std::result::Result<(), String> {
    if let Some(validator) = &options.validator {
        validator.validate(document)?;
    }
    Ok(())
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
struct ParsedUpdate {
    spec: UpdateSpec,
    array_filters: ArrayFilterSet,
}

#[derive(Clone, Debug)]
enum UpdateSpec {
    Replacement(Document),
    Modifier(UpdateModifiers),
    Pipeline(Vec<UpdatePipelineStage>),
}

#[derive(Clone, Debug)]
enum UpdatePipelineStage {
    AddFields(Vec<AggregationComputedField>),
    Unset(AggregateUnsetSpec),
    Project(Option<AggregateProjectSpec>),
    ReplaceRoot(AggregateReplaceRootSpec),
}

#[derive(Clone, Debug, Default)]
struct ArrayFilterSet {
    filters: HashMap<String, ArrayFilterPredicate>,
}

#[derive(Clone, Debug)]
struct ArrayFilterPredicate {
    document: Document,
    root: Option<Bson>,
}

#[derive(Clone, Debug, Default)]
struct UpdateModifiers {
    set: Document,
    unset: Document,
    inc: Document,
    rename: Document,
    min: Document,
    max: Document,
    mul: Document,
    set_on_insert: Document,
    push: Document,
    add_to_set: Document,
    pop: Document,
    pull: Document,
    pull_all: Document,
}

impl UpdateModifiers {
    fn is_empty(&self) -> bool {
        self.set.is_empty()
            && self.unset.is_empty()
            && self.inc.is_empty()
            && self.rename.is_empty()
            && self.min.is_empty()
            && self.max.is_empty()
            && self.mul.is_empty()
            && self.set_on_insert.is_empty()
            && self.push.is_empty()
            && self.add_to_set.is_empty()
            && self.pop.is_empty()
            && self.pull.is_empty()
            && self.pull_all.is_empty()
    }
}

fn parse_update_spec(
    update: &Bson,
    array_filters: Option<&Bson>,
) -> std::result::Result<ParsedUpdate, String> {
    let spec = match update {
        Bson::Document(update) => classify_update(update)?,
        Bson::Array(stages) => UpdateSpec::Pipeline(parse_update_pipeline(stages)?),
        _ => return Err("update entry requires u document or pipeline array".to_string()),
    };
    let array_filters = parse_array_filters(array_filters, &spec)?;
    Ok(ParsedUpdate {
        spec,
        array_filters,
    })
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

    let mut modifiers = UpdateModifiers::default();
    let mut paths = Vec::new();
    for (operator, operand) in update {
        let Bson::Document(operand) = operand else {
            return Err(format!("{operator} requires a document operand"));
        };
        match operator.as_str() {
            "$set" => {
                append_update_paths(operator, operand, &mut paths)?;
                modifiers.set = operand.clone();
            }
            "$unset" => {
                append_update_paths(operator, operand, &mut paths)?;
                modifiers.unset = operand.clone();
            }
            "$inc" => {
                append_update_paths(operator, operand, &mut paths)?;
                modifiers.inc = operand.clone();
            }
            "$rename" => {
                append_rename_paths(operator, operand, &mut paths)?;
                modifiers.rename = operand.clone();
            }
            "$min" => {
                append_update_paths(operator, operand, &mut paths)?;
                modifiers.min = operand.clone();
            }
            "$max" => {
                append_update_paths(operator, operand, &mut paths)?;
                modifiers.max = operand.clone();
            }
            "$mul" => {
                append_update_paths(operator, operand, &mut paths)?;
                modifiers.mul = operand.clone();
            }
            "$setOnInsert" => {
                append_update_paths(operator, operand, &mut paths)?;
                modifiers.set_on_insert = operand.clone();
            }
            "$push" => {
                append_push_paths(operator, operand, &mut paths)?;
                modifiers.push = operand.clone();
            }
            "$addToSet" => {
                append_add_to_set_paths(operator, operand, &mut paths)?;
                modifiers.add_to_set = operand.clone();
            }
            "$pop" => {
                append_pop_paths(operator, operand, &mut paths)?;
                modifiers.pop = operand.clone();
            }
            "$pull" => {
                append_update_paths(operator, operand, &mut paths)?;
                modifiers.pull = operand.clone();
            }
            "$pullAll" => {
                append_pull_all_paths(operator, operand, &mut paths)?;
                modifiers.pull_all = operand.clone();
            }
            _ => return Err(format!("unsupported update operator {operator}")),
        }
    }
    if modifiers.is_empty() {
        return Err("modifier update must contain at least one path".to_string());
    }
    reject_path_collisions(&paths, "update")?;
    Ok(UpdateSpec::Modifier(modifiers))
}

fn parse_update_pipeline(stages: &[Bson]) -> std::result::Result<Vec<UpdatePipelineStage>, String> {
    if stages.is_empty() {
        return Err("update pipeline must not be empty".to_string());
    }
    let mut parsed = Vec::new();
    for stage in stages {
        let Bson::Document(stage) = stage else {
            return Err("update pipeline stages must be documents".to_string());
        };
        if stage.len() != 1 {
            return Err("update pipeline stages must contain one operator".to_string());
        }
        let (operator, operand) = stage.iter().next().expect("stage length checked above");
        let parsed_stage = match operator.as_str() {
            "$set" | "$addFields" => {
                let Bson::Document(spec) = operand else {
                    return Err(format!("{operator} requires a document"));
                };
                let fields =
                    parse_aggregate_add_fields_stage(spec, operator).map_err(command_errmsg)?;
                validate_aggregation_computed_fields_static(&fields).map_err(command_errmsg)?;
                UpdatePipelineStage::AddFields(fields)
            }
            "$unset" => {
                let unset = parse_aggregate_unset_stage(operand).map_err(command_errmsg)?;
                UpdatePipelineStage::Unset(unset)
            }
            "$project" => {
                let Bson::Document(projection) = operand else {
                    return Err("$project requires a document".to_string());
                };
                let projection =
                    parse_aggregate_project_stage(projection).map_err(command_errmsg)?;
                if let Some(projection) = &projection {
                    validate_aggregate_project_static_expressions(projection)
                        .map_err(command_errmsg)?;
                }
                UpdatePipelineStage::Project(projection)
            }
            "$replaceRoot" | "$replaceWith" => {
                let replacement = parse_aggregate_replace_root_stage(operator, operand)
                    .map_err(command_errmsg)?;
                validate_aggregate_replace_root_static_expression(&replacement)
                    .map_err(command_errmsg)?;
                UpdatePipelineStage::ReplaceRoot(replacement)
            }
            _ => return Err(format!("update pipeline stage {operator} is not supported")),
        };
        parsed.push(parsed_stage);
    }
    Ok(parsed)
}

fn command_errmsg(document: Document) -> String {
    document
        .get_str("errmsg")
        .unwrap_or("command failed")
        .to_string()
}

fn parse_array_filters(
    value: Option<&Bson>,
    update: &UpdateSpec,
) -> std::result::Result<ArrayFilterSet, String> {
    let used = update.used_array_filter_ids()?;
    let Some(value) = value else {
        if !used.is_empty() {
            return Err(
                "arrayFilters must be specified for filtered positional updates".to_string(),
            );
        }
        return Ok(ArrayFilterSet::default());
    };
    let Bson::Array(filters) = value else {
        return Err("arrayFilters must be an array".to_string());
    };
    if filters.is_empty() {
        return Err("arrayFilters must not be empty".to_string());
    }
    if used.is_empty() {
        return Err("arrayFilters contains unused filters".to_string());
    }

    let mut parsed = HashMap::new();
    for filter in filters {
        let Bson::Document(filter) = filter else {
            return Err("arrayFilters entries must be documents".to_string());
        };
        if filter.is_empty() {
            return Err("arrayFilters entries must be non-empty documents".to_string());
        }
        let mut identifier = None::<String>;
        let mut document = Document::new();
        let mut root = None::<Bson>;
        for (path, condition) in filter {
            let (candidate, rest) = split_array_filter_path(path)?;
            match &identifier {
                Some(existing) if existing != candidate => {
                    return Err("arrayFilters entries must use one identifier".to_string());
                }
                None => identifier = Some(candidate.to_string()),
                _ => {}
            }
            if rest.is_empty() {
                if root.is_some() {
                    return Err("arrayFilters root predicate is duplicated".to_string());
                }
                validate_array_filter_condition(condition)?;
                root = Some(condition.clone());
            } else {
                validate_filter_shape(&doc! { rest.to_string(): condition.clone() })
                    .map_err(|err| err.errmsg)?;
                document.insert(rest, condition.clone());
            }
        }
        let identifier = identifier.expect("non-empty filter checked above");
        if parsed
            .insert(identifier.clone(), ArrayFilterPredicate { document, root })
            .is_some()
        {
            return Err(format!(
                "arrayFilters contains duplicate identifier {identifier}"
            ));
        }
    }

    for identifier in &used {
        if !parsed.contains_key(identifier) {
            return Err(format!("arrayFilters missing identifier {identifier}"));
        }
    }
    for identifier in parsed.keys() {
        if !used.contains(identifier) {
            return Err(format!(
                "arrayFilters contains unused identifier {identifier}"
            ));
        }
    }
    Ok(ArrayFilterSet { filters: parsed })
}

fn validate_array_filter_condition(condition: &Bson) -> std::result::Result<(), String> {
    if let Bson::Document(operators) = condition
        && operators.keys().any(|key| key.starts_with('$'))
    {
        validate_operator_document_shape(operators).map_err(|err| err.errmsg)?;
        return Ok(());
    }
    validate_filter_shape(&doc! { "value": condition.clone() }).map_err(|err| err.errmsg)
}

fn split_array_filter_path(path: &str) -> std::result::Result<(&str, &str), String> {
    let mut parts = path.splitn(2, '.');
    let identifier = parts.next().unwrap_or_default();
    if !is_valid_array_filter_identifier(identifier) {
        return Err(format!("arrayFilters identifier {identifier} is invalid"));
    }
    Ok((identifier, parts.next().unwrap_or_default()))
}

fn is_valid_array_filter_identifier(identifier: &str) -> bool {
    let mut chars = identifier.chars();
    matches!(chars.next(), Some(first) if first.is_ascii_lowercase())
        && chars.all(|ch| ch.is_ascii_alphanumeric() || ch == '_')
}

impl UpdateSpec {
    fn used_array_filter_ids(&self) -> std::result::Result<HashSet<String>, String> {
        let mut used = HashSet::new();
        if let UpdateSpec::Modifier(modifiers) = self {
            for path in modifiers.positional_candidate_paths() {
                for segment in parse_update_path("$[identifier]", path, true)? {
                    if let UpdatePathSegment::Filtered(identifier) = segment {
                        used.insert(identifier);
                    }
                }
            }
        }
        Ok(used)
    }
}

impl UpdateModifiers {
    fn positional_candidate_paths(&self) -> Vec<&str> {
        self.set
            .keys()
            .chain(self.unset.keys())
            .chain(self.inc.keys())
            .chain(self.min.keys())
            .chain(self.max.keys())
            .chain(self.mul.keys())
            .map(String::as_str)
            .collect()
    }
}

fn append_update_paths(
    operator: &str,
    document: &Document,
    paths: &mut Vec<String>,
) -> std::result::Result<(), String> {
    for key in document.keys() {
        validate_update_path_for_operator(operator, key)?;
        paths.push(key.to_string());
    }
    Ok(())
}

fn append_rename_paths(
    operator: &str,
    document: &Document,
    paths: &mut Vec<String>,
) -> std::result::Result<(), String> {
    for (source, destination) in document {
        validate_update_path(operator, source)?;
        let Bson::String(destination) = destination else {
            return Err("$rename destinations must be strings".to_string());
        };
        validate_update_path(operator, destination)?;
        paths.push(source.to_string());
        paths.push(destination.to_string());
    }
    Ok(())
}

fn append_push_paths(
    operator: &str,
    document: &Document,
    paths: &mut Vec<String>,
) -> std::result::Result<(), String> {
    for (path, operand) in document {
        validate_update_path(operator, path)?;
        parse_each_operand("$push", operand)?;
        paths.push(path.to_string());
    }
    Ok(())
}

fn append_add_to_set_paths(
    operator: &str,
    document: &Document,
    paths: &mut Vec<String>,
) -> std::result::Result<(), String> {
    for (path, operand) in document {
        validate_update_path(operator, path)?;
        parse_each_operand("$addToSet", operand)?;
        paths.push(path.to_string());
    }
    Ok(())
}

fn append_pop_paths(
    operator: &str,
    document: &Document,
    paths: &mut Vec<String>,
) -> std::result::Result<(), String> {
    for (path, operand) in document {
        validate_update_path(operator, path)?;
        match operand {
            Bson::Int32(1) | Bson::Int64(1) | Bson::Int32(-1) | Bson::Int64(-1) => {}
            _ => return Err("$pop operands must be 1 or -1".to_string()),
        }
        paths.push(path.to_string());
    }
    Ok(())
}

fn append_pull_all_paths(
    operator: &str,
    document: &Document,
    paths: &mut Vec<String>,
) -> std::result::Result<(), String> {
    for (path, operand) in document {
        validate_update_path(operator, path)?;
        if !matches!(operand, Bson::Array(_)) {
            return Err("$pullAll operands must be arrays".to_string());
        }
        paths.push(path.to_string());
    }
    Ok(())
}

fn parse_each_operand(operator: &str, operand: &Bson) -> std::result::Result<Vec<Bson>, String> {
    match operand {
        Bson::Document(document) if document.keys().any(|key| key.starts_with('$')) => {
            if document.keys().any(|key| !key.starts_with('$')) {
                return Err(format!(
                    "{operator} option documents cannot mix literal fields"
                ));
            }
            for key in document.keys() {
                if key != "$each" {
                    return Err(format!("{operator} option {key} is not supported"));
                }
            }
            let Some(Bson::Array(values)) = document.get("$each") else {
                return Err(format!("{operator} $each must be an array"));
            };
            Ok(values.clone())
        }
        _ => Ok(vec![operand.clone()]),
    }
}

fn validate_update_path(operator: &str, path: &str) -> std::result::Result<(), String> {
    parse_update_path(operator, path, false).map(|_| ())
}

fn validate_update_path_for_operator(
    operator: &str,
    path: &str,
) -> std::result::Result<(), String> {
    parse_update_path(operator, path, positional_operator_supported(operator)).map(|_| ())
}

fn positional_operator_supported(operator: &str) -> bool {
    matches!(
        operator,
        "$set" | "$unset" | "$inc" | "$min" | "$max" | "$mul"
    )
}

#[derive(Clone, Debug, PartialEq)]
enum UpdatePathSegment {
    Field(String),
    Positional,
    AllPositional,
    Filtered(String),
}

fn parse_update_path(
    operator: &str,
    path: &str,
    allow_positional: bool,
) -> std::result::Result<Vec<UpdatePathSegment>, String> {
    if path.is_empty() {
        return Err(format!("{operator} contains empty update path"));
    }
    if path.starts_with('$') {
        return Err(format!("{operator} contains unsupported path {path}"));
    }
    let mut segments = Vec::new();
    let mut positional_count = 0;
    for segment in path.split('.') {
        if segment.is_empty() {
            return Err(format!("{operator} contains unsupported path {path}"));
        }
        let parsed = match segment {
            "$" => UpdatePathSegment::Positional,
            "$[]" => UpdatePathSegment::AllPositional,
            _ if segment.starts_with("$[") && segment.ends_with(']') => {
                let identifier = &segment[2..segment.len() - 1];
                if !is_valid_array_filter_identifier(identifier) {
                    return Err(format!(
                        "{operator} contains invalid array filter identifier"
                    ));
                }
                UpdatePathSegment::Filtered(identifier.to_string())
            }
            _ if segment.contains('$') => {
                return Err(format!(
                    "{operator} contains unsupported positional path {path}"
                ));
            }
            _ => UpdatePathSegment::Field(segment.to_string()),
        };
        if !matches!(parsed, UpdatePathSegment::Field(_)) {
            if !allow_positional {
                return Err(format!("{operator} contains positional path {path}"));
            }
            positional_count += 1;
            if positional_count > 1 {
                return Err(format!(
                    "{operator} contains multiple positional segments {path}"
                ));
            }
        }
        segments.push(parsed);
    }
    if path == "_id" || path.starts_with("_id.") {
        return Err("update cannot change _id".to_string());
    }
    if segments
        .iter()
        .any(|segment| matches!(segment, UpdatePathSegment::Field(field) if field == "_id"))
    {
        return Err("update cannot change _id".to_string());
    }
    Ok(segments)
}

fn apply_update_to_document(
    original: &Document,
    update: &ParsedUpdate,
    query: &Document,
    collation: &Collation,
) -> std::result::Result<Document, String> {
    apply_update_to_document_for_context(original, update, false, query, collation)
}

fn apply_update_to_document_for_context(
    original: &Document,
    update: &ParsedUpdate,
    is_upsert_insert: bool,
    query: &Document,
    collation: &Collation,
) -> std::result::Result<Document, String> {
    match &update.spec {
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
        UpdateSpec::Pipeline(stages) => apply_update_pipeline(original, stages, collation),
        UpdateSpec::Modifier(modifiers) => {
            let mut document = original.clone();
            for (path, value) in &modifiers.set {
                apply_scalar_modifier_path(
                    &mut document,
                    "$set",
                    path,
                    Some(value),
                    query,
                    &update.array_filters,
                    collation,
                )?;
            }
            if is_upsert_insert {
                for (path, value) in &modifiers.set_on_insert {
                    set_update_path(&mut document, path, value.clone())?;
                }
            }
            for path in modifiers.unset.keys() {
                apply_scalar_modifier_path(
                    &mut document,
                    "$unset",
                    path,
                    None,
                    query,
                    &update.array_filters,
                    collation,
                )?;
            }
            for (path, operand) in &modifiers.inc {
                apply_scalar_modifier_path(
                    &mut document,
                    "$inc",
                    path,
                    Some(operand),
                    query,
                    &update.array_filters,
                    collation,
                )?;
            }
            for (source, destination) in &modifiers.rename {
                let Bson::String(destination) = destination else {
                    return Err("$rename destinations must be strings".to_string());
                };
                rename_update_path(&mut document, source, destination)?;
            }
            for (path, operand) in &modifiers.min {
                apply_scalar_modifier_path(
                    &mut document,
                    "$min",
                    path,
                    Some(operand),
                    query,
                    &update.array_filters,
                    collation,
                )?;
            }
            for (path, operand) in &modifiers.max {
                apply_scalar_modifier_path(
                    &mut document,
                    "$max",
                    path,
                    Some(operand),
                    query,
                    &update.array_filters,
                    collation,
                )?;
            }
            for (path, operand) in &modifiers.mul {
                apply_scalar_modifier_path(
                    &mut document,
                    "$mul",
                    path,
                    Some(operand),
                    query,
                    &update.array_filters,
                    collation,
                )?;
            }
            for (path, operand) in &modifiers.push {
                push_update_path(&mut document, path, operand)?;
            }
            for (path, operand) in &modifiers.add_to_set {
                add_to_set_update_path(&mut document, path, operand)?;
            }
            for (path, operand) in &modifiers.pop {
                pop_update_path(&mut document, path, operand)?;
            }
            for (path, operand) in &modifiers.pull {
                pull_update_path(&mut document, path, operand)?;
            }
            for (path, operand) in &modifiers.pull_all {
                pull_all_update_path(&mut document, path, operand)?;
            }
            Ok(document)
        }
    }
}

fn apply_update_pipeline(
    original: &Document,
    stages: &[UpdatePipelineStage],
    collation: &Collation,
) -> std::result::Result<Document, String> {
    let original_id = original
        .get("_id")
        .cloned()
        .ok_or_else(|| "pipeline update requires _id".to_string())?;
    let mut document = original.clone();
    for stage in stages {
        document = match stage {
            UpdatePipelineStage::AddFields(fields) => {
                apply_aggregate_add_fields_stage(document, fields, collation)
                    .map_err(command_errmsg)?
            }
            UpdatePipelineStage::Unset(unset) => {
                let mut next = document;
                for path in &unset.paths {
                    unset_document_path(&mut next, path);
                }
                next
            }
            UpdatePipelineStage::Project(None) => Document::new(),
            UpdatePipelineStage::Project(Some(projection)) => {
                apply_aggregate_project_stage(&document, projection, collation)
                    .map_err(command_errmsg)?
            }
            UpdatePipelineStage::ReplaceRoot(replacement) => {
                apply_aggregate_replace_root_stage(&document, replacement, collation)
                    .map_err(command_errmsg)?
            }
        };
    }
    match document.get("_id") {
        Some(new_id) if bson_values_equal(new_id, &original_id) => Ok(document),
        Some(_) => Err("pipeline update cannot change _id".to_string()),
        None => Err("pipeline update cannot remove _id".to_string()),
    }
}

fn update_contains_positional_paths(update: &ParsedUpdate) -> std::result::Result<bool, String> {
    let UpdateSpec::Modifier(modifiers) = &update.spec else {
        return Ok(false);
    };
    for path in modifiers.positional_candidate_paths() {
        if parse_update_path("$positional", path, true)?
            .iter()
            .any(|segment| !matches!(segment, UpdatePathSegment::Field(_)))
        {
            return Ok(true);
        }
    }
    Ok(false)
}

fn apply_scalar_modifier_path(
    document: &mut Document,
    operator: &str,
    path: &str,
    operand: Option<&Bson>,
    query: &Document,
    array_filters: &ArrayFilterSet,
    collation: &Collation,
) -> std::result::Result<(), String> {
    let segments = parse_update_path(operator, path, true)?;
    if segments
        .iter()
        .all(|segment| matches!(segment, UpdatePathSegment::Field(_)))
    {
        return apply_scalar_modifier_plain(document, operator, path, operand);
    }
    apply_positional_scalar_modifier(
        document,
        operator,
        operand,
        &segments,
        query,
        array_filters,
        collation,
    )
}

fn apply_scalar_modifier_plain(
    document: &mut Document,
    operator: &str,
    path: &str,
    operand: Option<&Bson>,
) -> std::result::Result<(), String> {
    match operator {
        "$set" => set_update_path(
            document,
            path,
            operand.expect("$set operand provided").clone(),
        ),
        "$unset" => unset_update_path(document, path),
        "$inc" => inc_update_path(document, path, operand.expect("$inc operand provided")),
        "$min" => min_update_path(document, path, operand.expect("$min operand provided")),
        "$max" => max_update_path(document, path, operand.expect("$max operand provided")),
        "$mul" => mul_update_path(document, path, operand.expect("$mul operand provided")),
        _ => Err(format!("unsupported positional modifier {operator}")),
    }
}

fn apply_positional_scalar_modifier(
    document: &mut Document,
    operator: &str,
    operand: Option<&Bson>,
    segments: &[UpdatePathSegment],
    query: &Document,
    array_filters: &ArrayFilterSet,
    collation: &Collation,
) -> std::result::Result<(), String> {
    let Some(positional_index) = segments
        .iter()
        .position(|segment| !matches!(segment, UpdatePathSegment::Field(_)))
    else {
        return Err("positional update path is missing positional segment".to_string());
    };
    if positional_index == 0 {
        return Err("positional update requires an array field prefix".to_string());
    }
    let array_path = field_segments_to_path(&segments[..positional_index])?;
    let tail_path = field_segments_to_path(&segments[positional_index + 1..])?;
    let selected_indices = match &segments[positional_index] {
        UpdatePathSegment::Positional => {
            vec![first_matching_array_index(
                document,
                &array_path,
                query,
                collation,
            )?]
        }
        UpdatePathSegment::AllPositional => {
            let len = array_values_at_path(document, &array_path)?.len();
            (0..len).collect()
        }
        UpdatePathSegment::Filtered(identifier) => {
            let predicate = array_filters
                .filters
                .get(identifier)
                .ok_or_else(|| format!("arrayFilters missing identifier {identifier}"))?;
            array_filter_matching_indices(document, &array_path, predicate, collation)?
        }
        UpdatePathSegment::Field(_) => unreachable!("position selected non-field"),
    };

    let mut values = array_values_at_path(document, &array_path)?.clone();
    for index in selected_indices {
        let Some(value) = values.get_mut(index) else {
            return Err("positional update resolved out of bounds element".to_string());
        };
        apply_scalar_modifier_to_array_element(value, operator, &tail_path, operand)?;
    }
    set_update_path(document, &array_path, Bson::Array(values))
}

fn field_segments_to_path(segments: &[UpdatePathSegment]) -> std::result::Result<String, String> {
    let mut parts = Vec::new();
    for segment in segments {
        let UpdatePathSegment::Field(field) = segment else {
            return Err("nested positional paths are not supported".to_string());
        };
        parts.push(field.as_str());
    }
    Ok(parts.join("."))
}

fn array_values_at_path<'a>(
    document: &'a Document,
    array_path: &str,
) -> std::result::Result<&'a Vec<Bson>, String> {
    match get_update_path_checked(document, array_path)? {
        Some(Bson::Array(values)) => {
            if values.iter().any(|value| matches!(value, Bson::Array(_))) {
                return Err("nested array positional updates are not supported".to_string());
            }
            Ok(values)
        }
        Some(_) => Err(format!(
            "{array_path} must be an array for positional update"
        )),
        None => Err(format!("{array_path} must exist for positional update")),
    }
}

fn apply_scalar_modifier_to_array_element(
    value: &mut Bson,
    operator: &str,
    tail_path: &str,
    operand: Option<&Bson>,
) -> std::result::Result<(), String> {
    if tail_path.is_empty() {
        let mut wrapper = doc! { "value": value.clone() };
        apply_scalar_modifier_plain(&mut wrapper, operator, "value", operand)?;
        *value = wrapper.remove("value").unwrap_or(Bson::Null);
        return Ok(());
    }
    let Bson::Document(document) = value else {
        return Err("positional update cannot traverse scalar array element".to_string());
    };
    apply_scalar_modifier_plain(document, operator, tail_path, operand)
}

fn first_matching_array_index(
    document: &Document,
    array_path: &str,
    query: &Document,
    collation: &Collation,
) -> std::result::Result<usize, String> {
    let values = array_values_at_path(document, array_path)?;
    let predicates = positional_query_predicates(array_path, query)?;
    for (index, value) in values.iter().enumerate() {
        if positional_element_matches(value, &predicates, collation)? {
            return Ok(index);
        }
    }
    Err(format!(
        "query does not match an element for positional path {array_path}"
    ))
}

fn positional_query_predicates(
    array_path: &str,
    query: &Document,
) -> std::result::Result<Vec<PositionalQueryPredicate>, String> {
    if query.keys().any(|key| key.starts_with('$')) {
        return Err("positional $ requires a direct array predicate".to_string());
    }
    let mut predicates = Vec::new();
    for (field, condition) in query {
        if field == array_path {
            if let Bson::Document(document) = condition
                && let Some(elem_match) = document.get("$elemMatch")
            {
                if !matches!(elem_match, Bson::Document(_)) {
                    return Err("$elemMatch requires a document".to_string());
                }
                validate_elem_match_shape(elem_match).map_err(|err| err.errmsg)?;
                predicates.push(PositionalQueryPredicate::ElemMatch(elem_match.clone()));
                continue;
            }
            validate_array_filter_condition(condition)?;
            predicates.push(PositionalQueryPredicate::ArrayFilter(
                ArrayFilterPredicate {
                    document: Document::new(),
                    root: Some(condition.clone()),
                },
            ));
        } else if let Some(rest) = field.strip_prefix(&format!("{array_path}.")) {
            validate_filter_shape(&doc! { rest.to_string(): condition.clone() })
                .map_err(|err| err.errmsg)?;
            predicates.push(PositionalQueryPredicate::ArrayFilter(
                ArrayFilterPredicate {
                    document: doc! { rest.to_string(): condition.clone() },
                    root: None,
                },
            ));
        }
    }
    if predicates.is_empty() {
        return Err("positional $ requires a supported array predicate".to_string());
    }
    Ok(predicates)
}

#[derive(Clone, Debug)]
enum PositionalQueryPredicate {
    ArrayFilter(ArrayFilterPredicate),
    ElemMatch(Bson),
}

fn positional_element_matches(
    value: &Bson,
    predicates: &[PositionalQueryPredicate],
    collation: &Collation,
) -> std::result::Result<bool, String> {
    for predicate in predicates {
        let matched = match predicate {
            PositionalQueryPredicate::ArrayFilter(predicate) => {
                array_filter_predicate_matches(value, predicate, collation)?
            }
            PositionalQueryPredicate::ElemMatch(operand) => {
                matches_elem_match_value(value, operand, collation).map_err(|err| err.errmsg)?
            }
        };
        if !matched {
            return Ok(false);
        }
    }
    Ok(true)
}

fn array_filter_matching_indices(
    document: &Document,
    array_path: &str,
    predicate: &ArrayFilterPredicate,
    collation: &Collation,
) -> std::result::Result<Vec<usize>, String> {
    let mut indices = Vec::new();
    for (index, value) in array_values_at_path(document, array_path)?
        .iter()
        .enumerate()
    {
        if array_filter_predicate_matches(value, predicate, collation)? {
            indices.push(index);
        }
    }
    Ok(indices)
}

fn array_filter_predicate_matches(
    value: &Bson,
    predicate: &ArrayFilterPredicate,
    collation: &Collation,
) -> std::result::Result<bool, String> {
    if let Some(root) = &predicate.root {
        let matched = if let Bson::Document(operators) = root
            && operators.keys().any(|key| key.starts_with('$'))
        {
            matches_operator_document_with_collation(&[value], operators, collation)
                .map_err(|err| err.errmsg)?
        } else {
            bson_values_equal_with_collation(value, root, collation)
        };
        if !matched {
            return Ok(false);
        }
    }
    if !predicate.document.is_empty() {
        let Bson::Document(document) = value else {
            return Ok(false);
        };
        if !matches_filter_with_collation(document, &predicate.document, collation)
            .map_err(|err| err.errmsg)?
        {
            return Ok(false);
        }
    }
    Ok(true)
}

fn build_upsert_document(
    query: &Document,
    update: &ParsedUpdate,
    collation: &Collation,
) -> std::result::Result<Document, String> {
    match &update.spec {
        UpdateSpec::Replacement(replacement) => {
            let mut document = replacement.clone();
            if !document.contains_key("_id")
                && let Some(id) = equality_id_from_filter(query)
            {
                document.insert("_id", id.clone());
            }
            Ok(document)
        }
        UpdateSpec::Modifier(_) => {
            if update_contains_positional_paths(update)? {
                return Err("positional updates are not supported for upsert inserts".to_string());
            }
            let mut document = equality_document_from_filter(query)?;
            document =
                apply_update_to_document_for_context(&document, update, true, query, collation)?;
            Ok(document)
        }
        UpdateSpec::Pipeline(stages) => {
            let mut document = equality_document_from_filter(query)?;
            ensure_document_id(&mut document);
            apply_update_pipeline(&document, stages, collation)
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

fn take_update_path(
    document: &mut Document,
    path: &str,
) -> std::result::Result<Option<Bson>, String> {
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
            None => return Ok(None),
        }
    }
    Ok(current.remove(last))
}

fn get_update_path_checked<'a>(
    document: &'a Document,
    path: &str,
) -> std::result::Result<Option<&'a Bson>, String> {
    let mut parts = path.split('.');
    let Some(first) = parts.next() else {
        return Err("update path must not be empty".to_string());
    };
    let Some(mut current) = document.get(first) else {
        return Ok(None);
    };
    for part in parts {
        let Bson::Document(nested) = current else {
            return Err(format!("cannot traverse scalar parent {part}"));
        };
        let Some(next) = nested.get(part) else {
            return Ok(None);
        };
        current = next;
    }
    Ok(Some(current))
}

fn rename_update_path(
    document: &mut Document,
    source: &str,
    destination: &str,
) -> std::result::Result<(), String> {
    let Some(value) = take_update_path(document, source)? else {
        return Ok(());
    };
    set_update_path(document, destination, value)
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

fn min_update_path(
    document: &mut Document,
    path: &str,
    operand: &Bson,
) -> std::result::Result<(), String> {
    let existing = get_document_path(document, path).cloned();
    match existing {
        None => set_update_path(document, path, operand.clone()),
        Some(current) if compare_bson_order(&current, operand).is_gt() => {
            set_update_path(document, path, operand.clone())
        }
        Some(_) => Ok(()),
    }
}

fn max_update_path(
    document: &mut Document,
    path: &str,
    operand: &Bson,
) -> std::result::Result<(), String> {
    let existing = get_document_path(document, path).cloned();
    match existing {
        None => set_update_path(document, path, operand.clone()),
        Some(current) if compare_bson_order(&current, operand).is_lt() => {
            set_update_path(document, path, operand.clone())
        }
        Some(_) => Ok(()),
    }
}

fn mul_update_path(
    document: &mut Document,
    path: &str,
    operand: &Bson,
) -> std::result::Result<(), String> {
    if numeric_value(operand).is_none() {
        return Err("$mul operands must be numeric".to_string());
    }
    let existing = get_document_path(document, path).cloned();
    match existing {
        None => set_update_path(document, path, zero_for_numeric_operand(operand)),
        Some(current) if numeric_value(&current).is_some() => {
            let updated = multiply_numeric_bson(&current, operand)?;
            set_update_path(document, path, updated)
        }
        Some(_) => Err("$mul can only apply to numeric fields".to_string()),
    }
}

fn push_update_path(
    document: &mut Document,
    path: &str,
    operand: &Bson,
) -> std::result::Result<(), String> {
    let mut values = match get_update_path_checked(document, path)?.cloned() {
        None => Vec::new(),
        Some(Bson::Array(values)) => values,
        Some(_) => return Err("$push can only apply to array fields".to_string()),
    };
    values.extend(parse_each_operand("$push", operand)?);
    set_update_path(document, path, Bson::Array(values))
}

fn add_to_set_update_path(
    document: &mut Document,
    path: &str,
    operand: &Bson,
) -> std::result::Result<(), String> {
    let mut values = match get_update_path_checked(document, path)?.cloned() {
        None => Vec::new(),
        Some(Bson::Array(values)) => values,
        Some(_) => return Err("$addToSet can only apply to array fields".to_string()),
    };
    for value in parse_each_operand("$addToSet", operand)? {
        if values
            .iter()
            .all(|existing| !update_values_equal(existing, &value))
        {
            values.push(value);
        }
    }
    set_update_path(document, path, Bson::Array(values))
}

fn pop_update_path(
    document: &mut Document,
    path: &str,
    operand: &Bson,
) -> std::result::Result<(), String> {
    let Some(existing) = get_update_path_checked(document, path)?.cloned() else {
        return Ok(());
    };
    let Bson::Array(mut values) = existing else {
        return Err("$pop can only apply to array fields".to_string());
    };
    match operand {
        Bson::Int32(1) | Bson::Int64(1) => {
            values.pop();
        }
        Bson::Int32(-1) | Bson::Int64(-1) => {
            if !values.is_empty() {
                values.remove(0);
            }
        }
        _ => return Err("$pop operands must be 1 or -1".to_string()),
    }
    set_update_path(document, path, Bson::Array(values))
}

fn pull_update_path(
    document: &mut Document,
    path: &str,
    operand: &Bson,
) -> std::result::Result<(), String> {
    let Some(existing) = get_update_path_checked(document, path)?.cloned() else {
        return Ok(());
    };
    let Bson::Array(values) = existing else {
        return Err("$pull can only apply to array fields".to_string());
    };
    let mut retained = Vec::new();
    for value in values {
        if !pull_matches(&value, operand)? {
            retained.push(value);
        }
    }
    set_update_path(document, path, Bson::Array(retained))
}

fn pull_all_update_path(
    document: &mut Document,
    path: &str,
    operand: &Bson,
) -> std::result::Result<(), String> {
    let Bson::Array(needles) = operand else {
        return Err("$pullAll operands must be arrays".to_string());
    };
    let Some(existing) = get_update_path_checked(document, path)?.cloned() else {
        return Ok(());
    };
    let Bson::Array(values) = existing else {
        return Err("$pullAll can only apply to array fields".to_string());
    };
    let retained = values
        .into_iter()
        .filter(|value| {
            needles
                .iter()
                .all(|needle| !update_values_equal(value, needle))
        })
        .collect::<Vec<_>>();
    set_update_path(document, path, Bson::Array(retained))
}

fn pull_matches(value: &Bson, condition: &Bson) -> std::result::Result<bool, String> {
    if let (Bson::Document(value), Bson::Document(condition)) = (value, condition)
        && condition
            .keys()
            .all(|key| !key.starts_with('$') || matches!(key.as_str(), "$and" | "$or" | "$nor"))
    {
        return matches_filter(value, condition).map_err(|err| err.errmsg);
    }
    if let Bson::Document(document) = condition
        && document.keys().any(|key| key.starts_with('$'))
    {
        return matches_operator_document(&[value], document).map_err(|err| err.errmsg);
    }
    Ok(update_values_equal(value, condition))
}

fn update_values_equal(candidate: &Bson, expected: &Bson) -> bool {
    match (numeric_value(candidate), numeric_value(expected)) {
        (Some(left), Some(right)) => left == right,
        _ => candidate == expected,
    }
}

fn zero_for_numeric_operand(operand: &Bson) -> Bson {
    match operand {
        Bson::Double(_) => Bson::Double(0.0),
        Bson::Int64(_) => Bson::Int64(0),
        _ => Bson::Int32(0),
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

fn multiply_numeric_bson(left: &Bson, right: &Bson) -> std::result::Result<Bson, String> {
    match (left, right) {
        (Bson::Double(left), _) | (_, Bson::Double(left)) if left.is_nan() => {
            Err("$mul does not support NaN".to_string())
        }
        (Bson::Double(_), _) | (_, Bson::Double(_)) => Ok(Bson::Double(
            numeric_value(left).unwrap() * numeric_value(right).unwrap(),
        )),
        (Bson::Int32(left), Bson::Int32(right)) => {
            let product = (*left as i64) * (*right as i64);
            if (i32::MIN as i64..=i32::MAX as i64).contains(&product) {
                Ok(Bson::Int32(product as i32))
            } else {
                Ok(Bson::Int64(product))
            }
        }
        (Bson::Int64(left), Bson::Int64(right)) => left
            .checked_mul(*right)
            .map(Bson::Int64)
            .ok_or_else(|| "$mul overflowed int64".to_string()),
        (Bson::Int64(left), Bson::Int32(right)) => left
            .checked_mul(*right as i64)
            .map(Bson::Int64)
            .ok_or_else(|| "$mul overflowed int64".to_string()),
        (Bson::Int32(left), Bson::Int64(right)) => (*left as i64)
            .checked_mul(*right)
            .map(Bson::Int64)
            .ok_or_else(|| "$mul overflowed int64".to_string()),
        _ => unreachable!("non-numeric $mul operands should be rejected before multiplication"),
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
    if let Some(errmsg) = reject_unsupported_command_keys(
        command,
        &[
            "delete",
            "deletes",
            "ordered",
            "writeConcern",
            "txnNumber",
            "$db",
            "lsid",
        ],
    ) {
        return Ok(command_error(72, &errmsg));
    }

    let namespace = namespace(db, collection);
    let tx = conn.unchecked_transaction()?;
    let mut removed = 0_i32;
    let mut write_errors = Vec::new();
    let mut swept = false;

    for (index, entry) in deletes.iter().enumerate() {
        if let Err(errmsg) = validate_delete_entry_shape_tx(&tx, &namespace, entry) {
            write_errors.push(write_error(index as i32, 2, &errmsg));
            if ordered {
                break;
            }
            continue;
        }
        if !swept {
            sweep_ttl_namespace_at_tx(&tx, &namespace, bson::DateTime::now())?;
            swept = true;
        }
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

fn validate_delete_entry_shape_tx(
    tx: &rusqlite::Transaction<'_>,
    namespace: &str,
    entry: &Bson,
) -> std::result::Result<(), String> {
    let Bson::Document(entry) = entry else {
        return Err("delete entries must be documents".to_string());
    };
    reject_unsupported_entry_keys(entry, &["q", "limit", "hint", "collation"])?;
    let query = entry
        .get_document("q")
        .map_err(|_| "delete entry requires q document".to_string())?;
    validate_filter_shape(query).map_err(|err| err.errmsg)?;
    let collation = Collation::parse_optional(entry, "collation")?;
    validate_filter_for_collation(query, &collation).map_err(|err| err.errmsg)?;
    match entry.get("limit") {
        Some(Bson::Int32(0)) | Some(Bson::Int64(0)) => {}
        Some(Bson::Int32(1)) | Some(Bson::Int64(1)) => {}
        Some(_) => return Err("delete limit must be 0 or 1".to_string()),
        None => return Err("delete entry requires limit".to_string()),
    }
    if let Some(hint) = parse_optional_hint(entry)? {
        let resolved = resolve_hint(
            indexes_for_namespace_tx(tx, namespace).map_err(|err| err.to_string())?,
            hint,
        )?;
        validate_hint_collation(&resolved, &collation, query)?;
    }
    Ok(())
}

fn apply_delete_entry(
    tx: &rusqlite::Transaction<'_>,
    namespace: &str,
    entry: &Bson,
) -> std::result::Result<i32, String> {
    let Bson::Document(entry) = entry else {
        return Err("delete entries must be documents".to_string());
    };
    reject_unsupported_entry_keys(entry, &["q", "limit", "hint", "collation"])?;
    let query = entry
        .get_document("q")
        .map_err(|_| "delete entry requires q document".to_string())?;
    let limit = match entry.get("limit") {
        Some(Bson::Int32(0)) | Some(Bson::Int64(0)) => 0,
        Some(Bson::Int32(1)) | Some(Bson::Int64(1)) => 1,
        Some(_) => return Err("delete limit must be 0 or 1".to_string()),
        None => return Err("delete entry requires limit".to_string()),
    };
    let hint = match parse_optional_hint(entry)? {
        Some(hint) => Some(resolve_hint(
            indexes_for_namespace_tx(tx, namespace).map_err(|err| err.to_string())?,
            hint,
        )?),
        None => None,
    };
    let collation = Collation::parse_optional(entry, "collation")?;

    let mut targets = Vec::new();
    for stored in
        transaction_candidate_documents_with_hint(tx, namespace, query, hint.as_ref(), &collation)?
    {
        match matches_filter_with_collation(&stored.document, query, &collation) {
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
    if let Err(err) = validate_filter_shape(&filter) {
        return Ok(command_error(err.code, &err.errmsg));
    }
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
            "hint",
            "explain",
            "collation",
            "readConcern",
            "$db",
            "lsid",
        ],
    ) {
        return Ok(command_error(72, &errmsg));
    }
    let collation = match Collation::parse_optional(command, "collation") {
        Ok(collation) => collation,
        Err(errmsg) => return Ok(command_error(72, &errmsg)),
    };
    if let Err(err) = validate_filter_for_collation(&filter, &collation) {
        return Ok(command_error(err.code, &err.errmsg));
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
    let explain = match optional_bool(command, "explain") {
        Ok(value) => value.unwrap_or(false),
        Err(errmsg) => return Ok(command_error(9, &errmsg)),
    };
    let ns = namespace(db, collection);
    let hint = match parse_optional_hint(command) {
        Ok(Some(hint)) => match resolve_hint(indexes_for_namespace(conn, &ns)?, hint) {
            Ok(hint) => {
                if let Err(errmsg) = validate_hint_collation(&hint, &collation, &filter) {
                    return Ok(command_error(2, &errmsg));
                }
                Some(hint)
            }
            Err(errmsg) => return Ok(command_error(2, &errmsg)),
        },
        Ok(None) => None,
        Err(errmsg) => return Ok(command_error(2, &errmsg)),
    };
    if explain {
        return match planner_v2_plan_for_find(
            conn,
            &ns,
            &filter,
            sort.as_deref(),
            hint.as_ref(),
            &collation,
        ) {
            Ok(plan) => Ok(explain_response(
                "find",
                &ns,
                &filter,
                hint.is_some(),
                &plan,
            )),
            Err(errmsg) => Ok(command_error(2, &errmsg)),
        };
    }
    sweep_ttl_namespace(conn, &ns)?;

    if hint.is_none()
        && sort.is_none()
        && skip == 0
        && limit.is_none()
        && projection.is_none()
        && batch_size > 0
        && let Some(id_filter) = simple_id_equality_filter(&filter)
        && id_equality_safe_for_collation(id_filter, &collation)
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

    let docs = match query_documents_with_hint(
        conn,
        &ns,
        &filter,
        sort.as_deref(),
        skip,
        limit,
        projection.as_ref(),
        hint.as_ref(),
        &collation,
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
    parse_sort_document(sort).map(Some)
}

fn parse_sort_document(sort: &Document) -> std::result::Result<Vec<(String, i32)>, String> {
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
    Ok(spec)
}

fn sort_documents_with_collation(
    documents: &mut [Document],
    sort: &[(String, i32)],
    collation: &Collation,
) {
    documents.sort_by(|left, right| compare_documents_for_sort(left, right, sort, collation));
}

fn compare_documents_for_sort(
    left: &Document,
    right: &Document,
    sort: &[(String, i32)],
    collation: &Collation,
) -> std::cmp::Ordering {
    for (field, direction) in sort {
        let ordering = compare_optional_bson(
            get_document_path(left, field),
            get_document_path(right, field),
            collation,
        );
        if !ordering.is_eq() {
            return if *direction == 1 {
                ordering
            } else {
                ordering.reverse()
            };
        }
    }
    compare_optional_bson(left.get("_id"), right.get("_id"), collation)
}

fn compare_optional_bson(
    left: Option<&Bson>,
    right: Option<&Bson>,
    collation: &Collation,
) -> std::cmp::Ordering {
    match (left, right) {
        (None, None) => std::cmp::Ordering::Equal,
        (None, Some(_)) => std::cmp::Ordering::Less,
        (Some(_), None) => std::cmp::Ordering::Greater,
        (Some(left), Some(right)) => compare_bson_order_with_collation(left, right, collation),
    }
}

fn compare_bson_order(left: &Bson, right: &Bson) -> std::cmp::Ordering {
    compare_bson_order_with_collation(left, right, &Collation::Simple)
}

fn compare_bson_order_with_collation(
    left: &Bson,
    right: &Bson,
    collation: &Collation,
) -> std::cmp::Ordering {
    collation.compare_order(left, right)
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

fn id_equality_safe_for_collation(value: &Bson, collation: &Collation) -> bool {
    collation.is_simple() || !matches!(value, Bson::String(_))
}

fn indexed_candidate_documents(
    conn: &Connection,
    namespace: &str,
    filter: &Document,
) -> Result<Option<Vec<Document>>> {
    indexed_candidate_documents_with_collation(conn, namespace, filter, &Collation::Simple)
}

fn indexed_candidate_documents_with_collation(
    conn: &Connection,
    namespace: &str,
    filter: &Document,
    collation: &Collation,
) -> Result<Option<Vec<Document>>> {
    for index in planner_indexes(indexes_for_namespace(conn, namespace)?) {
        if index.collation != *collation {
            continue;
        }
        let Some(key_value) = planner_key_for_filter(&index, filter) else {
            continue;
        };
        if !filter_implies_index_membership(&index, filter) {
            continue;
        }
        if !index_entries_safe_for_planner(conn, namespace, &index.name)? {
            continue;
        }
        return indexed_candidate_documents_by_key(conn, namespace, &index.name, &key_value);
    }
    for index in planner_indexes(indexes_for_namespace(conn, namespace)?) {
        if index.collation != *collation || !collation.is_simple() {
            continue;
        }
        let Some(range) = range_planner_key_for_filter(&index, filter) else {
            continue;
        };
        if !filter_implies_index_membership(&index, filter) {
            continue;
        }
        if !index_entries_safe_for_planner(conn, namespace, &index.name)? {
            continue;
        }
        return indexed_candidate_documents_by_range(conn, namespace, &index.name, &range);
    }
    for index in planner_indexes(indexes_for_namespace(conn, namespace)?) {
        if index.collation != *collation {
            continue;
        }
        let Some((_, key_value)) = prefix_planner_key_for_filter(&index, filter) else {
            continue;
        };
        if !filter_implies_index_membership(&index, filter) {
            continue;
        }
        if !index_entries_safe_for_planner(conn, namespace, &index.name)? {
            continue;
        }
        return indexed_candidate_documents_by_key(conn, namespace, &index.name, &key_value);
    }
    Ok(None)
}

fn stored_document_by_id_key(
    conn: &Connection,
    namespace: &str,
    id_key: &str,
) -> Result<Option<Document>> {
    conn.query_row(
        "SELECT bson FROM documents WHERE namespace = ?1 AND id_key = ?2",
        params![namespace, id_key],
        |row| row.get::<_, Vec<u8>>(0),
    )
    .optional()?
    .map(decode_document)
    .transpose()
}

fn hinted_candidate_documents(
    conn: &Connection,
    namespace: &str,
    filter: &Document,
    hint: &ResolvedHint,
    collation: &Collation,
) -> std::result::Result<Vec<Document>, String> {
    match hint {
        ResolvedHint::Id => {
            let Some(value) = exact_equality_filter_value(filter, "_id") else {
                return Err("hint _id_ is incompatible with this filter".to_string());
            };
            if !id_equality_safe_for_collation(value, collation) {
                return Err(
                    "hint _id_ is incompatible with non-simple string collation".to_string()
                );
            }
            stored_document_by_id_key(conn, namespace, &id_key_from_bson(value))
                .map(|document| document.into_iter().collect())
                .map_err(|err| err.to_string())
        }
        ResolvedHint::Index(index) => {
            hinted_candidate_documents_for_index(conn, namespace, filter, index, collation)
        }
    }
}

fn hinted_candidate_documents_for_index(
    conn: &Connection,
    namespace: &str,
    filter: &Document,
    index: &IndexSpec,
    collation: &Collation,
) -> std::result::Result<Vec<Document>, String> {
    if index.collation != *collation {
        return Err(format!(
            "hint index {} collation is incompatible with this query",
            index.name
        ));
    }
    if !filter_implies_index_membership(index, filter) {
        return Err(format!(
            "hint index {} is unsafe for this filter membership",
            index.name
        ));
    }
    if !index_entries_safe_for_planner(conn, namespace, &index.name)
        .map_err(|err| err.to_string())?
    {
        return Err(format!(
            "hint index {} has unsupported multikey omissions",
            index.name
        ));
    }
    if let Some(key_value) = planner_key_for_filter(index, filter) {
        return indexed_candidate_documents_by_key(conn, namespace, &index.name, &key_value)
            .map_err(|err| err.to_string())?
            .ok_or_else(|| format!("hint index {} could not be scanned", index.name));
    }
    if collation.is_simple()
        && let Some(range) = range_planner_key_for_filter(index, filter)
    {
        return indexed_candidate_documents_by_range(conn, namespace, &index.name, &range)
            .map_err(|err| err.to_string())?
            .ok_or_else(|| format!("hint index {} could not be range scanned", index.name));
    }
    if let Some((_, key_value)) = prefix_planner_key_for_filter(index, filter) {
        return indexed_candidate_documents_by_key(conn, namespace, &index.name, &key_value)
            .map_err(|err| err.to_string())?
            .ok_or_else(|| format!("hint index {} could not be prefix scanned", index.name));
    }
    Err(format!(
        "hint index {} is incompatible with this filter",
        index.name
    ))
}

fn indexed_candidate_documents_by_key(
    conn: &Connection,
    namespace: &str,
    index_name: &str,
    key_value: &str,
) -> Result<Option<Vec<Document>>> {
    let mut stmt = conn.prepare(
        r#"
        SELECT DISTINCT d.id_key, d.bson, d.created_at
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
        .query_map(params![namespace, index_name, key_value], |row| {
            row.get::<_, Vec<u8>>(1)
        })?
        .map(|row| decode_document(row?))
        .collect::<Result<Vec<_>>>()?;
    Ok(Some(documents))
}

fn indexed_candidate_documents_by_range(
    conn: &Connection,
    namespace: &str,
    index_name: &str,
    range: &RangePlannerKey,
) -> Result<Option<Vec<Document>>> {
    let bounds = range_sql_bounds(range);
    let mut stmt = conn.prepare(&format!(
        r#"
        SELECT DISTINCT d.id_key, d.bson, d.created_at
          FROM index_entries e
          JOIN documents d
            ON d.namespace = e.namespace
           AND d.id_key = e.id_key
         WHERE e.namespace = ?1
           AND e.index_name = ?2
           AND {}
         ORDER BY d.created_at
        "#,
        bounds.predicate
    ))?;
    let documents = stmt
        .query_map(
            params![namespace, index_name, bounds.lower, bounds.upper],
            |row| row.get::<_, Vec<u8>>(1),
        )?
        .map(|row| decode_document(row?))
        .collect::<Result<Vec<_>>>()?;
    Ok(Some(documents))
}

fn sort_pushdown_plan(
    conn: &Connection,
    namespace: &str,
    filter: &Document,
    sort: &[(String, i32)],
    hint: Option<&ResolvedHint>,
    collation: &Collation,
) -> std::result::Result<Option<SortPushdownPlan>, String> {
    if !collation.is_simple() {
        return Ok(None);
    }
    if sort.len() != 1 {
        return Ok(None);
    }
    let (sort_field, sort_direction) = &sort[0];
    let indexes = match hint {
        Some(ResolvedHint::Id) => return Ok(None),
        Some(ResolvedHint::Index(index)) => vec![index.clone()],
        None => {
            planner_indexes(indexes_for_namespace(conn, namespace).map_err(|err| err.to_string())?)
        }
    };
    for index in indexes {
        if !index.collation.is_simple() {
            continue;
        }
        if index.sparse || index.partial_filter.is_some() {
            continue;
        }
        if !index_entries_safe_for_planner(conn, namespace, &index.name)
            .map_err(|err| err.to_string())?
        {
            continue;
        }
        let Some(plan) = sort_pushdown_plan_for_index(&index, filter, sort_field, *sort_direction)
        else {
            continue;
        };
        if sort_pushdown_is_covered_and_unique(conn, namespace, filter, &plan)
            .map_err(|err| err.to_string())?
        {
            return Ok(Some(plan));
        }
    }
    Ok(None)
}

fn sort_pushdown_plan_for_index(
    index: &IndexSpec,
    filter: &Document,
    sort_field: &str,
    sort_direction: i32,
) -> Option<SortPushdownPlan> {
    if sort_direction != 1 && sort_direction != -1 {
        return None;
    }
    if let Some(field) = single_field_index_name(index) {
        if field != sort_field || !filter.is_empty() {
            return None;
        }
        if !index_field_supports_sort(index, field) {
            return None;
        }
        return Some(SortPushdownPlan {
            index: index.clone(),
            key_prefix: encode_range_planner_prefix(&[]),
            descending: sort_direction == -1,
        });
    }
    compound_sort_pushdown_plan_for_index(index, filter, sort_field, sort_direction)
}

fn compound_sort_pushdown_plan_for_index(
    index: &IndexSpec,
    filter: &Document,
    sort_field: &str,
    sort_direction: i32,
) -> Option<SortPushdownPlan> {
    if !is_compound_index(index) || filter.keys().any(|key| key.starts_with('$')) {
        return None;
    }
    let mut equality_parts = Vec::new();
    for field in index.key.keys() {
        if field == sort_field {
            if equality_parts.is_empty() || filter.len() != equality_parts.len() {
                return None;
            }
            if !index_field_supports_sort(index, field) {
                return None;
            }
            return Some(SortPushdownPlan {
                index: index.clone(),
                key_prefix: encode_range_planner_prefix(&equality_parts),
                descending: sort_direction == -1,
            });
        }
        let value = exact_equality_filter_part(filter, field)?;
        if !is_compound_planner_scalar(value) {
            return None;
        }
        equality_parts.push(index.collation.id_key_from_bson(value));
    }
    None
}

fn index_field_supports_sort(index: &IndexSpec, field: &str) -> bool {
    matches!(
        index.key.get(field),
        Some(Bson::Int32(1) | Bson::Int32(-1) | Bson::Int64(1) | Bson::Int64(-1))
    )
}

fn sort_pushdown_is_covered_and_unique(
    conn: &Connection,
    namespace: &str,
    filter: &Document,
    plan: &SortPushdownPlan,
) -> Result<bool> {
    let expected = if filter.is_empty() {
        sql_count_documents(conn, namespace)?
    } else {
        let Some((_, prefix_key)) = prefix_planner_key_for_filter(&plan.index, filter) else {
            return Ok(false);
        };
        sql_count_index_entries(conn, namespace, &plan.index.name, &prefix_key)?
    };
    if expected == 0 {
        return Ok(false);
    }
    let (entry_count, distinct_key_count, unsupported_key_count): (i64, i64, i64) = conn
        .query_row(
            r#"
        SELECT COUNT(DISTINCT id_key),
               COUNT(DISTINCT key_value),
               COALESCE(SUM(CASE
                   WHEN substr(key_value, ?5) LIKE 'bool:%'
                     OR substr(key_value, ?5) LIKE 'oid:%'
                     OR substr(key_value, ?5) LIKE 'date:%'
                   THEN 0
                   ELSE 1
               END), 0)
          FROM index_entries
         WHERE namespace = ?1
           AND index_name = ?2
           AND key_value >= ?3
           AND key_value < ?4
        "#,
            params![
                namespace,
                plan.index.name,
                plan.key_prefix,
                sort_prefix_upper_bound(&plan.key_prefix),
                plan.key_prefix.chars().count() as i64 + 1,
            ],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )?;
    Ok(entry_count == expected && distinct_key_count == expected && unsupported_key_count == 0)
}

fn sort_prefix_upper_bound(prefix: &str) -> String {
    format!("{prefix}\u{10ffff}")
}

fn indexed_candidate_documents_by_sort(
    conn: &Connection,
    namespace: &str,
    plan: &SortPushdownPlan,
) -> Result<Vec<Document>> {
    let direction = if plan.descending { "DESC" } else { "ASC" };
    let mut stmt = conn.prepare(&format!(
        r#"
        SELECT d.bson
          FROM index_entries e
          JOIN documents d
            ON d.namespace = e.namespace
           AND d.id_key = e.id_key
         WHERE e.namespace = ?1
           AND e.index_name = ?2
           AND e.key_value >= ?3
           AND e.key_value < ?4
         ORDER BY e.key_value {direction}, d.id_key ASC
        "#
    ))?;
    stmt.query_map(
        params![
            namespace,
            plan.index.name,
            plan.key_prefix,
            sort_prefix_upper_bound(&plan.key_prefix),
        ],
        |row| row.get::<_, Vec<u8>>(0),
    )?
    .map(|row| decode_document(row?))
    .collect::<Result<Vec<_>>>()
}

fn candidate_documents(
    conn: &Connection,
    namespace: &str,
    filter: &Document,
    collation: &Collation,
) -> Result<Vec<Document>> {
    match indexed_candidate_documents_with_collation(conn, namespace, filter, collation)? {
        Some(documents) => Ok(documents),
        None => documents_for_namespace(conn, namespace),
    }
}

fn candidate_documents_with_hint(
    conn: &Connection,
    namespace: &str,
    filter: &Document,
    hint: Option<&ResolvedHint>,
    collation: &Collation,
) -> std::result::Result<Vec<Document>, String> {
    match hint {
        Some(hint) => hinted_candidate_documents(conn, namespace, filter, hint, collation),
        None => {
            candidate_documents(conn, namespace, filter, collation).map_err(|err| err.to_string())
        }
    }
}

fn query_documents_with_hint(
    conn: &Connection,
    namespace: &str,
    filter: &Document,
    sort: Option<&[(String, i32)]>,
    skip: usize,
    limit: Option<usize>,
    projection: Option<&ProjectionSpec>,
    hint: Option<&ResolvedHint>,
    collation: &Collation,
) -> std::result::Result<Vec<Document>, MatchError> {
    if let Some(sort) = sort
        && let Some(plan) = sort_pushdown_plan(conn, namespace, filter, sort, hint, collation)
            .map_err(|err| match_error(2, err))?
    {
        let source_documents = indexed_candidate_documents_by_sort(conn, namespace, &plan)
            .map_err(|err| match_error(2, err.to_string()))?;
        return shape_documents(
            source_documents,
            filter,
            None,
            skip,
            limit,
            projection,
            collation,
        );
    }
    let source_documents = candidate_documents_with_hint(conn, namespace, filter, hint, collation)
        .map_err(|err| match_error(2, err))?;
    shape_documents(
        source_documents,
        filter,
        sort,
        skip,
        limit,
        projection,
        collation,
    )
}

fn shape_documents(
    source_documents: Vec<Document>,
    filter: &Document,
    sort: Option<&[(String, i32)]>,
    skip: usize,
    limit: Option<usize>,
    projection: Option<&ProjectionSpec>,
    collation: &Collation,
) -> MatchResult<Vec<Document>> {
    let mut docs = Vec::new();
    for document in source_documents {
        if matches_filter_with_collation(&document, filter, collation)? {
            docs.push(document);
        }
    }

    if let Some(sort) = sort {
        sort_documents_with_collation(&mut docs, sort, collation);
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
    matches_filter_with_collation(document, filter, &Collation::Simple)
}

fn matches_filter_with_collation(
    document: &Document,
    filter: &Document,
    collation: &Collation,
) -> MatchResult<bool> {
    for (key, condition) in filter {
        if key.starts_with('$') {
            if !matches_logical_operator(document, key, condition, collation)? {
                return Ok(false);
            }
        } else if !matches_field_condition(document, key, condition, collation)? {
            return Ok(false);
        }
    }
    Ok(true)
}

fn validate_filter_shape(filter: &Document) -> MatchResult<()> {
    for (key, condition) in filter {
        if key.starts_with('$') {
            validate_logical_operator_shape(key, condition)?;
        } else {
            validate_field_condition_shape(condition)?;
        }
    }
    Ok(())
}

fn validate_logical_operator_shape(operator: &str, operand: &Bson) -> MatchResult<()> {
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

    for clause in clauses {
        let Bson::Document(clause) = clause else {
            return Err(match_error(
                2,
                format!("{operator} entries must be documents"),
            ));
        };
        validate_filter_shape(clause)?;
    }
    Ok(())
}

fn validate_field_condition_shape(condition: &Bson) -> MatchResult<()> {
    if is_operator_document(condition) {
        let Bson::Document(operators) = condition else {
            unreachable!("operator document checked above");
        };
        validate_operator_document_shape(operators)?;
    } else if matches!(condition, Bson::RegularExpression(_)) {
        validate_regex_predicate_shape(condition, None)?;
    }
    Ok(())
}

fn validate_operator_document_shape(operators: &Document) -> MatchResult<()> {
    if operators.keys().any(|key| !key.starts_with('$')) {
        return Err(match_error(
            2,
            "field predicate cannot mix operators and literal document fields",
        ));
    }
    if operators.contains_key("$options") && !operators.contains_key("$regex") {
        return Err(match_error(2, "$options requires $regex"));
    }

    for (operator, operand) in operators {
        match operator.as_str() {
            "$eq" | "$ne" | "$gt" | "$gte" | "$lt" | "$lte" => {}
            "$in" => {
                if !matches!(operand, Bson::Array(_)) {
                    return Err(match_error(2, "$in requires an array"));
                }
            }
            "$nin" => {
                if !matches!(operand, Bson::Array(_)) {
                    return Err(match_error(2, "$nin requires an array"));
                }
            }
            "$exists" => {
                if !matches!(operand, Bson::Boolean(_)) {
                    return Err(match_error(2, "$exists requires a boolean"));
                }
            }
            "$not" => {
                let Bson::Document(nested) = operand else {
                    return Err(match_error(2, "$not requires a document"));
                };
                validate_operator_document_shape(nested)?;
            }
            "$regex" => {
                validate_regex_predicate_shape(operand, operators.get("$options"))?;
            }
            "$options" => {}
            "$type" => {
                parse_query_type_set(operand, "$type")?;
            }
            "$size" => {
                parse_non_negative_i32(operand, "$size")?;
            }
            "$all" => validate_all_predicate_shape(operand)?,
            "$elemMatch" => validate_elem_match_shape(operand)?,
            _ => {
                return Err(match_error(
                    2,
                    format!("unsupported query operator {operator}"),
                ));
            }
        }
    }
    Ok(())
}

fn validate_all_predicate_shape(operand: &Bson) -> MatchResult<()> {
    let Bson::Array(required) = operand else {
        return Err(match_error(2, "$all requires an array"));
    };
    for required_value in required {
        if let Bson::Document(document) = required_value {
            if document.len() == 1
                && let Some(elem_match) = document.get("$elemMatch")
            {
                validate_elem_match_shape(elem_match)?;
                continue;
            }
        }
        if is_operator_document(required_value) {
            return Err(match_error(2, "$all entries cannot be operator documents"));
        }
    }
    Ok(())
}

fn validate_elem_match_shape(operand: &Bson) -> MatchResult<()> {
    let Bson::Document(predicate) = operand else {
        return Err(match_error(2, "$elemMatch requires a document"));
    };
    if predicate.is_empty() {
        return Err(match_error(2, "$elemMatch requires a non-empty document"));
    }
    if predicate.keys().all(|key| {
        is_scalar_elem_match_operator(key) || (key.starts_with('$') && !is_logical_operator(key))
    }) {
        validate_operator_document_shape(predicate)
    } else {
        validate_filter_shape(predicate)
    }
}

fn is_logical_operator(operator: &str) -> bool {
    matches!(operator, "$and" | "$or" | "$nor")
}

fn validate_regex_predicate_shape(operand: &Bson, extra_options: Option<&Bson>) -> MatchResult<()> {
    let (pattern, regex_options) = match operand {
        Bson::String(pattern) => (pattern.as_str(), ""),
        Bson::RegularExpression(regex) => (regex.pattern.as_str(), regex.options.as_str()),
        _ => return Err(match_error(2, "$regex requires a string or BSON regex")),
    };
    let extra_options = match extra_options {
        None => "",
        Some(Bson::String(options)) => options.as_str(),
        Some(_) => return Err(match_error(2, "$options requires a string")),
    };
    build_query_regex(pattern, &[regex_options, extra_options])?;
    Ok(())
}

fn matches_logical_operator(
    document: &Document,
    operator: &str,
    operand: &Bson,
    collation: &Collation,
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
        results.push(matches_filter_with_collation(document, clause, collation)?);
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
    collation: &Collation,
) -> MatchResult<bool> {
    let values = values_at_path(document, field);
    if is_operator_document(condition) {
        let Bson::Document(operators) = condition else {
            unreachable!("operator document checked above");
        };
        return matches_operator_document_with_collation(&values, operators, collation);
    }
    if matches!(condition, Bson::RegularExpression(_)) {
        return matches_regex_predicate(&values, condition, None);
    }

    Ok(values
        .iter()
        .any(|candidate| bson_values_equal_with_collation(candidate, condition, collation)))
}

fn matches_operator_document(values: &[&Bson], operators: &Document) -> MatchResult<bool> {
    matches_operator_document_with_collation(values, operators, &Collation::Simple)
}

fn matches_operator_document_with_collation(
    values: &[&Bson],
    operators: &Document,
    collation: &Collation,
) -> MatchResult<bool> {
    if operators.keys().any(|key| !key.starts_with('$')) {
        return Err(match_error(
            2,
            "field predicate cannot mix operators and literal document fields",
        ));
    }
    if operators.contains_key("$options") && !operators.contains_key("$regex") {
        return Err(match_error(2, "$options requires $regex"));
    }

    for (operator, operand) in operators {
        if operator == "$options" {
            continue;
        }
        let matched = if operator == "$regex" {
            matches_regex_predicate(values, operand, operators.get("$options"))?
        } else {
            matches_operator_predicate(values, operator, operand, collation)?
        };
        if !matched {
            return Ok(false);
        }
    }
    Ok(true)
}

fn matches_operator_predicate(
    values: &[&Bson],
    operator: &str,
    operand: &Bson,
    collation: &Collation,
) -> MatchResult<bool> {
    match operator {
        "$eq" => Ok(values
            .iter()
            .any(|candidate| bson_values_equal_with_collation(candidate, operand, collation))),
        "$ne" => Ok(values
            .iter()
            .all(|candidate| !bson_values_equal_with_collation(candidate, operand, collation))),
        "$gt" => Ok(values
            .iter()
            .any(|candidate| compare_bson(candidate, operand, |ordering| ordering.is_gt()))),
        "$gte" => Ok(values
            .iter()
            .any(|candidate| compare_bson(candidate, operand, |ordering| !ordering.is_lt()))),
        "$lt" => Ok(values
            .iter()
            .any(|candidate| compare_bson(candidate, operand, |ordering| ordering.is_lt()))),
        "$lte" => Ok(values
            .iter()
            .any(|candidate| compare_bson(candidate, operand, |ordering| !ordering.is_gt()))),
        "$in" => {
            let Bson::Array(needles) = operand else {
                return Err(match_error(2, "$in requires an array"));
            };
            Ok(values.iter().any(|candidate| {
                needles
                    .iter()
                    .any(|needle| bson_values_equal_with_collation(candidate, needle, collation))
            }))
        }
        "$nin" => {
            let Bson::Array(needles) = operand else {
                return Err(match_error(2, "$nin requires an array"));
            };
            Ok(values.iter().all(|candidate| {
                needles
                    .iter()
                    .all(|needle| !bson_values_equal_with_collation(candidate, needle, collation))
            }))
        }
        "$exists" => {
            let Bson::Boolean(should_exist) = operand else {
                return Err(match_error(2, "$exists requires a boolean"));
            };
            Ok(!values.is_empty() == *should_exist)
        }
        "$not" => {
            let Bson::Document(nested) = operand else {
                return Err(match_error(2, "$not requires a document"));
            };
            Ok(!matches_operator_document_with_collation(
                values, nested, collation,
            )?)
        }
        "$type" => matches_type_predicate(values, operand),
        "$size" => matches_size_predicate(values, operand),
        "$all" => matches_all_predicate(values, operand, collation),
        "$elemMatch" => matches_elem_match_predicate(values, operand, collation),
        _ => Err(match_error(
            2,
            format!("unsupported query operator {operator}"),
        )),
    }
}

fn matches_type_predicate(values: &[&Bson], operand: &Bson) -> MatchResult<bool> {
    let expected = parse_query_type_set(operand, "$type")?;
    Ok(values.iter().any(|candidate| {
        expected
            .iter()
            .any(|kind| query_type_matches(kind, candidate))
    }))
}

fn query_type_matches(kind: &BsonTypeName, value: &Bson) -> bool {
    if kind.matches(value) {
        return true;
    }
    if *kind != BsonTypeName::Array {
        if let Bson::Array(values) = value {
            return values.iter().any(|value| query_type_matches(kind, value));
        }
    }
    false
}

fn parse_query_type_set(value: &Bson, path: &str) -> MatchResult<Vec<BsonTypeName>> {
    let values = match value {
        Bson::String(_) | Bson::Int32(_) | Bson::Int64(_) => vec![value],
        Bson::Array(values) if !values.is_empty() => values.iter().collect(),
        Bson::Array(_) => return Err(match_error(2, format!("{path} array must not be empty"))),
        _ => {
            return Err(match_error(
                2,
                format!("{path} requires a string alias, numeric code, or non-empty array"),
            ));
        }
    };
    let mut parsed = Vec::new();
    for value in values {
        let kind = parse_query_type_name(value, path)?;
        if !parsed.contains(&kind) {
            parsed.push(kind);
        }
    }
    Ok(parsed)
}

fn parse_query_type_name(value: &Bson, path: &str) -> MatchResult<BsonTypeName> {
    match value {
        Bson::String(alias) => BsonTypeName::parse(alias)
            .ok_or_else(|| match_error(2, format!("{path} alias {alias} is not supported"))),
        Bson::Int32(code) => query_type_name_for_code(*code, path),
        Bson::Int64(code) => {
            let Ok(code) = i32::try_from(*code) else {
                return Err(match_error(
                    2,
                    format!("{path} code {code} is not supported"),
                ));
            };
            query_type_name_for_code(code, path)
        }
        _ => Err(match_error(
            2,
            format!("{path} values must be string aliases or numeric codes"),
        )),
    }
}

fn query_type_name_for_code(code: i32, path: &str) -> MatchResult<BsonTypeName> {
    match code {
        1 => Ok(BsonTypeName::Double),
        2 => Ok(BsonTypeName::String),
        3 => Ok(BsonTypeName::Object),
        4 => Ok(BsonTypeName::Array),
        7 => Ok(BsonTypeName::ObjectId),
        8 => Ok(BsonTypeName::Bool),
        9 => Ok(BsonTypeName::Date),
        10 => Ok(BsonTypeName::Null),
        16 => Ok(BsonTypeName::Int),
        18 => Ok(BsonTypeName::Long),
        _ => Err(match_error(
            2,
            format!("{path} code {code} is not supported"),
        )),
    }
}

fn matches_size_predicate(values: &[&Bson], operand: &Bson) -> MatchResult<bool> {
    let size = parse_non_negative_i32(operand, "$size")? as usize;
    Ok(values
        .iter()
        .any(|candidate| matches!(candidate, Bson::Array(values) if values.len() == size)))
}

fn parse_non_negative_i32(value: &Bson, operator: &str) -> MatchResult<i32> {
    let size = match value {
        Bson::Int32(value) => *value,
        Bson::Int64(value) => i32::try_from(*value)
            .map_err(|_| match_error(2, format!("{operator} value is out of range")))?,
        _ => {
            return Err(match_error(
                2,
                format!("{operator} requires a non-negative integer"),
            ));
        }
    };
    if size < 0 {
        return Err(match_error(
            2,
            format!("{operator} requires a non-negative integer"),
        ));
    }
    Ok(size)
}

fn matches_all_predicate(
    values: &[&Bson],
    operand: &Bson,
    collation: &Collation,
) -> MatchResult<bool> {
    let Bson::Array(required) = operand else {
        return Err(match_error(2, "$all requires an array"));
    };
    if required.is_empty() {
        return Ok(false);
    }
    for required_value in required {
        if let Bson::Document(document) = required_value {
            if document.len() == 1 && document.contains_key("$elemMatch") {
                continue;
            }
        }
        if is_operator_document(required_value) {
            return Err(match_error(2, "$all entries cannot be operator documents"));
        }
    }
    for candidate in values {
        let Bson::Array(candidate_values) = candidate else {
            continue;
        };
        let mut all_matched = true;
        for required_value in required {
            let matched = match elem_match_operand(required_value) {
                Some(elem_match) => {
                    let mut matched = false;
                    for candidate_value in candidate_values {
                        if matches_elem_match_value(candidate_value, elem_match, collation)? {
                            matched = true;
                            break;
                        }
                    }
                    matched
                }
                None => candidate_values.iter().any(|candidate_value| {
                    bson_values_equal_with_collation(candidate_value, required_value, collation)
                }),
            };
            if !matched {
                all_matched = false;
                break;
            }
        }
        if all_matched {
            return Ok(true);
        }
    }
    Ok(false)
}

fn elem_match_operand(value: &Bson) -> Option<&Bson> {
    match value {
        Bson::Document(document) if document.len() == 1 => document.get("$elemMatch"),
        _ => None,
    }
}

fn matches_elem_match_predicate(
    values: &[&Bson],
    operand: &Bson,
    collation: &Collation,
) -> MatchResult<bool> {
    let Bson::Document(predicate) = operand else {
        return Err(match_error(2, "$elemMatch requires a document"));
    };
    if predicate.is_empty() {
        return Err(match_error(2, "$elemMatch requires a non-empty document"));
    }
    for candidate in values {
        let Bson::Array(candidate_values) = candidate else {
            continue;
        };
        for candidate_value in candidate_values {
            if matches_elem_match_value(candidate_value, operand, collation)? {
                return Ok(true);
            }
        }
    }
    Ok(false)
}

fn matches_elem_match_value(
    value: &Bson,
    operand: &Bson,
    collation: &Collation,
) -> MatchResult<bool> {
    let Bson::Document(predicate) = operand else {
        return Err(match_error(2, "$elemMatch requires a document"));
    };
    if predicate.is_empty() {
        return Err(match_error(2, "$elemMatch requires a non-empty document"));
    }
    match value {
        Bson::Document(document)
            if !predicate
                .keys()
                .all(|key| is_scalar_elem_match_operator(key)) =>
        {
            matches_filter_with_collation(document, predicate, collation)
        }
        _ => matches_operator_document_with_collation(&[value], predicate, collation),
    }
}

fn is_scalar_elem_match_operator(operator: &str) -> bool {
    matches!(
        operator,
        "$eq"
            | "$ne"
            | "$gt"
            | "$gte"
            | "$lt"
            | "$lte"
            | "$in"
            | "$nin"
            | "$exists"
            | "$not"
            | "$regex"
            | "$options"
            | "$type"
            | "$size"
            | "$all"
            | "$elemMatch"
    )
}

fn matches_regex_predicate(
    values: &[&Bson],
    operand: &Bson,
    extra_options: Option<&Bson>,
) -> MatchResult<bool> {
    let (pattern, regex_options) = match operand {
        Bson::String(pattern) => (pattern.as_str(), ""),
        Bson::RegularExpression(regex) => (regex.pattern.as_str(), regex.options.as_str()),
        _ => return Err(match_error(2, "$regex requires a string or BSON regex")),
    };
    let extra_options = match extra_options {
        None => "",
        Some(Bson::String(options)) => options.as_str(),
        Some(_) => return Err(match_error(2, "$options requires a string")),
    };
    let regex = build_query_regex(pattern, &[regex_options, extra_options])?;
    Ok(values
        .iter()
        .any(|candidate| regex_matches_bson(&regex, candidate)))
}

fn build_query_regex(pattern: &str, option_sets: &[&str]) -> MatchResult<regex::Regex> {
    let mut builder = RegexBuilder::new(pattern);
    for options in option_sets {
        for option in options.chars() {
            match option {
                'i' => {
                    builder.case_insensitive(true);
                }
                'm' => {
                    builder.multi_line(true);
                }
                's' => {
                    builder.dot_matches_new_line(true);
                }
                other => {
                    return Err(match_error(
                        2,
                        format!("$regex option {other} is not supported"),
                    ));
                }
            }
        }
    }
    builder
        .build()
        .map_err(|err| match_error(2, format!("invalid $regex pattern: {err}")))
}

fn regex_matches_bson(regex: &regex::Regex, value: &Bson) -> bool {
    match value {
        Bson::String(value) => regex.is_match(value),
        Bson::Array(values) => values.iter().any(|value| regex_matches_bson(regex, value)),
        _ => false,
    }
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
    bson_values_equal_with_collation(candidate, expected, &Collation::Simple)
}

fn bson_values_equal_with_collation(
    candidate: &Bson,
    expected: &Bson,
    collation: &Collation,
) -> bool {
    collation.values_equal(candidate, expected)
}

fn compare_bson(
    candidate: &Bson,
    expected: &Bson,
    predicate: impl Fn(std::cmp::Ordering) -> bool,
) -> bool {
    match (numeric_value(candidate), numeric_value(expected)) {
        (Some(left), Some(right)) => {
            return left.partial_cmp(&right).is_some_and(predicate);
        }
        (Some(_), None) | (None, Some(_)) => return false,
        (None, None) => {}
    }
    let Some((left_type, left)) = sortable_range_value_key(candidate) else {
        return false;
    };
    let Some((right_type, right)) = sortable_range_value_key(expected) else {
        return false;
    };
    left_type == right_type && predicate(left.cmp(&right))
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

fn decode_collation_sql(bytes: Option<Vec<u8>>) -> std::result::Result<Collation, rusqlite::Error> {
    let Some(bytes) = bytes else {
        return Ok(Collation::Simple);
    };
    let document = decode_document_sql(bytes)?;
    Collation::parse_document(&document).map_err(|err| {
        rusqlite::Error::FromSqlConversionFailure(
            0,
            rusqlite::types::Type::Blob,
            Box::new(std::io::Error::other(err)),
        )
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

    fn temp_sqlite_path(label: &str) -> PathBuf {
        let mut path = env::temp_dir();
        path.push(format!(
            "mongolino-test-{label}-{}-{}.sqlite3",
            std::process::id(),
            next_request_id()
        ));
        path
    }

    fn cleanup_sqlite_path(path: &PathBuf) {
        let _ = std::fs::remove_file(path);
        let _ = std::fs::remove_file(path.with_extension("sqlite3-wal"));
        let _ = std::fs::remove_file(path.with_extension("sqlite3-shm"));
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

    fn bson_strings(values: &[&str]) -> Vec<Bson> {
        values
            .iter()
            .map(|value| Bson::String((*value).to_string()))
            .collect()
    }

    fn bson_ints(values: &[i32]) -> Vec<Bson> {
        values.iter().copied().map(Bson::Int32).collect()
    }

    fn session_doc(seed: u8) -> Document {
        doc! {
            "id": Bson::Binary(bson::Binary {
                subtype: bson::spec::BinarySubtype::Uuid,
                bytes: vec![seed; 16],
            })
        }
    }

    fn bson_documents(values: Vec<Document>) -> Vec<Bson> {
        values.into_iter().map(Bson::Document).collect()
    }

    #[test]
    fn init_connection_sets_wal_normal_synchronous_and_foreign_keys() {
        let path = temp_sqlite_path("pragmas");
        let conn = Connection::open(&path).unwrap();
        init_connection(&conn).unwrap();

        let journal_mode: String = conn
            .query_row("PRAGMA journal_mode", [], |row| row.get(0))
            .unwrap();
        let foreign_keys: i64 = conn
            .query_row("PRAGMA foreign_keys", [], |row| row.get(0))
            .unwrap();

        assert_eq!(journal_mode.to_ascii_lowercase(), "wal");
        assert_eq!(sqlite_synchronous(&conn).unwrap(), 1);
        assert_eq!(foreign_keys, 1);

        drop(conn);
        cleanup_sqlite_path(&path);
    }

    #[test]
    fn driver_workflow_validates_sessions_and_end_sessions() {
        let conn = test_conn();
        let valid_session = Bson::Document(session_doc(7));
        let insert = handle_command(
            &conn,
            &doc! {
                "insert": "sessions",
                "$db": "app",
                "lsid": valid_session.clone(),
                "documents": [{ "_id": "ok" }],
            },
        )
        .unwrap();
        assert_eq!(insert.get_i32("n").unwrap(), 1);

        let malformed = handle_command(
            &conn,
            &doc! {
                "insert": "sessions",
                "$db": "app",
                "lsid": { "id": "not-binary" },
                "documents": [{ "_id": "bad" }],
            },
        )
        .unwrap();
        assert_command_error(&malformed);
        assert_eq!(
            raw_ids_in_namespace(&conn, "app.sessions"),
            vec!["ok".to_string()]
        );

        let ended = handle_command(
            &conn,
            &doc! {
                "endSessions": [valid_session],
                "$db": "admin",
            },
        )
        .unwrap();
        assert_eq!(ended.get_f64("ok").unwrap(), 1.0);

        let bad_end = handle_command(
            &conn,
            &doc! {
                "endSessions": [{ "id": Bson::Binary(bson::Binary {
                    subtype: bson::spec::BinarySubtype::Uuid,
                    bytes: vec![1; 15],
                }) }],
                "$db": "admin",
            },
        )
        .unwrap();
        assert_command_error(&bad_end);
    }

    #[test]
    fn driver_workflow_accepts_safe_read_and_write_concern_shapes() {
        let conn = test_conn();
        assert!(
            !parse_driver_workflow_options(
                "insert",
                &doc! { "insert": "concerns", "$db": "app", "documents": [{}] },
            )
            .unwrap()
            .write_concern
            .journaled
        );
        assert!(
            !parse_driver_workflow_options(
                "insert",
                &doc! {
                    "insert": "concerns",
                    "$db": "app",
                    "writeConcern": { "j": false },
                    "documents": [{}],
                },
            )
            .unwrap()
            .write_concern
            .journaled
        );
        assert!(
            parse_driver_workflow_options(
                "insert",
                &doc! {
                    "insert": "concerns",
                    "$db": "app",
                    "writeConcern": { "j": true },
                    "documents": [{}],
                },
            )
            .unwrap()
            .write_concern
            .journaled
        );
        assert!(
            parse_driver_workflow_options(
                "aggregate",
                &doc! {
                    "aggregate": "concerns",
                    "$db": "app",
                    "pipeline": [],
                    "cursor": {},
                    "writeConcern": {},
                },
            )
            .is_err()
        );
        let inserted = handle_command(
            &conn,
            &doc! {
                "insert": "concerns",
                "$db": "app",
                "writeConcern": { "w": "majority", "j": true, "wtimeoutMS": 0_i32 },
                "documents": [{ "_id": "c1", "name": "Ada" }],
            },
        )
        .unwrap();
        assert_eq!(inserted.get_i32("n").unwrap(), 1);

        let found = handle_command(
            &conn,
            &doc! {
                "find": "concerns",
                "$db": "app",
                "readConcern": { "level": "local" },
                "filter": { "_id": "c1" },
            },
        )
        .unwrap();
        assert_eq!(first_batch(&found).len(), 1);

        let found_available = handle_command(
            &conn,
            &doc! {
                "find": "concerns",
                "$db": "app",
                "readConcern": { "level": "available" },
                "filter": {},
            },
        )
        .unwrap();
        assert_eq!(first_batch(&found_available).len(), 1);
    }

    #[test]
    fn invalid_driver_workflow_options_do_not_mutate_or_sweep_ttl() {
        let conn = test_conn();

        seed_ttl_command_fixture(&conn, "bad_read_concern");
        let bad_read_concern = handle_command(
            &conn,
            &doc! {
                "find": "bad_read_concern",
                "$db": "app",
                "readConcern": { "level": "snapshot" },
                "filter": {},
            },
        )
        .unwrap();
        assert_invalid_ttl_read_preserves_expired(&conn, "bad_read_concern", bad_read_concern);

        seed_ttl_command_fixture(&conn, "bad_write_concern");
        let bad_write_concern = handle_command(
            &conn,
            &doc! {
                "insert": "bad_write_concern",
                "$db": "app",
                "writeConcern": { "w": 0_i32 },
                "documents": [{ "_id": "bad" }],
            },
        )
        .unwrap();
        assert_command_error(&bad_write_concern);
        assert_eq!(
            raw_ids_in_namespace(&conn, "app.bad_write_concern"),
            vec!["expired".to_string(), "future".to_string()]
        );

        seed_ttl_command_fixture(&conn, "bad_write_concern_j");
        let bad_write_concern_j = handle_command(
            &conn,
            &doc! {
                "insert": "bad_write_concern_j",
                "$db": "app",
                "writeConcern": { "j": "yes" },
                "documents": [{ "_id": "bad" }],
            },
        )
        .unwrap();
        assert_command_error(&bad_write_concern_j);
        assert_eq!(
            raw_ids_in_namespace(&conn, "app.bad_write_concern_j"),
            vec!["expired".to_string(), "future".to_string()]
        );
    }

    #[test]
    fn journaled_write_concern_uses_full_synchronous_and_restores() {
        let conn = test_conn();
        assert_eq!(sqlite_synchronous(&conn).unwrap(), 1);

        let observed = with_sqlite_synchronous_full(&conn, |conn| {
            assert_eq!(sqlite_synchronous(conn).unwrap(), SQLITE_SYNCHRONOUS_FULL);
            Ok("during-full")
        })
        .unwrap();
        assert_eq!(observed, "during-full");
        assert_eq!(sqlite_synchronous(&conn).unwrap(), 1);

        let inserted = handle_command(
            &conn,
            &doc! {
                "insert": "sync_success",
                "$db": "app",
                "writeConcern": { "j": true },
                "documents": [{ "_id": "ok" }],
            },
        )
        .unwrap();
        assert_eq!(inserted.get_i32("n").unwrap(), 1);
        assert_eq!(sqlite_synchronous(&conn).unwrap(), 1);

        let created = handle_command(
            &conn,
            &doc! {
                "create": "sync_error",
                "$db": "app",
                "writeConcern": { "j": true },
            },
        )
        .unwrap();
        assert_eq!(created.get_f64("ok").unwrap(), 1.0);

        let duplicate_create = handle_command(
            &conn,
            &doc! {
                "create": "sync_error",
                "$db": "app",
                "writeConcern": { "j": true },
            },
        )
        .unwrap();
        assert_command_error(&duplicate_create);
        assert_eq!(sqlite_synchronous(&conn).unwrap(), 1);
    }

    #[test]
    fn synchronous_full_guard_restores_after_rust_error() {
        let conn = test_conn();
        conn.pragma_update(None, "synchronous", "OFF").unwrap();
        assert_eq!(sqlite_synchronous(&conn).unwrap(), 0);

        let err = with_sqlite_synchronous_full(&conn, |conn| {
            assert_eq!(sqlite_synchronous(conn).unwrap(), SQLITE_SYNCHRONOUS_FULL);
            Err::<(), MongolinoError>(MongolinoError::Protocol("forced rust error".to_string()))
        })
        .unwrap_err();

        assert!(err.to_string().contains("forced rust error"));
        assert_eq!(sqlite_synchronous(&conn).unwrap(), 0);
        conn.pragma_update(None, "synchronous", "NORMAL").unwrap();
    }

    #[test]
    fn transaction_fields_are_rejected_before_mutation() {
        let conn = test_conn();
        let response = handle_command(
            &conn,
            &doc! {
                "insert": "transactions",
                "$db": "app",
                "lsid": session_doc(8),
                "startTransaction": true,
                "documents": [{ "_id": "bad" }],
            },
        )
        .unwrap();
        assert_command_error(&response);
        assert!(
            documents_for_namespace(&conn, "app.transactions")
                .unwrap()
                .is_empty()
        );

        let commit = handle_command(
            &conn,
            &doc! {
                "commitTransaction": 1_i32,
                "$db": "admin",
                "lsid": session_doc(8),
            },
        )
        .unwrap();
        assert_command_error(&commit);
    }

    #[test]
    fn retryable_writes_replay_exact_response_and_reject_conflicts() {
        let conn = test_conn();
        let mut state = ClientState::default();
        let lsid = Bson::Document(session_doc(9));
        let command = doc! {
            "insert": "retryable",
            "$db": "app",
            "lsid": lsid.clone(),
            "txnNumber": 1_i64,
            "documents": [{ "_id": "r1" }],
        };

        let first = handle_command_with_state(&conn, &mut state, &command).unwrap();
        assert_eq!(first.get_i32("n").unwrap(), 1);
        let second = handle_command_with_state(&conn, &mut state, &command).unwrap();
        assert_eq!(second, first);
        assert_eq!(
            raw_ids_in_namespace(&conn, "app.retryable"),
            vec!["r1".to_string()]
        );

        let conflict = handle_command_with_state(
            &conn,
            &mut state,
            &doc! {
                "insert": "retryable",
                "$db": "app",
                "lsid": lsid,
                "txnNumber": 1_i64,
                "documents": [{ "_id": "r2" }],
            },
        )
        .unwrap();
        assert_command_error(&conflict);
        assert_eq!(
            raw_ids_in_namespace(&conn, "app.retryable"),
            vec!["r1".to_string()]
        );

        handle_command(
            &conn,
            &doc! {
                "insert": "retryable",
                "$db": "app",
                "documents": [
                    { "_id": "u1", "score": 1_i32 },
                    { "_id": "d1", "gone": false },
                    { "_id": "f1", "score": 10_i32 },
                ],
            },
        )
        .unwrap();

        let update = doc! {
            "update": "retryable",
            "$db": "app",
            "lsid": Bson::Document(session_doc(9)),
            "txnNumber": 2_i64,
            "updates": [{ "q": { "_id": "u1" }, "u": { "$inc": { "score": 1_i32 } } }],
        };
        let first_update = handle_command_with_state(&conn, &mut state, &update).unwrap();
        assert_eq!(first_update.get_i32("nModified").unwrap(), 1);
        let second_update = handle_command_with_state(&conn, &mut state, &update).unwrap();
        assert_eq!(second_update, first_update);
        let updated = first_batch(
            &handle_command(
                &conn,
                &doc! { "find": "retryable", "$db": "app", "filter": { "_id": "u1" } },
            )
            .unwrap(),
        );
        assert_eq!(updated[0].get_i32("score").unwrap(), 2);

        let delete = doc! {
            "delete": "retryable",
            "$db": "app",
            "lsid": Bson::Document(session_doc(9)),
            "txnNumber": 3_i64,
            "deletes": [{ "q": { "_id": "d1" }, "limit": 1_i32 }],
        };
        let first_delete = handle_command_with_state(&conn, &mut state, &delete).unwrap();
        assert_eq!(first_delete.get_i32("n").unwrap(), 1);
        let second_delete = handle_command_with_state(&conn, &mut state, &delete).unwrap();
        assert_eq!(second_delete, first_delete);
        let deleted = first_batch(
            &handle_command(
                &conn,
                &doc! { "find": "retryable", "$db": "app", "filter": { "_id": "d1" } },
            )
            .unwrap(),
        );
        assert!(deleted.is_empty());

        let find_and_modify = doc! {
            "findAndModify": "retryable",
            "$db": "app",
            "lsid": Bson::Document(session_doc(9)),
            "txnNumber": 4_i64,
            "query": { "_id": "f1" },
            "update": { "$inc": { "score": 5_i32 } },
            "new": true,
        };
        let first_fam = handle_command_with_state(&conn, &mut state, &find_and_modify).unwrap();
        assert_eq!(
            first_fam
                .get_document("value")
                .unwrap()
                .get_i32("score")
                .unwrap(),
            15
        );
        let second_fam = handle_command_with_state(&conn, &mut state, &find_and_modify).unwrap();
        assert_eq!(second_fam, first_fam);
        let modified = first_batch(
            &handle_command(
                &conn,
                &doc! { "find": "retryable", "$db": "app", "filter": { "_id": "f1" } },
            )
            .unwrap(),
        );
        assert_eq!(modified[0].get_i32("score").unwrap(), 15);
    }

    #[test]
    fn retryable_write_cache_is_bounded_and_fifo() {
        let conn = test_conn();
        let mut state = ClientState::default();
        let lsid = Bson::Document(session_doc(10));
        for txn_number in 0..(RETRYABLE_WRITE_CACHE_LIMIT as i64 + 1) {
            let response = handle_command_with_state(
                &conn,
                &mut state,
                &doc! {
                    "insert": "retry_bounds",
                    "$db": "app",
                    "lsid": lsid.clone(),
                    "txnNumber": txn_number,
                    "documents": [{ "_id": format!("r{txn_number}") }],
                },
            )
            .unwrap();
            assert_eq!(response.get_i32("n").unwrap(), 1);
        }
        assert_eq!(state.retryable_writes.len(), RETRYABLE_WRITE_CACHE_LIMIT);

        let replay_evicted = handle_command_with_state(
            &conn,
            &mut state,
            &doc! {
                "insert": "retry_bounds",
                "$db": "app",
                "lsid": lsid,
                "txnNumber": 0_i64,
                "documents": [{ "_id": "r0" }],
            },
        )
        .unwrap();
        assert_eq!(replay_evicted.get_i32("n").unwrap(), 0);
        assert_eq!(
            write_errors(&replay_evicted)[0].get_i32("code").unwrap(),
            11000
        );
    }

    #[test]
    fn retryable_write_metadata_errors_happen_before_mutation() {
        let conn = test_conn();
        let mut state = ClientState::default();

        let missing_lsid = handle_command_with_state(
            &conn,
            &mut state,
            &doc! {
                "insert": "retry_errors",
                "$db": "app",
                "txnNumber": 1_i64,
                "documents": [{ "_id": "missing_lsid" }],
            },
        )
        .unwrap();
        assert_command_error(&missing_lsid);

        let malformed_txn = handle_command_with_state(
            &conn,
            &mut state,
            &doc! {
                "insert": "retry_errors",
                "$db": "app",
                "lsid": session_doc(11),
                "txnNumber": -1_i64,
                "documents": [{ "_id": "malformed_txn" }],
            },
        )
        .unwrap();
        assert_command_error(&malformed_txn);

        let read_txn = handle_command_with_state(
            &conn,
            &mut state,
            &doc! {
                "find": "retry_errors",
                "$db": "app",
                "lsid": session_doc(11),
                "txnNumber": 2_i64,
                "filter": {},
            },
        )
        .unwrap();
        assert_command_error(&read_txn);
        assert!(
            documents_for_namespace(&conn, "app.retry_errors")
                .unwrap()
                .is_empty()
        );
    }

    #[test]
    fn collation_parser_accepts_supported_subset_and_rejects_unsupported_shapes() {
        assert_eq!(
            Collation::parse_bson(&Bson::Document(doc! { "locale": "simple" })).unwrap(),
            Collation::Simple
        );
        assert_eq!(
            Collation::parse_bson(&Bson::Document(doc! { "locale": "en", "strength": 2_i32 }))
                .unwrap(),
            Collation::EnglishCaseInsensitive
        );
        assert_eq!(
            Collation::parse_bson(&Bson::Document(
                doc! { "locale": "en_US", "strength": 2_i64 }
            ))
            .unwrap(),
            Collation::EnglishCaseInsensitive
        );

        for value in [
            Bson::String("bad".to_string()),
            Bson::Document(doc! {}),
            Bson::Document(doc! { "locale": "simple", "strength": 2_i32 }),
            Bson::Document(doc! { "locale": "en" }),
            Bson::Document(doc! { "locale": "en", "strength": 1_i32 }),
            Bson::Document(doc! { "locale": "sv", "strength": 2_i32 }),
            Bson::Document(doc! { "locale": "en", "strength": 2_i32, "numericOrdering": true }),
            Bson::Document(doc! { "locale": "en", "strength": 2_i32, "caseLevel": false }),
        ] {
            assert!(Collation::parse_bson(&value).is_err(), "{value:?}");
        }
    }

    #[test]
    fn collation_comparison_preserves_binary_and_supports_case_insensitive_strings() {
        let simple = Collation::Simple;
        let ci = Collation::EnglishCaseInsensitive;

        assert!(!bson_values_equal_with_collation(
            &Bson::String("Ada".to_string()),
            &Bson::String("ada".to_string()),
            &simple,
        ));
        assert!(bson_values_equal_with_collation(
            &Bson::String("Ada".to_string()),
            &Bson::String("ada".to_string()),
            &ci,
        ));
        assert!(bson_values_equal_with_collation(
            &Bson::Array(bson_strings(&["Ada", "Grace"])),
            &Bson::String("ada".to_string()),
            &ci,
        ));
        assert!(bson_values_equal_with_collation(
            &Bson::Int32(1),
            &Bson::Double(1.0),
            &ci,
        ));
        assert!(!bson_values_equal_with_collation(
            &Bson::String("resume".to_string()),
            &Bson::String("resume\u{301}".to_string()),
            &ci,
        ));

        assert!(
            compare_bson_order_with_collation(
                &Bson::String("Ada".to_string()),
                &Bson::String("ada".to_string()),
                &ci,
            )
            .is_eq()
        );
        assert!(
            compare_bson_order_with_collation(
                &Bson::String("Ada".to_string()),
                &Bson::String("Grace".to_string()),
                &ci,
            )
            .is_lt()
        );
    }

    #[test]
    fn collation_matcher_and_sort_use_case_insensitive_subset() {
        let ci = Collation::EnglishCaseInsensitive;
        let document = doc! {
            "_id": "u1",
            "name": "Ada",
            "tags": ["Math", "Logic"],
            "nested": [{ "city": "ROME" }],
        };

        for filter in [
            doc! { "name": "ada" },
            doc! { "name": { "$eq": "ADA" } },
            doc! { "name": { "$in": ["grace", "ada"] } },
            doc! { "name": { "$ne": "grace" } },
            doc! { "tags": { "$all": ["math", "logic"] } },
            doc! { "nested": { "$elemMatch": { "city": "rome" } } },
            doc! { "$or": [{ "name": "grace" }, { "name": "ada" }] },
        ] {
            assert!(
                matches_filter_with_collation(&document, &filter, &ci).unwrap(),
                "{filter:?}"
            );
        }

        assert!(!matches_filter(&document, &doc! { "name": "ada" }).unwrap());
        assert!(validate_filter_for_collation(&doc! { "name": { "$gt": "a" } }, &ci).is_err());

        let mut documents = vec![
            doc! { "_id": "b", "name": "ada" },
            doc! { "_id": "a", "name": "Ada" },
            doc! { "_id": "c", "name": "Grace" },
        ];
        sort_documents_with_collation(&mut documents, &[("name".to_string(), 1)], &ci);
        assert_eq!(
            documents
                .iter()
                .map(|document| document.get_str("_id").unwrap())
                .collect::<Vec<_>>(),
            vec!["a", "b", "c"]
        );
    }

    #[test]
    fn collation_read_commands_match_sort_count_distinct_and_aggregate() {
        let conn = test_conn();
        insert_documents(
            &conn,
            &doc! {
                "insert": "users",
                "$db": "app",
                "documents": [
                    { "_id": "u1", "name": "Ada", "city": "ROME" },
                    { "_id": "u2", "name": "ada", "city": "rome" },
                    { "_id": "u3", "name": "Grace", "city": "London" },
                ],
            },
        )
        .unwrap();
        let collation = doc! { "locale": "en", "strength": 2_i32 };

        let found = find_documents(
            &conn,
            &doc! {
                "find": "users",
                "$db": "app",
                "filter": { "name": "ADA" },
                "sort": { "name": 1_i32 },
                "projection": { "_id": 1_i32 },
                "collation": collation.clone(),
            },
        )
        .unwrap();
        assert_eq!(
            first_batch(&found)
                .iter()
                .map(|document| document.get_str("_id").unwrap())
                .collect::<Vec<_>>(),
            vec!["u1", "u2"]
        );

        let simple = find_documents(
            &conn,
            &doc! {
                "find": "users",
                "$db": "app",
                "filter": { "name": "ada" },
                "sort": { "_id": 1_i32 },
                "projection": { "_id": 1_i32 },
                "collation": { "locale": "simple" },
            },
        )
        .unwrap();
        assert_eq!(first_batch(&simple), vec![doc! { "_id": "u2" }]);

        let count = count_documents_command(
            &conn,
            &doc! {
                "count": "users",
                "$db": "app",
                "query": { "city": "rome" },
                "collation": collation.clone(),
            },
        )
        .unwrap();
        assert_eq!(count.get_i64("n").unwrap(), 2);

        let distinct = distinct_command(
            &conn,
            &doc! {
                "distinct": "users",
                "$db": "app",
                "key": "name",
                "collation": collation.clone(),
            },
        )
        .unwrap();
        assert_eq!(
            distinct.get_array("values").unwrap(),
            &vec![
                Bson::String("Ada".to_string()),
                Bson::String("Grace".to_string()),
            ]
        );

        let aggregate = aggregate_command(
            &conn,
            &doc! {
                "aggregate": "users",
                "$db": "app",
                "pipeline": [
                    { "$match": { "city": "rome" } },
                    { "$sort": { "name": 1_i32 } },
                    { "$project": { "_id": 1_i32 } },
                ],
                "cursor": {},
                "collation": collation,
            },
        )
        .unwrap();
        assert_eq!(
            first_batch(&aggregate)
                .iter()
                .map(|document| document.get_str("_id").unwrap())
                .collect::<Vec<_>>(),
            vec!["u1", "u2"]
        );
    }

    #[test]
    fn invalid_read_collation_returns_error_before_ttl_sweep() {
        let conn = test_conn();
        insert_documents(
            &conn,
            &doc! {
                "insert": "events",
                "$db": "app",
                "documents": [
                    { "_id": "expired", "expiresAt": bson::DateTime::from_millis(1_700_000_000_000_i64), "name": "Ada" },
                    { "_id": "live", "expiresAt": bson::DateTime::now(), "name": "Grace" },
                ],
            },
        )
        .unwrap();
        create_indexes(
            &conn,
            &doc! {
                "createIndexes": "events",
                "$db": "app",
                "indexes": [{ "key": { "expiresAt": 1_i32 }, "name": "expires_ttl", "expireAfterSeconds": 0_i32 }],
            },
        )
        .unwrap();

        let response = find_documents(
            &conn,
            &doc! {
                "find": "events",
                "$db": "app",
                "filter": { "name": "ada" },
                "collation": { "locale": "en" },
            },
        )
        .unwrap();
        assert_command_error(&response);
        assert_eq!(response.get_i32("code").unwrap(), 72);

        let raw_documents = documents_for_namespace(&conn, "app.events").unwrap();
        assert!(
            raw_documents
                .iter()
                .any(|document| document.get_str("_id").unwrap() == "expired")
        );
    }

    #[test]
    fn collation_write_commands_target_case_insensitive_matches() {
        let conn = test_conn();
        let collation = doc! { "locale": "en", "strength": 2_i32 };
        insert_documents(
            &conn,
            &doc! {
                "insert": "users",
                "$db": "app",
                "documents": [
                    { "_id": "u1", "name": "Ada", "city": "ROME" },
                    { "_id": "u2", "name": "ada", "city": "rome" },
                    { "_id": "u3", "name": "Grace", "city": "London" },
                ],
            },
        )
        .unwrap();

        let update_one = update_documents(
            &conn,
            &doc! {
                "update": "users",
                "$db": "app",
                "updates": [
                    {
                        "q": { "name": "ADA" },
                        "u": { "$set": { "one": true } },
                        "multi": false,
                        "collation": collation.clone(),
                    }
                ],
            },
        )
        .unwrap();
        assert_eq!(update_one.get_i32("n").unwrap(), 1);
        assert_eq!(update_one.get_i32("nModified").unwrap(), 1);

        let update_many = update_documents(
            &conn,
            &doc! {
                "update": "users",
                "$db": "app",
                "updates": [
                    {
                        "q": { "city": "rome" },
                        "u": { "$set": { "many": true } },
                        "multi": true,
                        "collation": collation.clone(),
                    }
                ],
            },
        )
        .unwrap();
        assert_eq!(update_many.get_i32("n").unwrap(), 2);
        assert_eq!(update_many.get_i32("nModified").unwrap(), 2);
        assert_eq!(
            first_batch(
                &find_documents(
                    &conn,
                    &doc! {
                        "find": "users",
                        "$db": "app",
                        "filter": { "many": true },
                        "sort": { "_id": 1_i32 },
                        "projection": { "_id": 1_i32, "one": 1_i32 },
                    },
                )
                .unwrap()
            ),
            vec![doc! { "_id": "u1", "one": true }, doc! { "_id": "u2" }]
        );

        let delete_one = delete_documents(
            &conn,
            &doc! {
                "delete": "users",
                "$db": "app",
                "deletes": [{ "q": { "name": "ADA" }, "limit": 1_i32, "collation": collation.clone() }],
            },
        )
        .unwrap();
        assert_eq!(delete_one.get_i32("n").unwrap(), 1);

        let delete_many = delete_documents(
            &conn,
            &doc! {
                "delete": "users",
                "$db": "app",
                "deletes": [{ "q": { "city": "ROME" }, "limit": 0_i32, "collation": collation.clone() }],
            },
        )
        .unwrap();
        assert_eq!(delete_many.get_i32("n").unwrap(), 1);

        insert_documents(
            &conn,
            &doc! {
                "insert": "modify",
                "$db": "app",
                "documents": [
                    { "_id": "b", "name": "Ada" },
                    { "_id": "a", "name": "ada" },
                    { "_id": "c", "name": "Grace" },
                ],
            },
        )
        .unwrap();
        let modified = find_and_modify(
            &conn,
            "findAndModify",
            &doc! {
                "findAndModify": "modify",
                "$db": "app",
                "query": { "name": "ADA" },
                "sort": { "name": 1_i32 },
                "update": { "$set": { "winner": true } },
                "new": true,
                "collation": collation,
            },
        )
        .unwrap();
        assert_eq!(
            modified
                .get_document("value")
                .unwrap()
                .get_str("_id")
                .unwrap(),
            "a"
        );
        assert_eq!(
            modified
                .get_document("value")
                .unwrap()
                .get_bool("winner")
                .unwrap(),
            true
        );
    }

    #[test]
    fn invalid_write_collation_does_not_mutate_or_sweep_ttl() {
        let conn = test_conn();
        insert_documents(
            &conn,
            &doc! {
                "insert": "events",
                "$db": "app",
                "documents": [
                    { "_id": "expired", "expiresAt": bson::DateTime::from_millis(1_700_000_000_000_i64), "name": "Ada" },
                    { "_id": "live", "expiresAt": bson::DateTime::now(), "name": "Ada" },
                ],
            },
        )
        .unwrap();
        create_indexes(
            &conn,
            &doc! {
                "createIndexes": "events",
                "$db": "app",
                "indexes": [{ "key": { "expiresAt": 1_i32 }, "name": "expires_ttl", "expireAfterSeconds": 0_i32 }],
            },
        )
        .unwrap();

        let update = update_documents(
            &conn,
            &doc! {
                "update": "events",
                "$db": "app",
                "updates": [
                    {
                        "q": { "name": "ada" },
                        "u": { "$set": { "mutated": true } },
                        "multi": true,
                        "collation": { "locale": "en" },
                    }
                ],
            },
        )
        .unwrap();
        let errors = write_errors(&update);
        assert_eq!(errors[0].get_i32("code").unwrap(), 2);

        let raw_documents = documents_for_namespace(&conn, "app.events").unwrap();
        assert_eq!(raw_documents.len(), 2);
        assert!(
            raw_documents
                .iter()
                .all(|document| !document.contains_key("mutated"))
        );

        let find_and_modify_error = find_and_modify(
            &conn,
            "findAndModify",
            &doc! {
                "findAndModify": "events",
                "$db": "app",
                "query": { "name": "ada" },
                "update": { "$set": { "mutated": true } },
                "collation": { "locale": "en" },
            },
        )
        .unwrap();
        assert_command_error(&find_and_modify_error);
        assert_eq!(find_and_modify_error.get_i32("code").unwrap(), 72);
        assert_eq!(
            documents_for_namespace(&conn, "app.events").unwrap().len(),
            2
        );
    }

    #[test]
    fn collation_index_metadata_roundtrip_and_duplicate_spec_comparison() {
        let conn = test_conn();
        let created = create_indexes(
            &conn,
            &doc! {
                "createIndexes": "users",
                "$db": "app",
                "indexes": [
                    {
                        "key": { "name": 1_i32 },
                        "name": "name_ci",
                        "collation": { "locale": "en", "strength": 2_i32 },
                    }
                ],
            },
        )
        .unwrap();
        assert_eq!(created.get_i32("numIndexesAfter").unwrap(), 2);

        let listed = list_indexes(
            &conn,
            &doc! { "listIndexes": "users", "$db": "app", "cursor": {} },
        )
        .unwrap();
        let name_ci = first_batch(&listed)
            .into_iter()
            .find(|index| index.get_str("name").unwrap() == "name_ci")
            .unwrap();
        assert_eq!(
            name_ci.get_document("collation").unwrap(),
            &doc! { "locale": "en", "strength": 2_i32 }
        );

        let idempotent = create_indexes(
            &conn,
            &doc! {
                "createIndexes": "users",
                "$db": "app",
                "indexes": [
                    {
                        "key": { "name": 1_i32 },
                        "name": "name_ci",
                        "collation": { "locale": "en", "strength": 2_i32 },
                    }
                ],
            },
        )
        .unwrap();
        assert_eq!(idempotent.get_i32("numIndexesAfter").unwrap(), 2);

        let conflicting_simple = create_indexes(
            &conn,
            &doc! {
                "createIndexes": "users",
                "$db": "app",
                "indexes": [{ "key": { "name": 1_i32 }, "name": "name_ci" }],
            },
        )
        .unwrap();
        assert_command_error(&conflicting_simple);
        assert_eq!(conflicting_simple.get_i32("code").unwrap(), 85);
    }

    #[test]
    fn collation_index_rejects_invalid_and_unsafe_interactions() {
        let conn = test_conn();
        for spec in [
            doc! {
                "key": { "name": 1_i32 },
                "name": "bad_numeric",
                "collation": { "locale": "en", "strength": 2_i32, "numericOrdering": true },
            },
            doc! {
                "key": { "name": 1_i32 },
                "name": "bad_partial",
                "partialFilterExpression": { "active": true },
                "collation": { "locale": "en", "strength": 2_i32 },
            },
            doc! {
                "key": { "expiresAt": 1_i32 },
                "name": "bad_ttl",
                "expireAfterSeconds": 0_i32,
                "collation": { "locale": "en", "strength": 2_i32 },
            },
        ] {
            let response = create_indexes(
                &conn,
                &doc! { "createIndexes": "users", "$db": "app", "indexes": [spec] },
            )
            .unwrap();
            assert_command_error(&response);
            assert_eq!(response.get_i32("code").unwrap(), 72);
        }
        let listed = list_indexes(
            &conn,
            &doc! { "listIndexes": "users", "$db": "app", "cursor": {} },
        )
        .unwrap();
        assert_eq!(first_batch(&listed).len(), 1);
    }

    #[test]
    fn collation_unique_index_enforces_case_insensitive_strings() {
        let conn = test_conn();
        create_indexes(
            &conn,
            &doc! {
                "createIndexes": "users",
                "$db": "app",
                "indexes": [
                    {
                        "key": { "email": 1_i32 },
                        "name": "email_ci_unique",
                        "unique": true,
                        "collation": { "locale": "en", "strength": 2_i32 },
                    }
                ],
            },
        )
        .unwrap();
        insert_documents(
            &conn,
            &doc! {
                "insert": "users",
                "$db": "app",
                "documents": [
                    { "_id": "u1", "email": "Ada@example.test" },
                    { "_id": "u2", "email": "grace@example.test" },
                ],
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
                "updates": [{ "q": { "_id": "u2" }, "u": { "$set": { "email": "ADA@example.test" } } }],
            },
        )
        .unwrap();
        assert_eq!(
            write_errors(&duplicate_update)[0].get_i32("code").unwrap(),
            11000
        );
    }

    #[test]
    fn collation_planner_uses_matching_index_and_rejects_incompatible_hints_before_ttl() {
        let conn = test_conn();
        insert_documents(
            &conn,
            &doc! {
                "insert": "users",
                "$db": "app",
                "documents": [
                    { "_id": "u1", "name": "Ada" },
                    { "_id": "u2", "name": "ada" },
                    { "_id": "u3", "name": "Grace" },
                ],
            },
        )
        .unwrap();
        create_indexes(
            &conn,
            &doc! {
                "createIndexes": "users",
                "$db": "app",
                "indexes": [
                    { "key": { "name": 1_i32 }, "name": "name_simple" },
                    {
                        "key": { "name": 1_i32 },
                        "name": "name_ci",
                        "collation": { "locale": "en", "strength": 2_i32 },
                    },
                ],
            },
        )
        .unwrap();

        let explain = find_documents(
            &conn,
            &doc! {
                "find": "users",
                "$db": "app",
                "filter": { "name": "ADA" },
                "collation": { "locale": "en", "strength": 2_i32 },
                "explain": true,
            },
        )
        .unwrap();
        let plan = explain
            .get_document("queryPlanner")
            .unwrap()
            .get_document("winningPlan")
            .unwrap();
        assert_eq!(plan.get_str("scanStrategy").unwrap(), "indexExactEquality");
        assert_eq!(plan.get_str("indexName").unwrap(), "name_ci");

        let count = count_documents_command(
            &conn,
            &doc! {
                "count": "users",
                "$db": "app",
                "query": { "name": "ADA" },
                "collation": { "locale": "en", "strength": 2_i32 },
            },
        )
        .unwrap();
        assert_eq!(count.get_i64("n").unwrap(), 2);

        insert_documents(
            &conn,
            &doc! {
                "insert": "events",
                "$db": "app",
                "documents": [{ "_id": "expired", "name": "Ada", "expiresAt": bson::DateTime::from_millis(1_700_000_000_000_i64) }],
            },
        )
        .unwrap();
        create_indexes(
            &conn,
            &doc! {
                "createIndexes": "events",
                "$db": "app",
                "indexes": [
                    { "key": { "name": 1_i32 }, "name": "name_simple" },
                    { "key": { "expiresAt": 1_i32 }, "name": "expires_ttl", "expireAfterSeconds": 0_i32 },
                ],
            },
        )
        .unwrap();
        let hinted = find_documents(
            &conn,
            &doc! {
                "find": "events",
                "$db": "app",
                "filter": { "name": "ADA" },
                "hint": "name_simple",
                "collation": { "locale": "en", "strength": 2_i32 },
            },
        )
        .unwrap();
        assert_command_error(&hinted);
        assert_eq!(hinted.get_i32("code").unwrap(), 2);
        assert_eq!(
            documents_for_namespace(&conn, "app.events").unwrap().len(),
            1
        );
    }

    #[test]
    fn collation_compound_prefix_planner_uses_index_keys_for_targets_and_hints() {
        let conn = test_conn();
        let collation = doc! { "locale": "en", "strength": 2_i32 };
        insert_documents(
            &conn,
            &doc! {
                "insert": "events",
                "$db": "app",
                "documents": [
                    { "_id": "e1", "account": "Acme", "created": "2026-01-01", "state": "queued" },
                    { "_id": "e2", "account": "ACME", "created": "2026-01-02", "state": "queued" },
                    { "_id": "e3", "account": "Beta", "created": "2026-01-03", "state": "queued" },
                ],
            },
        )
        .unwrap();
        create_indexes(
            &conn,
            &doc! {
                "createIndexes": "events",
                "$db": "app",
                "indexes": [
                    {
                        "key": { "account": 1_i32, "created": 1_i32 },
                        "name": "account_created_ci",
                        "collation": collation.clone(),
                    },
                ],
            },
        )
        .unwrap();

        assert_eq!(
            prefix_planner_key_for_filter(
                &indexes_for_namespace(&conn, "app.events")
                    .unwrap()
                    .into_iter()
                    .find(|index| index.name == "account_created_ci")
                    .unwrap(),
                &doc! { "account": "ACME" },
            ),
            Some((1, "compound-prefix:1:11:str-ci:acme".to_string()))
        );
        assert!(matches!(
            planner_v2_plan_for_query(
                &conn,
                "app.events",
                &doc! { "account": "ACME" },
                None,
                &Collation::EnglishCaseInsensitive,
            )
            .unwrap(),
            PlannerV2Plan::IndexEqualityPrefix { index_name, prefix_len, key_value, .. }
                if index_name == "account_created_ci"
                    && prefix_len == 1
                    && key_value == "compound-prefix:1:11:str-ci:acme"
        ));

        let find = find_documents(
            &conn,
            &doc! {
                "find": "events",
                "$db": "app",
                "filter": { "account": "ACME" },
                "hint": "account_created_ci",
                "collation": collation.clone(),
            },
        )
        .unwrap();
        assert_eq!(
            first_batch(&find)
                .iter()
                .map(|doc| doc.get_str("_id").unwrap().to_string())
                .collect::<Vec<_>>(),
            vec!["e1", "e2"]
        );

        let count = count_documents_command(
            &conn,
            &doc! {
                "count": "events",
                "$db": "app",
                "query": { "account": "ACME" },
                "hint": "account_created_ci",
                "collation": collation.clone(),
            },
        )
        .unwrap();
        assert_eq!(count.get_i64("n").unwrap(), 2);

        let bad_hint = find_documents(
            &conn,
            &doc! {
                "find": "events",
                "$db": "app",
                "filter": { "account": "ACME" },
                "hint": "account_created_ci",
                "collation": { "locale": "simple" },
            },
        )
        .unwrap();
        assert_command_error(&bad_hint);
        assert_eq!(bad_hint.get_i32("code").unwrap(), 2);

        let bad_update = update_documents(
            &conn,
            &doc! {
                "update": "events",
                "$db": "app",
                "updates": [{
                    "q": { "account": "ACME" },
                    "u": { "$set": { "state": "bad" } },
                    "multi": true,
                    "hint": "account_created_ci",
                    "collation": { "locale": "simple" },
                }],
            },
        )
        .unwrap();
        assert_eq!(write_errors(&bad_update)[0].get_i32("code").unwrap(), 2);
        assert_eq!(
            find_ids_in(&conn, "events", doc! { "state": "bad" }),
            Vec::<String>::new()
        );

        let updated = update_documents(
            &conn,
            &doc! {
                "update": "events",
                "$db": "app",
                "updates": [{
                    "q": { "account": "ACME" },
                    "u": { "$set": { "state": "matched" } },
                    "multi": true,
                    "hint": "account_created_ci",
                    "collation": collation.clone(),
                }],
            },
        )
        .unwrap();
        assert_eq!(updated.get_i32("n").unwrap(), 2);
        assert_eq!(
            first_batch(
                &find_documents(
                    &conn,
                    &doc! {
                        "find": "events",
                        "$db": "app",
                        "filter": { "state": "matched" },
                        "sort": { "_id": 1_i32 },
                    },
                )
                .unwrap()
            )
            .iter()
            .map(|doc| doc.get_str("_id").unwrap().to_string())
            .collect::<Vec<_>>(),
            vec!["e1", "e2"]
        );

        let modified = find_and_modify(
            &conn,
            "findAndModify",
            &doc! {
                "findAndModify": "events",
                "$db": "app",
                "query": { "account": "ACME" },
                "update": { "$set": { "state": "fam" } },
                "hint": "account_created_ci",
                "collation": collation.clone(),
                "new": true,
            },
        )
        .unwrap();
        assert_eq!(
            modified
                .get_document("value")
                .unwrap()
                .get_str("state")
                .unwrap(),
            "fam"
        );
        assert_eq!(
            find_ids_in(&conn, "events", doc! { "state": "fam" }),
            vec!["e1"]
        );

        let bad_delete = delete_documents(
            &conn,
            &doc! {
                "delete": "events",
                "$db": "app",
                "deletes": [{
                    "q": { "account": "ACME" },
                    "limit": 1_i32,
                    "hint": "account_created_ci",
                    "collation": { "locale": "simple" },
                }],
            },
        )
        .unwrap();
        assert_eq!(write_errors(&bad_delete)[0].get_i32("code").unwrap(), 2);
        assert_eq!(
            count_documents_command(
                &conn,
                &doc! { "count": "events", "$db": "app", "query": { "account": "ACME" }, "collation": collation.clone() },
            )
            .unwrap()
            .get_i64("n")
            .unwrap(),
            2
        );

        let deleted = delete_documents(
            &conn,
            &doc! {
                "delete": "events",
                "$db": "app",
                "deletes": [{
                    "q": { "account": "ACME" },
                    "limit": 1_i32,
                    "hint": "account_created_ci",
                    "collation": collation,
                }],
            },
        )
        .unwrap();
        assert_eq!(deleted.get_i32("n").unwrap(), 1);
        assert_eq!(
            first_batch(
                &find_documents(
                    &conn,
                    &doc! {
                        "find": "events",
                        "$db": "app",
                        "filter": {},
                        "sort": { "_id": 1_i32 },
                    },
                )
                .unwrap()
            )
            .iter()
            .map(|doc| doc.get_str("_id").unwrap().to_string())
            .collect::<Vec<_>>(),
            vec!["e2", "e3"]
        );
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
    fn validator_migration_adds_missing_collection_options_column() {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch(
            r#"
            CREATE TABLE collections (
                namespace TEXT PRIMARY KEY,
                db TEXT NOT NULL,
                name TEXT NOT NULL,
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
            );
            "#,
        )
        .unwrap();

        init_connection(&conn).unwrap();

        let columns = conn
            .prepare("PRAGMA table_info(collections)")
            .unwrap()
            .query_map([], |row| row.get::<_, String>(1))
            .unwrap()
            .collect::<std::result::Result<Vec<_>, _>>()
            .unwrap();
        assert!(columns.contains(&"options_bson".to_string()));
        let migration: String = conn
            .query_row(
                "SELECT name FROM schema_migrations WHERE name = 'collection_options_bson'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(migration, "collection_options_bson");
    }

    #[test]
    fn validator_parser_accepts_supported_json_schema_subset() {
        let validator = JsonSchemaValidator::parse(&doc! {
            "$jsonSchema": {
                "bsonType": "object",
                "required": ["name", "profile"],
                "properties": {
                    "name": { "bsonType": "string" },
                    "score": { "bsonType": ["int", "long", "double"] },
                    "total": { "bsonType": "number" },
                    "profile": {
                        "bsonType": "object",
                        "required": ["city"],
                        "properties": {
                            "city": { "bsonType": "string" },
                            "verified": { "bsonType": "bool" }
                        }
                    }
                }
            }
        })
        .unwrap();

        validator
            .validate(&doc! {
                "name": "Ada",
                "score": 1_i64,
                "total": 2.5,
                "profile": { "city": "London", "verified": true }
            })
            .unwrap();
        let err = validator
            .validate(&doc! { "name": "Ada", "profile": {} })
            .unwrap_err();
        assert!(err.contains("$root.profile.city is required"));
        let err = validator
            .validate(&doc! { "name": "Ada", "profile": { "city": "London" }, "total": "2" })
            .unwrap_err();
        assert!(err.contains("$root.total must be number"));
    }

    #[test]
    fn validator_parser_rejects_unsupported_shapes() {
        for validator in [
            doc! {},
            doc! { "$jsonSchema": { "bsonType": "array" } },
            doc! { "$jsonSchema": { "bsonType": "object", "additionalProperties": false } },
            doc! { "$jsonSchema": { "bsonType": "object", "required": "name" } },
            doc! { "$jsonSchema": { "bsonType": "object", "required": [""] } },
            doc! { "$jsonSchema": { "bsonType": "object", "properties": [] } },
            doc! { "$jsonSchema": { "bsonType": "object", "properties": { "profile.city": { "bsonType": "string" } } } },
            doc! { "$jsonSchema": { "bsonType": "object", "properties": { "tags": { "bsonType": "array", "items": { "bsonType": "string" } } } } },
            doc! { "$jsonSchema": { "bsonType": "object", "properties": { "age": { "bsonType": "decimal" } } } },
        ] {
            assert!(JsonSchemaValidator::parse(&validator).is_err());
        }
    }

    #[test]
    fn validator_collection_options_roundtrip_from_connection_and_transaction() {
        let conn = test_conn();
        let ns = namespace("app", "users");
        insert_collection_catalog_with_options(&conn, "app", "users", &Document::new()).unwrap();
        let options = doc! {
            "validator": {
                "$jsonSchema": {
                    "bsonType": "object",
                    "required": ["name"],
                    "properties": { "name": { "bsonType": "string" } }
                }
            },
            "validationLevel": "strict",
            "validationAction": "error",
        };
        parse_collection_options(options.clone()).unwrap();

        let tx = conn.unchecked_transaction().unwrap();
        set_collection_options_tx(&tx, &ns, &options).unwrap();
        let tx_options = collection_options_tx(&tx, &ns).unwrap();
        assert_eq!(tx_options.document, options);
        assert!(tx_options.validator.is_some());
        tx.commit().unwrap();

        let loaded = collection_options(&conn, &ns).unwrap();
        assert_eq!(loaded.document, options);
        assert!(loaded.validator.is_some());
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
    fn validation_metadata_create_list_and_name_only_behave_as_expected() {
        let conn = test_conn();
        let validator = doc! {
            "$jsonSchema": {
                "bsonType": "object",
                "required": ["name"],
                "properties": { "name": { "bsonType": "string" } }
            }
        };

        let create = create_collection(
            &conn,
            &doc! {
                "create": "users",
                "$db": "app",
                "validator": validator.clone(),
                "validationLevel": "strict",
                "validationAction": "error",
            },
        )
        .unwrap();
        assert_eq!(create.get_f64("ok").unwrap(), 1.0);

        let list =
            list_collections(&conn, &doc! { "listCollections": 1_i32, "$db": "app" }).unwrap();
        let options = first_batch(&list)[0]
            .get_document("options")
            .unwrap()
            .clone();
        assert_eq!(options.get_document("validator").unwrap(), &validator);
        assert_eq!(options.get_str("validationLevel").unwrap(), "strict");
        assert_eq!(options.get_str("validationAction").unwrap(), "error");

        let name_only = list_collections(
            &conn,
            &doc! { "listCollections": 1_i32, "$db": "app", "nameOnly": true },
        )
        .unwrap();
        assert!(!first_batch(&name_only)[0].contains_key("options"));
    }

    #[test]
    fn coll_mod_updates_and_clears_validation_metadata() {
        let conn = test_conn();
        create_collection(&conn, &doc! { "create": "users", "$db": "app" }).unwrap();
        let validator = doc! {
            "$jsonSchema": {
                "bsonType": "object",
                "required": ["name"],
                "properties": { "name": { "bsonType": "string" } }
            }
        };

        let updated = coll_mod(
            &conn,
            &doc! {
                "collMod": "users",
                "$db": "app",
                "validator": validator.clone(),
                "validationLevel": "strict",
                "validationAction": "error",
            },
        )
        .unwrap();
        assert_eq!(updated.get_f64("ok").unwrap(), 1.0);
        let listed =
            list_collections(&conn, &doc! { "listCollections": 1_i32, "$db": "app" }).unwrap();
        assert_eq!(
            first_batch(&listed)[0]
                .get_document("options")
                .unwrap()
                .get_document("validator")
                .unwrap(),
            &validator
        );

        let cleared = coll_mod(
            &conn,
            &doc! { "collMod": "users", "$db": "app", "validator": {} },
        )
        .unwrap();
        assert_eq!(cleared.get_f64("ok").unwrap(), 1.0);
        let listed =
            list_collections(&conn, &doc! { "listCollections": 1_i32, "$db": "app" }).unwrap();
        assert!(
            !first_batch(&listed)[0]
                .get_document("options")
                .unwrap()
                .contains_key("validator")
        );
        assert!(
            !first_batch(&listed)[0]
                .get_document("options")
                .unwrap()
                .contains_key("validationLevel")
        );
        assert!(
            !first_batch(&listed)[0]
                .get_document("options")
                .unwrap()
                .contains_key("validationAction")
        );

        let cleared_with_new_level = coll_mod(
            &conn,
            &doc! { "collMod": "users", "$db": "app", "validator": {}, "validationLevel": "strict" },
        )
        .unwrap();
        assert_eq!(cleared_with_new_level.get_f64("ok").unwrap(), 1.0);
        let listed =
            list_collections(&conn, &doc! { "listCollections": 1_i32, "$db": "app" }).unwrap();
        let batch = first_batch(&listed);
        let options = batch[0].get_document("options").unwrap();
        assert!(!options.contains_key("validator"));
        assert_eq!(options.get_str("validationLevel").unwrap(), "strict");
        assert!(!options.contains_key("validationAction"));
    }

    #[test]
    fn coll_mod_updates_ttl_index_by_name_and_can_combine_with_validator() {
        let conn = test_conn();
        create_collection(&conn, &doc! { "create": "events", "$db": "app" }).unwrap();
        create_indexes(
            &conn,
            &doc! {
                "createIndexes": "events",
                "$db": "app",
                "indexes": [{ "key": { "expiresAt": 1_i32 }, "name": "expires_ttl", "expireAfterSeconds": 60_i32 }],
            },
        )
        .unwrap();

        let updated = coll_mod(
            &conn,
            &doc! {
                "collMod": "events",
                "$db": "app",
                "index": { "name": "expires_ttl", "expireAfterSeconds": 120_i32 },
            },
        )
        .unwrap();
        assert_eq!(updated.get_f64("ok").unwrap(), 1.0, "{updated:?}");
        assert_eq!(updated.get_i64("expireAfterSeconds_old").unwrap(), 60);
        assert_eq!(updated.get_i64("expireAfterSeconds_new").unwrap(), 120);
        let listed = list_indexes(&conn, &doc! { "listIndexes": "events", "$db": "app" }).unwrap();
        let ttl = first_batch(&listed)
            .into_iter()
            .find(|index| index.get_str("name").unwrap() == "expires_ttl")
            .unwrap();
        assert_eq!(ttl.get_i64("expireAfterSeconds").unwrap(), 120);

        let validator = doc! {
            "$jsonSchema": {
                "bsonType": "object",
                "required": ["expiresAt"],
                "properties": { "expiresAt": { "bsonType": "date" } }
            }
        };
        let combined = coll_mod(
            &conn,
            &doc! {
                "collMod": "events",
                "$db": "app",
                "validator": validator.clone(),
                "index": { "name": "expires_ttl", "expireAfterSeconds": 30_i64 },
            },
        )
        .unwrap();
        assert_eq!(combined.get_f64("ok").unwrap(), 1.0);
        let listed = list_indexes(&conn, &doc! { "listIndexes": "events", "$db": "app" }).unwrap();
        let ttl = first_batch(&listed)
            .into_iter()
            .find(|index| index.get_str("name").unwrap() == "expires_ttl")
            .unwrap();
        assert_eq!(ttl.get_i64("expireAfterSeconds").unwrap(), 30);
        let collections =
            list_collections(&conn, &doc! { "listCollections": 1_i32, "$db": "app" }).unwrap();
        assert_eq!(
            first_batch(&collections)[0]
                .get_document("options")
                .unwrap()
                .get_document("validator")
                .unwrap(),
            &validator
        );
    }

    #[test]
    fn coll_mod_ttl_rejects_bad_shapes_without_mutation() {
        let conn = test_conn();
        create_collection(&conn, &doc! { "create": "events", "$db": "app" }).unwrap();
        create_indexes(
            &conn,
            &doc! {
                "createIndexes": "events",
                "$db": "app",
                "indexes": [
                    { "key": { "expiresAt": 1_i32 }, "name": "expires_ttl", "expireAfterSeconds": 60_i32 },
                    { "key": { "name": 1_i32 }, "name": "name_1" },
                ],
            },
        )
        .unwrap();

        for index_update in [
            Bson::String("expires_ttl".to_string()),
            Bson::Document(doc! {}),
            Bson::Document(doc! { "name": "", "expireAfterSeconds": 30_i32 }),
            Bson::Document(doc! { "name": "expires_ttl" }),
            Bson::Document(doc! { "name": "expires_ttl", "expireAfterSeconds": -1_i32 }),
            Bson::Document(doc! { "name": "missing", "expireAfterSeconds": 30_i32 }),
            Bson::Document(doc! { "name": "_id_", "expireAfterSeconds": 30_i32 }),
            Bson::Document(doc! { "name": "name_1", "expireAfterSeconds": 30_i32 }),
            Bson::Document(
                doc! { "keyPattern": { "expiresAt": 1_i32 }, "expireAfterSeconds": 30_i32 },
            ),
            Bson::Document(
                doc! { "name": "expires_ttl", "expireAfterSeconds": 30_i32, "hidden": true },
            ),
        ] {
            let response = coll_mod(
                &conn,
                &doc! { "collMod": "events", "$db": "app", "index": index_update },
            )
            .unwrap();
            assert_command_error(&response);
        }

        let invalid_combined = coll_mod(
            &conn,
            &doc! {
                "collMod": "events",
                "$db": "app",
                "validator": {
                    "$jsonSchema": {
                        "bsonType": "object",
                        "required": ["expiresAt"],
                        "properties": { "expiresAt": { "bsonType": "date" } }
                    }
                },
                "index": { "name": "expires_ttl", "expireAfterSeconds": "30" },
            },
        )
        .unwrap();
        assert_command_error(&invalid_combined);

        let listed = list_indexes(&conn, &doc! { "listIndexes": "events", "$db": "app" }).unwrap();
        let ttl = first_batch(&listed)
            .into_iter()
            .find(|index| index.get_str("name").unwrap() == "expires_ttl")
            .unwrap();
        assert_eq!(ttl.get_i64("expireAfterSeconds").unwrap(), 60);
        let collections =
            list_collections(&conn, &doc! { "listCollections": 1_i32, "$db": "app" }).unwrap();
        assert!(
            !first_batch(&collections)[0]
                .get_document("options")
                .unwrap()
                .contains_key("validator")
        );
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
            &doc! { "insert": "users", "$db": "app", "documents": [{ "_id": "u1", "name": "Ada", "tags": ["math"], "scores": [1_i32] }] },
        )
        .unwrap();
        create_indexes(
            &conn,
            &doc! {
                "createIndexes": "users",
                "$db": "app",
                "indexes": [
                    { "key": { "name": 1_i32 }, "name": "name_1" },
                    { "key": { "tags": 1_i32 }, "name": "tags_1" },
                    { "key": { "scores": 1_i32 }, "name": "scores_1" },
                ],
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
        assert_eq!(entries_before_drop, 3);
        let omissions_before_drop: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM index_multikey_omissions WHERE namespace = 'app.users'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(omissions_before_drop, 1);

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
        let omissions_after_drop: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM index_multikey_omissions WHERE namespace = 'app.users'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(indexes_after_drop, 0);
        assert_eq!(entries_after_drop, 0);
        assert_eq!(omissions_after_drop, 0);
    }

    #[test]
    fn drop_database_removes_only_that_database() {
        let conn = test_conn();
        insert_documents(
            &conn,
            &doc! { "insert": "users", "$db": "app", "documents": [{ "_id": "u1", "scores": [1_i32] }] },
        )
        .unwrap();
        insert_documents(
            &conn,
            &doc! { "insert": "users", "$db": "other", "documents": [{ "_id": "u2", "scores": [1_i32] }] },
        )
        .unwrap();
        create_indexes(
            &conn,
            &doc! {
                "createIndexes": "users",
                "$db": "app",
                "indexes": [{ "key": { "scores": 1_i32 }, "name": "scores_1" }],
            },
        )
        .unwrap();
        create_indexes(
            &conn,
            &doc! {
                "createIndexes": "users",
                "$db": "other",
                "indexes": [{ "key": { "scores": 1_i32 }, "name": "scores_1" }],
            },
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
        let app_omissions: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM index_multikey_omissions WHERE namespace LIKE 'app.%'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        let other_omissions: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM index_multikey_omissions WHERE namespace = 'other.users'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(app_omissions, 0);
        assert_eq!(other_omissions, 1);
    }

    #[test]
    fn lifecycle_commands_reject_unsupported_options() {
        let conn = test_conn();

        for response in [
            create_collection(
                &conn,
                &doc! { "create": "users", "$db": "app", "capped": true },
            )
            .unwrap(),
            list_collections(
                &conn,
                &doc! { "listCollections": 1_i32, "$db": "app", "filter": { "type": "collection" } },
            )
            .unwrap(),
            drop_collection(&conn, &doc! { "drop": "users", "$db": "app", "comment": "nope" })
                .unwrap(),
            handle_command(
                &conn,
                &doc! { "dropDatabase": 1_i32, "$db": "app", "writeConcern": { "w": 0_i32 } },
            )
            .unwrap(),
        ] {
            assert_command_error(&response);
        }
    }

    #[test]
    fn validation_metadata_commands_reject_unsupported_and_malformed_options() {
        let conn = test_conn();
        create_collection(&conn, &doc! { "create": "users", "$db": "app" }).unwrap();

        for response in [
            create_collection(
                &conn,
                &doc! { "create": "bad", "$db": "app", "validator": { "$jsonSchema": { "bsonType": "object", "additionalProperties": false } } },
            )
            .unwrap(),
            create_collection(
                &conn,
                &doc! { "create": "bad", "$db": "app", "validationLevel": "moderate" },
            )
            .unwrap(),
            create_collection(
                &conn,
                &doc! { "create": "bad", "$db": "app", "validationAction": "warn" },
            )
            .unwrap(),
            coll_mod(
                &conn,
                &doc! { "collMod": "missing", "$db": "app", "validator": {} },
            )
            .unwrap(),
            coll_mod(
                &conn,
                &doc! { "collMod": "users", "$db": "app", "validator": { "$jsonSchema": { "bsonType": "object", "properties": { "a.b": { "bsonType": "string" } } } } },
            )
            .unwrap(),
            coll_mod(
                &conn,
                &doc! { "collMod": "users", "$db": "app", "expireAfterSeconds": 1_i32 },
            )
            .unwrap(),
            coll_mod(&conn, &doc! { "collMod": "users", "$db": "app" }).unwrap(),
        ] {
            assert_command_error(&response);
        }
    }

    fn validation_test_validator() -> Document {
        doc! {
            "$jsonSchema": {
                "bsonType": "object",
                "required": ["name"],
                "properties": {
                    "name": { "bsonType": "string" },
                    "age": { "bsonType": "number" },
                    "profile": {
                        "bsonType": "object",
                        "required": ["city"],
                        "properties": { "city": { "bsonType": "string" } }
                    }
                }
            }
        }
    }

    fn create_validation_test_collection(conn: &Connection) {
        create_collection(
            conn,
            &doc! {
                "create": "users",
                "$db": "app",
                "validator": validation_test_validator(),
            },
        )
        .unwrap();
    }

    #[test]
    fn validation_insert_ordered_unordered_and_bypass_behave_as_expected() {
        let conn = test_conn();
        create_validation_test_collection(&conn);

        let ordered = insert_documents(
            &conn,
            &doc! {
                "insert": "users",
                "$db": "app",
                "documents": [
                    { "_id": "u1", "name": "Ada" },
                    { "_id": "bad", "age": 1_i32 },
                    { "_id": "u2", "name": "Grace" },
                ],
            },
        )
        .unwrap();
        assert_eq!(ordered.get_i32("n").unwrap(), 1);
        assert_eq!(write_errors(&ordered)[0].get_i32("code").unwrap(), 121);
        assert!(
            documents_for_namespace(&conn, "app.users")
                .unwrap()
                .iter()
                .all(|document| document.get_str("_id").unwrap() != "u2")
        );

        let unordered = insert_documents(
            &conn,
            &doc! {
                "insert": "users",
                "$db": "app",
                "ordered": false,
                "documents": [
                    { "_id": "bad2", "age": 2_i32 },
                    { "_id": "u2", "name": "Grace" },
                    { "_id": "bad3", "profile": {} },
                ],
            },
        )
        .unwrap();
        assert_eq!(unordered.get_i32("n").unwrap(), 1);
        let errors = write_errors(&unordered);
        assert_eq!(errors.len(), 2);
        assert_eq!(errors[0].get_i32("index").unwrap(), 0);
        assert_eq!(errors[1].get_i32("index").unwrap(), 2);

        let bypassed = insert_documents(
            &conn,
            &doc! {
                "insert": "users",
                "$db": "app",
                "bypassDocumentValidation": true,
                "documents": [{ "_id": "bad4", "age": "old" }],
            },
        )
        .unwrap();
        assert_eq!(bypassed.get_i32("n").unwrap(), 1);

        let malformed = insert_documents(
            &conn,
            &doc! {
                "insert": "users",
                "$db": "app",
                "bypassDocumentValidation": "yes",
                "documents": [{ "_id": "x" }],
            },
        )
        .unwrap();
        assert_command_error(&malformed);
    }

    #[test]
    fn validation_update_replacement_modifier_upsert_and_noop_paths_are_enforced() {
        let conn = test_conn();
        create_validation_test_collection(&conn);
        insert_documents(
            &conn,
            &doc! {
                "insert": "users",
                "$db": "app",
                "documents": [{ "_id": "u1", "name": "Ada", "age": 37_i32 }],
            },
        )
        .unwrap();

        let bad_replacement = update_documents(
            &conn,
            &doc! {
                "update": "users",
                "$db": "app",
                "updates": [{ "q": { "_id": "u1" }, "u": { "_id": "u1", "age": 38_i32 } }],
            },
        )
        .unwrap();
        assert_eq!(
            write_errors(&bad_replacement)[0].get_i32("code").unwrap(),
            121
        );

        let bad_modifier = update_documents(
            &conn,
            &doc! {
                "update": "users",
                "$db": "app",
                "updates": [{ "q": { "_id": "u1" }, "u": { "$set": { "name": 5_i32 } } }],
            },
        )
        .unwrap();
        assert_eq!(write_errors(&bad_modifier)[0].get_i32("code").unwrap(), 121);

        let bad_upsert = update_documents(
            &conn,
            &doc! {
                "update": "users",
                "$db": "app",
                "updates": [{ "q": { "_id": "u2" }, "u": { "$set": { "age": 39_i32 } }, "upsert": true }],
            },
        )
        .unwrap();
        assert_eq!(write_errors(&bad_upsert)[0].get_i32("code").unwrap(), 121);

        let bypassed = update_documents(
            &conn,
            &doc! {
                "update": "users",
                "$db": "app",
                "bypassDocumentValidation": true,
                "updates": [{ "q": { "_id": "u1" }, "u": { "$set": { "name": 5_i32 } } }],
            },
        )
        .unwrap();
        assert_eq!(bypassed.get_i32("nModified").unwrap(), 1);

        let malformed = update_documents(
            &conn,
            &doc! {
                "update": "users",
                "$db": "app",
                "bypassDocumentValidation": "yes",
                "updates": [{ "q": {}, "u": { "$set": { "name": "Ada" } } }],
            },
        )
        .unwrap();
        assert_command_error(&malformed);

        let conn = test_conn();
        insert_documents(
            &conn,
            &doc! { "insert": "users", "$db": "app", "documents": [{ "_id": "legacy", "age": 1_i32 }] },
        )
        .unwrap();
        coll_mod(
            &conn,
            &doc! { "collMod": "users", "$db": "app", "validator": validation_test_validator() },
        )
        .unwrap();
        let noop = update_documents(
            &conn,
            &doc! {
                "update": "users",
                "$db": "app",
                "updates": [{ "q": { "_id": "legacy" }, "u": { "$set": { "age": 1_i32 } } }],
            },
        )
        .unwrap();
        assert_eq!(noop.get_i32("nModified").unwrap(), 0);
        let changed = update_documents(
            &conn,
            &doc! {
                "update": "users",
                "$db": "app",
                "updates": [{ "q": { "_id": "legacy" }, "u": { "$set": { "age": 2_i32 } } }],
            },
        )
        .unwrap();
        assert_eq!(write_errors(&changed)[0].get_i32("code").unwrap(), 121);
    }

    #[test]
    fn validation_find_and_modify_update_upsert_and_bypass_are_enforced() {
        let conn = test_conn();
        create_validation_test_collection(&conn);
        insert_documents(
            &conn,
            &doc! {
                "insert": "users",
                "$db": "app",
                "documents": [{ "_id": "u1", "name": "Ada" }],
            },
        )
        .unwrap();

        let invalid_update = find_and_modify(
            &conn,
            "findAndModify",
            &doc! {
                "findAndModify": "users",
                "$db": "app",
                "query": { "_id": "u1" },
                "update": { "$set": { "name": 5_i32 } },
            },
        )
        .unwrap();
        assert_command_error(&invalid_update);
        assert_eq!(invalid_update.get_i32("code").unwrap(), 121);

        let invalid_upsert = find_and_modify(
            &conn,
            "findAndModify",
            &doc! {
                "findAndModify": "users",
                "$db": "app",
                "query": { "_id": "u2" },
                "update": { "$set": { "age": 39_i32 } },
                "upsert": true,
            },
        )
        .unwrap();
        assert_command_error(&invalid_upsert);
        assert_eq!(invalid_upsert.get_i32("code").unwrap(), 121);

        let bypassed = find_and_modify(
            &conn,
            "findAndModify",
            &doc! {
                "findAndModify": "users",
                "$db": "app",
                "query": { "_id": "u1" },
                "update": { "$set": { "name": 5_i32 } },
                "new": true,
                "bypassDocumentValidation": true,
            },
        )
        .unwrap();
        assert_eq!(
            bypassed
                .get_document("value")
                .unwrap()
                .get_i32("name")
                .unwrap(),
            5
        );

        let snake_case_bypassed = find_and_modify(
            &conn,
            "findAndModify",
            &doc! {
                "findAndModify": "users",
                "$db": "app",
                "query": { "_id": "u1" },
                "update": { "$set": { "name": 6_i32 } },
                "new": true,
                "bypass_document_validation": true,
            },
        )
        .unwrap();
        assert_eq!(
            snake_case_bypassed
                .get_document("value")
                .unwrap()
                .get_i32("name")
                .unwrap(),
            6
        );

        let conflicting_aliases = find_and_modify(
            &conn,
            "findAndModify",
            &doc! {
                "findAndModify": "users",
                "$db": "app",
                "query": { "_id": "u1" },
                "update": { "$set": { "name": "Mutated" } },
                "new": true,
                "bypassDocumentValidation": true,
                "bypass_document_validation": false,
            },
        )
        .unwrap();
        assert_command_error(&conflicting_aliases);
        assert_eq!(conflicting_aliases.get_i32("code").unwrap(), 9);
        assert!(
            conflicting_aliases
                .get_str("errmsg")
                .unwrap()
                .contains("cannot conflict")
        );
        assert_eq!(
            first_batch(
                &find_documents(
                    &conn,
                    &doc! { "find": "users", "$db": "app", "filter": { "_id": "u1" } },
                )
                .unwrap()
            )[0]
            .get_i32("name")
            .unwrap(),
            6
        );

        let malformed_snake = find_and_modify(
            &conn,
            "findAndModify",
            &doc! {
                "findAndModify": "users",
                "$db": "app",
                "query": { "_id": "u1" },
                "update": { "$set": { "name": "Ada" } },
                "bypass_document_validation": "yes",
            },
        )
        .unwrap();
        assert_command_error(&malformed_snake);
        assert_eq!(malformed_snake.get_i32("code").unwrap(), 9);

        let malformed = find_and_modify(
            &conn,
            "findAndModify",
            &doc! {
                "findAndModify": "users",
                "$db": "app",
                "query": { "_id": "u1" },
                "update": { "$set": { "name": "Ada" } },
                "bypassDocumentValidation": "yes",
            },
        )
        .unwrap();
        assert_command_error(&malformed);
    }

    #[test]
    fn validation_bypass_does_not_bypass_unique_indexes() {
        let conn = test_conn();
        create_validation_test_collection(&conn);
        create_indexes(
            &conn,
            &doc! {
                "createIndexes": "users",
                "$db": "app",
                "indexes": [{ "key": { "email": 1_i32 }, "name": "email_1", "unique": true }],
            },
        )
        .unwrap();
        insert_documents(
            &conn,
            &doc! {
                "insert": "users",
                "$db": "app",
                "documents": [{ "_id": "u1", "name": "Ada", "email": "a@example.test" }],
            },
        )
        .unwrap();

        let duplicate = insert_documents(
            &conn,
            &doc! {
                "insert": "users",
                "$db": "app",
                "bypassDocumentValidation": true,
                "documents": [{ "_id": "u2", "email": "a@example.test" }],
            },
        )
        .unwrap();
        assert_eq!(write_errors(&duplicate)[0].get_i32("code").unwrap(), 11000);
    }

    #[test]
    fn validation_and_unique_indexes_apply_after_new_update_modifiers() {
        let conn = test_conn();
        create_validation_test_collection(&conn);
        insert_documents(
            &conn,
            &doc! {
                "insert": "users",
                "$db": "app",
                "documents": [{ "_id": "u1", "name": "Ada", "age": 37_i32 }],
            },
        )
        .unwrap();

        let invalid = update_documents(
            &conn,
            &doc! {
                "update": "users",
                "$db": "app",
                "updates": [{ "q": { "_id": "u1" }, "u": { "$rename": { "age": "name" } } }],
            },
        )
        .unwrap();
        assert_eq!(write_errors(&invalid)[0].get_i32("code").unwrap(), 121);
        assert_eq!(
            first_batch(
                &find_documents(
                    &conn,
                    &doc! { "find": "users", "$db": "app", "filter": { "_id": "u1" } },
                )
                .unwrap()
            )[0]
            .get_str("name")
            .unwrap(),
            "Ada"
        );

        let bypassed = update_documents(
            &conn,
            &doc! {
                "update": "users",
                "$db": "app",
                "bypassDocumentValidation": true,
                "updates": [{ "q": { "_id": "u1" }, "u": { "$rename": { "age": "name" } } }],
            },
        )
        .unwrap();
        assert_eq!(bypassed.get_i32("nModified").unwrap(), 1);

        let conn = test_conn();
        create_validation_test_collection(&conn);
        create_indexes(
            &conn,
            &doc! {
                "createIndexes": "users",
                "$db": "app",
                "indexes": [
                    { "key": { "email": 1_i32 }, "name": "email_1", "unique": true },
                    { "key": { "rank": 1_i32 }, "name": "rank_1", "unique": true }
                ],
            },
        )
        .unwrap();
        insert_documents(
            &conn,
            &doc! {
                "insert": "users",
                "$db": "app",
                "documents": [
                    { "_id": "u1", "name": "Ada", "email": "ada@example.test", "rank": 1_i32 },
                    { "_id": "u2", "name": "Grace", "altEmail": "ada@example.test", "rank": 5_i32 },
                ],
            },
        )
        .unwrap();

        for update in [
            doc! { "$rename": { "altEmail": "email" } },
            doc! { "$min": { "rank": 1_i32 } },
        ] {
            let duplicate = update_documents(
                &conn,
                &doc! {
                    "update": "users",
                    "$db": "app",
                    "bypassDocumentValidation": true,
                    "updates": [{ "q": { "_id": "u2" }, "u": update }],
                },
            )
            .unwrap();
            assert_eq!(write_errors(&duplicate)[0].get_i32("code").unwrap(), 11000);
        }

        let duplicate_upsert = update_documents(
            &conn,
            &doc! {
                "update": "users",
                "$db": "app",
                "bypassDocumentValidation": true,
                "updates": [
                    {
                        "q": { "_id": "u4" },
                        "u": { "$setOnInsert": { "email": "ada@example.test" } },
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

        let conn = test_conn();
        create_indexes(
            &conn,
            &doc! {
                "createIndexes": "users",
                "$db": "app",
                "indexes": [{ "key": { "tags": 1_i32 }, "name": "tags_1", "unique": true }],
            },
        )
        .unwrap();
        insert_documents(
            &conn,
            &doc! {
                "insert": "users",
                "$db": "app",
                "documents": [{ "_id": "u1" }],
            },
        )
        .unwrap();
        let array_rejected_by_unique_index = update_documents(
            &conn,
            &doc! {
                "update": "users",
                "$db": "app",
                "bypassDocumentValidation": true,
                "updates": [{ "q": { "_id": "u1" }, "u": { "$push": { "tags": "new" } } }],
            },
        )
        .unwrap();
        assert_eq!(
            write_errors(&array_rejected_by_unique_index)[0]
                .get_i32("code")
                .unwrap(),
            72
        );
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
    fn count_planner_classifies_safe_and_fallback_filters() {
        let conn = test_conn();
        seed_find_documents(&conn);
        create_indexes(
            &conn,
            &doc! {
                "createIndexes": "users",
                "$db": "app",
                "indexes": [
                    { "key": { "active": 1_i32 }, "name": "active_1" },
                    { "key": { "age": 1_i32 }, "name": "age_1" },
                    { "key": { "profile.city": 1_i32, "active": 1_i32 }, "name": "city_active_1" },
                ],
            },
        )
        .unwrap();

        assert_eq!(
            plan_count(&conn, "app.users", &doc! {}).unwrap(),
            CountPlan::Empty
        );
        assert_eq!(
            plan_count(&conn, "app.users", &doc! { "_id": "u1" }).unwrap(),
            CountPlan::IdEquality("str:u1".to_string())
        );
        assert_eq!(
            plan_count(&conn, "app.users", &doc! { "_id": { "$eq": "u1" } }).unwrap(),
            CountPlan::IdEquality("str:u1".to_string())
        );
        assert_eq!(
            plan_count(&conn, "app.users", &doc! { "active": true }).unwrap(),
            CountPlan::IndexedEquality {
                index_name: "active_1".to_string(),
                key_value: "bool:true".to_string(),
            }
        );
        assert_eq!(
            plan_count(&conn, "app.users", &doc! { "active": { "$eq": true } }).unwrap(),
            CountPlan::IndexedEquality {
                index_name: "active_1".to_string(),
                key_value: "bool:true".to_string(),
            }
        );
        assert_eq!(
            plan_count(
                &conn,
                "app.users",
                &doc! { "active": true, "profile.city": { "$eq": "Rome" } }
            )
            .unwrap(),
            CountPlan::IndexedEquality {
                index_name: "city_active_1".to_string(),
                key_value: "compound:2:8:str:Rome:9:bool:true".to_string(),
            }
        );
        assert_eq!(
            plan_count(&conn, "app.users", &doc! { "age": 37_i32 }).unwrap(),
            CountPlan::Fallback
        );
        assert_eq!(
            plan_count(&conn, "app.users", &doc! { "age": { "$eq": 37_i64 } }).unwrap(),
            CountPlan::Fallback
        );
        assert_eq!(
            plan_count(&conn, "app.users", &doc! { "age": 37.0 }).unwrap(),
            CountPlan::Fallback
        );

        for filter in [
            doc! { "tags": ["math"] },
            doc! { "$or": [{ "active": true }] },
            doc! { "active": { "$in": [true] } },
            doc! { "active": { "$ne": false } },
            doc! { "active": true, "name": "Ada" },
            doc! { "name": "Ada" },
            doc! { "profile.city": "Rome" },
            doc! { "active": null },
            doc! { "active": { "nested": true } },
            doc! { "profile.city": "Rome", "active": 1_i32 },
            doc! { "profile.city": "Rome", "active": true, "name": "Ada" },
            doc! { "profile.city": "Rome" },
        ] {
            assert_eq!(
                plan_count(&conn, "app.users", &filter).unwrap(),
                CountPlan::Fallback
            );
        }
    }

    #[test]
    fn compound_planner_key_uses_index_order_and_safe_scalars() {
        let spec = IndexSpec {
            name: "compound_1".to_string(),
            key: doc! { "profile.city": 1_i32, "active": -1_i32 },
            unique: false,
            sparse: false,
            partial_filter: None,
            expire_after_seconds: None,
            collation: Collation::Simple,
        };
        let document = doc! {
            "_id": "u1",
            "active": true,
            "profile": { "city": "Rome" },
        };

        assert_eq!(
            planner_key_for_document(&spec, &document),
            Some("compound:2:8:str:Rome:9:bool:true".to_string())
        );

        let reversed = IndexSpec {
            name: "compound_reversed_1".to_string(),
            key: doc! { "active": -1_i32, "profile.city": 1_i32 },
            unique: false,
            sparse: false,
            partial_filter: None,
            expire_after_seconds: None,
            collation: Collation::Simple,
        };
        assert_eq!(
            planner_key_for_document(&reversed, &document),
            Some("compound:2:9:bool:true:8:str:Rome".to_string())
        );

        for unsafe_document in [
            doc! { "_id": "missing", "profile": { "city": "Rome" } },
            doc! { "_id": "null", "profile": { "city": "Rome" }, "active": Bson::Null },
            doc! { "_id": "numeric", "profile": { "city": "Rome" }, "active": 1_i32 },
            doc! { "_id": "array", "profile": { "city": "Rome" }, "active": [true] },
            doc! { "_id": "document", "profile": { "city": "Rome" }, "active": { "nested": true } },
        ] {
            assert_eq!(planner_key_for_document(&spec, &unsafe_document), None);
        }
    }

    #[test]
    fn compound_filter_planner_requires_full_safe_equality_coverage() {
        let spec = IndexSpec {
            name: "compound_1".to_string(),
            key: doc! { "profile.city": 1_i32, "active": -1_i32 },
            unique: false,
            sparse: false,
            partial_filter: None,
            expire_after_seconds: None,
            collation: Collation::Simple,
        };

        assert_eq!(
            compound_planner_key_for_filter(
                &spec,
                &doc! { "active": true, "profile.city": { "$eq": "Rome" } },
            ),
            Some("compound:2:8:str:Rome:9:bool:true".to_string())
        );
        assert_eq!(
            compound_planner_key_for_filter(
                &spec,
                &doc! { "profile.city": "Rome", "active": true, "name": "Ada" },
            ),
            Some("compound:2:8:str:Rome:9:bool:true".to_string())
        );

        for filter in [
            doc! { "profile.city": "Rome" },
            doc! { "$or": [{ "profile.city": "Rome", "active": true }] },
            doc! { "profile.city": "Rome", "active": { "$in": [true] } },
            doc! { "profile.city": "Rome", "active": { "$ne": false } },
            doc! { "profile.city": "Rome", "active": { "$eq": true, "$exists": true } },
            doc! { "profile.city": "Rome", "active": Bson::Null },
            doc! { "profile.city": "Rome", "active": 1_i32 },
            doc! { "profile.city": "Rome", "active": [true] },
            doc! { "profile.city": "Rome", "active": { "nested": true } },
        ] {
            assert_eq!(compound_planner_key_for_filter(&spec, &filter), None);
        }
    }

    #[test]
    fn planner_v2_classifies_exact_prefix_range_and_fallback_shapes() {
        let exact = IndexSpec {
            name: "active_1".to_string(),
            key: doc! { "active": 1_i32 },
            unique: false,
            sparse: false,
            partial_filter: None,
            expire_after_seconds: None,
            collation: Collation::Simple,
        };
        let compound = IndexSpec {
            name: "city_active_created_1".to_string(),
            key: doc! { "profile.city": 1_i32, "active": 1_i32, "created": 1_i32 },
            unique: false,
            sparse: false,
            partial_filter: None,
            expire_after_seconds: None,
            collation: Collation::Simple,
        };
        let created = IndexSpec {
            name: "created_1".to_string(),
            key: doc! { "created": 1_i32 },
            unique: false,
            sparse: false,
            partial_filter: None,
            expire_after_seconds: None,
            collation: Collation::Simple,
        };

        assert!(matches!(
            planner_v2_plan_for_filter(vec![exact.clone()], &doc! { "active": true }),
            PlannerV2Plan::IndexExactEquality { index_name, key_value, diagnostic, .. }
                if index_name == "active_1"
                    && key_value == "bool:true"
                    && diagnostic.scan_strategy == PlannerScanStrategy::IndexExactEquality
                    && diagnostic.matcher_validation_required
        ));

        assert!(matches!(
            planner_v2_plan_for_filter(
                vec![compound.clone()],
                &doc! { "profile.city": "Rome" },
            ),
            PlannerV2Plan::IndexEqualityPrefix { index_name, prefix_len, key_value, diagnostic, .. }
                if index_name == "city_active_created_1"
                    && prefix_len == 1
                    && key_value == "compound-prefix:1:8:str:Rome"
                    && diagnostic.scan_strategy == PlannerScanStrategy::IndexEqualityPrefix
        ));

        assert!(matches!(
            planner_v2_plan_for_filter(
                vec![compound],
                &doc! { "profile.city": "Rome", "active": true, "created": { "$gte": "2026-01", "$lt": "2026-02" } },
            ),
            PlannerV2Plan::IndexRange { index_name, range, diagnostic, .. }
                if index_name == "city_active_created_1"
                    && range.field == "created"
                    && range.equality_prefix_len == 2
                    && range.key_prefix == "range:2:8:str:Rome:9:bool:true:"
                    && range.lower == Some(RangeBound { key: "str:2026-01".to_string(), inclusive: true })
                    && range.upper == Some(RangeBound { key: "str:2026-02".to_string(), inclusive: false })
                    && diagnostic.scan_strategy == PlannerScanStrategy::IndexRange
        ));

        assert!(matches!(
            planner_v2_plan_for_filter(
                vec![created],
                &doc! { "created": { "$gte": 1_i32 } },
            ),
            PlannerV2Plan::CollectionScan { diagnostic }
                if diagnostic
                    .fallback_reason
                    .as_deref()
                    .is_some_and(|reason| reason.contains("does not match supported"))
        ));
    }

    #[test]
    fn planner_v2_falls_back_for_unsupported_and_membership_unsafe_shapes() {
        let sparse = IndexSpec {
            name: "email_sparse".to_string(),
            key: doc! { "email": 1_i32, "active": 1_i32 },
            unique: false,
            sparse: true,
            partial_filter: None,
            expire_after_seconds: None,
            collation: Collation::Simple,
        };
        let partial = IndexSpec {
            name: "email_active_partial".to_string(),
            key: doc! { "email": 1_i32 },
            unique: false,
            sparse: false,
            partial_filter: Some(doc! { "active": true }),
            expire_after_seconds: None,
            collation: Collation::Simple,
        };
        let tags = IndexSpec {
            name: "tags_1".to_string(),
            key: doc! { "tags": 1_i32 },
            unique: false,
            sparse: false,
            partial_filter: None,
            expire_after_seconds: None,
            collation: Collation::Simple,
        };

        assert!(matches!(
            planner_v2_plan_for_filter(vec![tags.clone()], &doc! { "$or": [{ "tags": "math" }] }),
            PlannerV2Plan::CollectionScan { diagnostic }
                if diagnostic.fallback_reason == Some("top-level logical filters are not index-planned".to_string())
        ));
        assert!(matches!(
            planner_v2_plan_for_filter(vec![tags], &doc! { "tags": { "$in": ["math"] } }),
            PlannerV2Plan::CollectionScan { diagnostic }
                if diagnostic
                    .fallback_reason
                    .as_deref()
                    .is_some_and(|reason| reason.contains("does not match supported"))
        ));
        assert!(matches!(
            planner_v2_plan_for_filter(vec![sparse], &doc! { "email": "a@example.test" }),
            PlannerV2Plan::CollectionScan { diagnostic }
                if diagnostic
                    .fallback_reason
                    .as_deref()
                    .is_some_and(|reason| reason.contains("membership"))
        ));
        assert!(matches!(
            planner_v2_plan_for_filter(vec![partial], &doc! { "email": "a@example.test" }),
            PlannerV2Plan::CollectionScan { diagnostic }
                if diagnostic
                    .fallback_reason
                    .as_deref()
                    .is_some_and(|reason| reason.contains("membership"))
        ));
    }

    #[test]
    fn transaction_candidate_planner_classifies_safe_and_fallback_filters() {
        let conn = test_conn();
        seed_find_documents(&conn);
        create_indexes(
            &conn,
            &doc! {
                "createIndexes": "users",
                "$db": "app",
                "indexes": [
                    { "key": { "active": 1_i32 }, "name": "active_1" },
                    { "key": { "profile.city": 1_i32 }, "name": "city_1" },
                    { "key": { "age": 1_i32 }, "name": "age_1" },
                    { "key": { "profile.city": 1_i32, "active": 1_i32 }, "name": "city_active_1" },
                ],
            },
        )
        .unwrap();

        let tx = conn.unchecked_transaction().unwrap();
        assert_eq!(
            plan_transaction_candidates(&tx, "app.users", &doc! { "_id": "u1" }).unwrap(),
            TransactionCandidatePlan::IdEquality("str:u1".to_string())
        );
        assert_eq!(
            plan_transaction_candidates(&tx, "app.users", &doc! { "_id": { "$eq": "u1" } })
                .unwrap(),
            TransactionCandidatePlan::IdEquality("str:u1".to_string())
        );
        assert_eq!(
            plan_transaction_candidates(&tx, "app.users", &doc! { "active": true }).unwrap(),
            TransactionCandidatePlan::IndexedEquality {
                index_name: "active_1".to_string(),
                key_value: "bool:true".to_string(),
                unique: false,
            }
        );
        assert_eq!(
            plan_transaction_candidates(
                &tx,
                "app.users",
                &doc! { "profile.city": { "$eq": "Rome" } }
            )
            .unwrap(),
            TransactionCandidatePlan::IndexedEquality {
                index_name: "city_1".to_string(),
                key_value: "str:Rome".to_string(),
                unique: false,
            }
        );
        assert_eq!(
            plan_transaction_candidates(
                &tx,
                "app.users",
                &doc! { "profile.city": "Rome", "active": true }
            )
            .unwrap(),
            TransactionCandidatePlan::IndexedEquality {
                index_name: "city_active_1".to_string(),
                key_value: "compound:2:8:str:Rome:9:bool:true".to_string(),
                unique: false,
            }
        );
        assert_eq!(
            plan_transaction_candidates(&tx, "app.users", &doc! { "active": true, "name": "Ada" })
                .unwrap(),
            TransactionCandidatePlan::IndexedEquality {
                index_name: "active_1".to_string(),
                key_value: "bool:true".to_string(),
                unique: false,
            }
        );
        assert_eq!(
            plan_transaction_candidates(
                &tx,
                "app.users",
                &doc! { "profile.city": "Rome", "active": true, "name": "Ada" }
            )
            .unwrap(),
            TransactionCandidatePlan::IndexedEquality {
                index_name: "city_active_1".to_string(),
                key_value: "compound:2:8:str:Rome:9:bool:true".to_string(),
                unique: false,
            }
        );
        assert_eq!(
            plan_transaction_candidates(
                &tx,
                "app.users",
                &doc! { "profile.city": "Rome", "active": 1_i32 }
            )
            .unwrap(),
            TransactionCandidatePlan::IndexedEquality {
                index_name: "city_1".to_string(),
                key_value: "str:Rome".to_string(),
                unique: false,
            }
        );

        for filter in [
            doc! {},
            doc! { "tags": ["math"] },
            doc! { "$or": [{ "active": true }] },
            doc! { "active": { "$in": [true] } },
            doc! { "active": { "$ne": false } },
            doc! { "name": "Ada" },
            doc! { "active": null },
            doc! { "active": { "nested": true } },
            doc! { "age": 37_i32 },
            doc! { "age": 37_i64 },
            doc! { "age": 37.0 },
        ] {
            assert_eq!(
                plan_transaction_candidates(&tx, "app.users", &filter).unwrap(),
                TransactionCandidatePlan::Fallback
            );
        }
    }

    #[test]
    fn transaction_candidate_loader_returns_created_order_candidates() {
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

        let tx = conn.unchecked_transaction().unwrap();
        let id_candidates =
            transaction_candidate_documents(&tx, "app.users", &doc! { "_id": "u2" }).unwrap();
        assert_eq!(
            id_candidates
                .iter()
                .map(|stored| stored.document.get_str("_id").unwrap().to_string())
                .collect::<Vec<_>>(),
            vec!["u2"]
        );

        let indexed_candidates =
            transaction_candidate_documents(&tx, "app.users", &doc! { "profile.city": "Rome" })
                .unwrap();
        assert_eq!(
            indexed_candidates
                .iter()
                .map(|stored| stored.document.get_str("_id").unwrap().to_string())
                .collect::<Vec<_>>(),
            vec!["u1", "u3"]
        );
    }

    #[test]
    fn count_pushdown_handles_empty_and_id_filters_with_bounds() {
        let conn = test_conn();
        seed_find_documents(&conn);

        assert_eq!(
            pushed_down_count(&conn, "app.users", &doc! {}, 0, None)
                .unwrap()
                .unwrap(),
            3
        );
        assert_eq!(
            pushed_down_count(&conn, "app.users", &doc! {}, 1, Some(1))
                .unwrap()
                .unwrap(),
            1
        );
        assert_eq!(
            pushed_down_count(&conn, "app.users", &doc! { "_id": "u1" }, 0, None)
                .unwrap()
                .unwrap(),
            1
        );
        assert_eq!(
            pushed_down_count(
                &conn,
                "app.users",
                &doc! { "_id": { "$eq": "u1" } },
                1,
                None
            )
            .unwrap()
            .unwrap(),
            0
        );
        assert_eq!(
            pushed_down_count(&conn, "app.users", &doc! { "_id": "missing" }, 0, None)
                .unwrap()
                .unwrap(),
            0
        );
        assert_eq!(
            pushed_down_count(&conn, "app.users", &doc! { "active": true }, 0, None).unwrap(),
            None
        );
    }

    #[test]
    fn indexed_count_pushdown_tracks_index_entry_mutations() {
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

        assert_eq!(
            pushed_down_count(
                &conn,
                "app.users",
                &doc! { "profile.city": "Rome" },
                0,
                None
            )
            .unwrap(),
            Some(2)
        );
        assert_eq!(
            pushed_down_count(
                &conn,
                "app.users",
                &doc! { "profile.city": { "$eq": "Rome" } },
                1,
                Some(1)
            )
            .unwrap(),
            Some(1)
        );

        update_documents(
            &conn,
            &doc! {
                "update": "users",
                "$db": "app",
                "updates": [{ "q": { "_id": "u1" }, "u": { "$set": { "profile.city": "Milan" } } }],
            },
        )
        .unwrap();
        assert_eq!(
            pushed_down_count(
                &conn,
                "app.users",
                &doc! { "profile.city": "Rome" },
                0,
                None
            )
            .unwrap(),
            Some(1)
        );
        assert_eq!(
            pushed_down_count(
                &conn,
                "app.users",
                &doc! { "profile.city": "Milan" },
                0,
                None
            )
            .unwrap(),
            Some(1)
        );

        delete_documents(
            &conn,
            &doc! {
                "delete": "users",
                "$db": "app",
                "deletes": [{ "q": { "_id": "u3" }, "limit": 1_i32 }],
            },
        )
        .unwrap();
        assert_eq!(
            pushed_down_count(
                &conn,
                "app.users",
                &doc! { "profile.city": "Rome" },
                0,
                None
            )
            .unwrap(),
            Some(0)
        );

        drop_indexes(
            &conn,
            &doc! { "dropIndexes": "users", "$db": "app", "index": "city_1" },
        )
        .unwrap();
        assert_eq!(
            pushed_down_count(
                &conn,
                "app.users",
                &doc! { "profile.city": "Milan" },
                0,
                None
            )
            .unwrap(),
            None
        );
    }

    #[test]
    fn numeric_indexed_count_falls_back_to_matcher_semantics() {
        let conn = test_conn();
        insert_documents(
            &conn,
            &doc! {
                "insert": "numbers",
                "$db": "app",
                "documents": [
                    { "_id": "i32", "n": 1_i32 },
                    { "_id": "i64", "n": 1_i64 },
                    { "_id": "double", "n": 1.0 },
                    { "_id": "other", "n": 2_i32 },
                ],
            },
        )
        .unwrap();
        create_indexes(
            &conn,
            &doc! {
                "createIndexes": "numbers",
                "$db": "app",
                "indexes": [{ "key": { "n": 1_i32 }, "name": "n_1" }],
            },
        )
        .unwrap();

        for filter in [
            doc! { "n": 1_i32 },
            doc! { "n": { "$eq": 1_i64 } },
            doc! { "n": 1.0 },
        ] {
            assert_eq!(
                plan_count(&conn, "app.numbers", &filter).unwrap(),
                CountPlan::Fallback
            );
            assert_eq!(
                pushed_down_count(&conn, "app.numbers", &filter, 0, None).unwrap(),
                None
            );
            let response = count_documents_command(
                &conn,
                &doc! { "count": "numbers", "$db": "app", "query": filter },
            )
            .unwrap();
            assert_eq!(response.get_i64("n").unwrap(), 3);
        }
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
    fn aggregate_match_count_uses_safe_count_pushdown() {
        let conn = test_conn();
        seed_find_documents(&conn);
        create_indexes(
            &conn,
            &doc! {
                "createIndexes": "users",
                "$db": "app",
                "indexes": [{ "key": { "active": 1_i32 }, "name": "active_1" }],
            },
        )
        .unwrap();

        let indexed = aggregate_command(
            &conn,
            &doc! {
                "aggregate": "users",
                "$db": "app",
                "pipeline": [
                    { "$match": { "active": true } },
                    { "$count": "total" },
                ],
                "cursor": {},
            },
        )
        .unwrap();
        assert_eq!(first_batch(&indexed), vec![doc! { "total": 2_i64 }]);

        let id_match = aggregate_command(
            &conn,
            &doc! {
                "aggregate": "users",
                "$db": "app",
                "pipeline": [
                    { "$match": { "_id": "u1" } },
                    { "$count": "total" },
                ],
                "cursor": {},
            },
        )
        .unwrap();
        assert_eq!(first_batch(&id_match), vec![doc! { "total": 1_i64 }]);

        let empty_match = aggregate_command(
            &conn,
            &doc! {
                "aggregate": "users",
                "$db": "app",
                "pipeline": [
                    { "$match": { "active": false } },
                    { "$count": "total" },
                ],
                "cursor": {},
            },
        )
        .unwrap();
        assert_eq!(first_batch(&empty_match), vec![doc! { "total": 1_i64 }]);

        let no_results = aggregate_command(
            &conn,
            &doc! {
                "aggregate": "users",
                "$db": "app",
                "pipeline": [
                    { "$match": { "_id": "missing" } },
                    { "$count": "total" },
                ],
                "cursor": {},
            },
        )
        .unwrap();
        assert_eq!(first_batch(&no_results), Vec::<Document>::new());
    }

    #[test]
    fn aggregate_match_count_falls_back_for_unsupported_filters() {
        let conn = test_conn();
        seed_find_documents(&conn);

        let response = aggregate_command(
            &conn,
            &doc! {
                "aggregate": "users",
                "$db": "app",
                "pipeline": [
                    { "$match": { "$where": "this.active" } },
                    { "$count": "total" },
                ],
                "cursor": {},
            },
        )
        .unwrap();

        assert_command_error(&response);
        assert_eq!(response.get_i32("code").unwrap(), 2);
        assert!(response.get_str("errmsg").unwrap().contains("$where"));
    }

    #[test]
    fn aggregate_match_count_numeric_indexed_filter_uses_matcher_semantics() {
        let conn = test_conn();
        insert_documents(
            &conn,
            &doc! {
                "insert": "numbers",
                "$db": "app",
                "documents": [
                    { "_id": "i32", "n": 1_i32 },
                    { "_id": "i64", "n": 1_i64 },
                    { "_id": "double", "n": 1.0 },
                    { "_id": "other", "n": 2_i32 },
                ],
            },
        )
        .unwrap();
        create_indexes(
            &conn,
            &doc! {
                "createIndexes": "numbers",
                "$db": "app",
                "indexes": [{ "key": { "n": 1_i32 }, "name": "n_1" }],
            },
        )
        .unwrap();

        for filter in [
            doc! { "n": 1_i32 },
            doc! { "n": { "$eq": 1_i64 } },
            doc! { "n": 1.0 },
        ] {
            let response = aggregate_command(
                &conn,
                &doc! {
                    "aggregate": "numbers",
                    "$db": "app",
                    "pipeline": [
                        { "$match": filter },
                        { "$count": "total" },
                    ],
                    "cursor": {},
                },
            )
            .unwrap();
            assert_eq!(first_batch(&response), vec![doc! { "total": 3_i64 }]);
        }
    }

    #[test]
    fn aggregation_expression_parses_and_evaluates_supported_subset() {
        let document = doc! {
            "_id": "u1",
            "team": "red",
            "active": true,
            "first": "Ada",
            "last": "Lovelace",
            "score": 7_i32,
            "bonus": 2_i64,
            "profile": { "city": "Rome" },
        };
        let context = AggregationExpressionContext::new(&document, &Collation::Simple);

        let field = parse_aggregation_expression(
            &Bson::String("$profile.city".to_string()),
            "$group _id",
            false,
        )
        .unwrap();
        assert_eq!(
            field.evaluate(&context).unwrap(),
            Some(Bson::String("Rome".to_string()))
        );

        let literal =
            parse_aggregation_expression(&Bson::String("literal".to_string()), "$group _id", false)
                .unwrap();
        assert_eq!(
            literal.evaluate(&context).unwrap(),
            Some(Bson::String("literal".to_string()))
        );

        let key = parse_aggregation_expression(
            &Bson::Document(doc! { "team": "$team", "active": "$active", "missing": "$missing" }),
            "$group _id",
            true,
        )
        .unwrap();
        assert_eq!(
            key.evaluate(&context).unwrap(),
            Some(Bson::Document(
                doc! { "team": "red", "active": true, "missing": Bson::Null }
            ))
        );

        let variable = parse_aggregation_expression(
            &Bson::String("$$ROOT.profile.city".to_string()),
            "test",
            true,
        )
        .unwrap();
        assert_eq!(
            variable.evaluate(&context).unwrap(),
            Some(Bson::String("Rome".to_string()))
        );

        let array = parse_aggregation_expression(
            &Bson::Array(vec![
                Bson::String("$team".to_string()),
                Bson::String("literal".to_string()),
            ]),
            "test",
            true,
        )
        .unwrap();
        assert_eq!(
            array.evaluate(&context).unwrap(),
            Some(Bson::Array(vec![
                Bson::String("red".to_string()),
                Bson::String("literal".to_string())
            ]))
        );

        for (expression, expected) in [
            (
                doc! { "$concat": ["$first", " ", "$last"] },
                Bson::String("Ada Lovelace".to_string()),
            ),
            (
                doc! { "$toString": "$score" },
                Bson::String("7".to_string()),
            ),
            (
                doc! { "$toLower": "LOUD" },
                Bson::String("loud".to_string()),
            ),
            (
                doc! { "$toUpper": "$team" },
                Bson::String("RED".to_string()),
            ),
            (
                doc! { "$ifNull": ["$missing", "$team"] },
                Bson::String("red".to_string()),
            ),
            (doc! { "$eq": ["$team", "red"] }, Bson::Boolean(true)),
            (doc! { "$ne": ["$team", "blue"] }, Bson::Boolean(true)),
            (doc! { "$gt": ["$score", 3_i32] }, Bson::Boolean(true)),
            (doc! { "$gte": ["$score", 7_i32] }, Bson::Boolean(true)),
            (doc! { "$lt": ["$score", 8_i32] }, Bson::Boolean(true)),
            (doc! { "$lte": ["$score", 7_i32] }, Bson::Boolean(true)),
            (doc! { "$and": [true, "$active"] }, Bson::Boolean(true)),
            (doc! { "$or": [false, "$active"] }, Bson::Boolean(true)),
            (doc! { "$not": ["$missing"] }, Bson::Boolean(true)),
            (
                doc! { "$cond": [{ "$eq": ["$team", "red"] }, "yes", "no"] },
                Bson::String("yes".to_string()),
            ),
            (
                doc! { "$cond": { "if": "$active", "then": "yes", "else": "no" } },
                Bson::String("yes".to_string()),
            ),
            (
                doc! { "$add": ["$score", "$bonus", 1_i32] },
                Bson::Int64(10),
            ),
            (doc! { "$subtract": ["$score", 2_i32] }, Bson::Int64(5)),
            (doc! { "$multiply": ["$score", 2_i32] }, Bson::Int64(14)),
            (doc! { "$divide": ["$score", 2_i32] }, Bson::Double(3.5)),
        ] {
            let parsed = parse_aggregation_expression(&Bson::Document(expression), "test", true)
                .expect("expression should parse");
            assert_eq!(parsed.evaluate(&context).unwrap(), Some(expected));
        }

        let ci_context =
            AggregationExpressionContext::new(&document, &Collation::EnglishCaseInsensitive);
        let case_eq = parse_aggregation_expression(
            &Bson::Document(doc! { "$eq": ["$team", "RED"] }),
            "test",
            true,
        )
        .unwrap();
        assert_eq!(
            case_eq.evaluate(&ci_context).unwrap(),
            Some(Bson::Boolean(true))
        );
    }

    #[test]
    fn aggregation_expression_rejects_unsupported_shapes() {
        for value in [
            Bson::String("$".to_string()),
            Bson::String("$$NOW".to_string()),
            Bson::String("$profile..city".to_string()),
            Bson::Document(doc! { "$add": 1_i32 }),
            Bson::Document(doc! { "$subtract": [1_i32] }),
            Bson::Document(doc! { "$divide": [1_i32, 2_i32, 3_i32] }),
            Bson::Document(doc! { "$cond": { "if": true, "then": 1_i32 } }),
            Bson::Document(doc! { "$unknown": [1_i32] }),
            Bson::Document(doc! { "$eq": [1_i32, 1_i32], "$ne": [1_i32, 2_i32] }),
        ] {
            let response = parse_aggregation_expression(&value, "$group _id", false)
                .expect_err("expression should be rejected");
            assert_command_error(&response);
        }

        for value in [
            Bson::Document(doc! { "nested.field": "$team" }),
            Bson::Document(doc! { "$team": "$team" }),
        ] {
            let response = parse_aggregation_expression(&value, "$group _id", true)
                .expect_err("document key spec should be rejected");
            assert_command_error(&response);
        }

        let zero_document = doc! { "denominator": 0_i32 };
        let context = AggregationExpressionContext::new(&zero_document, &Collation::Simple);
        let division = parse_aggregation_expression(
            &Bson::Document(doc! { "$divide": [10_i32, "$denominator"] }),
            "test",
            true,
        )
        .unwrap();
        assert_command_error(
            &division
                .evaluate(&context)
                .expect_err("division by zero should fail"),
        );
    }

    #[test]
    fn aggregate_pipeline_match_sort_project_skip_limit_and_count() {
        let conn = test_conn();
        seed_find_documents(&conn);

        let response = aggregate_command(
            &conn,
            &doc! {
                "aggregate": "users",
                "$db": "app",
                "pipeline": [
                    { "$match": { "active": true } },
                    { "$sort": { "age": -1_i32 } },
                    { "$project": { "name": 1_i32, "age": 1_i32, "_id": 0_i32 } },
                    { "$skip": 1_i32 },
                    { "$limit": 1_i32 },
                ],
                "cursor": {},
            },
        )
        .unwrap();

        let batch = first_batch(&response);
        assert_eq!(batch, vec![doc! { "name": "Ada", "age": 37_i32 }]);

        let count = aggregate_command(
            &conn,
            &doc! {
                "aggregate": "users",
                "$db": "app",
                "pipeline": [
                    { "$match": { "profile.city": "Rome" } },
                    { "$count": "total" },
                ],
                "cursor": {},
            },
        )
        .unwrap();
        assert_eq!(first_batch(&count), vec![doc! { "total": 2_i64 }]);
    }

    #[test]
    fn aggregate_project_add_set_and_unset_compute_document_shapes() {
        let conn = test_conn();
        insert_documents(
            &conn,
            &doc! {
                "insert": "authors",
                "$db": "app",
                "documents": [
                    {
                        "_id": "a1",
                        "first": "Ada",
                        "last": "Lovelace",
                        "score": 7_i32,
                        "profile": { "city": "London", "hidden": true },
                        "tags": ["math", "logic"],
                    },
                    {
                        "_id": "a2",
                        "first": "Grace",
                        "last": "Hopper",
                        "score": 9_i32,
                        "profile": { "city": "Arlington", "hidden": true },
                        "tags": ["compiler"],
                    },
                ],
            },
        )
        .unwrap();

        let response = aggregate_command(
            &conn,
            &doc! {
                "aggregate": "authors",
                "$db": "app",
                "pipeline": [
                    { "$match": { "_id": "a1" } },
                    {
                        "$project": {
                            "_id": 0_i32,
                            "first": 1_i32,
                            "display": { "$concat": ["$first", " ", "$last"] },
                            "nested": { "city": "$profile.city", "scoreText": { "$toString": "$score" } },
                            "rootCopy": "$$ROOT._id",
                        }
                    },
                    {
                        "$addFields": {
                            "nested.lower": { "$toLower": "$display" },
                            "computed.total": { "$add": [4_i32, 6_i32] },
                        }
                    },
                    { "$set": { "alias": "$nested.city" } },
                    { "$unset": ["first", "nested.scoreText"] },
                ],
                "cursor": {},
            },
        )
        .unwrap();

        assert_eq!(
            first_batch(&response),
            vec![doc! {
                "display": "Ada Lovelace",
                "nested": {
                    "city": "London",
                    "lower": "ada lovelace",
                },
                "rootCopy": "a1",
                "computed": { "total": 10_i64 },
                "alias": "London",
            }]
        );

        let exclude = aggregate_command(
            &conn,
            &doc! {
                "aggregate": "authors",
                "$db": "app",
                "pipeline": [
                    { "$match": { "_id": "a2" } },
                    { "$unset": "profile.hidden" },
                    { "$project": { "tags": 0_i32, "_id": 0_i32 } },
                ],
                "cursor": {},
            },
        )
        .unwrap();
        assert_eq!(
            first_batch(&exclude),
            vec![doc! {
                "first": "Grace",
                "last": "Hopper",
                "score": 9_i32,
                "profile": { "city": "Arlington" },
            }]
        );
    }

    #[test]
    fn aggregate_shaping_rejects_adversarial_paths_and_expressions_before_ttl() {
        let conn = test_conn();
        seed_ttl_command_fixture(&conn, "bad_aggregate_shape");

        for pipeline in [
            vec![Bson::Document(
                doc! { "$project": { "name": 1_i32, "name.first": "$first" } },
            )],
            vec![Bson::Document(
                doc! { "$project": { "name": 0_i32, "display": { "$concat": ["$first"] } } },
            )],
            vec![Bson::Document(doc! { "$project": { "": "$first" } })],
            vec![Bson::Document(doc! { "$project": { "$bad": "$first" } })],
            vec![Bson::Document(
                doc! { "$addFields": { "profile": "$first", "profile.city": "$last" } },
            )],
            vec![Bson::Document(
                doc! { "$set": { "profile.$bad": "$first" } },
            )],
            vec![Bson::Document(
                doc! { "$unset": ["profile", "profile.city"] },
            )],
            vec![Bson::Document(doc! { "$unset": [1_i32] })],
        ] {
            let response = aggregate_command(
                &conn,
                &doc! {
                    "aggregate": "bad_aggregate_shape",
                    "$db": "app",
                    "pipeline": pipeline,
                    "cursor": {},
                },
            )
            .unwrap();
            assert_invalid_ttl_read_preserves_expired(&conn, "bad_aggregate_shape", response);
        }

        for pipeline in [
            vec![Bson::Document(
                doc! { "$addFields": { "ratio": { "$divide": [10_i32, 0_i32] } } },
            )],
            vec![Bson::Document(
                doc! { "$addFields": { "total": { "$add": [1_i32, "bad"] } } },
            )],
            vec![Bson::Document(
                doc! { "$set": { "total": { "$multiply": [2_i32, "bad"] } } },
            )],
            vec![Bson::Document(
                doc! { "$project": { "lower": { "$toLower": 1_i32 } } },
            )],
            vec![Bson::Document(
                doc! { "$project": { "label": { "$concat": ["ok", 1_i32] } } },
            )],
        ] {
            let response = aggregate_command(
                &conn,
                &doc! {
                    "aggregate": "bad_aggregate_shape",
                    "$db": "app",
                    "pipeline": pipeline,
                    "cursor": {},
                },
            )
            .unwrap();
            assert_invalid_ttl_read_preserves_expired(&conn, "bad_aggregate_shape", response);
        }

        let data_dependent = aggregate_command(
            &conn,
            &doc! {
                "aggregate": "bad_aggregate_shape",
                "$db": "app",
                "pipeline": [
                    { "$match": { "_id": "future" } },
                    { "$addFields": { "ratio": { "$divide": ["$category", 2_i32] } } },
                ],
                "cursor": {},
            },
        )
        .unwrap();
        assert_command_error(&data_dependent);
    }

    #[test]
    fn aggregate_replace_root_replace_with_and_group_computed_operands() {
        let conn = test_conn();
        insert_documents(
            &conn,
            &doc! {
                "insert": "orders",
                "$db": "app",
                "documents": [
                    {
                        "_id": "o1",
                        "customer": { "id": "c1", "name": "Ada" },
                        "price": 7_i32,
                        "tax": 2_i32,
                        "status": "open",
                    },
                    {
                        "_id": "o2",
                        "customer": { "id": "c1", "name": "Ada" },
                        "price": 5_i32,
                        "tax": 1_i32,
                        "status": "closed",
                    },
                    {
                        "_id": "o3",
                        "customer": { "id": "c2", "name": "Grace" },
                        "price": 9_i32,
                        "tax": 3_i32,
                        "status": "open",
                    },
                ],
            },
        )
        .unwrap();

        let replaced = aggregate_command(
            &conn,
            &doc! {
                "aggregate": "orders",
                "$db": "app",
                "pipeline": [
                    { "$match": { "_id": "o1" } },
                    { "$replaceRoot": { "newRoot": "$customer" } },
                ],
                "cursor": {},
            },
        )
        .unwrap();
        assert_eq!(
            first_batch(&replaced),
            vec![doc! { "id": "c1", "name": "Ada" }]
        );

        let computed_root = aggregate_command(
            &conn,
            &doc! {
                "aggregate": "orders",
                "$db": "app",
                "pipeline": [
                    { "$match": { "_id": "o3" } },
                    {
                        "$replaceWith": {
                            "customerId": "$customer.id",
                            "label": { "$concat": ["$customer.name", ":", "$status"] },
                            "total": { "$add": ["$price", "$tax"] },
                        }
                    },
                ],
                "cursor": {},
            },
        )
        .unwrap();
        assert_eq!(
            first_batch(&computed_root),
            vec![doc! { "customerId": "c2", "label": "Grace:open", "total": 12_i64 }]
        );

        let grouped = aggregate_command(
            &conn,
            &doc! {
                "aggregate": "orders",
                "$db": "app",
                "pipeline": [
                    {
                        "$group": {
                            "_id": { "customer": "$customer.id", "open": { "$eq": ["$status", "open"] } },
                            "gross": { "$sum": { "$add": ["$price", "$tax"] } },
                            "avgGross": { "$avg": { "$add": ["$price", "$tax"] } },
                            "labels": { "$push": { "$concat": ["$customer.name", ":", "$status"] } },
                            "snapshots": { "$addToSet": { "status": "$status", "total": { "$add": ["$price", "$tax"] } } },
                            "firstTotal": { "$first": { "$add": ["$price", "$tax"] } },
                            "lastUpper": { "$last": { "$toUpper": "$status" } },
                            "minTotal": { "$min": { "$add": ["$price", "$tax"] } },
                            "maxTotal": { "$max": { "$add": ["$price", "$tax"] } },
                        }
                    },
                    { "$sort": { "gross": -1_i32 } },
                    { "$limit": 1_i32 },
                ],
                "cursor": {},
            },
        )
        .unwrap();
        assert_eq!(
            first_batch(&grouped),
            vec![doc! {
                "_id": { "customer": "c2", "open": true },
                "gross": 12_i64,
                "avgGross": 12.0,
                "labels": ["Grace:open"],
                "snapshots": [{ "status": "open", "total": 12_i64 }],
                "firstTotal": 12_i64,
                "lastUpper": "OPEN",
                "minTotal": 12_i64,
                "maxTotal": 12_i64,
            }]
        );
    }

    #[test]
    fn aggregate_replace_root_rejects_malformed_and_non_document_results() {
        let conn = test_conn();
        seed_ttl_command_fixture(&conn, "bad_replace_root");

        for pipeline in [
            vec![Bson::Document(doc! { "$replaceRoot": "$profile" })],
            vec![Bson::Document(doc! { "$replaceRoot": {} })],
            vec![Bson::Document(
                doc! { "$replaceRoot": { "newRoot": "$profile", "extra": true } },
            )],
            vec![Bson::Document(doc! { "$replaceWith": { "$dateDiff": {} } })],
            vec![Bson::Document(
                doc! { "$replaceRoot": { "newRoot": 1_i32 } },
            )],
            vec![Bson::Document(doc! { "$replaceWith": "literal" })],
        ] {
            let response = aggregate_command(
                &conn,
                &doc! {
                    "aggregate": "bad_replace_root",
                    "$db": "app",
                    "pipeline": pipeline,
                    "cursor": {},
                },
            )
            .unwrap();
            assert_invalid_ttl_read_preserves_expired(&conn, "bad_replace_root", response);
        }

        for pipeline in [
            vec![Bson::Document(
                doc! { "$replaceRoot": { "newRoot": "$missing" } },
            )],
            vec![Bson::Document(doc! { "$replaceWith": "$expiresAt" })],
        ] {
            let response = aggregate_command(
                &conn,
                &doc! {
                    "aggregate": "bad_replace_root",
                    "$db": "app",
                    "pipeline": pipeline,
                    "cursor": {},
                },
            )
            .unwrap();
            assert_command_error(&response);
        }
    }

    #[test]
    fn aggregate_lookup_supports_simple_equality_arrays_null_collation_and_cursor() {
        let conn = test_conn();
        insert_documents(
            &conn,
            &doc! {
                "insert": "orders",
                "$db": "app",
                "documents": [
                    { "_id": "o1", "profileId": "p1", "profileIds": ["p2", "missing"], "owner": "ADA" },
                    { "_id": "o2", "profileId": Bson::Null, "owner": "grace" },
                    { "_id": "o3", "owner": "missing" },
                ],
            },
        )
        .unwrap();
        insert_documents(
            &conn,
            &doc! {
                "insert": "profiles",
                "$db": "app",
                "documents": [
                    { "_id": "p1", "name": "Ada", "owner": "ada" },
                    { "_id": "p2", "name": "Grace", "owner": "GRACE" },
                    { "_id": "p3", "name": "Nullish", "profileId": Bson::Null },
                    { "_id": "p4", "name": "MissingForeign" },
                ],
            },
        )
        .unwrap();
        let mut client_state = ClientState::default();

        let joined = aggregate_command_with_state(
            &conn,
            &mut client_state,
            &doc! {
                "aggregate": "orders",
                "$db": "app",
                "pipeline": [
                    {
                        "$lookup": {
                            "from": "profiles",
                            "localField": "profileId",
                            "foreignField": "_id",
                            "as": "profile",
                        }
                    },
                    {
                        "$lookup": {
                            "from": "profiles",
                            "localField": "profileIds",
                            "foreignField": "_id",
                            "as": "arrayMatches",
                        }
                    },
                    {
                        "$lookup": {
                            "from": "profiles",
                            "localField": "profileId",
                            "foreignField": "profileId",
                            "as": "nullMatches",
                        }
                    },
                    { "$sort": { "_id": 1_i32 } },
                    { "$project": { "_id": 1_i32, "profile": 1_i32, "arrayMatches": 1_i32, "nullMatches": 1_i32 } },
                ],
                "cursor": { "batchSize": 2_i32 },
            },
        )
        .unwrap();

        let id = cursor_id(&joined);
        assert!(id > 0);
        assert_eq!(
            first_batch(&joined),
            vec![
                doc! { "_id": "o1", "profile": [{ "_id": "p1", "name": "Ada", "owner": "ada" }], "arrayMatches": [{ "_id": "p2", "name": "Grace", "owner": "GRACE" }], "nullMatches": [] },
                doc! { "_id": "o2", "profile": [], "arrayMatches": [], "nullMatches": [
                    { "_id": "p1", "name": "Ada", "owner": "ada" },
                    { "_id": "p2", "name": "Grace", "owner": "GRACE" },
                    { "_id": "p3", "name": "Nullish", "profileId": Bson::Null },
                    { "_id": "p4", "name": "MissingForeign" },
                ] },
            ]
        );
        let next = get_more(
            &mut client_state,
            &doc! { "getMore": id, "collection": "orders", "$db": "app", "batchSize": 2_i32 },
        )
        .unwrap();
        assert_eq!(cursor_id(&next), 0);
        assert_eq!(
            next_batch(&next),
            vec![
                doc! { "_id": "o3", "profile": [], "arrayMatches": [], "nullMatches": [
                    { "_id": "p1", "name": "Ada", "owner": "ada" },
                    { "_id": "p2", "name": "Grace", "owner": "GRACE" },
                    { "_id": "p3", "name": "Nullish", "profileId": Bson::Null },
                    { "_id": "p4", "name": "MissingForeign" },
                ] }
            ]
        );

        let collation = aggregate_command(
            &conn,
            &doc! {
                "aggregate": "orders",
                "$db": "app",
                "pipeline": [
                    {
                        "$lookup": {
                            "from": "profiles",
                            "localField": "owner",
                            "foreignField": "owner",
                            "as": "owners",
                        }
                    },
                    { "$sort": { "_id": 1_i32 } },
                    { "$project": { "_id": 1_i32, "owners": 1_i32 } },
                ],
                "collation": { "locale": "en", "strength": 2_i32 },
                "cursor": {},
            },
        )
        .unwrap();
        assert_eq!(
            first_batch(&collation),
            vec![
                doc! { "_id": "o1", "owners": [{ "_id": "p1", "name": "Ada", "owner": "ada" }] },
                doc! { "_id": "o2", "owners": [{ "_id": "p2", "name": "Grace", "owner": "GRACE" }] },
                doc! { "_id": "o3", "owners": [] },
            ]
        );

        let self_lookup = aggregate_command(
            &conn,
            &doc! {
                "aggregate": "orders",
                "$db": "app",
                "pipeline": [
                    { "$match": { "_id": "o1" } },
                    {
                        "$lookup": {
                            "from": "orders",
                            "localField": "_id",
                            "foreignField": "_id",
                            "as": "self",
                        }
                    },
                    { "$project": { "_id": 1_i32, "self": 1_i32 } },
                ],
                "cursor": {},
            },
        )
        .unwrap();
        assert_eq!(
            first_batch(&self_lookup),
            vec![
                doc! { "_id": "o1", "self": [{ "_id": "o1", "profileId": "p1", "profileIds": ["p2", "missing"], "owner": "ADA" }] }
            ]
        );
    }

    #[test]
    fn aggregate_lookup_sweeps_foreign_ttl_and_malformed_lookup_sweeps_none() {
        let conn = test_conn();
        let past =
            bson::DateTime::from_millis(bson::DateTime::now().timestamp_millis() - 86_400_000);
        let future =
            bson::DateTime::from_millis(bson::DateTime::now().timestamp_millis() + 86_400_000);

        insert_documents(
            &conn,
            &doc! {
                "insert": "lookup_orders",
                "$db": "app",
                "documents": [
                    { "_id": "o1", "profileId": "live" },
                    { "_id": "o2", "profileId": "expired" },
                ],
            },
        )
        .unwrap();
        create_indexes(
            &conn,
            &doc! {
                "createIndexes": "lookup_profiles",
                "$db": "app",
                "indexes": [
                    { "key": { "expiresAt": 1_i32 }, "name": "expires_ttl", "expireAfterSeconds": 60_i32 },
                ],
            },
        )
        .unwrap();
        insert_documents(
            &conn,
            &doc! {
                "insert": "lookup_profiles",
                "$db": "app",
                "documents": [
                    { "_id": "live", "expiresAt": future, "name": "Live" },
                    { "_id": "expired", "expiresAt": past, "name": "Expired" },
                ],
            },
        )
        .unwrap();

        let joined = aggregate_command(
            &conn,
            &doc! {
                "aggregate": "lookup_orders",
                "$db": "app",
                "pipeline": [
                    {
                        "$lookup": {
                            "from": "lookup_profiles",
                            "localField": "profileId",
                            "foreignField": "_id",
                            "as": "profile",
                        }
                    },
                    { "$sort": { "_id": 1_i32 } },
                    { "$project": { "_id": 1_i32, "profile": 1_i32 } },
                ],
                "cursor": {},
            },
        )
        .unwrap();
        assert_eq!(
            first_batch(&joined),
            vec![
                doc! { "_id": "o1", "profile": [{ "_id": "live", "expiresAt": future, "name": "Live" }] },
                doc! { "_id": "o2", "profile": [] },
            ]
        );
        assert_eq!(
            raw_ids_in_namespace(&conn, "app.lookup_profiles"),
            vec!["live"]
        );

        seed_ttl_command_fixture(&conn, "bad_lookup_source");
        seed_ttl_command_fixture(&conn, "bad_lookup_foreign");
        let response = aggregate_command(
            &conn,
            &doc! {
                "aggregate": "bad_lookup_source",
                "$db": "app",
                "pipeline": [
                    {
                        "$lookup": {
                            "from": "bad_lookup_foreign",
                            "localField": "category",
                            "foreignField": "category",
                            "as": "matches",
                            "pipeline": [],
                        }
                    },
                ],
                "cursor": {},
            },
        )
        .unwrap();
        assert_invalid_ttl_read_preserves_expired(&conn, "bad_lookup_source", response);
        assert_eq!(
            raw_ids_in_namespace(&conn, "app.bad_lookup_foreign"),
            vec!["expired", "future"]
        );
    }

    #[test]
    fn aggregate_lookup_rejects_malformed_and_unsupported_forms_before_ttl() {
        let conn = test_conn();
        seed_ttl_command_fixture(&conn, "bad_lookup");

        for pipeline in [
            vec![Bson::Document(doc! { "$lookup": "bad" })],
            vec![Bson::Document(doc! { "$lookup": { "from": "profiles" } })],
            vec![Bson::Document(doc! {
                "$lookup": {
                    "from": "other.profiles",
                    "localField": "profileId",
                    "foreignField": "_id",
                    "as": "profile",
                }
            })],
            vec![Bson::Document(doc! {
                "$lookup": {
                    "from": "profiles",
                    "localField": "profileId",
                    "foreignField": "_id",
                    "as": "profile",
                    "pipeline": [],
                }
            })],
            vec![Bson::Document(doc! {
                "$lookup": {
                    "from": "profiles",
                    "localField": "profileId",
                    "foreignField": "_id",
                    "as": "profile",
                    "let": {},
                }
            })],
            vec![Bson::Document(doc! {
                "$lookup": {
                    "from": "profiles",
                    "localField": "profile..id",
                    "foreignField": "_id",
                    "as": "profile",
                }
            })],
            vec![Bson::Document(doc! {
                "$lookup": {
                    "from": "profiles",
                    "localField": "profileId",
                    "foreignField": "_id",
                    "as": "$profile",
                }
            })],
        ] {
            let response = aggregate_command(
                &conn,
                &doc! {
                    "aggregate": "bad_lookup",
                    "$db": "app",
                    "pipeline": pipeline,
                    "cursor": {},
                },
            )
            .unwrap();
            assert_invalid_ttl_read_preserves_expired(&conn, "bad_lookup", response);
        }
    }

    #[test]
    fn aggregate_pipeline_stage_order_is_sequential() {
        let conn = test_conn();
        seed_find_documents(&conn);

        let limit_then_skip = aggregate_command(
            &conn,
            &doc! {
                "aggregate": "users",
                "$db": "app",
                "pipeline": [
                    { "$sort": { "_id": 1_i32 } },
                    { "$limit": 1_i32 },
                    { "$skip": 1_i32 },
                ],
                "cursor": {},
            },
        )
        .unwrap();
        assert_eq!(first_batch(&limit_then_skip), Vec::<Document>::new());

        let skip_then_limit = aggregate_command(
            &conn,
            &doc! {
                "aggregate": "users",
                "$db": "app",
                "pipeline": [
                    { "$sort": { "_id": 1_i32 } },
                    { "$skip": 1_i32 },
                    { "$limit": 1_i32 },
                    { "$project": { "_id": 1_i32 } },
                ],
                "cursor": {},
            },
        )
        .unwrap();
        assert_eq!(first_batch(&skip_then_limit), vec![doc! { "_id": "u2" }]);
    }

    #[test]
    fn aggregate_unwind_expands_arrays_and_preserves_when_requested() {
        let conn = test_conn();
        insert_documents(
            &conn,
            &doc! {
                "insert": "items",
                "$db": "app",
                "documents": [
                    { "_id": "a", "tags": ["red", "blue"] },
                    { "_id": "b", "tags": [] },
                    { "_id": "c" },
                    { "_id": "d", "tags": Bson::Null },
                    { "_id": "e", "tags": "green" },
                ],
            },
        )
        .unwrap();

        let default = aggregate_command(
            &conn,
            &doc! {
                "aggregate": "items",
                "$db": "app",
                "pipeline": [
                    { "$unwind": "$tags" },
                    { "$project": { "_id": 1_i32, "tags": 1_i32 } },
                ],
                "cursor": {},
            },
        )
        .unwrap();
        assert_eq!(
            first_batch(&default),
            vec![
                doc! { "_id": "a", "tags": "red" },
                doc! { "_id": "a", "tags": "blue" },
                doc! { "_id": "e", "tags": "green" },
            ]
        );

        let preserved = aggregate_command(
            &conn,
            &doc! {
                "aggregate": "items",
                "$db": "app",
                "pipeline": [
                    {
                        "$unwind": {
                            "path": "$tags",
                            "preserveNullAndEmptyArrays": true,
                            "includeArrayIndex": "idx",
                        }
                    },
                    { "$project": { "_id": 1_i32, "tags": 1_i32, "idx": 1_i32 } },
                ],
                "cursor": {},
            },
        )
        .unwrap();
        assert_eq!(
            first_batch(&preserved),
            vec![
                doc! { "_id": "a", "tags": "red", "idx": 0_i32 },
                doc! { "_id": "a", "tags": "blue", "idx": 1_i32 },
                doc! { "_id": "b", "idx": Bson::Null },
                doc! { "_id": "c", "idx": Bson::Null },
                doc! { "_id": "d", "tags": Bson::Null, "idx": Bson::Null },
                doc! { "_id": "e", "tags": "green", "idx": Bson::Null },
            ]
        );
    }

    #[test]
    fn aggregate_group_supports_keys_and_scalar_accumulators() {
        let conn = test_conn();
        insert_documents(
            &conn,
            &doc! {
                "insert": "scores",
                "$db": "app",
                "documents": [
                    { "_id": "s1", "team": "red", "score": 7_i32, "active": true },
                    { "_id": "s2", "team": "blue", "score": 5_i32, "active": false },
                    { "_id": "s3", "team": "red", "score": 11_i32, "active": true },
                    { "_id": "s4", "team": "red", "score": "bad", "active": false },
                    { "_id": "s5", "team": "blue", "active": true },
                ],
            },
        )
        .unwrap();

        let response = aggregate_command(
            &conn,
            &doc! {
                "aggregate": "scores",
                "$db": "app",
                "pipeline": [
                    {
                        "$group": {
                            "_id": "$team",
                            "n": { "$sum": 1_i32 },
                            "scoreTotal": { "$sum": "$score" },
                            "avgScore": { "$avg": "$score" },
                            "minScore": { "$min": "$score" },
                            "maxScore": { "$max": "$score" },
                            "firstId": { "$first": "$_id" },
                            "lastActive": { "$last": "$active" },
                        }
                    },
                    { "$sort": { "_id": 1_i32 } },
                ],
                "cursor": {},
            },
        )
        .unwrap();

        assert_eq!(
            first_batch(&response),
            vec![
                doc! {
                    "_id": "blue",
                    "n": 2_i64,
                    "scoreTotal": 5_i64,
                    "avgScore": 5.0,
                    "minScore": 5_i32,
                    "maxScore": 5_i32,
                    "firstId": "s2",
                    "lastActive": true,
                },
                doc! {
                    "_id": "red",
                    "n": 3_i64,
                    "scoreTotal": 18_i64,
                    "avgScore": 9.0,
                    "minScore": 7_i32,
                    "maxScore": "bad",
                    "firstId": "s1",
                    "lastActive": false,
                },
            ]
        );

        let document_key = aggregate_command(
            &conn,
            &doc! {
                "aggregate": "scores",
                "$db": "app",
                "pipeline": [
                    {
                        "$group": {
                            "_id": { "team": "$team", "active": "$active" },
                            "n": { "$sum": 1_i32 },
                        }
                    },
                    { "$sort": { "n": -1_i32 } },
                    { "$limit": 1_i32 },
                ],
                "cursor": {},
            },
        )
        .unwrap();
        assert_eq!(
            first_batch(&document_key),
            vec![doc! { "_id": { "team": "red", "active": true }, "n": 2_i64 }]
        );
    }

    #[test]
    fn aggregate_group_push_add_to_set_and_unwind_compose_with_cursor() {
        let conn = test_conn();
        insert_documents(
            &conn,
            &doc! {
                "insert": "posts",
                "$db": "app",
                "documents": [
                    { "_id": "p1", "active": true, "tags": ["red", "blue"], "score": 7_i32 },
                    { "_id": "p2", "active": true, "tags": ["red"], "score": 5_i32 },
                    { "_id": "p3", "active": true, "tags": ["blue", "red"] },
                    { "_id": "p4", "active": false, "tags": ["red"], "score": 99_i32 },
                ],
            },
        )
        .unwrap();
        let mut client_state = ClientState::default();

        let response = aggregate_command_with_state(
            &conn,
            &mut client_state,
            &doc! {
                "aggregate": "posts",
                "$db": "app",
                "pipeline": [
                    { "$match": { "active": true } },
                    { "$unwind": "$tags" },
                    {
                        "$group": {
                            "_id": "$tags",
                            "ids": { "$push": "$_id" },
                            "scores": { "$push": "$score" },
                            "uniqueIds": { "$addToSet": "$_id" },
                            "uniqueLiteral": { "$addToSet": "seen" },
                        }
                    },
                    { "$sort": { "_id": 1_i32 } },
                    { "$project": { "_id": 1_i32, "ids": 1_i32, "scores": 1_i32, "uniqueIds": 1_i32, "uniqueLiteral": 1_i32 } },
                ],
                "cursor": { "batchSize": 1_i32 },
            },
        )
        .unwrap();

        let id = cursor_id(&response);
        assert!(id > 0);
        assert_eq!(
            first_batch(&response),
            vec![doc! {
                "_id": "blue",
                "ids": ["p1", "p3"],
                "scores": [7_i32, Bson::Null],
                "uniqueIds": ["p1", "p3"],
                "uniqueLiteral": ["seen"],
            }]
        );

        let next = get_more(
            &mut client_state,
            &doc! { "getMore": id, "collection": "posts", "$db": "app", "batchSize": 1_i32 },
        )
        .unwrap();
        assert_eq!(cursor_id(&next), 0);
        assert_eq!(
            next_batch(&next),
            vec![doc! {
                "_id": "red",
                "ids": ["p1", "p2", "p3"],
                "scores": [7_i32, 5_i32, Bson::Null],
                "uniqueIds": ["p1", "p2", "p3"],
                "uniqueLiteral": ["seen"],
            }]
        );
    }

    #[test]
    fn aggregate_group_add_to_set_uses_whole_value_equality() {
        let conn = test_conn();
        insert_documents(
            &conn,
            &doc! {
                "insert": "set_values",
                "$db": "app",
                "documents": [
                    {
                        "_id": "a1",
                        "case": "array-first",
                        "value": [1_i32, 2_i32],
                        "docValue": { "shape": "same", "nested": [1_i32, 2_i32] },
                        "number": 1_i32,
                    },
                    {
                        "_id": "a2",
                        "case": "array-first",
                        "value": 1_i32,
                        "docValue": { "shape": "same", "nested": [1_i32, 2_i32] },
                        "number": 1.0,
                    },
                    {
                        "_id": "a3",
                        "case": "array-first",
                        "value": [1_i32, 2_i32],
                        "docValue": { "shape": "same", "nested": [1_i32, 2_i32] },
                        "number": 1_i64,
                    },
                    {
                        "_id": "a4",
                        "case": "array-first",
                        "value": [2_i32, 1_i32],
                        "docValue": { "shape": "other", "nested": [1_i32, 2_i32] },
                        "number": 2.0,
                    },
                    {
                        "_id": "s1",
                        "case": "scalar-first",
                        "value": 1_i32,
                        "docValue": { "shape": "same", "nested": [1_i32, 2_i32] },
                        "number": 1.0,
                    },
                    {
                        "_id": "s2",
                        "case": "scalar-first",
                        "value": [1_i32, 2_i32],
                        "docValue": { "shape": "same", "nested": [1_i32, 2_i32] },
                        "number": 1_i32,
                    },
                    {
                        "_id": "s3",
                        "case": "scalar-first",
                        "value": 1_i32,
                        "docValue": { "shape": "same", "nested": [1_i32, 2_i32] },
                        "number": 1_i64,
                    },
                ],
            },
        )
        .unwrap();

        let response = aggregate_command(
            &conn,
            &doc! {
                "aggregate": "set_values",
                "$db": "app",
                "pipeline": [
                    {
                        "$group": {
                            "_id": "$case",
                            "values": { "$addToSet": "$value" },
                            "documents": { "$addToSet": "$docValue" },
                            "numbers": { "$addToSet": "$number" },
                            "pushed": { "$push": "$value" },
                        }
                    },
                    { "$sort": { "_id": 1_i32 } },
                ],
                "cursor": {},
            },
        )
        .unwrap();

        assert_eq!(
            first_batch(&response),
            vec![
                doc! {
                    "_id": "array-first",
                    "values": [[1_i32, 2_i32], 1_i32, [2_i32, 1_i32]],
                    "documents": [
                        { "shape": "same", "nested": [1_i32, 2_i32] },
                        { "shape": "other", "nested": [1_i32, 2_i32] },
                    ],
                    "numbers": [1_i32, 2.0],
                    "pushed": [[1_i32, 2_i32], 1_i32, [1_i32, 2_i32], [2_i32, 1_i32]],
                },
                doc! {
                    "_id": "scalar-first",
                    "values": [1_i32, [1_i32, 2_i32]],
                    "documents": [{ "shape": "same", "nested": [1_i32, 2_i32] }],
                    "numbers": [1.0],
                    "pushed": [1_i32, [1_i32, 2_i32], 1_i32],
                },
            ]
        );
    }

    #[test]
    fn aggregate_cursor_batch_size_uses_get_more_and_cleans_up() {
        let conn = test_conn();
        seed_find_documents(&conn);
        let mut client_state = ClientState::default();

        let response = aggregate_command_with_state(
            &conn,
            &mut client_state,
            &doc! {
                "aggregate": "users",
                "$db": "app",
                "pipeline": [
                    { "$sort": { "_id": 1_i32 } },
                    { "$project": { "_id": 1_i32 } },
                ],
                "cursor": { "batchSize": 1_i32 },
            },
        )
        .unwrap();

        let id = cursor_id(&response);
        assert!(id > 0);
        assert_eq!(first_batch(&response), vec![doc! { "_id": "u1" }]);
        assert_eq!(client_state.cursors.len(), 1);

        let next = get_more(
            &mut client_state,
            &doc! { "getMore": id, "collection": "users", "$db": "app", "batchSize": 1_i32 },
        )
        .unwrap();
        assert_eq!(cursor_id(&next), id);
        assert_eq!(next_batch(&next), vec![doc! { "_id": "u2" }]);

        let final_batch = get_more(
            &mut client_state,
            &doc! { "getMore": id, "collection": "users", "$db": "app", "batchSize": 10_i32 },
        )
        .unwrap();
        assert_eq!(cursor_id(&final_batch), 0);
        assert_eq!(next_batch(&final_batch), vec![doc! { "_id": "u3" }]);
        assert!(client_state.cursors.is_empty());
    }

    #[test]
    fn aggregate_rejects_malformed_cursor_and_unsupported_options() {
        let conn = test_conn();
        seed_find_documents(&conn);

        for command in [
            doc! { "aggregate": "users", "$db": "app", "pipeline": [], "cursor": "bad" },
            doc! { "aggregate": "users", "$db": "app", "pipeline": [], "cursor": { "batchSize": 0_i32 } },
            doc! { "aggregate": "users", "$db": "app", "pipeline": [], "cursor": { "batchSize": -1_i32 } },
            doc! { "aggregate": "users", "$db": "app", "pipeline": [], "cursor": { "batchSize": "bad" } },
            doc! { "aggregate": "users", "$db": "app", "pipeline": [], "cursor": { "batchSize": 1001_i32 } },
            doc! { "aggregate": "users", "$db": "app", "pipeline": [], "cursor": { "foo": 1_i32 } },
            doc! { "aggregate": "users", "$db": "app", "pipeline": [], "cursor": {}, "allowDiskUse": true },
            doc! { "aggregate": "users", "$db": "app", "pipeline": [], "cursor": {}, "explain": true },
            doc! { "aggregate": "users", "$db": "app", "pipeline": [], "cursor": {}, "collation": {} },
            doc! { "aggregate": "users", "$db": "app", "pipeline": [], "cursor": {}, "hint": "_id_" },
            doc! { "aggregate": "users", "$db": "app", "pipeline": [], "cursor": {}, "comment": "nope" },
            doc! { "aggregate": "users", "$db": "app", "pipeline": [], "cursor": {}, "maxTimeMS": 1_i32 },
            doc! { "aggregate": "users", "$db": "app", "pipeline": [], "cursor": {}, "bypassDocumentValidation": true },
            doc! { "aggregate": "users", "$db": "app", "pipeline": [], "cursor": {}, "writeConcern": {} },
            doc! { "aggregate": "users", "$db": "app", "pipeline": [], "cursor": {}, "let": {} },
        ] {
            let response = aggregate_command(&conn, &command).unwrap();
            assert_command_error(&response);
        }
    }

    #[test]
    fn aggregate_count_empty_match_returns_empty_count_batch() {
        let conn = test_conn();
        seed_find_documents(&conn);

        let response = aggregate_command(
            &conn,
            &doc! {
                "aggregate": "users",
                "$db": "app",
                "pipeline": [
                    { "$match": { "profile.city": "Nowhere" } },
                    { "$count": "total" },
                ],
                "cursor": {},
            },
        )
        .unwrap();

        assert_eq!(first_batch(&response), Vec::<Document>::new());
    }

    #[test]
    fn aggregate_pipeline_rejects_malformed_and_unsupported_stages() {
        let conn = test_conn();
        seed_find_documents(&conn);

        for pipeline in [
            vec![Bson::Int32(1)],
            vec![Bson::Document(doc! {})],
            vec![Bson::Document(
                doc! { "$match": {}, "$sort": { "_id": 1_i32 } },
            )],
            vec![Bson::Document(doc! { "$match": "bad" })],
            vec![Bson::Document(doc! { "$sort": "bad" })],
            vec![Bson::Document(doc! { "$sort": { "age": 2_i32 } })],
            vec![Bson::Document(doc! { "$skip": -1_i32 })],
            vec![Bson::Document(doc! { "$limit": "bad" })],
            vec![Bson::Document(doc! { "$project": "bad" })],
            vec![Bson::Document(
                doc! { "$project": { "name": 0_i32, "display": { "$literal": 1_i32 } } },
            )],
            vec![Bson::Document(doc! { "$count": "" })],
            vec![Bson::Document(doc! { "$count": 1_i32 })],
            vec![Bson::Document(doc! { "$lookup": { "from": "other" } })],
            vec![Bson::Document(doc! { "$unwind": "$" })],
            vec![Bson::Document(doc! { "$unwind": { "path": "tags" } })],
            vec![Bson::Document(
                doc! { "$unwind": { "path": "$tags", "preserveNullAndEmptyArrays": "yes" } },
            )],
            vec![Bson::Document(
                doc! { "$unwind": { "path": "$tags", "includeArrayIndex": "$idx" } },
            )],
            vec![Bson::Document(
                doc! { "$unwind": { "path": "$tags", "includeArrayIndex": "tags.idx" } },
            )],
            vec![Bson::Document(
                doc! { "$unwind": { "path": "$tags", "unknown": true } },
            )],
            vec![Bson::Document(
                doc! { "$group": { "_id": 1_i32, "n": { "$sum": 1_i32, "extra": 1_i32 } } },
            )],
            vec![Bson::Document(
                doc! { "$group": { "n": { "$sum": 1_i32 } } },
            )],
            vec![Bson::Document(
                doc! { "$group": { "_id": "$team", "n": { "$median": "$score" } } },
            )],
            vec![Bson::Document(
                doc! { "$group": { "_id": "$team", "n": { "$avg": 1_i32 } } },
            )],
            vec![Bson::Document(
                doc! { "$group": { "_id": "$team", "n": { "$sum": "literal" } } },
            )],
            vec![Bson::Document(
                doc! { "$group": { "_id": "$team", "n": { "$first": { "$dateDiff": { "startDate": "$created", "endDate": "$updated", "unit": "day" } } } } },
            )],
        ] {
            let response = aggregate_command(
                &conn,
                &doc! { "aggregate": "users", "$db": "app", "pipeline": pipeline, "cursor": {} },
            )
            .unwrap();
            assert_command_error(&response);
        }
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
                    "pipeline": [{ "$lookup": { "from": "other" } }],
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
    fn index_commands_roundtrip_sparse_and_partial_metadata() {
        let conn = test_conn();

        let created = create_indexes(
            &conn,
            &doc! {
                "createIndexes": "users",
                "$db": "app",
                "indexes": [
                    { "key": { "email": 1_i32 }, "name": "email_sparse", "sparse": true },
                    {
                        "key": { "email": 1_i32 },
                        "name": "email_active_partial",
                        "partialFilterExpression": { "active": true },
                    },
                    {
                        "key": { "handle": 1_i32 },
                        "name": "handle_exists_partial",
                        "partialFilterExpression": { "$and": [{ "handle": { "$exists": true } }] },
                    },
                ],
            },
        )
        .unwrap();
        assert_eq!(created.get_i32("numIndexesAfter").unwrap(), 4);

        let listed = list_indexes(
            &conn,
            &doc! { "listIndexes": "users", "$db": "app", "cursor": {} },
        )
        .unwrap();
        let batch = first_batch(&listed);
        let sparse = batch
            .iter()
            .find(|index| index.get_str("name").unwrap() == "email_sparse")
            .unwrap();
        assert_eq!(sparse.get_bool("sparse").unwrap(), true);
        let partial = batch
            .iter()
            .find(|index| index.get_str("name").unwrap() == "email_active_partial")
            .unwrap();
        assert_eq!(
            partial.get_document("partialFilterExpression").unwrap(),
            &doc! { "active": true }
        );
    }

    #[test]
    fn create_indexes_rejects_top_level_sparse_and_partial_options() {
        let conn = test_conn();

        for command in [
            doc! {
                "createIndexes": "users",
                "$db": "app",
                "indexes": [{ "key": { "email": 1_i32 }, "name": "email_1" }],
                "sparse": true,
            },
            doc! {
                "createIndexes": "users",
                "$db": "app",
                "indexes": [{ "key": { "email": 1_i32 }, "name": "email_1" }],
                "partialFilterExpression": { "active": true },
            },
        ] {
            let response = create_indexes(&conn, &command).unwrap();
            assert_command_error(&response);
            assert_eq!(response.get_i32("code").unwrap(), 72);
        }
    }

    #[test]
    fn drop_indexes_all_removes_multikey_omission_sentinels() {
        let conn = test_conn();
        insert_documents(
            &conn,
            &doc! {
                "insert": "users",
                "$db": "app",
                "documents": [{ "_id": "u1", "scores": [1_i32] }],
            },
        )
        .unwrap();
        create_indexes(
            &conn,
            &doc! {
                "createIndexes": "users",
                "$db": "app",
                "indexes": [{ "key": { "scores": 1_i32 }, "name": "scores_1" }],
            },
        )
        .unwrap();
        let omissions_before_drop: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM index_multikey_omissions WHERE namespace = 'app.users'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(omissions_before_drop, 1);

        let response = drop_indexes(
            &conn,
            &doc! { "dropIndexes": "users", "$db": "app", "index": "*" },
        )
        .unwrap();
        assert_eq!(response.get_i32("numIndexesAfter").unwrap(), 1);
        let omissions_after_drop: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM index_multikey_omissions WHERE namespace = 'app.users'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(omissions_after_drop, 0);
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
    fn ttl_index_create_list_and_duplicate_spec_validation() {
        let conn = test_conn();

        let generated = create_indexes(
            &conn,
            &doc! {
                "createIndexes": "events",
                "$db": "app",
                "indexes": [{ "key": { "createdAt": 1_i32 }, "expireAfterSeconds": 3600_i32 }],
            },
        )
        .unwrap();
        assert_eq!(generated.get_i32("numIndexesAfter").unwrap(), 2);
        let explicit = create_indexes(
            &conn,
            &doc! {
                "createIndexes": "events",
                "$db": "app",
                "indexes": [
                    {
                        "key": { "deletedAt": -1_i32 },
                        "name": "deleted_ttl",
                        "expireAfterSeconds": 0_i64,
                    },
                ],
            },
        )
        .unwrap();
        assert_eq!(explicit.get_i32("numIndexesAfter").unwrap(), 3);

        let repeated = create_indexes(
            &conn,
            &doc! {
                "createIndexes": "events",
                "$db": "app",
                "indexes": [{ "key": { "createdAt": 1_i32 }, "expireAfterSeconds": 3600_i64 }],
            },
        )
        .unwrap();
        assert_eq!(repeated.get_f64("ok").unwrap(), 1.0);

        let conflict = create_indexes(
            &conn,
            &doc! {
                "createIndexes": "events",
                "$db": "app",
                "indexes": [{ "key": { "createdAt": 1_i32 }, "expireAfterSeconds": 7200_i32 }],
            },
        )
        .unwrap();
        assert_command_error(&conflict);
        assert_eq!(conflict.get_i32("code").unwrap(), 85);

        let listed = list_indexes(
            &conn,
            &doc! { "listIndexes": "events", "$db": "app", "cursor": {} },
        )
        .unwrap();
        let batch = first_batch(&listed);
        let created = batch
            .iter()
            .find(|index| index.get_str("name").unwrap() == "createdAt_1")
            .unwrap();
        assert_eq!(created.get_i64("expireAfterSeconds").unwrap(), 3600);
        let deleted = batch
            .iter()
            .find(|index| index.get_str("name").unwrap() == "deleted_ttl")
            .unwrap();
        assert_eq!(deleted.get_i64("expireAfterSeconds").unwrap(), 0);
        let id_index = batch
            .iter()
            .find(|index| index.get_str("name").unwrap() == "_id_")
            .unwrap();
        assert!(!id_index.contains_key("expireAfterSeconds"));
    }

    #[test]
    fn ttl_index_rejects_invalid_specs_without_mutation() {
        let conn = test_conn();
        insert_documents(
            &conn,
            &doc! {
                "insert": "events",
                "$db": "app",
                "documents": [{ "_id": "e1", "createdAt": bson::DateTime::from_millis(1_700_000_000_000_i64) }],
            },
        )
        .unwrap();

        for spec in [
            doc! { "key": { "createdAt": 1_i32 }, "name": "ttl_negative", "expireAfterSeconds": -1_i32 },
            doc! { "key": { "createdAt": 1_i32 }, "name": "ttl_bool", "expireAfterSeconds": true },
            doc! { "key": { "createdAt": 1_i32 }, "name": "ttl_double", "expireAfterSeconds": 1.5 },
            doc! { "key": { "createdAt": 1_i32 }, "name": "ttl_string", "expireAfterSeconds": "60" },
            doc! { "key": { "createdAt": 1_i32 }, "name": "ttl_null", "expireAfterSeconds": Bson::Null },
            doc! { "key": { "createdAt": 1_i32 }, "name": "ttl_array", "expireAfterSeconds": [60_i32] },
            doc! { "key": { "createdAt": 1_i32 }, "name": "ttl_document", "expireAfterSeconds": { "seconds": 60_i32 } },
            doc! { "key": { "createdAt": 1_i32, "tenant": 1_i32 }, "name": "ttl_compound", "expireAfterSeconds": 60_i32 },
            doc! { "key": { "_id": 1_i32 }, "name": "_id_ttl", "expireAfterSeconds": 60_i32 },
            doc! { "key": { "createdAt": "hashed" }, "name": "ttl_hashed", "expireAfterSeconds": 60_i32 },
            doc! { "key": { "createdAt": 1_i32 }, "name": "ttl_sparse", "sparse": true, "expireAfterSeconds": 60_i32 },
            doc! { "key": { "createdAt": 1_i32 }, "name": "ttl_partial", "partialFilterExpression": { "active": true }, "expireAfterSeconds": 60_i32 },
        ] {
            let response = create_indexes(
                &conn,
                &doc! { "createIndexes": "events", "$db": "app", "indexes": [spec] },
            )
            .unwrap();
            assert_command_error(&response);
            assert_eq!(response.get_i32("code").unwrap(), 72);
        }

        let index_count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM indexes WHERE namespace = 'app.events'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        let entry_count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM index_entries WHERE namespace = 'app.events'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(index_count, 0);
        assert_eq!(entry_count, 0);
    }

    #[test]
    fn ttl_sweep_deletes_expired_dates_and_cleans_index_entries() {
        let conn = test_conn();
        create_indexes(
            &conn,
            &doc! {
                "createIndexes": "events",
                "$db": "app",
                "indexes": [
                    { "key": { "expiresAt": 1_i32 }, "name": "expires_ttl", "expireAfterSeconds": 60_i32 },
                    { "key": { "status": 1_i32 }, "name": "status_1" },
                ],
            },
        )
        .unwrap();
        insert_documents(
            &conn,
            &doc! {
                "insert": "events",
                "$db": "app",
                "documents": [
                    { "_id": "expired", "status": "old", "expiresAt": bson::DateTime::from_millis(1_699_999_940_000_i64) },
                    { "_id": "older", "status": "old", "expiresAt": bson::DateTime::from_millis(1_699_999_939_999_i64) },
                    { "_id": "future", "status": "new", "expiresAt": bson::DateTime::from_millis(1_699_999_940_001_i64) },
                ],
            },
        )
        .unwrap();

        let removed = sweep_ttl_namespace_at(
            &conn,
            "app.events",
            bson::DateTime::from_millis(1_700_000_000_000_i64),
        )
        .unwrap();

        assert_eq!(removed, 2);
        let remaining = documents_for_namespace(&conn, "app.events").unwrap();
        assert_eq!(
            remaining
                .iter()
                .map(|document| document.get_str("_id").unwrap())
                .collect::<Vec<_>>(),
            vec!["future"]
        );
        for id in ["expired", "older"] {
            let entry_count: i64 = conn
                .query_row(
                    "SELECT COUNT(*) FROM index_entries WHERE namespace = 'app.events' AND id_key = ?1",
                    params![id_key_from_bson(&Bson::String(id.to_string()))],
                    |row| row.get(0),
                )
                .unwrap();
            let omission_count: i64 = conn
                .query_row(
                    "SELECT COUNT(*) FROM index_multikey_omissions WHERE namespace = 'app.events' AND id_key = ?1",
                    params![id_key_from_bson(&Bson::String(id.to_string()))],
                    |row| row.get(0),
                )
                .unwrap();
            assert_eq!(entry_count, 0);
            assert_eq!(omission_count, 0);
        }
        assert_eq!(
            sweep_ttl_namespace_at(
                &conn,
                "app.events",
                bson::DateTime::from_millis(1_700_000_000_000_i64),
            )
            .unwrap(),
            0
        );
    }

    #[test]
    fn ttl_sweep_keeps_non_expiring_values_and_non_ttl_namespaces() {
        let conn = test_conn();
        create_indexes(
            &conn,
            &doc! {
                "createIndexes": "events",
                "$db": "app",
                "indexes": [{ "key": { "expiresAt": 1_i32 }, "name": "expires_ttl", "expireAfterSeconds": 0_i32 }],
            },
        )
        .unwrap();
        insert_documents(
            &conn,
            &doc! {
                "insert": "events",
                "$db": "app",
                "documents": [
                    { "_id": "future", "expiresAt": bson::DateTime::from_millis(1_700_000_000_001_i64) },
                    { "_id": "missing" },
                    { "_id": "null", "expiresAt": Bson::Null },
                    { "_id": "string", "expiresAt": "2024-01-01" },
                    { "_id": "number", "expiresAt": 1_i32 },
                    { "_id": "array", "expiresAt": [bson::DateTime::from_millis(1_i64)] },
                    { "_id": "document", "expiresAt": { "nested": bson::DateTime::from_millis(1_i64) } },
                ],
            },
        )
        .unwrap();
        insert_documents(
            &conn,
            &doc! {
                "insert": "ordinary",
                "$db": "app",
                "documents": [{ "_id": "o1", "expiresAt": bson::DateTime::from_millis(1_i64) }],
            },
        )
        .unwrap();

        let removed = sweep_ttl_namespace_at(
            &conn,
            "app.events",
            bson::DateTime::from_millis(1_700_000_000_000_i64),
        )
        .unwrap();

        assert_eq!(removed, 0);
        assert_eq!(
            documents_for_namespace(&conn, "app.events").unwrap().len(),
            7
        );
        assert_eq!(
            sweep_ttl_namespace_at(
                &conn,
                "app.ordinary",
                bson::DateTime::from_millis(1_700_000_000_000_i64),
            )
            .unwrap(),
            0
        );
        assert_eq!(
            documents_for_namespace(&conn, "app.ordinary")
                .unwrap()
                .len(),
            1
        );
    }

    #[test]
    fn ttl_sweep_expires_when_any_ttl_index_matches() {
        let conn = test_conn();
        create_indexes(
            &conn,
            &doc! {
                "createIndexes": "events",
                "$db": "app",
                "indexes": [
                    { "key": { "createdAt": 1_i32 }, "name": "created_ttl", "expireAfterSeconds": 60_i32 },
                    { "key": { "deletedAt": 1_i32 }, "name": "deleted_ttl", "expireAfterSeconds": 0_i32 },
                ],
            },
        )
        .unwrap();
        insert_documents(
            &conn,
            &doc! {
                "insert": "events",
                "$db": "app",
                "documents": [
                    {
                        "_id": "deleted",
                        "createdAt": bson::DateTime::from_millis(1_700_000_000_001_i64),
                        "deletedAt": bson::DateTime::from_millis(1_700_000_000_000_i64),
                    },
                    {
                        "_id": "created",
                        "createdAt": bson::DateTime::from_millis(1_699_999_940_000_i64),
                        "deletedAt": bson::DateTime::from_millis(1_700_000_000_001_i64),
                    },
                    {
                        "_id": "kept",
                        "createdAt": bson::DateTime::from_millis(1_700_000_000_001_i64),
                        "deletedAt": bson::DateTime::from_millis(1_700_000_000_001_i64),
                    },
                ],
            },
        )
        .unwrap();

        let removed = sweep_ttl_namespace_at(
            &conn,
            "app.events",
            bson::DateTime::from_millis(1_700_000_000_000_i64),
        )
        .unwrap();

        assert_eq!(removed, 2);
        let remaining = documents_for_namespace(&conn, "app.events").unwrap();
        assert_eq!(remaining[0].get_str("_id").unwrap(), "kept");
    }

    fn seed_ttl_command_fixture(conn: &Connection, collection: &str) {
        let past =
            bson::DateTime::from_millis(bson::DateTime::now().timestamp_millis() - 86_400_000);
        let future =
            bson::DateTime::from_millis(bson::DateTime::now().timestamp_millis() + 86_400_000);
        create_indexes(
            conn,
            &doc! {
                "createIndexes": collection,
                "$db": "app",
                "indexes": [
                    { "key": { "expiresAt": 1_i32 }, "name": "expires_ttl", "expireAfterSeconds": 60_i32 },
                    { "key": { "category": 1_i32 }, "name": "category_1" },
                ],
            },
        )
        .unwrap();
        insert_documents(
            conn,
            &doc! {
                "insert": collection,
                "$db": "app",
                "documents": [
                    { "_id": "expired", "category": "old", "expiresAt": past },
                    { "_id": "future", "category": "new", "expiresAt": future },
                ],
            },
        )
        .unwrap();
    }

    fn raw_ids_in_namespace(conn: &Connection, namespace: &str) -> Vec<String> {
        documents_for_namespace(conn, namespace)
            .unwrap()
            .into_iter()
            .map(|document| document.get_str("_id").unwrap().to_string())
            .collect()
    }

    fn assert_invalid_ttl_read_preserves_expired(
        conn: &Connection,
        collection: &str,
        response: Document,
    ) {
        assert_command_error(&response);
        assert_eq!(
            raw_ids_in_namespace(conn, &format!("app.{collection}")),
            vec!["expired", "future"]
        );
    }

    fn ttl_name_validator() -> Document {
        doc! {
            "$jsonSchema": {
                "bsonType": "object",
                "required": ["name"],
                "properties": { "name": { "bsonType": "string" } }
            }
        }
    }

    fn seed_validated_ttl_name_fixture(conn: &Connection, collection: &str) {
        let past =
            bson::DateTime::from_millis(bson::DateTime::now().timestamp_millis() - 86_400_000);
        let future =
            bson::DateTime::from_millis(bson::DateTime::now().timestamp_millis() + 86_400_000);
        create_indexes(
            conn,
            &doc! {
                "createIndexes": collection,
                "$db": "app",
                "indexes": [{ "key": { "expiresAt": 1_i32 }, "name": "expires_ttl", "expireAfterSeconds": 60_i32 }],
            },
        )
        .unwrap();
        insert_documents(
            conn,
            &doc! {
                "insert": collection,
                "$db": "app",
                "documents": [
                    { "_id": "expired", "name": "Ada", "expiresAt": past },
                    { "_id": "future", "name": "Grace", "expiresAt": future },
                ],
            },
        )
        .unwrap();
    }

    #[test]
    fn ttl_invalid_read_commands_do_not_sweep_expired_documents() {
        let conn = test_conn();

        seed_ttl_command_fixture(&conn, "bad_find_field");
        let response = find_documents(
            &conn,
            &doc! {
                "find": "bad_find_field",
                "$db": "app",
                "filter": { "category": { "$near": "old" } },
            },
        )
        .unwrap();
        assert_invalid_ttl_read_preserves_expired(&conn, "bad_find_field", response);

        seed_ttl_command_fixture(&conn, "bad_find_top");
        let response = find_documents(
            &conn,
            &doc! {
                "find": "bad_find_top",
                "$db": "app",
                "filter": { "$where": "this.category == 'old'" },
            },
        )
        .unwrap();
        assert_invalid_ttl_read_preserves_expired(&conn, "bad_find_top", response);

        seed_ttl_command_fixture(&conn, "bad_count");
        let response = count_documents_command(
            &conn,
            &doc! {
                "count": "bad_count",
                "$db": "app",
                "query": { "category": { "$near": "old" } },
            },
        )
        .unwrap();
        assert_invalid_ttl_read_preserves_expired(&conn, "bad_count", response);

        seed_ttl_command_fixture(&conn, "bad_distinct");
        let response = distinct_command(
            &conn,
            &doc! {
                "distinct": "bad_distinct",
                "$db": "app",
                "key": "category",
                "query": { "category": { "$near": "old" } },
            },
        )
        .unwrap();
        assert_invalid_ttl_read_preserves_expired(&conn, "bad_distinct", response);

        seed_ttl_command_fixture(&conn, "bad_aggregate_stage");
        let response = aggregate_command(
            &conn,
            &doc! {
                "aggregate": "bad_aggregate_stage",
                "$db": "app",
                "pipeline": [{ "$lookup": { "from": "other" } }],
                "cursor": {},
            },
        )
        .unwrap();
        assert_invalid_ttl_read_preserves_expired(&conn, "bad_aggregate_stage", response);

        seed_ttl_command_fixture(&conn, "bad_aggregate_match");
        let response = aggregate_command(
            &conn,
            &doc! {
                "aggregate": "bad_aggregate_match",
                "$db": "app",
                "pipeline": [{ "$match": { "category": { "$near": "old" } } }],
                "cursor": {},
            },
        )
        .unwrap();
        assert_invalid_ttl_read_preserves_expired(&conn, "bad_aggregate_match", response);
    }

    #[test]
    fn ttl_invalid_filter_errors_are_not_masked_when_all_documents_expired() {
        let conn = test_conn();
        let past =
            bson::DateTime::from_millis(bson::DateTime::now().timestamp_millis() - 86_400_000);
        create_indexes(
            &conn,
            &doc! {
                "createIndexes": "all_expired",
                "$db": "app",
                "indexes": [{ "key": { "expiresAt": 1_i32 }, "name": "expires_ttl", "expireAfterSeconds": 60_i32 }],
            },
        )
        .unwrap();
        insert_documents(
            &conn,
            &doc! {
                "insert": "all_expired",
                "$db": "app",
                "documents": [{ "_id": "expired", "expiresAt": past }],
            },
        )
        .unwrap();

        let response = find_documents(
            &conn,
            &doc! {
                "find": "all_expired",
                "$db": "app",
                "filter": { "category": { "$near": "old" } },
            },
        )
        .unwrap();

        assert_command_error(&response);
        assert_eq!(
            response.get_str("errmsg").unwrap(),
            "unsupported query operator $near"
        );
        assert_eq!(
            raw_ids_in_namespace(&conn, "app.all_expired"),
            vec!["expired"]
        );
    }

    #[test]
    fn ttl_invalid_write_commands_do_not_sweep_before_preflight_errors() {
        let conn = test_conn();

        create_collection(
            &conn,
            &doc! {
                "create": "bad_insert_validation",
                "$db": "app",
                "validator": ttl_name_validator(),
            },
        )
        .unwrap();
        seed_validated_ttl_name_fixture(&conn, "bad_insert_validation");
        let insert = insert_documents(
            &conn,
            &doc! {
                "insert": "bad_insert_validation",
                "$db": "app",
                "documents": [{ "_id": "bad", "expiresAt": bson::DateTime::now() }],
            },
        )
        .unwrap();
        assert_eq!(insert.get_i32("n").unwrap(), 0);
        assert_eq!(write_errors(&insert)[0].get_i32("code").unwrap(), 121);
        assert_eq!(
            raw_ids_in_namespace(&conn, "app.bad_insert_validation"),
            vec!["expired", "future"]
        );

        seed_ttl_command_fixture(&conn, "bad_update_entry");
        let update = update_documents(
            &conn,
            &doc! {
                "update": "bad_update_entry",
                "$db": "app",
                "updates": [1_i32],
            },
        )
        .unwrap();
        assert_eq!(write_errors(&update)[0].get_i32("index").unwrap(), 0);
        assert_eq!(
            raw_ids_in_namespace(&conn, "app.bad_update_entry"),
            vec!["expired", "future"]
        );

        seed_ttl_command_fixture(&conn, "bad_update_query");
        let update = update_documents(
            &conn,
            &doc! {
                "update": "bad_update_query",
                "$db": "app",
                "updates": [{ "q": { "category": { "$near": "old" } }, "u": { "$set": { "seen": true } } }],
            },
        )
        .unwrap();
        assert_eq!(
            write_errors(&update)[0].get_str("errmsg").unwrap(),
            "unsupported query operator $near"
        );
        assert_eq!(
            raw_ids_in_namespace(&conn, "app.bad_update_query"),
            vec!["expired", "future"]
        );

        seed_ttl_command_fixture(&conn, "bad_delete_entry");
        let delete = delete_documents(
            &conn,
            &doc! {
                "delete": "bad_delete_entry",
                "$db": "app",
                "deletes": [1_i32],
            },
        )
        .unwrap();
        assert_eq!(write_errors(&delete)[0].get_i32("index").unwrap(), 0);
        assert_eq!(
            raw_ids_in_namespace(&conn, "app.bad_delete_entry"),
            vec!["expired", "future"]
        );

        seed_ttl_command_fixture(&conn, "bad_delete_query");
        let delete = delete_documents(
            &conn,
            &doc! {
                "delete": "bad_delete_query",
                "$db": "app",
                "deletes": [{ "q": { "category": { "$near": "old" } }, "limit": 1_i32 }],
            },
        )
        .unwrap();
        assert_eq!(
            write_errors(&delete)[0].get_str("errmsg").unwrap(),
            "unsupported query operator $near"
        );
        assert_eq!(
            raw_ids_in_namespace(&conn, "app.bad_delete_query"),
            vec!["expired", "future"]
        );
    }

    #[test]
    fn ttl_invalid_find_and_modify_paths_do_not_sweep_expired_documents() {
        let conn = test_conn();

        seed_ttl_command_fixture(&conn, "bad_fam_hint");
        let response = find_and_modify(
            &conn,
            "findAndModify",
            &doc! {
                "findAndModify": "bad_fam_hint",
                "$db": "app",
                "query": { "category": "old" },
                "hint": "missing_1",
                "update": { "$set": { "seen": true } },
            },
        )
        .unwrap();
        assert_command_error(&response);
        assert_eq!(
            raw_ids_in_namespace(&conn, "app.bad_fam_hint"),
            vec!["expired", "future"]
        );

        seed_ttl_command_fixture(&conn, "bad_fam_update");
        let response = find_and_modify(
            &conn,
            "findAndModify",
            &doc! {
                "findAndModify": "bad_fam_update",
                "$db": "app",
                "query": { "category": "old" },
                "update": { "$set": { "_id": "changed" } },
            },
        )
        .unwrap();
        assert_command_error(&response);
        assert_eq!(
            raw_ids_in_namespace(&conn, "app.bad_fam_update"),
            vec!["expired", "future"]
        );

        create_collection(
            &conn,
            &doc! {
                "create": "bad_fam_validation",
                "$db": "app",
                "validator": ttl_name_validator(),
            },
        )
        .unwrap();
        create_indexes(
            &conn,
            &doc! {
                "createIndexes": "bad_fam_validation",
                "$db": "app",
                "indexes": [{ "key": { "expiresAt": 1_i32 }, "name": "expires_ttl", "expireAfterSeconds": 60_i32 }],
            },
        )
        .unwrap();
        let past =
            bson::DateTime::from_millis(bson::DateTime::now().timestamp_millis() - 86_400_000);
        insert_documents(
            &conn,
            &doc! {
                "insert": "bad_fam_validation",
                "$db": "app",
                "documents": [{ "_id": "expired", "name": "Ada", "expiresAt": past }],
            },
        )
        .unwrap();

        let response = find_and_modify(
            &conn,
            "findAndModify",
            &doc! {
                "findAndModify": "bad_fam_validation",
                "$db": "app",
                "query": { "_id": "expired" },
                "update": { "$set": { "name": 5_i32 } },
            },
        )
        .unwrap();

        assert_command_error(&response);
        assert_eq!(response.get_i32("code").unwrap(), 121);
        assert_eq!(
            raw_ids_in_namespace(&conn, "app.bad_fam_validation"),
            vec!["expired"]
        );
    }

    #[test]
    fn ttl_sweeps_before_observable_read_paths() {
        let conn = test_conn();

        seed_ttl_command_fixture(&conn, "finds");
        let find = find_documents(
            &conn,
            &doc! {
                "find": "finds",
                "$db": "app",
                "filter": { "category": "old" },
                "hint": "category_1",
            },
        )
        .unwrap();
        assert!(first_batch(&find).is_empty());
        assert_eq!(
            documents_for_namespace(&conn, "app.finds").unwrap().len(),
            1
        );

        seed_ttl_command_fixture(&conn, "counts");
        let count = count_documents_command(
            &conn,
            &doc! {
                "count": "counts",
                "$db": "app",
                "query": { "category": "old" },
                "hint": "category_1",
            },
        )
        .unwrap();
        assert_eq!(count.get_i64("n").unwrap(), 0);
        assert_eq!(
            documents_for_namespace(&conn, "app.counts").unwrap().len(),
            1
        );

        seed_ttl_command_fixture(&conn, "aggregates");
        let aggregate = aggregate_command(
            &conn,
            &doc! {
                "aggregate": "aggregates",
                "$db": "app",
                "pipeline": [{ "$match": {} }, { "$count": "n" }],
                "cursor": {},
            },
        )
        .unwrap();
        let batch = first_batch(&aggregate);
        assert_eq!(batch[0].get_i64("n").unwrap(), 1);

        seed_ttl_command_fixture(&conn, "distincts");
        let distinct = distinct_command(
            &conn,
            &doc! { "distinct": "distincts", "$db": "app", "key": "category" },
        )
        .unwrap();
        assert_eq!(
            distinct.get_array("values").unwrap(),
            &vec![Bson::String("new".to_string())]
        );
    }

    #[test]
    fn ttl_sweeps_before_writes_and_releases_unique_conflicts() {
        let conn = test_conn();
        let past =
            bson::DateTime::from_millis(bson::DateTime::now().timestamp_millis() - 86_400_000);
        let future =
            bson::DateTime::from_millis(bson::DateTime::now().timestamp_millis() + 86_400_000);

        create_indexes(
            &conn,
            &doc! {
                "createIndexes": "users",
                "$db": "app",
                "indexes": [
                    { "key": { "expiresAt": 1_i32 }, "name": "expires_ttl", "expireAfterSeconds": 60_i32 },
                    { "key": { "email": 1_i32 }, "name": "email_1", "unique": true },
                ],
            },
        )
        .unwrap();
        insert_documents(
            &conn,
            &doc! {
                "insert": "users",
                "$db": "app",
                "documents": [{ "_id": "expired", "email": "same@example.test", "expiresAt": past }],
            },
        )
        .unwrap();
        let insert = insert_documents(
            &conn,
            &doc! {
                "insert": "users",
                "$db": "app",
                "documents": [{ "_id": "replacement", "email": "same@example.test", "expiresAt": future }],
            },
        )
        .unwrap();
        assert!(!insert.contains_key("writeErrors"));
        assert_eq!(
            documents_for_namespace(&conn, "app.users").unwrap().len(),
            1
        );

        seed_ttl_command_fixture(&conn, "updates");
        let update = update_documents(
            &conn,
            &doc! {
                "update": "updates",
                "$db": "app",
                "updates": [{ "q": { "category": "old" }, "u": { "$set": { "seen": true } }, "multi": true }],
            },
        )
        .unwrap();
        assert_eq!(update.get_i32("n").unwrap(), 0);
        assert_eq!(
            documents_for_namespace(&conn, "app.updates").unwrap().len(),
            1
        );

        seed_ttl_command_fixture(&conn, "deletes");
        let delete = delete_documents(
            &conn,
            &doc! {
                "delete": "deletes",
                "$db": "app",
                "deletes": [{ "q": { "category": "old" }, "limit": 0_i32 }],
            },
        )
        .unwrap();
        assert_eq!(delete.get_i32("n").unwrap(), 0);
        assert_eq!(
            documents_for_namespace(&conn, "app.deletes").unwrap().len(),
            1
        );

        seed_ttl_command_fixture(&conn, "fam");
        let fam = find_and_modify(
            &conn,
            "findAndModify",
            &doc! {
                "findAndModify": "fam",
                "$db": "app",
                "query": { "category": "old" },
                "update": { "$set": { "seen": true } },
                "new": true,
            },
        )
        .unwrap();
        assert_eq!(fam.get("value").unwrap(), &Bson::Null);
        assert_eq!(documents_for_namespace(&conn, "app.fam").unwrap().len(), 1);
    }

    #[test]
    fn ttl_does_not_sweep_when_read_hint_validation_fails() {
        let conn = test_conn();
        seed_ttl_command_fixture(&conn, "hints");

        let response = find_documents(
            &conn,
            &doc! {
                "find": "hints",
                "$db": "app",
                "filter": { "category": "old" },
                "hint": "missing_1",
            },
        )
        .unwrap();

        assert_command_error(&response);
        assert_eq!(
            documents_for_namespace(&conn, "app.hints").unwrap().len(),
            2
        );
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
                    "indexes": [{ "key": { "name": 1_i32 }, "partialFilterExpression": { "age": { "$gt": 30_i32 } } }],
                },
            )
            .unwrap(),
            create_indexes(
                &conn,
                &doc! {
                    "createIndexes": "users",
                    "$db": "app",
                    "indexes": [{ "key": { "name": 1_i32 }, "partialFilterExpression": { "age": 30_i32 } }],
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
    fn sparse_unique_index_enforces_only_present_members() {
        let conn = test_conn();
        insert_documents(
            &conn,
            &doc! {
                "insert": "users",
                "$db": "app",
                "documents": [
                    { "_id": "u1", "name": "missing-a" },
                    { "_id": "u2", "name": "missing-b" },
                    { "_id": "u3", "email": null },
                    { "_id": "u4", "email": "ada@example.test" },
                ],
            },
        )
        .unwrap();
        create_indexes(
            &conn,
            &doc! {
                "createIndexes": "users",
                "$db": "app",
                "indexes": [{ "key": { "email": 1_i32 }, "name": "email_sparse", "unique": true, "sparse": true }],
            },
        )
        .unwrap();

        let entries: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM index_entries WHERE namespace = 'app.users' AND index_name = 'email_sparse'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(entries, 3);

        insert_documents(
            &conn,
            &doc! {
                "insert": "users",
                "$db": "app",
                "documents": [{ "_id": "u5", "name": "missing-c" }],
            },
        )
        .unwrap();
        let duplicate_null = insert_documents(
            &conn,
            &doc! {
                "insert": "users",
                "$db": "app",
                "documents": [{ "_id": "u6", "email": null }],
            },
        )
        .unwrap();
        assert_eq!(
            write_errors(&duplicate_null)[0].get_i32("code").unwrap(),
            11000
        );
        let duplicate_email = insert_documents(
            &conn,
            &doc! {
                "insert": "users",
                "$db": "app",
                "documents": [{ "_id": "u7", "email": "ada@example.test" }],
            },
        )
        .unwrap();
        assert_eq!(
            write_errors(&duplicate_email)[0].get_i32("code").unwrap(),
            11000
        );
    }

    #[test]
    fn compound_sparse_index_requires_every_key_field_present() {
        let conn = test_conn();
        insert_documents(
            &conn,
            &doc! {
                "insert": "users",
                "$db": "app",
                "documents": [
                    { "_id": "u1", "email": "ada@example.test" },
                    { "_id": "u2", "role": "admin" },
                    { "_id": "u3", "email": "ada@example.test", "role": "admin" },
                    { "_id": "u4", "email": "grace@example.test", "role": "admin" },
                ],
            },
        )
        .unwrap();
        create_indexes(
            &conn,
            &doc! {
                "createIndexes": "users",
                "$db": "app",
                "indexes": [{
                    "key": { "email": 1_i32, "role": 1_i32 },
                    "name": "email_role_sparse",
                    "unique": true,
                    "sparse": true,
                }],
            },
        )
        .unwrap();

        let entries: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM index_entries WHERE namespace = 'app.users' AND index_name = 'email_role_sparse'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(entries, 8);
        let duplicate = insert_documents(
            &conn,
            &doc! {
                "insert": "users",
                "$db": "app",
                "documents": [{ "_id": "u5", "email": "ada@example.test", "role": "admin" }],
            },
        )
        .unwrap();
        assert_eq!(write_errors(&duplicate)[0].get_i32("code").unwrap(), 11000);
    }

    #[test]
    fn partial_unique_index_enforces_only_matching_members() {
        let conn = test_conn();
        insert_documents(
            &conn,
            &doc! {
                "insert": "users",
                "$db": "app",
                "documents": [
                    { "_id": "u1", "email": "same@example.test", "active": false },
                    { "_id": "u2", "email": "same@example.test" },
                    { "_id": "u3", "email": "same@example.test", "active": true },
                ],
            },
        )
        .unwrap();
        create_indexes(
            &conn,
            &doc! {
                "createIndexes": "users",
                "$db": "app",
                "indexes": [{
                    "key": { "email": 1_i32 },
                    "name": "email_active_partial",
                    "unique": true,
                    "partialFilterExpression": { "active": true },
                }],
            },
        )
        .unwrap();

        let entries: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM index_entries WHERE namespace = 'app.users' AND index_name = 'email_active_partial'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(entries, 2);

        let inactive = insert_documents(
            &conn,
            &doc! {
                "insert": "users",
                "$db": "app",
                "documents": [{ "_id": "u4", "email": "same@example.test", "active": false }],
            },
        )
        .unwrap();
        assert!(!inactive.contains_key("writeErrors"));
        let duplicate_active = insert_documents(
            &conn,
            &doc! {
                "insert": "users",
                "$db": "app",
                "documents": [{ "_id": "u5", "email": "same@example.test", "active": true }],
            },
        )
        .unwrap();
        assert_eq!(
            write_errors(&duplicate_active)[0].get_i32("code").unwrap(),
            11000
        );
    }

    #[test]
    fn partial_filter_membership_supports_eq_exists_and_and() {
        let conn = test_conn();
        insert_documents(
            &conn,
            &doc! {
                "insert": "users",
                "$db": "app",
                "documents": [
                    { "_id": "u1", "email": "a@example.test", "active": true, "handle": "ada" },
                    { "_id": "u2", "email": "b@example.test", "active": true },
                    { "_id": "u3", "email": "c@example.test", "active": false, "handle": "grace" },
                ],
            },
        )
        .unwrap();
        create_indexes(
            &conn,
            &doc! {
                "createIndexes": "users",
                "$db": "app",
                "indexes": [{
                    "key": { "email": 1_i32 },
                    "name": "email_active_handle_partial",
                    "unique": true,
                    "partialFilterExpression": {
                        "$and": [
                            { "active": { "$eq": true } },
                            { "handle": { "$exists": true } },
                        ],
                    },
                }],
            },
        )
        .unwrap();

        let entries: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM index_entries WHERE namespace = 'app.users' AND index_name = 'email_active_handle_partial'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(entries, 2);
    }

    #[test]
    fn numeric_unique_conflicts_use_fallback_scan() {
        let conn = test_conn();
        insert_documents(
            &conn,
            &doc! {
                "insert": "users",
                "$db": "app",
                "documents": [
                    { "_id": "u1", "n": 1_i32 },
                    { "_id": "u2", "n": 2_i32 },
                ],
            },
        )
        .unwrap();
        create_indexes(
            &conn,
            &doc! {
                "createIndexes": "users",
                "$db": "app",
                "indexes": [{ "key": { "n": 1_i32 }, "name": "n_1", "unique": true }],
            },
        )
        .unwrap();

        let duplicate_insert = insert_documents(
            &conn,
            &doc! {
                "insert": "users",
                "$db": "app",
                "documents": [{ "_id": "u3", "n": Bson::Int64(1) }],
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
                "updates": [{ "q": { "_id": "u2" }, "u": { "$set": { "n": 1.0 } } }],
            },
        )
        .unwrap();
        assert_eq!(
            write_errors(&duplicate_update)[0].get_i32("code").unwrap(),
            11000
        );

        let duplicate_upsert = update_documents(
            &conn,
            &doc! {
                "update": "users",
                "$db": "app",
                "updates": [
                    {
                        "q": { "_id": "u4" },
                        "u": { "$set": { "n": Bson::Int64(1) } },
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

        let duplicate_find_and_modify = find_and_modify(
            &conn,
            "findAndModify",
            &doc! {
                "findAndModify": "users",
                "$db": "app",
                "query": { "_id": "u2" },
                "update": { "$set": { "n": Bson::Int64(1) } },
            },
        )
        .unwrap();
        assert_command_error(&duplicate_find_and_modify);
        assert_eq!(duplicate_find_and_modify.get_i32("code").unwrap(), 11000);
        assert_eq!(
            first_batch(
                &find_documents(
                    &conn,
                    &doc! { "find": "users", "$db": "app", "filter": { "_id": "u2" } },
                )
                .unwrap()
            )[0]
            .get_i32("n")
            .unwrap(),
            2
        );
    }

    #[test]
    fn unique_conflict_pushdown_uses_index_entries_for_safe_single_field_scalars() {
        let conn = test_conn();
        insert_documents(
            &conn,
            &doc! {
                "insert": "users",
                "$db": "app",
                "documents": [
                    { "_id": "u1", "email": "ada@example.test", "rank": 1_i32, "role": "admin" },
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
                "indexes": [
                    { "key": { "email": 1_i32 }, "name": "email_1", "unique": true },
                    { "key": { "rank": 1_i32 }, "name": "rank_1", "unique": true },
                    { "key": { "email": 1_i32, "rank": 1_i32 }, "name": "compound_1", "unique": true },
                    { "key": { "email": 1_i32, "role": 1_i32 }, "name": "email_role_1", "unique": true }
                ],
            },
        )
        .unwrap();

        let tx = conn.unchecked_transaction().unwrap();
        let email = index_by_name_tx(&tx, "app.users", "email_1")
            .unwrap()
            .unwrap();
        let rank = index_by_name_tx(&tx, "app.users", "rank_1")
            .unwrap()
            .unwrap();
        let compound = index_by_name_tx(&tx, "app.users", "compound_1")
            .unwrap()
            .unwrap();
        let email_role = index_by_name_tx(&tx, "app.users", "email_role_1")
            .unwrap()
            .unwrap();

        let conflict = unique_conflict_check_with_index_entries_tx(
            &tx,
            "app.users",
            &email,
            &doc! { "_id": "u3", "email": "ada@example.test" },
            None,
        )
        .unwrap_err();
        assert!(conflict.contains("duplicate key error"));
        assert!(
            unique_conflict_check_with_index_entries_tx(
                &tx,
                "app.users",
                &email,
                &doc! { "_id": "u1", "email": "ada@example.test" },
                Some("str:u1"),
            )
            .unwrap()
        );
        assert!(
            !unique_conflict_check_with_index_entries_tx(
                &tx,
                "app.users",
                &rank,
                &doc! { "_id": "u3", "rank": 2_i32 },
                None,
            )
            .unwrap()
        );
        let compound_conflict = unique_conflict_check_with_index_entries_tx(
            &tx,
            "app.users",
            &email_role,
            &doc! { "_id": "u3", "email": "ada@example.test", "role": "admin" },
            None,
        )
        .unwrap_err();
        assert!(compound_conflict.contains("duplicate key error"));

        for (index, document) in [
            (&email, doc! { "_id": "missing" }),
            (&email, doc! { "_id": "null", "email": Bson::Null }),
            (&email, doc! { "_id": "array", "email": ["a@example.test"] }),
            (
                &email,
                doc! { "_id": "document", "email": { "nested": true } },
            ),
            (
                &compound,
                doc! { "_id": "compound", "email": "ada@example.test", "rank": 1_i32 },
            ),
        ] {
            assert!(
                !unique_conflict_check_with_index_entries_tx(
                    &tx,
                    "app.users",
                    index,
                    &document,
                    None
                )
                .unwrap()
            );
        }
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
            doc! { "findAndModify": "users", "$db": "app", "update": "bad" },
            doc! { "findAndModify": "users", "$db": "app", "arrayFilters": [], "update": { "$set": { "name": "x" } } },
            doc! { "findAndModify": "users", "$db": "app", "collation": {}, "update": { "$set": { "name": "x" } } },
            doc! { "findAndModify": "users", "$db": "app", "hint": "_id_", "update": { "$set": { "name": "x" } } },
            doc! { "findAndModify": "users", "$db": "app", "writeConcern": { "w": 0_i32 }, "update": { "$set": { "name": "x" } } },
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
    fn find_and_modify_rejects_ambiguous_command_aliases_before_mutation() {
        let conn = test_conn();
        seed_find_documents(&conn);

        let response = handle_command(
            &conn,
            &doc! {
                "findAndModify": "users",
                "findandmodify": "users",
                "$db": "app",
                "query": { "_id": "u1" },
                "update": { "$set": { "name": "Mutated" } },
                "new": true,
            },
        )
        .unwrap();

        assert_command_error(&response);
        assert!(
            response
                .get_str("errmsg")
                .unwrap()
                .contains("both command aliases")
        );
        assert_eq!(find_ids(&conn, doc! { "name": "Ada" }), vec!["u1"]);
        assert_eq!(
            find_ids(&conn, doc! { "name": "Mutated" }),
            Vec::<String>::new()
        );
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
    fn planner_uses_single_field_multikey_entries_for_scalar_arrays() {
        let conn = test_conn();
        seed_find_documents(&conn);
        create_indexes(
            &conn,
            &doc! {
                "createIndexes": "users",
                "$db": "app",
                "indexes": [
                    { "key": { "tags": 1_i32 }, "name": "tags_1" },
                    { "key": { "nested.kind": 1_i32 }, "name": "nested_kind_1" },
                ],
            },
        )
        .unwrap();

        assert!(index_entries_safe_for_planner(&conn, "app.users", "tags_1").unwrap());
        assert!(index_entries_safe_for_planner(&conn, "app.users", "nested_kind_1").unwrap());
        assert_eq!(
            indexed_candidate_documents(&conn, "app.users", &doc! { "tags": "math" })
                .unwrap()
                .unwrap()
                .iter()
                .map(|doc| doc.get_str("_id").unwrap().to_string())
                .collect::<Vec<_>>(),
            vec!["u1"]
        );
        assert_eq!(
            indexed_candidate_documents(&conn, "app.users", &doc! { "nested.kind": "second" })
                .unwrap()
                .unwrap()
                .iter()
                .map(|doc| doc.get_str("_id").unwrap().to_string())
                .collect::<Vec<_>>(),
            vec!["u1"]
        );
        assert_eq!(find_ids(&conn, doc! { "tags": "math" }), vec!["u1"]);
        assert_eq!(
            find_ids(&conn, doc! { "nested.kind": "second" }),
            vec!["u1"]
        );
        assert_eq!(
            plan_count(&conn, "app.users", &doc! { "tags": "math" }).unwrap(),
            CountPlan::IndexedEquality {
                index_name: "tags_1".to_string(),
                key_value: "str:math".to_string(),
            }
        );
    }

    #[test]
    fn multikey_entries_deduplicate_repeated_array_values_and_reject_numeric_arrays() {
        let conn = test_conn();
        insert_documents(
            &conn,
            &doc! {
                "insert": "users",
                "$db": "app",
                "documents": [
                    { "_id": "u1", "tags": ["math", "math", "logic"], "scores": [1_i32, 2_i32] },
                    { "_id": "u2", "tags": "math", "scores": 1_i32 },
                ],
            },
        )
        .unwrap();
        create_indexes(
            &conn,
            &doc! {
                "createIndexes": "users",
                "$db": "app",
                "indexes": [
                    { "key": { "tags": 1_i32 }, "name": "tags_1" },
                    { "key": { "scores": 1_i32 }, "name": "scores_1" },
                ],
            },
        )
        .unwrap();

        let math_entries: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM index_entries WHERE namespace = 'app.users' AND index_name = 'tags_1' AND key_value = 'str:math'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(math_entries, 2);
        assert_eq!(
            pushed_down_count(&conn, "app.users", &doc! { "tags": "math" }, 0, None).unwrap(),
            Some(2)
        );
        assert!(!index_entries_safe_for_planner(&conn, "app.users", "scores_1").unwrap());
        assert_eq!(
            plan_count(&conn, "app.users", &doc! { "scores": 1_i32 }).unwrap(),
            CountPlan::Fallback
        );
    }

    #[test]
    fn planner_uses_compound_index_entries_for_full_equality() {
        let conn = test_conn();
        seed_find_documents(&conn);
        create_indexes(
            &conn,
            &doc! {
                "createIndexes": "users",
                "$db": "app",
                "indexes": [{ "key": { "profile.city": 1_i32, "active": 1_i32 }, "name": "city_active_1" }],
            },
        )
        .unwrap();

        let candidates = indexed_candidate_documents(
            &conn,
            "app.users",
            &doc! { "active": true, "profile.city": "Rome" },
        )
        .unwrap()
        .unwrap();
        let candidate_ids = candidates
            .iter()
            .map(|doc| doc.get_str("_id").unwrap().to_string())
            .collect::<Vec<_>>();
        assert_eq!(candidate_ids, vec!["u1", "u3"]);

        assert_eq!(
            find_ids(&conn, doc! { "profile.city": "Rome", "active": true }),
            vec!["u1", "u3"]
        );
        let prefix_candidates =
            indexed_candidate_documents(&conn, "app.users", &doc! { "profile.city": "Rome" })
                .unwrap()
                .unwrap();
        assert_eq!(
            prefix_candidates
                .iter()
                .map(|doc| doc.get_str("_id").unwrap().to_string())
                .collect::<Vec<_>>(),
            vec!["u1", "u3"]
        );
        assert!(
            indexed_candidate_documents(
                &conn,
                "app.users",
                &doc! { "profile.city": "Rome", "active": 1_i32 },
            )
            .unwrap()
            .is_none()
        );
    }

    #[test]
    fn planner_uses_compound_prefix_entries_for_reads_and_write_targeting() {
        let conn = test_conn();
        seed_find_documents(&conn);
        create_indexes(
            &conn,
            &doc! {
                "createIndexes": "users",
                "$db": "app",
                "indexes": [{ "key": { "profile.city": 1_i32, "active": 1_i32 }, "name": "city_active_1" }],
            },
        )
        .unwrap();

        assert_eq!(
            plan_transaction_candidates(
                &conn.unchecked_transaction().unwrap(),
                "app.users",
                &doc! { "profile.city": "Rome" }
            )
            .unwrap(),
            TransactionCandidatePlan::IndexedPrefix {
                index_name: "city_active_1".to_string(),
                key_value: "compound-prefix:1:8:str:Rome".to_string(),
            }
        );
        assert_eq!(
            find_ids(&conn, doc! { "profile.city": "Rome" }),
            vec!["u1", "u3"]
        );

        insert_documents(
            &conn,
            &doc! {
                "insert": "users",
                "$db": "app",
                "documents": [{ "_id": "u4", "profile": { "city": "Rome" }, "active": false }],
            },
        )
        .unwrap();
        assert_eq!(
            find_ids(&conn, doc! { "profile.city": "Rome" }),
            vec!["u1", "u3", "u4"]
        );

        let updated = update_documents(
            &conn,
            &doc! {
                "update": "users",
                "$db": "app",
                "updates": [{ "q": { "profile.city": "Rome" }, "u": { "$set": { "profile.city": "Milan" } }, "multi": true }],
            },
        )
        .unwrap();
        assert_eq!(updated.get_i32("n").unwrap(), 3);
        assert_eq!(
            find_ids(&conn, doc! { "profile.city": "Rome" }),
            Vec::<String>::new()
        );
        assert_eq!(
            find_ids(&conn, doc! { "profile.city": "Milan" }),
            vec!["u1", "u3", "u4"]
        );

        let deleted = delete_documents(
            &conn,
            &doc! {
                "delete": "users",
                "$db": "app",
                "deletes": [{ "q": { "profile.city": "Milan" }, "limit": 0_i32 }],
            },
        )
        .unwrap();
        assert_eq!(deleted.get_i32("n").unwrap(), 3);
        assert_eq!(
            find_ids(&conn, doc! { "profile.city": "Milan" }),
            Vec::<String>::new()
        );

        let moved = find_and_modify(
            &conn,
            "findAndModify",
            &doc! {
                "findAndModify": "users",
                "$db": "app",
                "query": { "profile.city": "London" },
                "update": { "$set": { "profile.city": "Rome", "active": true } },
                "new": true,
            },
        )
        .unwrap();
        assert_eq!(
            moved
                .get_document("value")
                .unwrap()
                .get_document("profile")
                .unwrap()
                .get_str("city")
                .unwrap(),
            "Rome"
        );
        assert_eq!(find_ids(&conn, doc! { "profile.city": "Rome" }), vec!["u2"]);
    }

    #[test]
    fn range_planner_uses_index_entries_for_find_count_and_write_targets() {
        let conn = test_conn();
        insert_documents(
            &conn,
            &doc! {
                "insert": "events",
                "$db": "app",
                "documents": [
                    { "_id": "e1", "account": "a", "created": "2026-01-01", "state": "queued" },
                    { "_id": "e2", "account": "a", "created": "2026-02-01", "state": "queued" },
                    { "_id": "e3", "account": "a", "created": "2026-03-01", "state": "done" },
                    { "_id": "e4", "account": "b", "created": "2026-03-01", "state": "queued" },
                ],
            },
        )
        .unwrap();
        create_indexes(
            &conn,
            &doc! {
                "createIndexes": "events",
                "$db": "app",
                "indexes": [
                    { "key": { "created": 1_i32 }, "name": "created_1" },
                    { "key": { "account": 1_i32, "created": 1_i32 }, "name": "account_created_1" },
                ],
            },
        )
        .unwrap();

        assert_eq!(
            find_ids_in(
                &conn,
                "events",
                doc! { "created": { "$gte": "2026-02-01", "$lte": "2026-03-01" } },
            ),
            vec!["e2", "e3", "e4"]
        );
        assert_eq!(
            find_ids_in(&conn, "events", doc! { "created": { "$gt": "2026-03-01" } },),
            Vec::<String>::new()
        );
        assert_eq!(
            plan_count(
                &conn,
                "app.events",
                &doc! { "account": "a", "created": { "$gte": "2026-02-01", "$lt": "2026-04-01" } }
            )
            .unwrap(),
            CountPlan::IndexedRange {
                index_name: "account_created_1".to_string(),
                range: RangePlannerKey {
                    field: "created".to_string(),
                    equality_prefix_len: 1,
                    key_prefix: "range:1:5:str:a:".to_string(),
                    lower: Some(RangeBound {
                        key: "str:2026-02-01".to_string(),
                        inclusive: true,
                    }),
                    upper: Some(RangeBound {
                        key: "str:2026-04-01".to_string(),
                        inclusive: false,
                    }),
                },
            }
        );
        assert_eq!(
            pushed_down_count(
                &conn,
                "app.events",
                &doc! { "account": "a", "created": { "$gte": "2026-02-01", "$lt": "2026-04-01" } },
                0,
                None,
            )
            .unwrap(),
            Some(2)
        );
        assert_eq!(
            plan_count(
                &conn,
                "app.events",
                &doc! { "created": { "$gte": "2026-02-01" }, "state": "queued" }
            )
            .unwrap(),
            CountPlan::Fallback
        );

        let updated = update_documents(
            &conn,
            &doc! {
                "update": "events",
                "$db": "app",
                "updates": [{ "q": { "account": "a", "created": { "$gte": "2026-02-01" } }, "u": { "$set": { "state": "range-updated" } }, "multi": true }],
            },
        )
        .unwrap();
        assert_eq!(updated.get_i32("n").unwrap(), 2);
        assert_eq!(
            find_ids_in(&conn, "events", doc! { "state": "range-updated" }),
            vec!["e2", "e3"]
        );

        let moved = find_and_modify(
            &conn,
            "findAndModify",
            &doc! {
                "findAndModify": "events",
                "$db": "app",
                "query": { "account": "b", "created": { "$gte": "2026-03-01" } },
                "update": { "$set": { "created": "2026-04-01" } },
                "new": true,
            },
        )
        .unwrap();
        assert_eq!(
            moved
                .get_document("value")
                .unwrap()
                .get_str("created")
                .unwrap(),
            "2026-04-01"
        );
        assert_eq!(
            find_ids_in(
                &conn,
                "events",
                doc! { "created": { "$gte": "2026-04-01" } },
            ),
            vec!["e4"]
        );

        let deleted = delete_documents(
            &conn,
            &doc! {
                "delete": "events",
                "$db": "app",
                "deletes": [{ "q": { "created": { "$gte": "2026-04-01" } }, "limit": 0_i32 }],
            },
        )
        .unwrap();
        assert_eq!(deleted.get_i32("n").unwrap(), 1);
        assert_eq!(
            find_ids_in(
                &conn,
                "events",
                doc! { "created": { "$gte": "2026-04-01" } },
            ),
            Vec::<String>::new()
        );
    }

    #[test]
    fn range_planner_falls_back_for_unsafe_shapes_and_membership() {
        let conn = test_conn();
        insert_documents(
            &conn,
            &doc! {
                "insert": "events",
                "$db": "app",
                "documents": [
                    { "_id": "e1", "created": "2026-01-01", "score": 1_i32 },
                    { "_id": "e2", "created": "2026-02-01", "score": 2_i32 },
                    { "_id": "e3", "score": 3_i32 },
                ],
            },
        )
        .unwrap();
        create_indexes(
            &conn,
            &doc! {
                "createIndexes": "events",
                "$db": "app",
                "indexes": [
                    { "key": { "created": 1_i32 }, "name": "created_sparse", "sparse": true },
                    { "key": { "score": 1_i32 }, "name": "score_1" },
                ],
            },
        )
        .unwrap();

        assert_eq!(
            plan_transaction_candidates(
                &conn.unchecked_transaction().unwrap(),
                "app.events",
                &doc! { "created": { "$gte": "2026-01-01" } }
            )
            .unwrap(),
            TransactionCandidatePlan::Fallback
        );
        assert_eq!(
            plan_count(&conn, "app.events", &doc! { "score": { "$gte": 1_i32 } }).unwrap(),
            CountPlan::Fallback
        );
        assert_eq!(
            plan_count(
                &conn,
                "app.events",
                &doc! { "created": { "$in": ["2026-01-01"] } }
            )
            .unwrap(),
            CountPlan::Fallback
        );
    }

    #[test]
    fn hint_accepts_name_and_key_for_read_and_write_paths() {
        let conn = test_conn();
        seed_find_documents(&conn);
        create_indexes(
            &conn,
            &doc! {
                "createIndexes": "users",
                "$db": "app",
                "indexes": [
                    { "key": { "profile.city": 1_i32, "active": 1_i32 }, "name": "city_active_1" },
                    { "key": { "name": 1_i32 }, "name": "name_1" },
                ],
            },
        )
        .unwrap();

        let hinted_find = find_documents(
            &conn,
            &doc! {
                "find": "users",
                "$db": "app",
                "filter": { "profile.city": "Rome" },
                "hint": "city_active_1",
            },
        )
        .unwrap();
        assert_eq!(
            first_batch(&hinted_find)
                .iter()
                .map(|doc| doc.get_str("_id").unwrap().to_string())
                .collect::<Vec<_>>(),
            vec!["u1", "u3"]
        );

        let hinted_count = count_documents_command(
            &conn,
            &doc! {
                "count": "users",
                "$db": "app",
                "query": { "name": { "$gte": "G" } },
                "hint": { "name": 1_i32 },
            },
        )
        .unwrap();
        assert_eq!(hinted_count.get_i64("n").unwrap(), 2);

        let hinted_update = update_documents(
            &conn,
            &doc! {
                "update": "users",
                "$db": "app",
                "updates": [{
                    "q": { "profile.city": "Rome" },
                    "u": { "$set": { "team": "hint-prefix" } },
                    "multi": true,
                    "hint": { "profile.city": 1_i32, "active": 1_i32 },
                }],
            },
        )
        .unwrap();
        assert_eq!(hinted_update.get_i32("n").unwrap(), 2);

        let hinted_delete = delete_documents(
            &conn,
            &doc! {
                "delete": "users",
                "$db": "app",
                "deletes": [{ "q": { "_id": "u2" }, "limit": 1_i32, "hint": "_id_" }],
            },
        )
        .unwrap();
        assert_eq!(hinted_delete.get_i32("n").unwrap(), 1);

        let hinted_find_and_modify = find_and_modify(
            &conn,
            "findAndModify",
            &doc! {
                "findAndModify": "users",
                "$db": "app",
                "query": { "name": { "$gte": "K" } },
                "update": { "$set": { "team": "hint-range" } },
                "new": true,
                "hint": "name_1",
            },
        )
        .unwrap();
        assert_eq!(
            hinted_find_and_modify
                .get_document("value")
                .unwrap()
                .get_str("_id")
                .unwrap(),
            "u3"
        );
    }

    #[test]
    fn hint_errors_do_not_mutate_documents() {
        let conn = test_conn();
        seed_find_documents(&conn);
        create_indexes(
            &conn,
            &doc! {
                "createIndexes": "users",
                "$db": "app",
                "indexes": [{ "key": { "profile.city": 1_i32, "active": 1_i32 }, "name": "city_active_1" }],
            },
        )
        .unwrap();

        let bad_find = find_documents(
            &conn,
            &doc! {
                "find": "users",
                "$db": "app",
                "filter": { "name": "Ada" },
                "hint": "city_active_1",
            },
        )
        .unwrap();
        assert_command_error(&bad_find);

        let bad_update = update_documents(
            &conn,
            &doc! {
                "update": "users",
                "$db": "app",
                "updates": [{
                    "q": { "name": "Ada" },
                    "u": { "$set": { "team": "bad-hint" } },
                    "hint": "city_active_1",
                }],
            },
        )
        .unwrap();
        assert_eq!(write_errors(&bad_update)[0].get_i32("code").unwrap(), 2);
        assert_eq!(
            find_ids(&conn, doc! { "team": "bad-hint" }),
            Vec::<String>::new()
        );

        let bad_delete = delete_documents(
            &conn,
            &doc! {
                "delete": "users",
                "$db": "app",
                "deletes": [{ "q": { "name": "Ada" }, "limit": 1_i32, "hint": "missing_1" }],
            },
        )
        .unwrap();
        assert_eq!(write_errors(&bad_delete)[0].get_i32("code").unwrap(), 2);
        assert_eq!(find_ids(&conn, doc! { "name": "Ada" }), vec!["u1"]);

        let bad_find_and_modify = find_and_modify(
            &conn,
            "findAndModify",
            &doc! {
                "findAndModify": "users",
                "$db": "app",
                "query": { "name": "Ada" },
                "update": { "$set": { "team": "bad-fam-hint" } },
                "hint": "missing_1",
            },
        )
        .unwrap();
        assert_command_error(&bad_find_and_modify);
        assert_eq!(
            find_ids(&conn, doc! { "team": "bad-fam-hint" }),
            Vec::<String>::new()
        );
    }

    #[test]
    fn explain_reports_find_and_count_planner_diagnostics() {
        let conn = test_conn();
        seed_find_documents(&conn);
        create_indexes(
            &conn,
            &doc! {
                "createIndexes": "users",
                "$db": "app",
                "indexes": [
                    { "key": { "profile.city": 1_i32, "active": 1_i32 }, "name": "city_active_1" },
                    { "key": { "name": 1_i32 }, "name": "name_1" },
                ],
            },
        )
        .unwrap();

        let id_explain = find_documents(
            &conn,
            &doc! {
                "find": "users",
                "$db": "app",
                "filter": { "_id": "u1" },
                "explain": true,
            },
        )
        .unwrap();
        let id_plan = id_explain
            .get_document("queryPlanner")
            .unwrap()
            .get_document("winningPlan")
            .unwrap();
        assert_eq!(id_plan.get_str("stage").unwrap(), "IDHACK");
        assert_eq!(id_plan.get_str("scanStrategy").unwrap(), "idExact");

        let exact_explain = find_documents(
            &conn,
            &doc! {
                "find": "users",
                "$db": "app",
                "filter": { "name": "Ada" },
                "explain": true,
            },
        )
        .unwrap();
        let exact_plan = exact_explain
            .get_document("queryPlanner")
            .unwrap()
            .get_document("winningPlan")
            .unwrap();
        assert_eq!(exact_plan.get_str("stage").unwrap(), "IXSCAN");
        assert_eq!(
            exact_plan.get_str("scanStrategy").unwrap(),
            "indexExactEquality"
        );
        assert_eq!(exact_plan.get_str("indexName").unwrap(), "name_1");

        let hinted_prefix = find_documents(
            &conn,
            &doc! {
                "find": "users",
                "$db": "app",
                "filter": { "profile.city": "Rome" },
                "hint": "city_active_1",
                "explain": true,
            },
        )
        .unwrap();
        let prefix_planner = hinted_prefix.get_document("queryPlanner").unwrap();
        assert!(prefix_planner.get_bool("hintProvided").unwrap());
        let prefix_plan = prefix_planner.get_document("winningPlan").unwrap();
        assert_eq!(
            prefix_plan.get_str("scanStrategy").unwrap(),
            "indexEqualityPrefix"
        );
        assert_eq!(prefix_plan.get_i32("prefixLen").unwrap(), 1);

        let range_count = count_documents_command(
            &conn,
            &doc! {
                "count": "users",
                "$db": "app",
                "query": { "name": { "$gte": "G" } },
                "explain": true,
            },
        )
        .unwrap();
        let range_plan = range_count
            .get_document("queryPlanner")
            .unwrap()
            .get_document("winningPlan")
            .unwrap();
        assert_eq!(range_plan.get_str("scanStrategy").unwrap(), "indexRange");
        assert_eq!(range_plan.get_str("indexName").unwrap(), "name_1");

        let fallback_explain = find_documents(
            &conn,
            &doc! {
                "find": "users",
                "$db": "app",
                "filter": { "$or": [{ "name": "Ada" }, { "name": "Grace" }] },
                "explain": true,
            },
        )
        .unwrap();
        let fallback_plan = fallback_explain
            .get_document("queryPlanner")
            .unwrap()
            .get_document("winningPlan")
            .unwrap();
        assert_eq!(fallback_plan.get_str("stage").unwrap(), "COLLSCAN");
        assert_eq!(
            fallback_plan.get_str("scanStrategy").unwrap(),
            "collectionScan"
        );
        assert!(
            fallback_plan
                .get_str("fallbackReason")
                .unwrap()
                .contains("not index-planned")
        );
    }

    #[test]
    fn explain_validates_hints_and_unsupported_command_paths() {
        let conn = test_conn();
        seed_find_documents(&conn);
        create_indexes(
            &conn,
            &doc! {
                "createIndexes": "users",
                "$db": "app",
                "indexes": [{ "key": { "profile.city": 1_i32, "active": 1_i32 }, "name": "city_active_1" }],
            },
        )
        .unwrap();

        let bad_hint = find_documents(
            &conn,
            &doc! {
                "find": "users",
                "$db": "app",
                "filter": { "name": "Ada" },
                "hint": "city_active_1",
                "explain": true,
            },
        )
        .unwrap();
        assert_command_error(&bad_hint);
        assert_eq!(bad_hint.get_i32("code").unwrap(), 2);

        let bad_sort = find_documents(
            &conn,
            &doc! {
                "find": "users",
                "$db": "app",
                "filter": {},
                "sort": { "name": "asc" },
                "explain": true,
            },
        )
        .unwrap();
        assert_command_error(&bad_sort);
        assert_eq!(bad_sort.get_i32("code").unwrap(), 2);

        let unsupported_update = update_documents(
            &conn,
            &doc! {
                "update": "users",
                "$db": "app",
                "updates": [{
                    "q": { "name": "Ada" },
                    "u": { "$set": { "team": "explain" } },
                }],
                "explain": true,
            },
        )
        .unwrap();
        assert_command_error(&unsupported_update);
        assert_eq!(unsupported_update.get_i32("code").unwrap(), 72);
        assert_eq!(
            find_ids(&conn, doc! { "team": "explain" }),
            Vec::<String>::new()
        );
    }

    #[test]
    fn sort_pushdown_uses_safe_index_order_and_falls_back_for_unsafe_shapes() {
        let conn = test_conn();
        insert_documents(
            &conn,
            &doc! {
                "insert": "events",
                "$db": "app",
                "documents": [
                    { "_id": "e1", "account": "a", "created": bson::DateTime::from_millis(1_700_000_000_000_i64) },
                    { "_id": "e2", "account": "a", "created": bson::DateTime::from_millis(1_600_000_000_000_i64) },
                    { "_id": "e3", "account": "a", "created": bson::DateTime::from_millis(1_800_000_000_000_i64) },
                    { "_id": "e4", "account": "b", "created": bson::DateTime::from_millis(1_900_000_000_000_i64) },
                ],
            },
        )
        .unwrap();
        create_indexes(
            &conn,
            &doc! {
                "createIndexes": "events",
                "$db": "app",
                "indexes": [
                    { "key": { "created": 1_i32 }, "name": "created_1" },
                    { "key": { "account": 1_i32, "created": 1_i32 }, "name": "account_created_1" },
                ],
            },
        )
        .unwrap();

        let asc = find_documents(
            &conn,
            &doc! {
                "find": "events",
                "$db": "app",
                "filter": {},
                "sort": { "created": 1_i32 },
                "skip": 1_i32,
                "limit": 2_i32,
            },
        )
        .unwrap();
        assert_eq!(
            first_batch(&asc)
                .iter()
                .map(|doc| doc.get_str("_id").unwrap().to_string())
                .collect::<Vec<_>>(),
            vec!["e1", "e3"]
        );

        let desc = find_documents(
            &conn,
            &doc! {
                "find": "events",
                "$db": "app",
                "filter": {},
                "sort": { "created": -1_i32 },
                "limit": 2_i32,
            },
        )
        .unwrap();
        assert_eq!(
            first_batch(&desc)
                .iter()
                .map(|doc| doc.get_str("_id").unwrap().to_string())
                .collect::<Vec<_>>(),
            vec!["e4", "e3"]
        );

        let compound = find_documents(
            &conn,
            &doc! {
                "find": "events",
                "$db": "app",
                "filter": { "account": "a" },
                "sort": { "created": -1_i32 },
            },
        )
        .unwrap();
        assert_eq!(
            first_batch(&compound)
                .iter()
                .map(|doc| doc.get_str("_id").unwrap().to_string())
                .collect::<Vec<_>>(),
            vec!["e3", "e1", "e2"]
        );

        let explain = find_documents(
            &conn,
            &doc! {
                "find": "events",
                "$db": "app",
                "filter": { "account": "a" },
                "sort": { "created": 1_i32 },
                "explain": true,
            },
        )
        .unwrap();
        assert_eq!(
            explain
                .get_document("queryPlanner")
                .unwrap()
                .get_document("winningPlan")
                .unwrap()
                .get_str("scanStrategy")
                .unwrap(),
            "indexSort"
        );

        insert_documents(
            &conn,
            &doc! {
                "insert": "events",
                "$db": "app",
                "documents": [{ "_id": "e5", "account": "c" }],
            },
        )
        .unwrap();
        let missing_fallback = find_documents(
            &conn,
            &doc! {
                "find": "events",
                "$db": "app",
                "filter": {},
                "sort": { "created": 1_i32 },
                "explain": true,
            },
        )
        .unwrap();
        assert_ne!(
            missing_fallback
                .get_document("queryPlanner")
                .unwrap()
                .get_document("winningPlan")
                .unwrap()
                .get_str("scanStrategy")
                .unwrap(),
            "indexSort"
        );

        insert_documents(
            &conn,
            &doc! {
                "insert": "people",
                "$db": "app",
                "documents": [
                    { "_id": "p1", "name": "Ada" },
                    { "_id": "p2", "name": "Grace" },
                ],
            },
        )
        .unwrap();
        create_indexes(
            &conn,
            &doc! {
                "createIndexes": "people",
                "$db": "app",
                "indexes": [{ "key": { "name": 1_i32 }, "name": "name_1" }],
            },
        )
        .unwrap();
        let string_sort = find_documents(
            &conn,
            &doc! {
                "find": "people",
                "$db": "app",
                "filter": {},
                "sort": { "name": 1_i32 },
                "explain": true,
            },
        )
        .unwrap();
        assert_ne!(
            string_sort
                .get_document("queryPlanner")
                .unwrap()
                .get_document("winningPlan")
                .unwrap()
                .get_str("scanStrategy")
                .unwrap(),
            "indexSort"
        );
    }

    #[test]
    fn planner_uses_sparse_and_partial_indexes_only_when_filter_implies_membership() {
        let conn = test_conn();
        insert_documents(
            &conn,
            &doc! {
                "insert": "users",
                "$db": "app",
                "documents": [
                    { "_id": "u1", "email": "same@example.test", "active": true, "handle": "ada" },
                    { "_id": "u2", "email": "same@example.test", "active": false },
                    { "_id": "u3", "name": "missing" },
                    { "_id": "u4", "email": "other@example.test", "active": true, "handle": "grace" },
                ],
            },
        )
        .unwrap();
        create_indexes(
            &conn,
            &doc! {
                "createIndexes": "users",
                "$db": "app",
                "indexes": [
                    { "key": { "email": 1_i32 }, "name": "email_sparse", "sparse": true },
                    {
                        "key": { "email": 1_i32 },
                        "name": "email_active_partial",
                        "partialFilterExpression": { "active": true },
                    },
                    {
                        "key": { "email": 1_i32 },
                        "name": "email_active_handle_partial",
                        "partialFilterExpression": {
                            "$and": [
                                { "active": { "$eq": true } },
                                { "handle": { "$exists": true } },
                            ],
                        },
                    },
                ],
            },
        )
        .unwrap();

        assert_eq!(
            pushed_down_count(
                &conn,
                "app.users",
                &doc! { "email": "same@example.test" },
                0,
                None,
            )
            .unwrap(),
            Some(2)
        );
        assert_eq!(
            plan_count(
                &conn,
                "app.users",
                &doc! { "email": "same@example.test", "active": true }
            )
            .unwrap(),
            CountPlan::IndexedEquality {
                index_name: "email_active_partial".to_string(),
                key_value: "str:same@example.test".to_string(),
            }
        );
        assert_eq!(
            pushed_down_count(
                &conn,
                "app.users",
                &doc! { "email": "same@example.test", "active": true },
                0,
                None,
            )
            .unwrap(),
            Some(1)
        );
        assert_eq!(
            plan_count(
                &conn,
                "app.users",
                &doc! { "email": "same@example.test", "active": true, "handle": "ada" }
            )
            .unwrap(),
            CountPlan::IndexedEquality {
                index_name: "email_active_handle_partial".to_string(),
                key_value: "str:same@example.test".to_string(),
            }
        );
        assert_eq!(
            plan_count(&conn, "app.users", &doc! { "active": true }).unwrap(),
            CountPlan::Fallback
        );
        assert_eq!(
            plan_count(
                &conn,
                "app.users",
                &doc! { "email": "same@example.test", "active": false }
            )
            .unwrap(),
            CountPlan::Fallback
        );
        assert_eq!(
            find_ids(
                &conn,
                doc! { "email": "same@example.test", "active": false }
            ),
            vec!["u2"]
        );
    }

    #[test]
    fn planner_falls_back_when_compound_index_omits_array_values() {
        let conn = test_conn();
        insert_documents(
            &conn,
            &doc! {
                "insert": "users",
                "$db": "app",
                "documents": [
                    { "_id": "u1", "tags": ["math"], "active": true },
                    { "_id": "u2", "tags": "math", "active": true },
                    { "_id": "u3", "tags": "math", "active": false },
                ],
            },
        )
        .unwrap();
        create_indexes(
            &conn,
            &doc! {
                "createIndexes": "users",
                "$db": "app",
                "indexes": [{ "key": { "tags": 1_i32, "active": 1_i32 }, "name": "tags_active_1" }],
            },
        )
        .unwrap();

        assert!(!index_entries_safe_for_planner(&conn, "app.users", "tags_active_1").unwrap());
        assert!(
            indexed_candidate_documents(
                &conn,
                "app.users",
                &doc! { "tags": "math", "active": true },
            )
            .unwrap()
            .is_none()
        );
        assert_eq!(
            find_ids(&conn, doc! { "tags": "math", "active": true }),
            vec!["u1", "u2"]
        );
        assert_eq!(
            plan_count(&conn, "app.users", &doc! { "tags": "math", "active": true }).unwrap(),
            CountPlan::Fallback
        );
        let tx = conn.unchecked_transaction().unwrap();
        assert_eq!(
            plan_transaction_candidates(&tx, "app.users", &doc! { "tags": "math", "active": true })
                .unwrap(),
            TransactionCandidatePlan::Fallback
        );
    }

    #[test]
    fn planner_keeps_multikey_index_usable_after_array_is_replaced() {
        let conn = test_conn();
        insert_documents(
            &conn,
            &doc! {
                "insert": "users",
                "$db": "app",
                "documents": [
                    { "_id": "u1", "tag": ["math"] },
                    { "_id": "u2", "tag": "math" },
                ],
            },
        )
        .unwrap();
        create_indexes(
            &conn,
            &doc! {
                "createIndexes": "users",
                "$db": "app",
                "indexes": [{ "key": { "tag": 1_i32 }, "name": "tag_1" }],
            },
        )
        .unwrap();
        assert!(index_entries_safe_for_planner(&conn, "app.users", "tag_1").unwrap());
        assert_eq!(
            indexed_candidate_documents(&conn, "app.users", &doc! { "tag": "math" })
                .unwrap()
                .unwrap()
                .iter()
                .map(|doc| doc.get_str("_id").unwrap().to_string())
                .collect::<Vec<_>>(),
            vec!["u1", "u2"]
        );

        update_documents(
            &conn,
            &doc! {
                "update": "users",
                "$db": "app",
                "updates": [{ "q": { "_id": "u1" }, "u": { "$set": { "tag": "math" } } }],
            },
        )
        .unwrap();

        assert!(index_entries_safe_for_planner(&conn, "app.users", "tag_1").unwrap());
        let candidates = indexed_candidate_documents(&conn, "app.users", &doc! { "tag": "math" })
            .unwrap()
            .unwrap();
        assert_eq!(
            candidates
                .iter()
                .map(|doc| doc.get_str("_id").unwrap().to_string())
                .collect::<Vec<_>>(),
            vec!["u1", "u2"]
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
    fn compound_planner_entries_are_rebuilt_refreshed_and_dropped() {
        let conn = test_conn();
        seed_find_documents(&conn);
        insert_documents(
            &conn,
            &doc! {
                "insert": "users",
                "$db": "app",
                "documents": [
                    { "_id": "unsafe_numeric", "profile": { "city": "Rome" }, "active": 1_i32 },
                    { "_id": "unsafe_array", "profile": { "city": "Rome" }, "active": [true] },
                    { "_id": "unsafe_missing", "profile": { "city": "Rome" } },
                    { "_id": "unsafe_document", "profile": { "city": "Rome" }, "active": { "nested": true } },
                ],
            },
        )
        .unwrap();
        create_indexes(
            &conn,
            &doc! {
                "createIndexes": "users",
                "$db": "app",
                "indexes": [{ "key": { "profile.city": 1_i32, "active": 1_i32 }, "name": "city_active_1" }],
            },
        )
        .unwrap();

        let count: i32 = conn
            .query_row(
                "SELECT COUNT(*) FROM index_entries WHERE namespace = 'app.users' AND index_name = 'city_active_1'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(count, 20);
        let omissions: i32 = conn
            .query_row(
                "SELECT COUNT(*) FROM index_multikey_omissions WHERE namespace = 'app.users' AND index_name = 'city_active_1'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(omissions, 1);
        assert_eq!(
            find_ids(&conn, doc! { "profile.city": "Rome", "active": true }),
            vec!["u1", "u3", "unsafe_array"]
        );

        update_documents(
            &conn,
            &doc! {
                "update": "users",
                "$db": "app",
                "updates": [{ "q": { "_id": "u1" }, "u": { "$set": { "profile.city": "Milan" } } }],
            },
        )
        .unwrap();
        assert_eq!(
            find_ids(&conn, doc! { "profile.city": "Rome", "active": true }),
            vec!["u3", "unsafe_array"]
        );
        assert_eq!(
            find_ids(&conn, doc! { "profile.city": "Milan", "active": true }),
            vec!["u1"]
        );

        find_and_modify(
            &conn,
            "findAndModify",
            &doc! {
                "findAndModify": "users",
                "$db": "app",
                "query": { "_id": "u3" },
                "update": { "$set": { "active": false } },
            },
        )
        .unwrap();
        assert_eq!(
            find_ids(&conn, doc! { "profile.city": "Rome", "active": true }),
            vec!["unsafe_array"]
        );
        assert_eq!(
            find_ids(&conn, doc! { "profile.city": "Rome", "active": false }),
            vec!["u3"]
        );

        delete_documents(
            &conn,
            &doc! { "delete": "users", "$db": "app", "deletes": [{ "q": { "_id": "u3" }, "limit": 1_i32 }] },
        )
        .unwrap();
        assert_eq!(
            find_ids(&conn, doc! { "profile.city": "Rome", "active": false }),
            Vec::<String>::new()
        );

        drop_indexes(
            &conn,
            &doc! { "dropIndexes": "users", "$db": "app", "index": "city_active_1" },
        )
        .unwrap();
        let count: i32 = conn
            .query_row(
                "SELECT COUNT(*) FROM index_entries WHERE namespace = 'app.users' AND index_name = 'city_active_1'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(count, 0);
        let omissions: i32 = conn
            .query_row(
                "SELECT COUNT(*) FROM index_multikey_omissions WHERE namespace = 'app.users' AND index_name = 'city_active_1'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(omissions, 0);
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
        find_ids_in(conn, "users", filter)
    }

    fn find_ids_in(conn: &Connection, collection: &str, filter: Document) -> Vec<String> {
        first_batch(
            &find_documents(
                &conn,
                &doc! { "find": collection, "$db": "app", "filter": filter },
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
    fn find_matcher_supports_regex_predicates() {
        let conn = test_conn();
        insert_documents(
            &conn,
            &doc! {
                "insert": "users",
                "$db": "app",
                "documents": [
                    { "_id": "u1", "name": "Ada Lovelace", "bio": "first\nprogrammer", "tags": ["Math", "logic"], "age": 37_i32 },
                    { "_id": "u2", "name": "Grace Hopper", "bio": "COBOL\npioneer", "tags": ["navy"], "age": 39_i32 },
                    { "_id": "u3", "name": "Katherine Johnson", "bio": "orbital math", "tags": ["space"], "age": 41_i32 },
                    { "_id": "u4", "name": 42_i32, "tags": [1_i32, 2_i32] },
                ],
            },
        )
        .unwrap();

        assert_eq!(
            find_ids(&conn, doc! { "name": { "$regex": "^Ada" } }),
            vec!["u1"]
        );
        assert_eq!(
            find_ids(
                &conn,
                doc! { "name": { "$regex": "^ada", "$options": "i" } }
            ),
            vec!["u1"]
        );
        assert_eq!(
            find_ids(
                &conn,
                doc! { "bio": { "$regex": "^programmer$", "$options": "m" } }
            ),
            vec!["u1"]
        );
        assert_eq!(
            find_ids(
                &conn,
                doc! { "bio": { "$regex": "COBOL.*pioneer", "$options": "s" } }
            ),
            vec!["u2"]
        );
        assert_eq!(
            find_ids(
                &conn,
                doc! { "tags": { "$regex": "^mat", "$options": "i" } }
            ),
            vec!["u1"]
        );
        assert_eq!(
            find_ids(
                &conn,
                doc! { "name": Bson::RegularExpression(bson::Regex { pattern: "hopper$".to_string(), options: "i".to_string() }) }
            ),
            vec!["u2"]
        );
        assert!(find_ids(&conn, doc! { "age": { "$regex": "^37" } }).is_empty());

        for filter in [
            doc! { "name": { "$regex": "(" } },
            doc! { "name": { "$regex": "Ada", "$options": "x" } },
            doc! { "name": { "$regex": 1_i32 } },
            doc! { "name": { "$options": "i" } },
            doc! { "name": { "$regex": "Ada", "$options": 1_i32 } },
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
    fn find_matcher_supports_type_size_and_all_predicates() {
        let conn = test_conn();
        let object_id = ObjectId::new();
        insert_documents(
            &conn,
            &doc! {
                "insert": "users",
                "$db": "app",
                "documents": [
                    {
                        "_id": "u1",
                        "name": "Ada",
                        "profile": { "city": "Rome" },
                        "tags": ["math", "logic"],
                        "scores": [1_i32, 2_i32, 2_i64],
                        "nothing": Bson::Null,
                        "active": true,
                        "oid": object_id,
                        "age": 37_i32,
                        "long": 37_i64,
                        "ratio": 1.5,
                        "created": bson::DateTime::from_millis(1_000),
                    },
                    {
                        "_id": "u2",
                        "name": "Grace",
                        "tags": ["navy"],
                        "scores": [3_i32],
                        "age": 39_i64,
                    },
                    {
                        "_id": "u3",
                        "name": "Katherine",
                        "tags": [],
                        "scores": "none",
                        "age": 41.0,
                    },
                ],
            },
        )
        .unwrap();

        assert_eq!(
            find_ids(&conn, doc! { "name": { "$type": "string" } }),
            vec!["u1", "u2", "u3"]
        );
        assert_eq!(
            find_ids(&conn, doc! { "profile": { "$type": "object" } }),
            vec!["u1"]
        );
        assert_eq!(
            find_ids(&conn, doc! { "tags": { "$type": "array" } }),
            vec!["u1", "u2", "u3"]
        );
        assert_eq!(
            find_ids(&conn, doc! { "tags": { "$type": "string" } }),
            vec!["u1", "u2"]
        );
        assert_eq!(
            find_ids(&conn, doc! { "active": { "$type": "bool" } }),
            vec!["u1"]
        );
        assert_eq!(
            find_ids(&conn, doc! { "oid": { "$type": "objectId" } }),
            vec!["u1"]
        );
        assert_eq!(
            find_ids(&conn, doc! { "oid": { "$type": 7_i32 } }),
            vec!["u1"]
        );
        assert_eq!(
            find_ids(&conn, doc! { "oid": { "$type": [7_i32, "string"] } }),
            vec!["u1"]
        );
        assert_eq!(
            find_ids(&conn, doc! { "created": { "$type": 9_i32 } }),
            vec!["u1"]
        );
        assert_eq!(
            find_ids(&conn, doc! { "nothing": { "$type": 10_i64 } }),
            vec!["u1"]
        );
        assert_eq!(
            find_ids(&conn, doc! { "age": { "$type": ["int", "long"] } }),
            vec!["u1", "u2"]
        );
        assert_eq!(
            find_ids(&conn, doc! { "age": { "$type": "number" } }),
            vec!["u1", "u2", "u3"]
        );
        assert_eq!(
            find_ids(&conn, doc! { "ratio": { "$type": 1_i32 } }),
            vec!["u1"]
        );

        assert_eq!(
            find_ids(&conn, doc! { "tags": { "$size": 2_i32 } }),
            vec!["u1"]
        );
        assert_eq!(
            find_ids(&conn, doc! { "tags": { "$size": 0_i64 } }),
            vec!["u3"]
        );
        assert!(find_ids(&conn, doc! { "name": { "$size": 1_i32 } }).is_empty());

        assert_eq!(
            find_ids(&conn, doc! { "tags": { "$all": ["logic", "math"] } }),
            vec!["u1"]
        );
        assert_eq!(
            find_ids(&conn, doc! { "scores": { "$all": [2_i32, 2_i32] } }),
            vec!["u1"]
        );
        assert!(find_ids(&conn, doc! { "tags": { "$all": [] } }).is_empty());
        assert!(find_ids(&conn, doc! { "tags": { "$all": ["math", "missing"] } }).is_empty());

        for filter in [
            doc! { "name": { "$type": "decimal" } },
            doc! { "name": { "$type": 99_i32 } },
            doc! { "name": { "$type": [] } },
            doc! { "tags": { "$size": -1_i32 } },
            doc! { "tags": { "$size": 1.5 } },
            doc! { "tags": { "$all": "math" } },
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
    fn find_matcher_supports_elem_match_predicates() {
        let conn = test_conn();
        insert_documents(
            &conn,
            &doc! {
                "insert": "users",
                "$db": "app",
                "documents": [
                    {
                        "_id": "u1",
                        "scores": [1_i32, 5_i32, 8_i32],
                        "tags": ["math", "logic"],
                        "items": [
                            { "kind": "a", "score": 1_i32, "meta": { "flag": false } },
                            { "kind": "b", "score": 5_i32, "meta": { "flag": true } },
                        ],
                    },
                    {
                        "_id": "u2",
                        "scores": [2_i32, 9_i32],
                        "tags": ["navy"],
                        "items": [
                            { "kind": "a", "score": 6_i32, "meta": { "flag": false } },
                            { "kind": "b", "score": 2_i32, "meta": { "flag": true } },
                        ],
                    },
                    {
                        "_id": "u3",
                        "scores": [3_i32],
                        "tags": ["space"],
                        "items": [
                            { "kind": "a", "score": 1_i32, "meta": { "flag": true } },
                        ],
                    },
                ],
            },
        )
        .unwrap();

        assert_eq!(
            find_ids(
                &conn,
                doc! { "scores": { "$elemMatch": { "$gt": 4_i32, "$lt": 7_i32 } } }
            ),
            vec!["u1"]
        );
        assert_eq!(
            find_ids(
                &conn,
                doc! { "tags": { "$elemMatch": { "$regex": "^LOG", "$options": "i" } } }
            ),
            vec!["u1"]
        );
        assert_eq!(
            find_ids(
                &conn,
                doc! { "items": { "$elemMatch": { "kind": "a", "score": { "$gte": 5_i32 } } } }
            ),
            vec!["u2"]
        );
        assert_eq!(
            find_ids(
                &conn,
                doc! { "items": { "$elemMatch": { "kind": "a", "meta.flag": true } } }
            ),
            vec!["u3"]
        );
        assert_eq!(
            find_ids(
                &conn,
                doc! { "items": { "$elemMatch": { "$or": [{ "score": { "$gte": 6_i32 } }, { "meta.flag": false }] } } }
            ),
            vec!["u1", "u2"]
        );
        assert!(
            find_ids(
                &conn,
                doc! { "items.kind": "a", "items.score": { "$gte": 5_i32 } }
            )
            .contains(&"u1".to_string())
        );
        assert_eq!(
            find_ids(
                &conn,
                doc! { "items": { "$elemMatch": { "kind": "a", "score": { "$gte": 5_i32 } } } }
            ),
            vec!["u2"]
        );
        assert_eq!(
            find_ids(
                &conn,
                doc! { "items": { "$all": [
                    { "$elemMatch": { "kind": "a", "score": { "$gte": 5_i32 } } },
                    { "$elemMatch": { "kind": "b", "score": { "$lte": 2_i32 } } },
                ] } }
            ),
            vec!["u2"]
        );

        for filter in [
            doc! { "scores": { "$elemMatch": 5_i32 } },
            doc! { "scores": { "$elemMatch": {} } },
            doc! { "scores": { "$elemMatch": { "$where": "bad" } } },
            doc! { "items": { "$elemMatch": { "$and": [] } } },
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
    fn update_scalar_modifiers_support_rename_min_max_mul_and_set_on_insert() {
        let conn = test_conn();
        insert_documents(
            &conn,
            &doc! {
                "insert": "users",
                "$db": "app",
                "documents": [
                    {
                        "_id": "u1",
                        "age": 37_i32,
                        "score": 7_i32,
                        "multiplier": 4_i32,
                        "profile": { "city": "Rome" },
                    }
                ],
            },
        )
        .unwrap();

        let existing = update_documents(
            &conn,
            &doc! {
                "update": "users",
                "$db": "app",
                "updates": [
                    {
                        "q": { "_id": "u1" },
                        "u": {
                            "$rename": { "profile.city": "location" },
                            "$min": { "age": 35_i32, "floor": 4_i32 },
                            "$max": { "score": 10_i32, "ceiling": 8_i32 },
                            "$mul": { "multiplier": 3_i32, "missingProduct": 2_i32 },
                            "$setOnInsert": { "created": true },
                        },
                    }
                ],
            },
        )
        .unwrap();
        assert_eq!(existing.get_i32("n").unwrap(), 1);
        assert_eq!(existing.get_i32("nModified").unwrap(), 1);

        let docs = first_batch(
            &find_documents(
                &conn,
                &doc! { "find": "users", "$db": "app", "filter": { "_id": "u1" } },
            )
            .unwrap(),
        );
        assert_eq!(docs[0].get_str("location").unwrap(), "Rome");
        assert!(
            !docs[0]
                .get_document("profile")
                .unwrap()
                .contains_key("city")
        );
        assert_eq!(docs[0].get_i32("age").unwrap(), 35);
        assert_eq!(docs[0].get_i32("floor").unwrap(), 4);
        assert_eq!(docs[0].get_i32("score").unwrap(), 10);
        assert_eq!(docs[0].get_i32("ceiling").unwrap(), 8);
        assert_eq!(docs[0].get_i32("multiplier").unwrap(), 12);
        assert_eq!(docs[0].get_i32("missingProduct").unwrap(), 0);
        assert!(!docs[0].contains_key("created"));

        let upsert = update_documents(
            &conn,
            &doc! {
                "update": "users",
                "$db": "app",
                "updates": [
                    {
                        "q": { "_id": "u2", "email": "new@example.test" },
                        "u": {
                            "$set": { "name": "New" },
                            "$setOnInsert": { "created": true },
                            "$mul": { "count": 2_i32 },
                        },
                        "upsert": true,
                    }
                ],
            },
        )
        .unwrap();
        assert_eq!(upsert.get_i32("nUpserted").unwrap(), 1);
        let inserted = first_batch(
            &find_documents(
                &conn,
                &doc! { "find": "users", "$db": "app", "filter": { "_id": "u2" } },
            )
            .unwrap(),
        );
        assert_eq!(inserted[0].get_str("email").unwrap(), "new@example.test");
        assert_eq!(inserted[0].get_str("name").unwrap(), "New");
        assert!(inserted[0].get_bool("created").unwrap());
        assert_eq!(inserted[0].get_i32("count").unwrap(), 0);
    }

    #[test]
    fn find_and_modify_uses_scalar_modifiers_and_ignores_set_on_insert_for_matches() {
        let conn = test_conn();
        seed_find_documents(&conn);

        let response = find_and_modify(
            &conn,
            "findAndModify",
            &doc! {
                "findAndModify": "users",
                "$db": "app",
                "query": { "_id": "u2" },
                "update": {
                    "$rename": { "profile.city": "city" },
                    "$mul": { "age": 2_i32 },
                    "$min": { "score": 5_i32 },
                    "$setOnInsert": { "created": true },
                },
                "new": true,
            },
        )
        .unwrap();

        let value = response.get_document("value").unwrap();
        assert_eq!(value.get_str("city").unwrap(), "London");
        assert_eq!(value.get_i64("age").unwrap(), 78);
        assert_eq!(value.get_i32("score").unwrap(), 5);
        assert!(!value.contains_key("created"));
    }

    #[test]
    fn update_scalar_modifiers_reject_invalid_operands_and_paths() {
        let conn = test_conn();
        insert_documents(
            &conn,
            &doc! {
                "insert": "users",
                "$db": "app",
                "documents": [
                    { "_id": "u1", "name": "Ada", "profile": { "city": "Rome" }, "count": "many" },
                    { "_id": "overflow", "value": Bson::Int64(i64::MAX) },
                ],
            },
        )
        .unwrap();

        for update in [
            doc! { "$rename": { "name": 5_i32 } },
            doc! { "$rename": { "_id": "other" } },
            doc! { "$rename": { "name": "_id" } },
            doc! { "$rename": { "profile": "profile.city" } },
            doc! { "$rename": { "items.$.name": "name" } },
            doc! { "$mul": { "count": 2_i32 } },
            doc! { "$mul": { "count": "bad" } },
            doc! { "$set": { "created": false }, "$setOnInsert": { "created": true } },
        ] {
            let response = update_documents(
                &conn,
                &doc! {
                    "update": "users",
                    "$db": "app",
                    "updates": [{ "q": { "_id": "u1" }, "u": update }],
                },
            )
            .unwrap();
            assert_eq!(response.get_f64("ok").unwrap(), 1.0);
            assert!(!write_errors(&response).is_empty());
        }

        let overflow = update_documents(
            &conn,
            &doc! {
                "update": "users",
                "$db": "app",
                "updates": [
                    { "q": { "_id": "overflow" }, "u": { "$mul": { "value": 2_i32 } } }
                ],
            },
        )
        .unwrap();
        assert!(!write_errors(&overflow).is_empty());
        let stored = first_batch(
            &find_documents(
                &conn,
                &doc! { "find": "users", "$db": "app", "filter": { "_id": "overflow" } },
            )
            .unwrap(),
        );
        assert_eq!(stored[0].get_i64("value").unwrap(), i64::MAX);
    }

    #[test]
    fn update_array_modifiers_support_practical_subset() {
        let conn = test_conn();
        insert_documents(
            &conn,
            &doc! {
                "insert": "users",
                "$db": "app",
                "documents": [
                    {
                        "_id": "u1",
                        "active": true,
                        "tags": ["math"],
                        "batch": [],
                        "unique": ["math"],
                        "numbers": [1_i32, 2_i32, 3_i32],
                        "scores": [1_i32, 3_i32, 5_i32],
                        "docs": [
                            { "kind": "a", "score": 1_i32, "meta": { "flag": false } },
                            { "kind": "a", "score": 3_i32, "meta": { "flag": true } },
                            { "kind": "b", "score": 4_i32, "meta": { "flag": true } },
                            { "kind": "c", "score": 2_i32, "meta": { "flag": true } },
                        ],
                        "letters": ["x", "y", "z"],
                    },
                    {
                        "_id": "u2",
                        "active": true,
                        "docs": [
                            { "kind": "a", "score": 4_i32 },
                            { "kind": "b", "score": 2_i32 },
                        ],
                    },
                ],
            },
        )
        .unwrap();

        let single = update_documents(
            &conn,
            &doc! {
                "update": "users",
                "$db": "app",
                "updates": [
                    {
                        "q": { "_id": "u1" },
                        "u": {
                            "$push": { "tags": "logic", "batch": { "$each": ["a", "b"] } },
                            "$addToSet": { "unique": { "$each": ["math", "logic"] } },
                            "$pop": { "numbers": 1_i32 },
                            "$pull": {
                                "scores": { "$gte": 3_i32 },
                                "docs": { "kind": "a", "score": { "$gte": 2_i32 } },
                            },
                            "$pullAll": { "letters": ["x", "z"] },
                        },
                    }
                ],
            },
        )
        .unwrap();
        assert_eq!(single.get_i32("nModified").unwrap(), 1);

        let multi = update_documents(
            &conn,
            &doc! {
                "update": "users",
                "$db": "app",
                "updates": [
                    {
                        "q": { "active": true },
                        "u": { "$push": { "events": "seen" }, "$pull": { "docs": { "kind": "b" } } },
                        "multi": true,
                    }
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
            docs[0].get_array("tags").unwrap(),
            &bson_strings(&["math", "logic"])
        );
        assert_eq!(
            docs[0].get_array("batch").unwrap(),
            &bson_strings(&["a", "b"])
        );
        assert_eq!(
            docs[0].get_array("unique").unwrap(),
            &bson_strings(&["math", "logic"])
        );
        assert_eq!(docs[0].get_array("numbers").unwrap(), &bson_ints(&[1, 2]));
        assert_eq!(docs[0].get_array("scores").unwrap(), &bson_ints(&[1]));
        assert_eq!(
            docs[0].get_array("docs").unwrap(),
            &bson_documents(vec![
                doc! { "kind": "a", "score": 1_i32, "meta": { "flag": false } },
                doc! { "kind": "c", "score": 2_i32, "meta": { "flag": true } },
            ])
        );
        assert_eq!(docs[0].get_array("letters").unwrap(), &bson_strings(&["y"]));
        assert_eq!(
            docs[0].get_array("events").unwrap(),
            &bson_strings(&["seen"])
        );
        assert_eq!(
            docs[1].get_array("events").unwrap(),
            &bson_strings(&["seen"])
        );
        assert_eq!(
            docs[1].get_array("docs").unwrap(),
            &bson_documents(vec![doc! { "kind": "a", "score": 4_i32 }])
        );
    }

    #[test]
    fn pull_document_arrays_supports_logical_predicates() {
        let conn = test_conn();
        insert_documents(
            &conn,
            &doc! {
                "insert": "users",
                "$db": "app",
                "documents": [{
                    "_id": "u1",
                    "or_items": [
                        { "kind": "active", "score": 5_i32 },
                        { "kind": "archived", "score": 2_i32 },
                        { "kind": "active", "score": 0_i32 },
                        { "kind": "review", "score": 3_i32 },
                    ],
                    "and_items": [
                        { "kind": "active", "score": 1_i32 },
                        { "kind": "active", "score": 4_i32 },
                        { "kind": "archived", "score": 1_i32 },
                    ],
                    "nor_items": [
                        { "kind": "active", "score": 5_i32 },
                        { "kind": "archived", "score": 2_i32 },
                        { "kind": "active", "score": 0_i32 },
                    ],
                    "none_items": [
                        { "kind": "active", "score": 5_i32 },
                        { "kind": "review", "score": 3_i32 },
                    ],
                    "regex_items": [
                        { "name": "Ada" },
                        { "name": "Grace" },
                    ],
                    "scores": [1_i32, 3_i32, 5_i32],
                    "docs": [{ "kind": "a" }, { "kind": "b" }],
                }],
            },
        )
        .unwrap();

        let response = update_documents(
            &conn,
            &doc! {
                "update": "users",
                "$db": "app",
                "updates": [{
                    "q": { "_id": "u1" },
                    "u": {
                        "$pull": {
                            "or_items": {
                                "$or": [
                                    { "kind": "archived" },
                                    { "score": { "$lte": 0_i32 } },
                                ],
                            },
                            "and_items": {
                                "$and": [
                                    { "kind": "active" },
                                    { "score": { "$lte": 1_i32 } },
                                ],
                            },
                            "nor_items": {
                                "$nor": [
                                    { "kind": "archived" },
                                    { "score": { "$lte": 0_i32 } },
                                ],
                            },
                            "none_items": { "$or": [{ "kind": "missing" }] },
                            "regex_items": { "name": { "$regex": "^a", "$options": "i" } },
                            "scores": { "$gte": 3_i32 },
                            "docs": { "$eq": { "kind": "a" } },
                        },
                    },
                }],
            },
        )
        .unwrap();
        assert_eq!(response.get_i32("nModified").unwrap(), 1);

        let docs = first_batch(
            &find_documents(
                &conn,
                &doc! { "find": "users", "$db": "app", "filter": { "_id": "u1" } },
            )
            .unwrap(),
        );
        let document = &docs[0];
        assert_eq!(
            document.get_array("or_items").unwrap(),
            &bson_documents(vec![
                doc! { "kind": "active", "score": 5_i32 },
                doc! { "kind": "review", "score": 3_i32 },
            ])
        );
        assert_eq!(
            document.get_array("and_items").unwrap(),
            &bson_documents(vec![
                doc! { "kind": "active", "score": 4_i32 },
                doc! { "kind": "archived", "score": 1_i32 },
            ])
        );
        assert_eq!(
            document.get_array("nor_items").unwrap(),
            &bson_documents(vec![
                doc! { "kind": "archived", "score": 2_i32 },
                doc! { "kind": "active", "score": 0_i32 },
            ])
        );
        assert_eq!(
            document.get_array("none_items").unwrap(),
            &bson_documents(vec![
                doc! { "kind": "active", "score": 5_i32 },
                doc! { "kind": "review", "score": 3_i32 },
            ])
        );
        assert_eq!(
            document.get_array("regex_items").unwrap(),
            &bson_documents(vec![doc! { "name": "Grace" }])
        );
        assert_eq!(document.get_array("scores").unwrap(), &bson_ints(&[1]));
        assert_eq!(
            document.get_array("docs").unwrap(),
            &bson_documents(vec![doc! { "kind": "b" }])
        );
    }

    #[test]
    fn find_and_modify_uses_array_modifiers() {
        let conn = test_conn();
        insert_documents(
            &conn,
            &doc! {
                "insert": "users",
                "$db": "app",
                "documents": [
                    {
                        "_id": "u1",
                        "tags": ["math"],
                        "unique": ["math"],
                        "numbers": [1_i32, 2_i32],
                        "scores": [1_i32, 4_i32],
                        "docs": [
                            { "kind": "a", "score": 1_i32 },
                            { "kind": "a", "score": 3_i32 },
                            { "kind": "b", "score": 5_i32 },
                        ],
                        "letters": ["x", "y"],
                    }
                ],
            },
        )
        .unwrap();

        let response = find_and_modify(
            &conn,
            "findAndModify",
            &doc! {
                "findAndModify": "users",
                "$db": "app",
                "query": { "_id": "u1" },
                "update": {
                    "$push": { "tags": { "$each": ["logic", "systems"] } },
                    "$addToSet": { "unique": { "$each": ["math", "logic"] } },
                    "$pop": { "numbers": -1_i32 },
                    "$pull": {
                        "scores": { "$gt": 2_i32 },
                        "docs": { "kind": "a", "score": { "$gte": 2_i32 } },
                    },
                    "$pullAll": { "letters": ["x"] },
                },
                "new": true,
            },
        )
        .unwrap();

        let value = response.get_document("value").unwrap();
        assert_eq!(
            value.get_array("tags").unwrap(),
            &bson_strings(&["math", "logic", "systems"])
        );
        assert_eq!(
            value.get_array("unique").unwrap(),
            &bson_strings(&["math", "logic"])
        );
        assert_eq!(value.get_array("numbers").unwrap(), &bson_ints(&[2]));
        assert_eq!(value.get_array("scores").unwrap(), &bson_ints(&[1]));
        assert_eq!(
            value.get_array("docs").unwrap(),
            &bson_documents(vec![
                doc! { "kind": "a", "score": 1_i32 },
                doc! { "kind": "b", "score": 5_i32 },
            ])
        );
        assert_eq!(value.get_array("letters").unwrap(), &bson_strings(&["y"]));
    }

    #[test]
    fn new_modifiers_refresh_index_entries_and_find_and_modify_images() {
        let conn = test_conn();
        insert_documents(
            &conn,
            &doc! {
                "insert": "users",
                "$db": "app",
                "documents": [
                    {
                        "_id": "u1",
                        "profile": { "city": "Rome" },
                        "score": 4_i32,
                        "tags": ["math"],
                    }
                ],
            },
        )
        .unwrap();
        create_indexes(
            &conn,
            &doc! {
                "createIndexes": "users",
                "$db": "app",
                "indexes": [
                    { "key": { "city": 1_i32 }, "name": "city_1" },
                    { "key": { "score": 1_i32 }, "name": "score_1" },
                ],
            },
        )
        .unwrap();

        let pre_image = find_and_modify(
            &conn,
            "findAndModify",
            &doc! {
                "findAndModify": "users",
                "$db": "app",
                "query": { "_id": "u1" },
                "update": {
                    "$rename": { "profile.city": "city" },
                    "$mul": { "score": 3_i32 },
                    "$push": { "tags": "logic" },
                },
            },
        )
        .unwrap();
        let value = pre_image.get_document("value").unwrap();
        assert_eq!(
            value
                .get_document("profile")
                .unwrap()
                .get_str("city")
                .unwrap(),
            "Rome"
        );
        assert_eq!(value.get_i32("score").unwrap(), 4);
        assert_eq!(value.get_array("tags").unwrap(), &bson_strings(&["math"]));

        assert_eq!(find_ids(&conn, doc! { "city": "Rome" }), vec!["u1"]);
        assert_eq!(
            find_ids(&conn, doc! { "profile.city": "Rome" }),
            Vec::<String>::new()
        );
        assert_eq!(find_ids(&conn, doc! { "score": 12_i32 }), vec!["u1"]);
        assert_eq!(
            find_ids(&conn, doc! { "score": 4_i32 }),
            Vec::<String>::new()
        );

        let post_image = find_and_modify(
            &conn,
            "findAndModify",
            &doc! {
                "findAndModify": "users",
                "$db": "app",
                "query": { "_id": "u1" },
                "update": { "$pull": { "tags": "math" }, "$max": { "score": 20_i32 } },
                "new": true,
            },
        )
        .unwrap();
        let value = post_image.get_document("value").unwrap();
        assert_eq!(value.get_i32("score").unwrap(), 20);
        assert_eq!(value.get_array("tags").unwrap(), &bson_strings(&["logic"]));
        assert_eq!(find_ids(&conn, doc! { "score": 20_i32 }), vec!["u1"]);
        assert_eq!(
            find_ids(&conn, doc! { "score": 12_i32 }),
            Vec::<String>::new()
        );
    }

    #[test]
    fn new_modifier_batch_failures_preserve_ordered_and_unordered_semantics() {
        let conn = test_conn();
        insert_documents(
            &conn,
            &doc! {
                "insert": "users",
                "$db": "app",
                "documents": [
                    { "_id": "u1", "name": "Ada", "tags": [] },
                    { "_id": "u2", "name": "Grace", "tags": [] },
                ],
            },
        )
        .unwrap();

        let ordered = update_documents(
            &conn,
            &doc! {
                "update": "users",
                "$db": "app",
                "ordered": true,
                "updates": [
                    { "q": { "_id": "u1" }, "u": { "$push": { "name": "bad" } } },
                    { "q": { "_id": "u2" }, "u": { "$rename": { "name": "displayName" } } },
                ],
            },
        )
        .unwrap();
        assert_eq!(ordered.get_i32("n").unwrap(), 0);
        assert_eq!(write_errors(&ordered)[0].get_i32("index").unwrap(), 0);
        assert!(find_ids(&conn, doc! { "displayName": "Grace" }).is_empty());

        let unordered = update_documents(
            &conn,
            &doc! {
                "update": "users",
                "$db": "app",
                "ordered": false,
                "updates": [
                    { "q": { "_id": "u1" }, "u": { "$push": { "name": "bad" } } },
                    { "q": { "_id": "u2" }, "u": { "$rename": { "name": "displayName" } } },
                ],
            },
        )
        .unwrap();
        assert_eq!(unordered.get_i32("n").unwrap(), 1);
        assert_eq!(unordered.get_i32("nModified").unwrap(), 1);
        assert_eq!(write_errors(&unordered)[0].get_i32("index").unwrap(), 0);
        assert_eq!(find_ids(&conn, doc! { "displayName": "Grace" }), vec!["u2"]);
    }

    #[test]
    fn update_array_modifiers_reject_unsupported_and_adversarial_shapes() {
        let conn = test_conn();
        insert_documents(
            &conn,
            &doc! {
                "insert": "users",
                "$db": "app",
                "documents": [{
                    "_id": "u1",
                    "tags": [],
                    "name": "Ada",
                    "profile": "flat",
                    "docs": [{ "name": "Ada" }, { "name": "Grace" }],
                }],
            },
        )
        .unwrap();

        for update in [
            doc! { "$push": { "tags": { "$each": ["x"], "$position": 0_i32 } } },
            doc! { "$push": { "tags": { "$slice": 1_i32 } } },
            doc! { "$push": { "tags": { "$sort": 1_i32 } } },
            doc! { "$push": { "tags": { "$each": "x" } } },
            doc! { "$addToSet": { "tags": { "$each": "x" } } },
            doc! { "$pop": { "tags": 0_i32 } },
            doc! { "$pullAll": { "tags": "x" } },
            doc! { "$push": { "name": "x" } },
            doc! { "$pull": { "name": "Ada" } },
            doc! { "$pull": { "docs": { "name": { "$regex": "^A", "$options": "x" } } } },
            doc! { "$pull": { "docs": { "$or": [{ "name": "Ada" }], "$where": "bad" } } },
            doc! { "$pull": { "docs": { "$and": [] } } },
            doc! { "$pull": { "docs": { "$nor": [{ "name": "Ada" }, "bad"] } } },
            doc! { "$push": { "profile.tags": "x" } },
            doc! { "$push": { "tags.$": "x" } },
        ] {
            let response = update_documents(
                &conn,
                &doc! {
                    "update": "users",
                    "$db": "app",
                    "updates": [{ "q": { "_id": "u1" }, "u": update }],
                },
            )
            .unwrap();
            assert_eq!(response.get_f64("ok").unwrap(), 1.0);
            assert!(!write_errors(&response).is_empty());
        }

        let stored = first_batch(
            &find_documents(
                &conn,
                &doc! { "find": "users", "$db": "app", "filter": { "_id": "u1" } },
            )
            .unwrap(),
        );
        assert_eq!(
            stored[0].get_array("docs").unwrap(),
            &bson_documents(vec![doc! { "name": "Ada" }, doc! { "name": "Grace" }])
        );
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
    fn update_pipeline_subset_supports_update_find_and_modify_and_upsert() {
        let conn = test_conn();
        insert_documents(
            &conn,
            &doc! {
                "insert": "users",
                "$db": "app",
                "documents": [
                    { "_id": "u1", "active": true, "first": "Ada", "last": "Lovelace", "score": 2_i32 },
                    { "_id": "u2", "active": true, "first": "Grace", "last": "Hopper", "score": 3_i32 },
                ],
            },
        )
        .unwrap();

        let updated = update_documents(
            &conn,
            &doc! {
                "update": "users",
                "$db": "app",
                "updates": [{
                    "q": { "active": true },
                    "u": [
                        { "$set": {
                            "full": { "$concat": ["$first", " ", "$last"] },
                            "doubleScore": { "$multiply": ["$score", 2_i32] }
                        } },
                        { "$unset": "last" }
                    ],
                    "multi": true,
                }],
            },
        )
        .unwrap();
        assert_eq!(updated.get_i32("n").unwrap(), 2);
        assert_eq!(updated.get_i32("nModified").unwrap(), 2);
        let ada = first_batch(
            &find_documents(
                &conn,
                &doc! { "find": "users", "$db": "app", "filter": { "_id": "u1" } },
            )
            .unwrap(),
        )
        .remove(0);
        assert_eq!(ada.get_str("full").unwrap(), "Ada Lovelace");
        assert_eq!(ada.get_i64("doubleScore").unwrap(), 4);
        assert!(!ada.contains_key("last"));

        let modified = find_and_modify(
            &conn,
            "findAndModify",
            &doc! {
                "findAndModify": "users",
                "$db": "app",
                "query": { "_id": "u1" },
                "update": [
                    { "$project": { "_id": 1_i32, "full": 1_i32, "score": 1_i32 } },
                    { "$set": { "projected": true } }
                ],
                "new": true,
            },
        )
        .unwrap();
        let value = modified.get_document("value").unwrap();
        assert_eq!(value.get_str("full").unwrap(), "Ada Lovelace");
        assert!(value.get_bool("projected").unwrap());
        assert!(!value.contains_key("first"));

        let upsert = update_documents(
            &conn,
            &doc! {
                "update": "users",
                "$db": "app",
                "updates": [{
                    "q": { "_id": "u3", "first": "Katherine" },
                    "u": [
                        { "$set": { "full": { "$concat": ["$first", " Johnson"] }, "score": 5_i32 } }
                    ],
                    "upsert": true,
                }],
            },
        )
        .unwrap();
        assert_eq!(upsert.get_i32("nUpserted").unwrap(), 1);
        assert_eq!(
            first_batch(
                &find_documents(
                    &conn,
                    &doc! { "find": "users", "$db": "app", "filter": { "_id": "u3" } },
                )
                .unwrap()
            )[0]
            .get_str("full")
            .unwrap(),
            "Katherine Johnson"
        );

        let bad = update_documents(
            &conn,
            &doc! {
                "update": "users",
                "$db": "app",
                "updates": [{
                    "q": { "_id": "u2" },
                    "u": [{ "$set": { "_id": "changed" } }],
                }],
            },
        )
        .unwrap();
        assert_eq!(write_errors(&bad)[0].get_i32("code").unwrap(), 2);
        assert_eq!(find_ids(&conn, doc! { "_id": "u2" }), vec!["u2"]);
    }

    #[test]
    fn positional_update_subset_supports_first_all_and_array_filters() {
        let conn = test_conn();
        insert_documents(
            &conn,
            &doc! {
                "insert": "orders",
                "$db": "app",
                "documents": [{
                    "_id": "o1",
                    "items": [
                        { "kind": "open", "status": "new", "score": 1_i32 },
                        { "kind": "closed", "status": "done", "score": 5_i32 },
                        { "kind": "open", "status": "new", "score": 3_i32 }
                    ]
                }],
            },
        )
        .unwrap();

        let first = update_documents(
            &conn,
            &doc! {
                "update": "orders",
                "$db": "app",
                "updates": [{
                    "q": { "items.kind": "open" },
                    "u": { "$set": { "items.$.status": "working" }, "$inc": { "items.$.score": 2_i32 } },
                }],
            },
        )
        .unwrap();
        assert_eq!(first.get_i32("nModified").unwrap(), 1);

        let all = update_documents(
            &conn,
            &doc! {
                "update": "orders",
                "$db": "app",
                "updates": [{
                    "q": { "_id": "o1" },
                    "u": { "$mul": { "items.$[].score": 2_i32 } },
                }],
            },
        )
        .unwrap();
        assert_eq!(all.get_i32("nModified").unwrap(), 1);

        let filtered = update_documents(
            &conn,
            &doc! {
                "update": "orders",
                "$db": "app",
                "updates": [{
                    "q": { "_id": "o1" },
                    "u": { "$set": { "items.$[open].status": "closed" }, "$max": { "items.$[open].score": 10_i32 } },
                    "arrayFilters": [{ "open.kind": "open" }],
                }],
            },
        )
        .unwrap();
        assert_eq!(filtered.get_i32("nModified").unwrap(), 1);

        let stored = first_batch(
            &find_documents(
                &conn,
                &doc! { "find": "orders", "$db": "app", "filter": { "_id": "o1" } },
            )
            .unwrap(),
        )
        .remove(0);
        assert_eq!(
            stored.get_array("items").unwrap(),
            &bson_documents(vec![
                doc! { "kind": "open", "status": "closed", "score": 10_i32 },
                doc! { "kind": "closed", "status": "done", "score": 10_i32 },
                doc! { "kind": "open", "status": "closed", "score": 10_i32 },
            ])
        );

        let bad = update_documents(
            &conn,
            &doc! {
                "update": "orders",
                "$db": "app",
                "updates": [{
                    "q": { "_id": "o1" },
                    "u": { "$inc": { "items.$[open].status": 1_i32 } },
                    "arrayFilters": [{ "open.kind": "open" }],
                }],
            },
        )
        .unwrap();
        assert_eq!(write_errors(&bad)[0].get_i32("index").unwrap(), 0);
        assert_eq!(
            first_batch(
                &find_documents(
                    &conn,
                    &doc! { "find": "orders", "$db": "app", "filter": { "_id": "o1" } },
                )
                .unwrap()
            )[0]
            .get_array("items")
            .unwrap(),
            stored.get_array("items").unwrap()
        );
    }

    #[test]
    fn positional_update_binds_scalar_elem_match_predicates() {
        let conn = test_conn();
        insert_documents(
            &conn,
            &doc! {
                "insert": "users",
                "$db": "app",
                "documents": [{
                    "_id": "u1",
                    "scores": [1_i32, 5_i32, 7_i32, 11_i32],
                    "tags": ["Alpha", "BETA", "beta"],
                    "items": [
                        { "kind": "closed", "score": 9_i32, "status": "old" },
                        { "kind": "open", "score": 6_i32, "status": "new" },
                        { "kind": "open", "score": 3_i32, "status": "old" }
                    ]
                }],
            },
        )
        .unwrap();

        let numeric = update_documents(
            &conn,
            &doc! {
                "update": "users",
                "$db": "app",
                "updates": [{
                    "q": { "scores": { "$elemMatch": { "$gte": 5_i32, "$lt": 10_i32 } } },
                    "u": { "$set": { "scores.$": 99_i32 } },
                }],
            },
        )
        .unwrap();
        assert_eq!(numeric.get_i32("nModified").unwrap(), 1);

        let collated = update_documents(
            &conn,
            &doc! {
                "update": "users",
                "$db": "app",
                "updates": [{
                    "q": { "tags": { "$elemMatch": { "$eq": "beta" } } },
                    "u": { "$set": { "tags.$": "MATCH" } },
                    "collation": { "locale": "en", "strength": 2_i32 },
                }],
            },
        )
        .unwrap();
        assert_eq!(collated.get_i32("nModified").unwrap(), 1);

        let document = update_documents(
            &conn,
            &doc! {
                "update": "users",
                "$db": "app",
                "updates": [{
                    "q": { "items": { "$elemMatch": { "kind": "open", "score": { "$gte": 5_i32 } } } },
                    "u": { "$set": { "items.$.status": "working" } },
                }],
            },
        )
        .unwrap();
        assert_eq!(document.get_i32("nModified").unwrap(), 1);

        let stored = first_batch(
            &find_documents(
                &conn,
                &doc! { "find": "users", "$db": "app", "filter": { "_id": "u1" } },
            )
            .unwrap(),
        )
        .remove(0);
        assert_eq!(
            stored.get_array("scores").unwrap(),
            &bson_ints(&[1, 99, 7, 11])
        );
        assert_eq!(
            stored.get_array("tags").unwrap(),
            &bson_strings(&["Alpha", "MATCH", "beta"])
        );
        assert_eq!(
            stored.get_array("items").unwrap(),
            &bson_documents(vec![
                doc! { "kind": "closed", "score": 9_i32, "status": "old" },
                doc! { "kind": "open", "score": 6_i32, "status": "working" },
                doc! { "kind": "open", "score": 3_i32, "status": "old" },
            ])
        );
    }

    #[test]
    fn positional_scalar_elem_match_errors_preserve_documents() {
        let conn = test_conn();
        insert_documents(
            &conn,
            &doc! {
                "insert": "users",
                "$db": "app",
                "documents": [
                    { "_id": "u1", "scores": [1_i32, 5_i32, 7_i32] },
                    { "_id": "u2", "scores": [2_i32, 6_i32, 8_i32] },
                ],
            },
        )
        .unwrap();

        let before = first_batch(
            &find_documents(
                &conn,
                &doc! { "find": "users", "$db": "app", "sort": { "_id": 1_i32 } },
            )
            .unwrap(),
        );
        let bad_operator = update_documents(
            &conn,
            &doc! {
                "update": "users",
                "$db": "app",
                "updates": [{
                    "q": { "scores": { "$elemMatch": { "$where": "bad" } } },
                    "u": { "$set": { "scores.$": 99_i32 } },
                }],
            },
        )
        .unwrap();
        assert_eq!(write_errors(&bad_operator)[0].get_i32("index").unwrap(), 0);
        assert_eq!(
            first_batch(
                &find_documents(
                    &conn,
                    &doc! { "find": "users", "$db": "app", "sort": { "_id": 1_i32 } },
                )
                .unwrap()
            ),
            before
        );

        insert_documents(
            &conn,
            &doc! {
                "insert": "orders",
                "$db": "app",
                "documents": [
                    { "_id": "o1", "active": true, "items": [{ "status": "new" }] },
                    { "_id": "o2", "active": true, "items": ["scalar"] },
                ],
            },
        )
        .unwrap();
        let partial_before = first_batch(
            &find_documents(
                &conn,
                &doc! { "find": "orders", "$db": "app", "sort": { "_id": 1_i32 } },
            )
            .unwrap(),
        );
        let failed_multi = update_documents(
            &conn,
            &doc! {
                "update": "orders",
                "$db": "app",
                "updates": [{
                    "q": { "active": true, "items": { "$exists": true } },
                    "u": { "$set": { "items.$.status": "done" } },
                    "multi": true,
                }],
            },
        )
        .unwrap();
        assert_eq!(write_errors(&failed_multi)[0].get_i32("index").unwrap(), 0);
        assert_eq!(
            first_batch(
                &find_documents(
                    &conn,
                    &doc! { "find": "orders", "$db": "app", "sort": { "_id": 1_i32 } },
                )
                .unwrap()
            ),
            partial_before
        );
    }

    #[test]
    fn pipeline_and_positional_invariants_preserve_documents_indexes_validation_and_ttl() {
        let conn = test_conn();
        create_collection(
            &conn,
            &doc! {
                "create": "users",
                "$db": "app",
                "validator": { "$jsonSchema": { "bsonType": "object", "properties": { "score": { "bsonType": "int" } } } },
            },
        )
        .unwrap();
        insert_documents(
            &conn,
            &doc! {
                "insert": "users",
                "$db": "app",
                "documents": [
                    { "_id": "u1", "email": "a@example.test", "score": 4_i32, "den": 2_i32, "items": [{ "kind": "open", "tag": "old" }] },
                    { "_id": "u2", "email": "b@example.test", "score": 6_i32, "den": 0_i32, "items": [{ "kind": "open", "tag": "old" }] },
                ],
            },
        )
        .unwrap();
        create_indexes(
            &conn,
            &doc! {
                "createIndexes": "users",
                "$db": "app",
                "indexes": [
                    { "key": { "email": 1_i32 }, "name": "email_1", "unique": true },
                    { "key": { "items.tag": 1_i32 }, "name": "tag_1" },
                ],
            },
        )
        .unwrap();

        let runtime_error = update_documents(
            &conn,
            &doc! {
                "update": "users",
                "$db": "app",
                "updates": [{
                    "q": {},
                    "u": [{ "$set": { "ratio": { "$divide": ["$score", "$den"] } } }],
                    "multi": true,
                }],
            },
        )
        .unwrap();
        assert_eq!(write_errors(&runtime_error)[0].get_i32("index").unwrap(), 0);
        assert!(find_ids(&conn, doc! { "ratio": { "$exists": true } }).is_empty());

        let validation_error = update_documents(
            &conn,
            &doc! {
                "update": "users",
                "$db": "app",
                "updates": [{ "q": { "_id": "u1" }, "u": [{ "$set": { "score": "bad" } }] }],
            },
        )
        .unwrap();
        assert_eq!(
            write_errors(&validation_error)[0].get_i32("code").unwrap(),
            DOCUMENT_VALIDATION_ERROR_CODE
        );
        assert_eq!(
            first_batch(
                &find_documents(
                    &conn,
                    &doc! { "find": "users", "$db": "app", "filter": { "_id": "u1" } },
                )
                .unwrap()
            )[0]
            .get_i32("score")
            .unwrap(),
            4
        );

        let duplicate = update_documents(
            &conn,
            &doc! {
                "update": "users",
                "$db": "app",
                "updates": [{ "q": { "_id": "u2" }, "u": [{ "$set": { "email": "a@example.test" } }] }],
            },
        )
        .unwrap();
        assert_eq!(write_errors(&duplicate)[0].get_i32("code").unwrap(), 11000);
        assert_eq!(
            find_ids(&conn, doc! { "email": "b@example.test" }),
            vec!["u2"]
        );

        let positional = update_documents(
            &conn,
            &doc! {
                "update": "users",
                "$db": "app",
                "updates": [{
                    "q": { "_id": "u1" },
                    "u": { "$set": { "items.$[open].tag": "fresh" } },
                    "arrayFilters": [{ "open.kind": "open" }],
                }],
            },
        )
        .unwrap();
        assert_eq!(positional.get_i32("nModified").unwrap(), 1);
        assert_eq!(find_ids(&conn, doc! { "items.tag": "fresh" }), vec!["u1"]);
        assert!(find_ids(&conn, doc! { "items.tag": "old" }).contains(&"u2".to_string()));

        insert_documents(
            &conn,
            &doc! {
                "insert": "events",
                "$db": "app",
                "documents": [
                    { "_id": "expired", "expiresAt": bson::DateTime::from_millis(1_700_000_000_000_i64), "name": "old" },
                    { "_id": "live", "expiresAt": bson::DateTime::now(), "name": "new" },
                ],
            },
        )
        .unwrap();
        create_indexes(
            &conn,
            &doc! {
                "createIndexes": "events",
                "$db": "app",
                "indexes": [{ "key": { "expiresAt": 1_i32 }, "name": "expires_ttl", "expireAfterSeconds": 0_i32 }],
            },
        )
        .unwrap();
        let invalid = update_documents(
            &conn,
            &doc! {
                "update": "events",
                "$db": "app",
                "updates": [{ "q": { "_id": "live" }, "u": [{ "$lookup": { "from": "other" } }] }],
            },
        )
        .unwrap();
        assert_eq!(write_errors(&invalid)[0].get_i32("index").unwrap(), 0);
        assert_eq!(
            documents_for_namespace(&conn, "app.events").unwrap().len(),
            2
        );
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
    fn update_modifier_path_validation_rejects_protected_and_unsupported_positional_paths() {
        for update in [
            doc! { "$set": { "": 1_i32 } },
            doc! { "$set": { ".name": 1_i32 } },
            doc! { "$set": { "name.": 1_i32 } },
            doc! { "$set": { "$name": 1_i32 } },
            doc! { "$set": { "items.$[].$[bad].name": 1_i32 } },
            doc! { "$push": { "items.$.name": 1_i32 } },
            doc! { "$set": { "_id": "changed" } },
            doc! { "$set": { "_id.value": "changed" } },
            doc! { "$set": { "items.$._id": "changed" } },
            doc! { "$set": { "profile": {}, "profile.city": "Rome" } },
        ] {
            assert!(classify_update(&update).is_err(), "{update:?}");
        }

        assert!(classify_update(&doc! { "$set": { "items.$.name": 1_i32 } }).is_ok());

        for update in [
            doc! { "$bit": { "age": { "and": 1_i32 } } },
            doc! { "$currentDate": { "updatedAt": true } },
            doc! { "$setWindowFields": { "x": 1_i32 } },
        ] {
            let err = classify_update(&update).unwrap_err();
            assert!(err.contains("unsupported update operator"), "{err}");
        }
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
            doc! { "q": { "_id": "u1" }, "u": { "$bit": { "age": { "and": 1_i32 } } } },
            doc! { "q": { "_id": "u1" }, "u": { "$push": { "tags": { "$position": 0_i32 } } } },
            doc! { "q": { "_id": "u1" }, "u": { "$pullAll": { "tags": "x" } } },
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
            doc! { "q": { "_id": "u1" }, "limit": 1_i32, "hint": "missing_1" },
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
