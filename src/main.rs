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
    conn.execute_batch(
        r#"
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

    while let Some(message) = read_wire_message(&mut stream).await? {
        let response = handle_wire_message(&conn, message)?;
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

fn handle_wire_message(conn: &Connection, message: WireMessage) -> Result<Vec<u8>> {
    match message.opcode {
        OP_MSG => handle_op_msg(conn, message),
        OP_QUERY => handle_op_query(conn, message),
        opcode => build_op_msg_response(
            message.request_id,
            command_error(59, &format!("unsupported opcode {opcode}")),
        ),
    }
}

fn handle_op_msg(conn: &Connection, message: WireMessage) -> Result<Vec<u8>> {
    let command = parse_op_msg_document(&message.payload)?;
    let response = handle_command(conn, &command)?;
    build_op_msg_response(message.request_id, response)
}

fn handle_op_query(conn: &Connection, message: WireMessage) -> Result<Vec<u8>> {
    let (full_collection_name, query) = parse_op_query(&message.payload)?;
    let db_name = full_collection_name
        .split_once('.')
        .map(|(db, _)| db)
        .unwrap_or("admin");
    let mut command = query;
    command.insert("$db", db_name);
    let response = handle_command(conn, &command)?;
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

    while offset < payload.len() {
        let section_kind = payload[offset];
        offset += 1;

        match section_kind {
            0 => {
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
                offset += size;
            }
            other => {
                return Err(MongolinoError::Protocol(format!(
                    "unsupported OP_MSG section kind {other}"
                )));
            }
        }
    }

    body.ok_or_else(|| MongolinoError::Protocol("OP_MSG body section missing".to_string()))
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

fn handle_command(conn: &Connection, command: &Document) -> Result<Document> {
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
        "insert" => insert_documents(conn, command),
        "find" => find_documents(conn, command),
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
        "SELECT DISTINCT substr(namespace, 1, instr(namespace, '.') - 1) FROM documents ORDER BY 1",
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

fn insert_documents(conn: &Connection, command: &Document) -> Result<Document> {
    let db = command.get_str("$db").unwrap_or("test");
    let collection = command.get_str("insert").map_err(|_| {
        MongolinoError::Protocol("insert command requires a collection name".to_string())
    })?;
    let documents = command.get_array("documents").map_err(|_| {
        MongolinoError::Protocol("insert command requires a documents array".to_string())
    })?;
    let namespace = namespace(db, collection);

    let tx = conn.unchecked_transaction()?;
    let mut inserted = 0_i32;

    {
        let mut stmt = tx.prepare(
            "INSERT OR REPLACE INTO documents(namespace, id_key, bson, updated_at)
             VALUES (?1, ?2, ?3, CURRENT_TIMESTAMP)",
        )?;

        for value in documents {
            let Bson::Document(original) = value else {
                return Ok(command_error(2, "insert documents must be BSON documents"));
            };
            let mut document = original.clone();
            ensure_document_id(&mut document);

            let id_key = id_key(&document)?;
            let mut encoded = Vec::new();
            document.to_writer(&mut encoded)?;
            stmt.execute(params![namespace, id_key, encoded])?;
            inserted += 1;
        }
    }

    tx.commit()?;
    Ok(doc! {
        "n": inserted,
        "ok": 1.0,
    })
}

fn find_documents(conn: &Connection, command: &Document) -> Result<Document> {
    let db = command.get_str("$db").unwrap_or("test");
    let collection = command
        .get_str("find")
        .map_err(|_| MongolinoError::Protocol("find command requires a collection".to_string()))?;
    let filter = command.get_document("filter").ok();
    let batch_size = command
        .get_i32("batchSize")
        .or_else(|_| command.get_i64("batchSize").map(|value| value as i32))
        .unwrap_or(101)
        .clamp(1, 1000);
    let namespace = namespace(db, collection);

    if let Some(id_filter) = filter.and_then(|filter| filter.get("_id")) {
        let wanted_id = id_key_from_bson(id_filter);
        if let Some(document) = conn
            .query_row(
                "SELECT bson FROM documents WHERE namespace = ?1 AND id_key = ?2",
                params![namespace, wanted_id],
                |row| row.get::<_, Vec<u8>>(0),
            )
            .optional()?
            .map(decode_document)
            .transpose()?
        {
            return Ok(cursor_response(db, collection, vec![document]));
        }
        return Ok(cursor_response(db, collection, vec![]));
    }

    let mut stmt = conn
        .prepare("SELECT bson FROM documents WHERE namespace = ?1 ORDER BY created_at LIMIT ?2")?;
    let docs = stmt
        .query_map(params![namespace, batch_size], |row| {
            row.get::<_, Vec<u8>>(0)
        })?
        .map(|row| decode_document(row?))
        .collect::<Result<Vec<_>>>()?;

    Ok(cursor_response(db, collection, docs))
}

fn cursor_response(db: &str, collection: &str, documents: Vec<Document>) -> Document {
    doc! {
        "cursor": {
            "id": 0_i64,
            "ns": namespace(db, collection),
            "firstBatch": documents.into_iter().map(Bson::Document).collect::<Vec<_>>(),
        },
        "ok": 1.0,
    }
}

fn decode_document(bytes: Vec<u8>) -> Result<Document> {
    Ok(Document::from_reader(&mut Cursor::new(bytes))?)
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
    fn insert_and_find_roundtrip_through_sqlite() {
        let conn = Connection::open_in_memory().unwrap();
        init_connection(&conn).unwrap();
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
        let cursor = find_response.get_document("cursor").unwrap();
        let batch = cursor.get_array("firstBatch").unwrap();
        assert_eq!(batch.len(), 1);
        assert_eq!(
            batch[0].as_document().unwrap().get_str("name").unwrap(),
            "Grace"
        );
    }
}
