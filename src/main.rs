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
        prepared.push((id_key, encoded));
    }

    let tx = conn.unchecked_transaction()?;
    let mut inserted = 0_i32;
    let mut write_errors = Vec::new();

    {
        let mut stmt = tx.prepare(
            "INSERT INTO documents(namespace, id_key, bson, updated_at)
             VALUES (?1, ?2, ?3, CURRENT_TIMESTAMP)",
        )?;

        for (index, (id_key, encoded)) in prepared.iter().enumerate() {
            match stmt.execute(params![namespace, id_key, encoded]) {
                Ok(_) => inserted += 1,
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

fn find_documents(conn: &Connection, command: &Document) -> Result<Document> {
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
    let batch_size = command
        .get_i32("batchSize")
        .or_else(|_| command.get_i64("batchSize").map(|value| value as i32))
        .unwrap_or(101)
        .clamp(1, 1000);
    let namespace = namespace(db, collection);

    if let Some(id_filter) = simple_id_equality_filter(&filter) {
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

    let mut stmt =
        conn.prepare("SELECT bson FROM documents WHERE namespace = ?1 ORDER BY created_at")?;
    let mut docs = Vec::new();
    for row in stmt.query_map(params![namespace], |row| row.get::<_, Vec<u8>>(0))? {
        let document = decode_document(row?)?;
        match matches_filter(&document, &filter) {
            Ok(true) => docs.push(document),
            Ok(false) => {}
            Err(err) => return Ok(command_error(err.code, &err.errmsg)),
        }
        if docs.len() >= batch_size as usize {
            break;
        }
    }

    Ok(cursor_response(db, collection, docs))
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
        _ => Err(match_error(
            2,
            format!("unsupported top-level query operator {operator}"),
        )),
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

    fn test_conn() -> Connection {
        let conn = Connection::open_in_memory().unwrap();
        init_connection(&conn).unwrap();
        conn
    }

    fn first_batch(response: &Document) -> Vec<Document> {
        response
            .get_document("cursor")
            .unwrap()
            .get_array("firstBatch")
            .unwrap()
            .iter()
            .map(|value| value.as_document().unwrap().clone())
            .collect()
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
    fn empty_and_unknown_commands_are_command_errors() {
        let conn = test_conn();

        let empty = handle_command(&conn, &doc! {}).unwrap();
        assert_command_error(&empty);
        assert!(empty.get_str("errmsg").unwrap().contains("empty command"));

        let unknown = handle_command(&conn, &doc! { "drop": "users", "$db": "app" }).unwrap();
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
}
