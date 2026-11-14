//! `celld d1 export`: a database as SQL that replays into an empty database.
//!
//! The dump reads one snapshot. The caller opens a read transaction, and the
//! schema and every table are read inside it, so a write that lands while the
//! dump runs appears in none of it. The SQL it writes runs under the same
//! limits a D1 database applies to any statement, and the same authorizer, so
//! `celld d1 execute --file dump.sql` replays it.
//!
//! What it leaves out, and why:
//! - celld's own tables (`_cf_*`, the replicator's control tables) and SQLite's
//!   (`sqlite_*`): they belong to the cell, not to the application.
//! - Shadow tables of virtual tables. A regular `fts5` table is replayed
//!   through the virtual table, an external-content one is rebuilt from its
//!   content table after the data, and a `vec0` table is replayed through the
//!   virtual table. A contentless `fts5` table stores no content to dump.
//! - Generated columns, which SQLite computes again.

use rusqlite::types::ValueRef;
use rusqlite::Connection;

/// The most SQL one export returns.
pub const EXPORT_MAX_BYTES: usize = 32 * 1024 * 1024;

/// The longest statement a replay accepts: the limit the cell sets on the
/// connection. A row that needs a longer INSERT cannot be replayed.
pub const STATEMENT_MAX_BYTES: usize = 100_000;

#[derive(Clone, Debug)]
pub struct ExportOptions {
    /// Only these tables, with their indexes and triggers. Empty means all.
    pub tables: Vec<String>,
    pub schema: bool,
    pub data: bool,
}

impl Default for ExportOptions {
    fn default() -> Self {
        Self {
            tables: Vec::new(),
            schema: true,
            data: true,
        }
    }
}

struct Object {
    kind: String,
    name: String,
    table: String,
    sql: String,
}

struct Column {
    name: String,
    declared_type: String,
    primary_key: i64,
    hidden: i64,
}

struct Out {
    text: String,
    max: usize,
}

impl Out {
    fn push(&mut self, statement: &str) -> Result<(), String> {
        if self.text.len() + statement.len() + 2 > self.max {
            return Err(format!(
                "the export is larger than {} bytes; export fewer tables at a time \
                 with --table",
                self.max
            ));
        }
        self.text.push_str(statement);
        self.text.push_str(";\n");
        Ok(())
    }
}

fn error(error: rusqlite::Error) -> String {
    error.to_string()
}

fn quote_identifier(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

fn quote_text(text: &str) -> String {
    format!("'{}'", text.replace('\'', "''"))
}

/// A name that belongs to the cell and not to the application.
fn is_internal(name: &str) -> bool {
    let lower = name.to_ascii_lowercase();
    lower.starts_with("sqlite_")
        || lower.starts_with("_cf_")
        || celld_ltx::db::is_control_table(name)
}

/// A SQL literal that reads back as exactly the stored value.
///
/// A TEXT value that holds a NUL byte or is not valid UTF-8 cannot be written
/// as a quoted literal, because SQL text ends at the first NUL and the parser
/// rejects the rest. It is cast from a blob instead, which keeps every byte.
fn literal(value: ValueRef<'_>) -> String {
    match value {
        ValueRef::Null => "NULL".to_string(),
        ValueRef::Integer(number) => number.to_string(),
        ValueRef::Real(number) if number.is_nan() => "NULL".to_string(),
        ValueRef::Real(number) if number.is_infinite() => {
            if number > 0.0 { "9e999" } else { "-9e999" }.to_string()
        }
        ValueRef::Real(number) => format!("{number:?}"),
        ValueRef::Text(bytes) => match std::str::from_utf8(bytes) {
            Ok(text) if !text.contains('\0') => quote_text(text),
            _ => format!("CAST({} AS TEXT)", hex(bytes)),
        },
        ValueRef::Blob(bytes) => hex(bytes),
    }
}

fn hex(bytes: &[u8]) -> String {
    use std::fmt::Write as _;
    let mut text = String::with_capacity(bytes.len() * 2 + 3);
    text.push_str("X'");
    for byte in bytes {
        let _ = write!(text, "{byte:02x}");
    }
    text.push('\'');
    text
}

/// The module of a `CREATE VIRTUAL TABLE ... USING module(...)` statement.
fn virtual_module(sql: &str) -> Option<String> {
    let lower = sql.to_ascii_lowercase();
    let after = &sql[lower.find(" using ")? + " using ".len()..];
    let module: String = after
        .trim_start()
        .chars()
        .take_while(|c| c.is_ascii_alphanumeric() || *c == '_')
        .collect();
    (!module.is_empty()).then(|| module.to_ascii_lowercase())
}

#[derive(Debug, PartialEq, Eq)]
enum Fts5Content {
    Regular,
    External,
    Contentless,
}

/// How an `fts5` table keeps its content, from its `content` option.
fn fts5_content(sql: &str) -> Fts5Content {
    let Some(open) = sql.find('(') else {
        return Fts5Content::Regular;
    };
    let body = &sql[open + 1..];
    let (mut depth, mut quote, mut start) = (0_i32, None::<char>, 0);
    let mut arguments = Vec::new();
    for (index, c) in body.char_indices() {
        match (quote, c) {
            (Some(q), c) if c == q => quote = None,
            (Some(_), _) => {}
            (None, '\'' | '"' | '`') => quote = Some(c),
            (None, '(') => depth += 1,
            (None, ')') if depth == 0 => {
                arguments.push(&body[start..index]);
                break;
            }
            (None, ')') => depth -= 1,
            (None, ',') if depth == 0 => {
                arguments.push(&body[start..index]);
                start = index + 1;
            }
            _ => {}
        }
    }
    for argument in arguments {
        let Some((name, value)) = argument.split_once('=') else {
            continue;
        };
        if name.trim().eq_ignore_ascii_case("content") {
            let value = value.trim().trim_matches(|c| matches!(c, '\'' | '"' | '`'));
            return if value.is_empty() {
                Fts5Content::Contentless
            } else {
                Fts5Content::External
            };
        }
    }
    Fts5Content::Regular
}

fn columns(connection: &Connection, table: &str) -> Result<Vec<Column>, String> {
    let mut statement = connection
        .prepare(&format!("PRAGMA table_xinfo({})", quote_identifier(table)))
        .map_err(error)?;
    let rows = statement
        .query_map([], |row| {
            Ok(Column {
                name: row.get(1)?,
                declared_type: row.get(2)?,
                primary_key: row.get(5)?,
                hidden: row.get(6)?,
            })
        })
        .map_err(error)?;
    rows.collect::<Result<_, _>>().map_err(error)
}

/// Export `connection`'s main database as SQL.
///
/// The caller must have a read transaction open, so that the schema and the
/// data come from one snapshot. `between` runs before the rows of each table
/// are read, which is where a test writes from another connection.
pub fn export(
    connection: &Connection,
    options: &ExportOptions,
    max_bytes: usize,
    mut between: impl FnMut(&str),
) -> Result<String, String> {
    if connection.is_autocommit() {
        return Err(
            "an export needs a read transaction, so that its schema and data \
                    come from one snapshot"
                .to_string(),
        );
    }
    if !options.schema && !options.data {
        return Err("--no-schema and --no-data leave nothing to export".to_string());
    }
    let objects: Vec<Object> = {
        let mut statement = connection
            .prepare(
                "SELECT type, name, tbl_name, sql FROM sqlite_schema \
                 WHERE sql IS NOT NULL ORDER BY rowid",
            )
            .map_err(error)?;
        let rows = statement
            .query_map([], |row| {
                Ok(Object {
                    kind: row.get(0)?,
                    name: row.get(1)?,
                    table: row.get(2)?,
                    sql: row.get(3)?,
                })
            })
            .map_err(error)?;
        rows.collect::<Result<_, _>>().map_err(error)?
    };
    let table_kinds: std::collections::HashMap<String, (String, bool)> = {
        let mut statement = connection.prepare("PRAGMA table_list").map_err(error)?;
        let rows = statement
            .query_map([], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, i64>(4)?,
                ))
            })
            .map_err(error)?;
        let mut kinds = std::collections::HashMap::new();
        for row in rows {
            let (schema, name, kind, without_rowid) = row.map_err(error)?;
            if schema == "main" {
                kinds.insert(name, (kind, without_rowid != 0));
            }
        }
        kinds
    };

    // The tables a vec0 table keeps its data in. SQLite does not report all of
    // them as shadow tables, so they are told apart by name: the table's own
    // name and one of the suffixes vec0 creates.
    let vec0_tables: Vec<String> = objects
        .iter()
        .filter(|object| {
            object.kind == "table" && virtual_module(&object.sql).as_deref() == Some("vec0")
        })
        .map(|object| object.name.to_ascii_lowercase())
        .collect();
    let is_vec0_shadow = |name: &str| {
        let lower = name.to_ascii_lowercase();
        vec0_tables.iter().any(|owner| {
            lower.strip_prefix(owner.as_str()).is_some_and(|rest| {
                rest.strip_prefix('_').is_some_and(|suffix| {
                    ["chunks", "rowids", "info", "auxiliary"].contains(&suffix)
                        || ["vector_chunks", "metadatachunks", "metadatatext"]
                            .iter()
                            .any(|prefix| suffix.starts_with(prefix))
                })
            })
        })
    };

    // Every table that could be exported, in the order it was created.
    let candidates: Vec<&Object> = objects
        .iter()
        .filter(|object| object.kind == "table")
        .filter(|object| !is_internal(&object.name))
        .filter(|object| !is_vec0_shadow(&object.name))
        .filter(|object| {
            table_kinds
                .get(&object.name)
                .is_none_or(|(kind, _)| kind != "shadow")
        })
        .collect();
    let mut chosen: Vec<&Object> = Vec::new();
    if options.tables.is_empty() {
        chosen = candidates;
    } else {
        for wanted in &options.tables {
            let Some(found) = candidates
                .iter()
                .find(|object| object.name.eq_ignore_ascii_case(wanted))
            else {
                return Err(if is_internal(wanted) {
                    format!("{wanted} is not a table of the application")
                } else {
                    format!("no such table: {wanted}")
                });
            };
            if !chosen.iter().any(|object| std::ptr::eq(*object, *found)) {
                chosen.push(found);
            }
        }
        // The creation order, not the order the operator named them in: a
        // virtual table's rebuild and a trigger's body depend on it.
        chosen.sort_by_key(|object| {
            objects
                .iter()
                .position(|candidate| std::ptr::eq(candidate, *object))
        });
    }
    let included = |name: &str| chosen.iter().any(|object| object.name == name);

    let mut counters = std::collections::HashMap::new();
    if objects
        .iter()
        .any(|object| object.name == "sqlite_sequence")
    {
        let mut statement = connection
            .prepare("SELECT name, seq FROM sqlite_sequence")
            .map_err(error)?;
        let rows = statement
            .query_map([], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?))
            })
            .map_err(error)?;
        for row in rows {
            let (name, seq) = row.map_err(error)?;
            counters.insert(name, seq);
        }
    }

    let mut out = Out {
        text: String::new(),
        max: max_bytes,
    };
    if options.data {
        // The rows arrive in table order, not in dependency order.
        out.push("PRAGMA defer_foreign_keys=TRUE")?;
    }
    if options.schema {
        for object in &chosen {
            out.push(&object.sql)?;
        }
    }
    let mut rebuild = Vec::new();
    if options.data {
        for object in &chosen {
            between(&object.name);
            let (kind, without_rowid) = table_kinds.get(&object.name).cloned().unwrap_or_default();
            let is_virtual = kind == "virtual";
            let module = virtual_module(&object.sql).filter(|_| is_virtual);
            match module.as_deref() {
                Some("fts5vocab") => continue,
                Some("fts5") => match fts5_content(&object.sql) {
                    Fts5Content::Regular => {}
                    Fts5Content::External => {
                        rebuild.push(&object.name);
                        continue;
                    }
                    Fts5Content::Contentless => {
                        out.text.push_str(&format!(
                            "-- {} is a contentless fts5 table; it stores no content to export\n",
                            quote_identifier(&object.name).replace('\n', " ")
                        ));
                        continue;
                    }
                },
                _ => {}
            }
            dump_rows(
                connection,
                &mut out,
                &object.name,
                is_virtual,
                without_rowid,
            )?;
            if let Some(seq) = counters.get(&object.name) {
                // Scoped to this table's counter. A bare DELETE would reset the
                // counters of tables this dump does not carry.
                out.push(&format!(
                    "DELETE FROM sqlite_sequence WHERE name = {}",
                    quote_text(&object.name)
                ))?;
                out.push(&format!(
                    "INSERT INTO sqlite_sequence (name, seq) VALUES ({}, {seq})",
                    quote_text(&object.name)
                ))?;
            }
        }
    }
    if options.schema {
        // After the data, so loading a table fires no trigger and maintains no
        // index. Views are part of the whole database only: a view of a table
        // the dump leaves out would not replay.
        for object in &objects {
            let wanted = match object.kind.as_str() {
                "index" | "trigger" => included(&object.table),
                "view" => options.tables.is_empty() && !is_internal(&object.name),
                _ => false,
            };
            if wanted && !is_internal(&object.name) {
                out.push(&object.sql)?;
            }
        }
    }
    for name in rebuild {
        let quoted = quote_identifier(name);
        out.push(&format!("INSERT INTO {quoted}({quoted}) VALUES('rebuild')"))?;
    }
    Ok(out.text)
}

fn dump_rows(
    connection: &Connection,
    out: &mut Out,
    table: &str,
    is_virtual: bool,
    without_rowid: bool,
) -> Result<(), String> {
    let all = columns(connection, table)?;
    // A virtual table may list `rowid` as a column (vec0 does); it is read as
    // the rowid below, so it must not be inserted twice.
    let stored: Vec<&Column> = all
        .iter()
        .filter(|column| column.hidden == 0)
        .filter(|column| !(is_virtual && column.name.eq_ignore_ascii_case("rowid")))
        .collect();
    // A table with no `INTEGER PRIMARY KEY` hands out its rowids itself, and a
    // replay would number them again from 1. Carrying the rowid keeps every row
    // where it was, which matters when something else refers to it.
    let alias = {
        let keys: Vec<&&Column> = stored.iter().filter(|c| c.primary_key > 0).collect();
        keys.len() == 1 && keys[0].declared_type.eq_ignore_ascii_case("INTEGER")
    };
    let rowid = if is_virtual {
        Some("rowid")
    } else if without_rowid || alias {
        None
    } else {
        ["rowid", "oid", "_rowid_"].into_iter().find(|candidate| {
            !all.iter()
                .any(|column| column.name.eq_ignore_ascii_case(candidate))
        })
    };
    let mut names: Vec<String> = Vec::new();
    if let Some(rowid) = rowid {
        names.push(rowid.to_string());
    }
    names.extend(stored.iter().map(|column| quote_identifier(&column.name)));
    let order = if without_rowid {
        let mut keys: Vec<&&Column> = stored.iter().filter(|c| c.primary_key > 0).collect();
        keys.sort_by_key(|column| column.primary_key);
        keys.iter()
            .map(|column| quote_identifier(&column.name))
            .collect::<Vec<_>>()
            .join(", ")
    } else if let Some(rowid) = rowid {
        rowid.to_string()
    } else if alias {
        quote_identifier(
            &stored
                .iter()
                .find(|column| column.primary_key > 0)
                .map(|column| column.name.clone())
                .unwrap_or_default(),
        )
    } else {
        String::new()
    };
    let verb = format!("INSERT INTO {}", quote_identifier(table));
    let head = if names.is_empty() {
        format!("{verb} DEFAULT VALUES")
    } else {
        format!("{verb} ({}) VALUES (", names.join(", "))
    };
    let mut select = if names.is_empty() {
        format!("SELECT 1 FROM {}", quote_identifier(table))
    } else {
        format!(
            "SELECT {} FROM {}",
            names.join(", "),
            quote_identifier(table)
        )
    };
    if !order.is_empty() {
        select.push_str(" ORDER BY ");
        select.push_str(&order);
    }
    let mut statement = connection.prepare(&select).map_err(error)?;
    let width = names.len();
    let mut rows = statement.query([]).map_err(error)?;
    let mut ordinal = 0_u64;
    while let Some(row) = rows.next().map_err(error)? {
        ordinal += 1;
        let line = if width == 0 {
            head.clone()
        } else {
            let mut values = Vec::with_capacity(width);
            for index in 0..width {
                values.push(literal(row.get_ref(index).map_err(error)?));
            }
            format!("{head}{})", values.join(", "))
        };
        if line.len() + 1 > STATEMENT_MAX_BYTES {
            return Err(format!(
                "row {ordinal} of table {table} needs a {}-byte INSERT, and a replay \
                 accepts statements of at most {STATEMENT_MAX_BYTES} bytes; leave \
                 the table out of the export, or shrink the row",
                line.len() + 1
            ));
        }
        out.push(&line)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    const SCHEMA: &str = r#"
        CREATE TABLE notes (id INTEGER PRIMARY KEY AUTOINCREMENT, body TEXT, score REAL, raw BLOB);
        CREATE TABLE tags (id INTEGER PRIMARY KEY AUTOINCREMENT, note_id INTEGER REFERENCES notes(id), tag TEXT);
        CREATE TABLE loose (a TEXT, b INTEGER);
        CREATE TABLE keyed (k TEXT, n INTEGER, PRIMARY KEY (k, n)) WITHOUT ROWID;
        CREATE TABLE area (
            w INTEGER, h INTEGER,
            virt INTEGER GENERATED ALWAYS AS (w * h) VIRTUAL,
            stored INTEGER GENERATED ALWAYS AS (w + h) STORED
        );
        CREATE TABLE "odd ""name" ("col ""x" TEXT);
        CREATE VIRTUAL TABLE docs USING fts5(title, body);
        CREATE TABLE articles (id INTEGER PRIMARY KEY, headline TEXT);
        CREATE VIRTUAL TABLE articles_fts USING fts5(headline, content='articles', content_rowid='id');
        CREATE VIRTUAL TABLE docs_vocab USING fts5vocab(docs, row);
        CREATE INDEX notes_body ON notes (body);
        CREATE TRIGGER notes_tag AFTER INSERT ON notes BEGIN
            INSERT INTO tags (note_id, tag) VALUES (new.id, 'auto');
        END;
        CREATE VIEW note_bodies AS SELECT id, body FROM notes;
    "#;

    fn populate(connection: &Connection) {
        connection.execute_batch(SCHEMA).unwrap();
        // The trigger is created last, so these rows and their tags are plain data.
        connection
            .execute_batch(
                "DROP TRIGGER notes_tag;
                 INSERT INTO notes (body, score, raw) VALUES ('it''s', 1.5, X'00ff10');
                 INSERT INTO notes (body, score, raw) VALUES ('line' || char(10) || 'two', -0.0, NULL);
                 INSERT INTO notes (body, score, raw) VALUES (NULL, 1e300, X'');
                 INSERT INTO notes (body, score, raw) VALUES ('gone', 9e999, NULL);
                 DELETE FROM notes WHERE body = 'gone';
                 INSERT INTO tags (note_id, tag) VALUES (1, 'a'), (2, 'b');
                 INSERT INTO loose VALUES ('x', 1), ('y', 2), ('z', 3);
                 DELETE FROM loose WHERE a = 'y';
                 INSERT INTO keyed VALUES ('b', 2), ('a', 9), ('a', 1);
                 INSERT INTO area (w, h) VALUES (3, 4), (5, 6);
                 INSERT INTO \"odd \"\"name\" VALUES ('q');
                 INSERT INTO docs (title, body) VALUES ('Moby', 'call me ishmael'), ('Emma', 'woodhouse');
                 INSERT INTO articles VALUES (7, 'rust wins'), (9, 'go loses');
                 INSERT INTO articles_fts(articles_fts) VALUES ('rebuild');
                 CREATE TRIGGER notes_tag AFTER INSERT ON notes BEGIN
                     INSERT INTO tags (note_id, tag) VALUES (new.id, 'auto');
                 END;",
            )
            .unwrap();
        // TEXT that is not a valid literal: a NUL, and bytes that are not UTF-8.
        connection
            .execute(
                "INSERT INTO loose VALUES (CAST(X'6100620063' AS TEXT), 4)",
                [],
            )
            .unwrap();
        connection
            .execute("INSERT INTO loose VALUES (CAST(X'fffe41' AS TEXT), 5)", [])
            .unwrap();
    }

    fn dump(connection: &Connection, options: &ExportOptions) -> Result<String, String> {
        connection.execute_batch("BEGIN").unwrap();
        let sql = export(connection, options, EXPORT_MAX_BYTES, |_| {});
        connection.execute_batch("ROLLBACK").unwrap();
        sql
    }

    fn all(connection: &Connection) -> String {
        dump(connection, &ExportOptions::default()).unwrap()
    }

    /// Every stored value as its type and bytes, so a NUL or a bad byte that a
    /// text comparison would hide shows up.
    fn bytes(connection: &Connection, query: &str) -> Vec<String> {
        let mut statement = connection.prepare(query).unwrap();
        let rows = statement
            .query_map([], |row| {
                let mut cells = Vec::new();
                for index in 0..row.as_ref().column_count() {
                    cells.push(match row.get_ref(index)? {
                        ValueRef::Null => "null".to_string(),
                        ValueRef::Integer(n) => format!("i{n}"),
                        ValueRef::Real(n) => format!("r{:?}", n),
                        ValueRef::Text(b) => format!("t{}", hex(b)),
                        ValueRef::Blob(b) => format!("b{}", hex(b)),
                    });
                }
                Ok(cells.join("|"))
            })
            .unwrap();
        rows.map(Result::unwrap).collect()
    }

    #[test]
    fn a_dump_replays_into_an_identical_database() {
        let source = Connection::open_in_memory().unwrap();
        populate(&source);
        let sql = all(&source);

        let replica = Connection::open_in_memory().unwrap();
        replica
            .execute_batch(&sql)
            .unwrap_or_else(|e| panic!("{e}\n{sql}"));

        // Replaying must reproduce what the dump itself reads, byte for byte.
        assert_eq!(all(&replica), sql);
        for query in [
            "SELECT id, body, score, raw FROM notes ORDER BY id",
            "SELECT rowid, a, b FROM loose ORDER BY rowid",
            "SELECT k, n FROM keyed ORDER BY k, n",
            "SELECT w, h, virt, stored FROM area ORDER BY w",
            "SELECT * FROM \"odd \"\"name\"",
            "SELECT note_id, tag FROM tags ORDER BY id",
            "SELECT name, seq FROM sqlite_sequence ORDER BY name",
            "SELECT rowid, title, body FROM docs ORDER BY rowid",
            "SELECT rowid, headline FROM articles_fts ORDER BY rowid",
            "SELECT id, body FROM note_bodies ORDER BY id",
            "SELECT type, name FROM sqlite_schema ORDER BY name",
        ] {
            assert_eq!(bytes(&replica, query), bytes(&source, query), "{query}");
        }
    }

    #[test]
    fn text_with_a_nul_or_bad_utf8_keeps_every_byte() {
        let source = Connection::open_in_memory().unwrap();
        populate(&source);
        let replica = Connection::open_in_memory().unwrap();
        replica.execute_batch(&all(&source)).unwrap();
        // The NUL is in the stored bytes, and the value is still TEXT.
        assert_eq!(
            bytes(
                &replica,
                "SELECT hex(CAST(a AS BLOB)), typeof(a) FROM loose WHERE b = 4"
            ),
            vec![format!("t{}|t{}", hex(b"6100620063"), hex(b"text"))]
        );
        assert_eq!(
            bytes(
                &replica,
                "SELECT hex(CAST(a AS BLOB)) FROM loose WHERE b = 5"
            ),
            vec![format!("t{}", hex(b"FFFE41"))]
        );
    }

    #[test]
    fn generated_columns_are_computed_again_and_not_inserted() {
        let source = Connection::open_in_memory().unwrap();
        populate(&source);
        let sql = all(&source);
        assert!(
            sql.contains("(rowid, \"w\", \"h\") VALUES (1, 3, 4)"),
            "{sql}"
        );
        assert!(!sql.contains("\"virt\", \"stored\""), "{sql}");
    }

    #[test]
    fn an_fts5_table_replays_without_its_shadow_tables() {
        let source = Connection::open_in_memory().unwrap();
        populate(&source);
        let sql = all(&source);
        for shadow in [
            "docs_data",
            "docs_idx",
            "docs_content",
            "docs_docsize",
            "docs_config",
        ] {
            assert!(!sql.contains(shadow), "{shadow} leaked into the dump");
        }
        assert!(sql.contains("INSERT INTO \"articles_fts\"(\"articles_fts\") VALUES('rebuild')"));
        let replica = Connection::open_in_memory().unwrap();
        replica.execute_batch(&sql).unwrap();
        let hits: Vec<String> = bytes(
            &replica,
            "SELECT rowid FROM docs WHERE docs MATCH 'ishmael'",
        );
        assert_eq!(hits, vec!["i1".to_string()]);
        let external = bytes(
            &replica,
            "SELECT rowid FROM articles_fts WHERE articles_fts MATCH 'rust'",
        );
        assert_eq!(external, vec!["i7".to_string()]);
    }

    #[test]
    fn rowids_of_a_table_without_a_key_survive_deleted_rows() {
        let source = Connection::open_in_memory().unwrap();
        populate(&source);
        let replica = Connection::open_in_memory().unwrap();
        replica.execute_batch(&all(&source)).unwrap();
        let ids = bytes(&replica, "SELECT rowid FROM loose ORDER BY rowid");
        // 'y' (rowid 2) was deleted, and the rows after it stayed where they were.
        assert_eq!(ids, vec!["i1", "i3", "i4", "i5"]);
    }

    #[test]
    fn the_counters_of_a_filtered_dump_are_scoped_to_its_own_tables() {
        let source = Connection::open_in_memory().unwrap();
        populate(&source);
        for options in [
            ExportOptions {
                tables: vec!["notes".into()],
                ..ExportOptions::default()
            },
            ExportOptions {
                tables: vec!["notes".into()],
                schema: false,
                ..ExportOptions::default()
            },
            ExportOptions {
                schema: false,
                ..ExportOptions::default()
            },
        ] {
            let sql = dump(&source, &options).unwrap();
            assert!(
                !sql.lines()
                    .any(|line| line.trim_end_matches(';') == "DELETE FROM sqlite_sequence"),
                "{sql}"
            );
            if !options.tables.is_empty() {
                assert!(!sql.contains("'tags'"), "{sql}");
            }
        }

        // Replay a data-only dump of `notes` over a database whose `tags`
        // counter has moved on: that counter must not change.
        let target = Connection::open_in_memory().unwrap();
        target.pragma_update(None, "foreign_keys", false).unwrap();
        target.execute_batch(SCHEMA).unwrap();
        target
            .execute_batch(
                "DROP TRIGGER notes_tag;
                 INSERT INTO tags (note_id, tag) VALUES (1, 'x');
                 UPDATE sqlite_sequence SET seq = 50 WHERE name = 'tags';",
            )
            .unwrap();
        let sql = dump(
            &source,
            &ExportOptions {
                tables: vec!["notes".into()],
                schema: false,
                ..ExportOptions::default()
            },
        )
        .unwrap();
        // As a D1 exec runs it: in one transaction, where the dump's deferred
        // foreign keys apply.
        target
            .execute_batch(&format!("BEGIN;\n{sql}COMMIT;"))
            .unwrap();
        // `notes` counted four ids before one was deleted; `tags` kept its 50.
        assert_eq!(
            bytes(
                &target,
                "SELECT name, seq FROM sqlite_sequence ORDER BY name"
            ),
            vec![
                format!("t{}|i4", hex(b"notes")),
                format!("t{}|i50", hex(b"tags")),
            ]
        );
    }

    #[test]
    fn a_schema_only_dump_has_no_rows_or_counters() {
        let source = Connection::open_in_memory().unwrap();
        populate(&source);
        let sql = dump(
            &source,
            &ExportOptions {
                data: false,
                ..ExportOptions::default()
            },
        )
        .unwrap();
        assert!(
            !sql.lines().any(|line| line.starts_with("INSERT INTO")),
            "{sql}"
        );
        assert!(!sql.contains("defer_foreign_keys"), "{sql}");
        let replica = Connection::open_in_memory().unwrap();
        replica.execute_batch(&sql).unwrap();
        assert_eq!(bytes(&replica, "SELECT count(*) FROM notes"), vec!["i0"]);
    }

    #[test]
    fn a_filter_keeps_the_indexes_and_triggers_of_its_tables_and_no_view() {
        let source = Connection::open_in_memory().unwrap();
        populate(&source);
        let sql = dump(
            &source,
            &ExportOptions {
                tables: vec!["NOTES".into()],
                ..ExportOptions::default()
            },
        )
        .unwrap();
        assert!(sql.contains("CREATE INDEX notes_body"), "{sql}");
        assert!(sql.contains("CREATE TRIGGER notes_tag"), "{sql}");
        assert!(!sql.contains("CREATE VIEW"), "{sql}");
        assert!(!sql.contains("CREATE TABLE tags"), "{sql}");
    }

    #[test]
    fn unknown_internal_and_empty_requests_are_refused() {
        let source = Connection::open_in_memory().unwrap();
        populate(&source);
        source
            .execute_batch("CREATE TABLE _cf_KV (k TEXT)")
            .unwrap();
        let refusal = |options: ExportOptions| dump(&source, &options).unwrap_err();
        assert!(refusal(ExportOptions {
            tables: vec!["missing".into()],
            ..ExportOptions::default()
        })
        .contains("no such table: missing"));
        for internal in ["_cf_KV", "sqlite_sequence", "docs_data"] {
            let message = refusal(ExportOptions {
                tables: vec![internal.into()],
                ..ExportOptions::default()
            });
            assert!(message.contains(internal), "{message}");
        }
        assert!(refusal(ExportOptions {
            schema: false,
            data: false,
            ..ExportOptions::default()
        })
        .contains("nothing to export"));
        // The cell's own tables stay out of a whole-database dump too.
        assert!(!all(&source).contains("_cf_KV"));
    }

    #[test]
    fn an_export_needs_a_read_transaction() {
        let source = Connection::open_in_memory().unwrap();
        let message =
            export(&source, &ExportOptions::default(), EXPORT_MAX_BYTES, |_| {}).unwrap_err();
        assert!(message.contains("read transaction"), "{message}");
    }

    #[test]
    fn the_size_cap_stops_an_export_with_a_way_out() {
        let source = Connection::open_in_memory().unwrap();
        populate(&source);
        source.execute_batch("BEGIN").unwrap();
        let message = export(&source, &ExportOptions::default(), 400, |_| {}).unwrap_err();
        assert!(message.contains("larger than 400 bytes"), "{message}");
        assert!(message.contains("--table"), "{message}");
    }

    #[test]
    fn a_row_too_long_to_replay_is_named() {
        let source = Connection::open_in_memory().unwrap();
        source
            .execute_batch("CREATE TABLE big (id INTEGER PRIMARY KEY, body BLOB)")
            .unwrap();
        source
            .execute("INSERT INTO big (body) VALUES (zeroblob(60000))", [])
            .unwrap();
        let message = dump(&source, &ExportOptions::default()).unwrap_err();
        assert!(message.contains("row 1 of table big"), "{message}");
        assert!(message.contains("100000"), "{message}");
    }

    #[test]
    fn a_writer_on_another_connection_never_shows_in_the_dump() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("snapshot.db");
        let reader = Connection::open(&path).unwrap();
        reader.pragma_update(None, "journal_mode", "WAL").unwrap();
        populate(&reader);
        let before = all(&reader);

        let writer = Connection::open(&path).unwrap();
        let mut writes = 0;
        reader.execute_batch("BEGIN").unwrap();
        let during = export(&reader, &ExportOptions::default(), EXPORT_MAX_BYTES, |_| {
            // Between two tables, once: data, a new row in a table already
            // dumped, a table that did not exist, and a dropped one.
            if writes > 0 {
                writes += 1;
                return;
            }
            writer
                .execute_batch(
                    "INSERT INTO notes (body) VALUES ('late');
                     INSERT INTO loose VALUES ('late', 99);
                     CREATE TABLE late (x);
                     INSERT INTO late VALUES (1);
                     DROP TABLE keyed;",
                )
                .unwrap();
            writes += 1;
        })
        .unwrap();
        reader.execute_batch("ROLLBACK").unwrap();

        assert!(writes > 1, "the hook must have run between tables");
        assert_eq!(during, before);
        assert!(!during.contains("late"), "{during}");
        // And the writes did land, so the equality above was not vacuous.
        assert_eq!(bytes(&writer, "SELECT count(*) FROM late"), vec!["i1"]);
    }

    #[test]
    fn a_vec0_table_replays_without_its_shadow_tables() {
        let open = || {
            let connection = Connection::open_in_memory().unwrap();
            crate::storage::register_vec0_extension(&connection).unwrap();
            connection
        };
        let source = open();
        source
            .execute_batch(
                "CREATE VIRTUAL TABLE vecs USING vec0(embedding float[3]);
                 INSERT INTO vecs (rowid, embedding) VALUES (1, '[1,0,0]'), (2, '[0,1,0]'), (5, '[0,0,1]');
                 DELETE FROM vecs WHERE rowid = 2;",
            )
            .unwrap();
        let sql = all(&source);
        assert!(sql.contains("CREATE VIRTUAL TABLE vecs"), "{sql}");
        assert!(!sql.contains("vecs_"), "a shadow table leaked:\n{sql}");
        let replica = open();
        replica
            .execute_batch(&sql)
            .unwrap_or_else(|e| panic!("{e}\n{sql}"));
        let query = "SELECT rowid, hex(embedding) FROM vecs ORDER BY rowid";
        assert_eq!(bytes(&replica, query), bytes(&source, query));
        assert_eq!(bytes(&replica, query).len(), 2);
    }

    #[test]
    fn fts5_content_options_are_told_apart() {
        for (sql, expected) in [
            (
                "CREATE VIRTUAL TABLE t USING fts5(a, b)",
                Fts5Content::Regular,
            ),
            (
                "CREATE VIRTUAL TABLE t USING fts5(a, content='src', content_rowid='id')",
                Fts5Content::External,
            ),
            (
                "CREATE VIRTUAL TABLE t USING fts5(a, content = \"\")",
                Fts5Content::Contentless,
            ),
            (
                "CREATE VIRTUAL TABLE t USING fts5(a, content='', contentless_delete=1)",
                Fts5Content::Contentless,
            ),
            (
                "CREATE VIRTUAL TABLE t USING fts5(a, tokenize='porter unicode61')",
                Fts5Content::Regular,
            ),
            (
                "CREATE VIRTUAL TABLE t USING fts5(a, contentless_delete=1, content='x')",
                Fts5Content::External,
            ),
        ] {
            assert_eq!(fts5_content(sql), expected, "{sql}");
        }
    }

    #[test]
    fn a_contentless_fts5_table_is_created_and_noted() {
        let source = Connection::open_in_memory().unwrap();
        source
            .execute_batch(
                "CREATE VIRTUAL TABLE bare USING fts5(a, content='');
                 INSERT INTO bare (rowid, a) VALUES (1, 'x');",
            )
            .unwrap();
        let sql = all(&source);
        assert!(sql.contains("CREATE VIRTUAL TABLE bare"), "{sql}");
        assert!(sql.contains("contentless fts5 table"), "{sql}");
        assert!(!sql.contains("INSERT INTO"), "{sql}");
        Connection::open_in_memory()
            .unwrap()
            .execute_batch(&sql)
            .unwrap();
    }
}
