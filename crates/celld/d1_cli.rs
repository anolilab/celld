// Copyright 2026 Deno Land Inc. Apache-2.0 license.

// The D1 CLI reads operator input and migration files outside node storage.
#![allow(clippy::disallowed_methods)]
// Reaching stdout past `Output` is what mixed the result rows and the
// summary onto one stream here in the first place.

//! `celld d1` — run SQL and migrations against a deployed D1 database.
//!
//! A D1 database is a cell, so it is only reachable through the node that owns
//! it. This command finds the fleet the way `celld diagnose` does — it walks
//! the node leases in the bucket, and it reads the same shared secret — and
//! then sends the SQL to a live node's `/runtime/` route, which forwards to the
//! owner. The CLI therefore holds no ownership logic and no SQLite: it reaches
//! the database over the dispatch a Worker's `env.DB` reaches it over.
//!
//! The route is authenticated. A D1 database holds application data and
//! answers arbitrary SQL, and its scope is an HMAC over the database identity
//! rather than a secret, so the unauthenticated `/do/` route
//! refuses a D1 scope and sends the caller here.

use std::path::PathBuf;

use anyhow::{anyhow, bail, Context};
use serde_json::Value;

use crate::cli_options::FleetFlags;
use crate::cli_options::FLEET_HELP;
use crate::cli_output::{align, Format, Output, Record};
use crate::note;
use crate::operator_cell::{Fleet, Reachable, Subject};
use std::borrow::Cow;

/// One row of a `d1 execute` result set.
///
/// The text is rendered when the set is laid out, because a column's width
/// depends on every row in it. The JSON keeps the column names as keys, so
/// `--json` output is self-describing without the header line.
struct Row {
    object: Value,
    text: String,
}

impl Record for Row {
    fn json(&self) -> Value {
        self.object.clone()
    }

    fn text(&self) -> Cow<'_, str> {
        Cow::Borrowed(&self.text)
    }
}

/// One migration, listed rather than applied.
struct Pending {
    name: String,
}

impl Record for Pending {
    fn json(&self) -> Value {
        serde_json::json!({ "migration": self.name })
    }

    fn text(&self) -> Cow<'_, str> {
        Cow::Borrowed(&self.name)
    }
}

/// A SQL value as a table cell. JSON keeps the real type; this is only the
/// human rendering, so a string prints bare and a null prints as nothing.
fn cell(value: &Value) -> String {
    match value {
        Value::Null => String::new(),
        Value::String(text) => text.clone(),
        other => other.to_string(),
    }
}

pub async fn run(arguments: Vec<String>) -> anyhow::Result<()> {
    let Some(command) = Command::parse(arguments)? else {
        print_help();
        return Ok(());
    };
    // Read before any connection is made: a malformed batch is the operator's
    // mistake, and it should not cost a bucket round trip to learn it.
    let batch = match command.action {
        Action::Batch => Some(read_batch(std::io::stdin().lock())?),
        _ => None,
    };
    let project = crate::deploy::read_d1_project(command.project.clone())?;
    // The declaration, not just the name: `migrations_dir` belongs to the
    // binding, so the directory has to come from the database the command
    // names or one database's migrations reach another.
    let declaration = project
        .databases
        .iter()
        .find(|database| database.database_name == command.database)
        .ok_or_else(|| {
            anyhow!(
                "the project declares no D1 database named {:?}; it declares {}",
                command.database,
                if project.databases.is_empty() {
                    "none".to_string()
                } else {
                    project
                        .databases
                        .iter()
                        .map(|database| format!("{:?}", database.database_name))
                        .collect::<Vec<_>>()
                        .join(", ")
                }
            )
        })?;
    let scope = crate::js::d1_cell_scope(&declaration.database_identity);
    let storage = command.fleet.clone().resolve("celld d1")?;
    let database = Reachable::open(
        Fleet {
            bucket: &storage.bucket,
            endpoint: storage.endpoint.as_deref(),
            region: &storage.region,
            unsafe_public_advertise: command.unsafe_public_advertise,
        },
        Subject {
            noun: "database",
            source: "d1",
            // A migration on a large table is not a diagnostic ping.
            timeout: std::time::Duration::from_secs(120),
        },
        scope,
        None,
    )
    .await?;

    let mut out = Output::new(if command.json {
        Format::Json
    } else {
        Format::Text
    });
    match command.action {
        Action::Execute { command: sql, file } => {
            let source = match (sql, file) {
                (Some(sql), _) => sql,
                (None, Some(file)) => std::fs::read_to_string(&file)
                    .with_context(|| format!("read {}", file.display()))?,
                (None, None) => unreachable!("parse refuses neither --command nor --file"),
            };
            let result = database.exec(&source).await?;
            // The rows, not only the count: an operator running a SELECT is
            // asking for its result, and `wrangler d1 execute` prints it.
            for (index, set) in result
                .get("results")
                .and_then(Value::as_array)
                .into_iter()
                .flatten()
                .enumerate()
            {
                let columns: Vec<String> = set
                    .get("columns")
                    .and_then(Value::as_array)
                    .into_iter()
                    .flatten()
                    .filter_map(Value::as_str)
                    .map(str::to_string)
                    .collect();
                let values: Vec<Vec<Value>> = set
                    .get("rows")
                    .and_then(Value::as_array)
                    .into_iter()
                    .flatten()
                    .filter_map(Value::as_array)
                    .map(|row| row.to_vec())
                    .collect();
                let cells: Vec<Vec<String>> = values
                    .iter()
                    .map(|row| row.iter().map(cell).collect())
                    .collect();
                let (header, rule, lines) = align(&columns, &cells);
                // A blank line between result sets, so several statements
                // read as several tables rather than one ragged one.
                if index > 0 {
                    out.header("")?;
                }
                out.header(&header)?;
                out.header(&rule)?;
                for (row, text) in values.iter().zip(lines) {
                    let object =
                        Value::Object(columns.iter().cloned().zip(row.iter().cloned()).collect());
                    out.row(&Row { object, text })?;
                }
            }
            // stderr, not stdout. This line used to follow the rows on the
            // same stream, so `celld d1 execute | jq` parsed prose.
            let count = result
                .get("count")
                .and_then(Value::as_u64)
                .unwrap_or_default();
            let milliseconds = result
                .get("duration")
                .and_then(Value::as_f64)
                .unwrap_or_default();
            // "0.00 sec" reads as a missing measurement rather than a fast
            // statement, the same way "0.0s" did on a listing summary.
            let took = if milliseconds < 1000.0 {
                format!("{}ms", milliseconds.round() as u64)
            } else {
                format!("{:.2} sec", milliseconds / 1000.0)
            };
            note!("Executed {count} statement(s) in {took}");
        }
        Action::Batch => {
            let statements = batch.expect("a batch action reads its statements first");
            let count = statements.len();
            let results = database.batch(statements).await?;
            // One array, the shape `batch()` hands a Worker, so a script can
            // treat the two alike. `Output` prints a row per line, which would
            // split it, so this writes the whole array as one line.
            drop(out);
            Output::new(Format::Json).line(format_args!("{}", serde_json::to_string(&results)?))?;
            note!("Executed {count} statement(s) in one batch");
            return Ok(());
        }
        Action::MigrationsList => {
            let pending = database.pending(declaration).await?;
            if pending.is_empty() {
                note!("No migrations to apply.");
                return out.finish();
            }
            note!("Migrations to be applied:");
            for migration in &pending {
                out.row(&Pending {
                    name: migration.name.clone(),
                })?;
            }
        }
        Action::MigrationsApply => {
            let pending = database.pending(declaration).await?;
            if pending.is_empty() {
                note!("No migrations to apply.");
                return out.finish();
            }
            for migration in pending {
                database
                    .apply(&migration, &declaration.migrations_table)
                    .await?;
                // Applying is an action, not data: its progress belongs on
                // stderr so a redirect of the listing stays parseable.
                note!("Applied {}", migration.name);
            }
        }
    }
    out.finish()
}

struct Migration {
    name: String,
    sql: String,
}

/// One entrance into the fleet: a node's address and the identity a request
/// to it must be signed for.
impl Reachable {
    async fn exec(&self, source: &str) -> anyhow::Result<Value> {
        // `rows: true` is the CLI's difference from the Worker binding's
        // exec(): the operator asked to see the result, not only the count.
        self.call(serde_json::json!({ "exec": { "sql": source, "rows": true } }))
            .await
    }

    /// `batch(statements)` as a Worker runs it: one transaction, so a failing
    /// statement rolls the whole batch back, and one result per statement.
    async fn batch(&self, statements: Vec<Value>) -> anyhow::Result<Value> {
        self.call(serde_json::json!({ "batch": { "statements": statements } }))
            .await
    }

    /// Rows for one statement, as `all()` would shape them.
    async fn query(&self, sql: &str) -> anyhow::Result<Vec<Vec<Value>>> {
        let result = self
            .call(serde_json::json!({ "statements": [{ "sql": sql, "params": [] }] }))
            .await?;
        let rows = result
            .get(0)
            .and_then(|first| first.get("rows"))
            .and_then(Value::as_array)
            .ok_or_else(|| anyhow!("the database reply carried no rows"))?;
        Ok(rows
            .iter()
            .filter_map(|row| row.as_array().cloned())
            .collect())
    }

    /// Every migration file the database has not recorded, in wrangler's
    /// order. The bookkeeping table is the binding's `migrations_table`,
    /// validated at config read, so the name joined into this SQL is a plain
    /// identifier and nothing else.
    async fn pending(
        &self,
        declaration: &crate::deploy::D1Declaration,
    ) -> anyhow::Result<Vec<Migration>> {
        let on_disk = read_migrations(&declaration.migrations_dir)?;
        let table = &declaration.migrations_table;
        self.exec(&format!(
            "CREATE TABLE IF NOT EXISTS \"{table}\" (\
               id INTEGER PRIMARY KEY AUTOINCREMENT, \
               name TEXT UNIQUE, \
               applied_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP);"
        ))
        .await?;
        let applied = self
            .query(&format!("SELECT name FROM \"{table}\""))
            .await?
            .into_iter()
            .filter_map(|row| row.first().and_then(Value::as_str).map(str::to_string))
            .collect::<std::collections::BTreeSet<_>>();
        Ok(on_disk
            .into_iter()
            .filter(|migration| !applied.contains(&migration.name))
            .collect())
    }

    /// The cell applies the file and records it in one transaction, so a
    /// migration that fails part-way rolls back whole and a re-run starts
    /// clean. The CLI sends the file and the name separately and joins no SQL:
    /// the name reaches the bookkeeping row as a bound parameter, and a file
    /// that ends in a comment or an unterminated statement is refused by the
    /// cell rather than repaired by a guess here.
    async fn apply(&self, migration: &Migration, table: &str) -> anyhow::Result<()> {
        self.call(serde_json::json!({
            "migrate": { "name": migration.name, "sql": migration.sql, "table": table },
        }))
        .await
        .with_context(|| format!("apply {}", migration.name))?;
        Ok(())
    }
}

/// The most stdin a batch may carry. The cell's request body has a limit of
/// its own; this one only stops an unbounded pipe from filling memory first.
const MAX_BATCH_BYTES: u64 = 32 * 1024 * 1024;

fn read_batch(input: impl std::io::Read) -> anyhow::Result<Vec<Value>> {
    use std::io::Read as _;
    let mut text = String::new();
    // One byte past the cap tells a full read from an oversized one.
    input
        .take(MAX_BATCH_BYTES + 1)
        .read_to_string(&mut text)
        .context("read the batch from stdin")?;
    if text.len() as u64 > MAX_BATCH_BYTES {
        bail!("the batch on stdin is larger than {MAX_BATCH_BYTES} bytes");
    }
    parse_batch(&text)
}

/// A JSON array whose elements are each a SQL string, or `{ "sql", "params" }`
/// as `prepare(sql).bind(...params)` builds one. Both normalize to the object
/// form the cell takes. Anything else is refused by position, because an
/// unknown key that is silently dropped (`"param"`) would run a statement
/// without the values the operator meant to bind.
fn parse_batch(text: &str) -> anyhow::Result<Vec<Value>> {
    let value: Value = serde_json::from_str(text).context("the batch on stdin is not JSON")?;
    let Value::Array(items) = value else {
        bail!("the batch must be a JSON array of statements");
    };
    items
        .into_iter()
        .enumerate()
        .map(|(index, item)| match item {
            Value::String(sql) => Ok(serde_json::json!({ "sql": sql, "params": [] })),
            Value::Object(mut object) => {
                if let Some(key) = object
                    .keys()
                    .find(|key| !matches!(key.as_str(), "sql" | "params"))
                {
                    bail!(
                        "statement {index} has an unknown key {key:?}; use \"sql\" and \"params\""
                    );
                }
                if !matches!(object.get("sql"), Some(Value::String(_))) {
                    bail!("statement {index} needs a \"sql\" string");
                }
                match object.remove("params") {
                    None => {
                        object.insert("params".into(), Value::Array(Vec::new()));
                    }
                    Some(params @ Value::Array(_)) => {
                        object.insert("params".into(), params);
                    }
                    Some(_) => bail!("statement {index}: \"params\" must be an array"),
                }
                Ok(Value::Object(object))
            }
            _ => bail!(
                "statement {index} must be a SQL string or a {{ \"sql\", \"params\" }} object"
            ),
        })
        .collect()
}

fn has_sql_extension(path: &std::path::Path) -> bool {
    path.extension()
        .and_then(std::ffi::OsStr::to_str)
        .is_some_and(|extension| extension.eq_ignore_ascii_case("sql"))
}

fn read_migrations(directory: &std::path::Path) -> anyhow::Result<Vec<Migration>> {
    if !directory.exists() {
        bail!(
            "no migrations directory at {}; create it, or set `migrations_dir` \
             on the d1_databases entry",
            directory.display()
        );
    }
    let mut migrations = Vec::new();
    for entry in
        std::fs::read_dir(directory).with_context(|| format!("read {}", directory.display()))?
    {
        let path = entry?.path();
        // A directory is never a migration, whatever it is named — but a
        // directory HOLDING migrations is a layout this command does not
        // read, and skipping it silently would apply half a schema.
        if path.is_dir() {
            let nested = std::fs::read_dir(&path)
                .map(|entries| {
                    entries
                        .flatten()
                        .any(|entry| has_sql_extension(&entry.path()))
                })
                .unwrap_or(false);
            if nested {
                bail!(
                    "{} holds .sql files, and celld reads migrations from a flat \
                     directory only; move them into {} itself",
                    path.display(),
                    directory.display()
                );
            }
            continue;
        }
        if !has_sql_extension(&path) {
            continue;
        }
        let name = path
            .file_name()
            .and_then(|name| name.to_str())
            .ok_or_else(|| anyhow!("migration {} has a non-UTF-8 name", path.display()))?
            .to_string();
        let sql =
            std::fs::read_to_string(&path).with_context(|| format!("read {}", path.display()))?;
        migrations.push(Migration { name, sql });
    }
    // Wrangler orders migrations by their numeric prefix, not by the file
    // name as text: `10_` runs after `9_`, where a text sort runs it first
    // and the ALTERs land before the CREATE they depend on. A file without
    // the prefix has no place in that order, so it is refused rather than
    // guessed about.
    let mut ordered = Vec::with_capacity(migrations.len());
    for migration in migrations {
        let digits: String = migration
            .name
            .chars()
            .take_while(char::is_ascii_digit)
            .collect();
        if digits.is_empty() {
            bail!(
                "migration {:?} has no numeric prefix; wrangler names migrations \
                 NNNN_description.sql, and the prefix is the application order",
                migration.name
            );
        }
        // A parse failure past this point is overflow, not absence: 20 or
        // more digits do not fit a u64. Reporting that as "no numeric
        // prefix" sent the operator to rename a file that plainly starts
        // with digits.
        let number: u64 = digits.parse().map_err(|_| {
            anyhow!(
                "migration {:?} has a numeric prefix that does not fit in an \
                 unsigned 64-bit number; use a shorter prefix",
                migration.name
            )
        })?;
        ordered.push((number, migration));
    }
    ordered.sort_by(|left, right| (left.0, &left.1.name).cmp(&(right.0, &right.1.name)));
    Ok(ordered
        .into_iter()
        .map(|(_, migration)| migration)
        .collect())
}

/// Every node with an unexpired lease and a usable address, as (id, address).
/// The id is not decoration: a peer signature binds the node it is addressed
/// to. Any of them will do, because the route forwards to the owner, so the
/// CLI does not resolve ownership and cannot race a cell that moves while it
/// works. They are all returned rather than only the first, because a lease
/// outlives the node that wrote it and a draining node keeps its lease while
/// it refuses work.
enum Action {
    /// Exactly one of the two, enforced at parse.
    Execute {
        command: Option<String>,
        file: Option<PathBuf>,
    },
    /// `execute --batch`: a JSON array of statements on stdin.
    Batch,
    MigrationsList,
    MigrationsApply,
}

struct Command {
    action: Action,
    database: String,
    project: Option<PathBuf>,
    fleet: FleetFlags,
    json: bool,
    unsafe_public_advertise: bool,
}

impl Command {
    fn parse(arguments: Vec<String>) -> anyhow::Result<Option<Self>> {
        let mut arguments = arguments.into_iter().peekable();
        let Some(first) = arguments.next() else {
            return Ok(None);
        };
        let action = match first.as_str() {
            "--help" | "-h" | "help" => return Ok(None),
            "execute" => None,
            // An option in the subcommand slot means the subcommand is
            // missing, not that the option names one. Reporting `--bucket` as
            // an unknown subcommand would send an operator looking for a
            // spelling mistake they did not make.
            "migrations" => match arguments.next().as_deref() {
                Some("apply") => Some(Action::MigrationsApply),
                Some("list") => Some(Action::MigrationsList),
                Some(other) if !other.starts_with('-') => {
                    bail!("unknown `celld d1 migrations` subcommand: {other}")
                }
                _ => bail!("`celld d1 migrations` needs `apply` or `list`"),
            },
            other => bail!("unknown `celld d1` subcommand: {other}"),
        };
        let database = arguments
            .next()
            .filter(|value| !value.starts_with('-'))
            .ok_or_else(|| anyhow!("celld d1 needs a database name"))?;

        let mut sql = None;
        let mut file = None;
        let mut project = None;
        let mut fleet = FleetFlags::default();
        let mut json = false;
        let mut batch = false;
        let mut unsafe_public_advertise = false;
        while let Some(argument) = arguments.next() {
            let mut value = |flag: &str| {
                arguments
                    .next()
                    .ok_or_else(|| anyhow!("{flag} requires a value"))
            };
            match argument.as_str() {
                "--command" => sql = Some(value("--command")?),
                "--file" => file = Some(PathBuf::from(value("--file")?)),
                "--json" => json = true,
                "--batch" => batch = true,
                "--unsafe-public-advertise" => unsafe_public_advertise = true,
                "--help" | "-h" => return Ok(None),
                other => {
                    // The fleet flags are shared, so `--bucket gs://name`
                    // resolves here exactly as it does for every other
                    // command rather than through a second copy of the rule.
                    if fleet.consume(other, &mut value)? {
                        continue;
                    }
                    if other.starts_with('-') {
                        bail!("unknown option: {other}; run `celld d1 --help` for usage")
                    }
                    project = Some(PathBuf::from(other));
                }
            }
        }
        let action = match action {
            Some(action) => {
                // Accepting the flag and ignoring it would tell an operator
                // their SQL ran when nothing read it.
                if sql.is_some() || file.is_some() {
                    bail!(
                        "`celld d1 migrations` takes no --command or --file; \
                         migrations come from the migrations directory"
                    );
                }
                if batch {
                    bail!("--batch belongs to `celld d1 execute`, not to migrations");
                }
                action
            }
            None if batch => {
                if sql.is_some() || file.is_some() {
                    bail!(
                        "--batch reads its statements from stdin and takes no \
                         --command or --file"
                    );
                }
                // The result is one JSON array shaped like `batch()`'s; a table
                // has no honest form for several result sets that must commit
                // together, so the format is stated rather than defaulted.
                if !json {
                    bail!("--batch prints one JSON array, so it requires --json");
                }
                Action::Batch
            }
            None => {
                if sql.is_some() == file.is_some() {
                    bail!("celld d1 execute requires exactly one of --command or --file");
                }
                Action::Execute { command: sql, file }
            }
        };
        Ok(Some(Self {
            action,
            database,
            project,
            // Resolution is deferred to `run`, so the missing-bucket message
            // is the one every command shares.
            fleet: fleet.with_environment(),
            json,
            unsafe_public_advertise,
        }))
    }
}

pub fn print_help() {
    let text = format!(
        r#"celld d1 — run SQL and migrations against a deployed D1 database

USAGE:
  celld d1 execute DATABASE (--command SQL | --file PATH) [PROJECT] --bucket NAME
  celld d1 execute DATABASE --batch --json [PROJECT] --bucket NAME < statements.json
  celld d1 migrations apply DATABASE [PROJECT] --bucket NAME
  celld d1 migrations list  DATABASE [PROJECT] --bucket NAME

DATABASE is a `database_name` the project declares in `d1_databases`. PROJECT
is the directory holding wrangler.jsonc, and defaults to the working directory.
The `database_id` is the stable database identity when the declaration has one.
Otherwise, celld uses the `database_name` as the identity.

celld d1 needs a running fleet. It finds a node through the bucket's node
leases and sends the SQL to that node, which forwards it to the node that owns
the database. It signs each request with the fleet's shared secret, which it
reads from the bucket, so it needs the same bucket credentials celld deploy
needs.

Migration files are `NNNN_description.sql` in `migrations/`. The `sql`
extension is ASCII case-insensitive. celld applies the files in the order of
their numeric prefixes as wrangler applies them. `migrations_dir` on the
d1_databases entry moves the directory, and `migrations_table` renames the
bookkeeping table. celld records applied migrations in that table
(`d1_migrations` by default), the same table and columns
`wrangler d1 migrations` uses.

`execute` prints a table, and every message goes to stderr, so its stdout
carries only result rows. Pass --json for one JSON object per row.

`execute --batch` runs a JSON array of statements from stdin as one batch, the
way `batch()` does in a Worker: one transaction, so a statement that fails
rolls the whole batch back. Each element is a SQL string, or an object
`{{"sql": "...", "params": [...]}}` whose params bind to `?` as `bind()` does.
It prints one JSON array on stdout with one result per statement, shaped like
`batch()`'s: `{{success, meta, results}}`, with `results` as row objects.

OPTIONS:
  --command SQL       SQL to run; several statements are permitted
  --file PATH         Read the SQL from a file instead
  --batch             Run the JSON array of statements on stdin as one
                      transactional batch; requires --json and takes neither
                      --command nor --file
  --json              Print one JSON object per row instead of a table
{FLEET_HELP}
  --unsafe-public-advertise
                      Permit a node whose advertised address is a public IP
  -h, --help          Show this help"#
    );
    let _ = Output::new(Format::Text).help(&text);
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(arguments: &[&str]) -> anyhow::Result<Option<Command>> {
        Command::parse(
            arguments
                .iter()
                .map(|argument| argument.to_string())
                .collect(),
        )
    }

    fn refusal(arguments: &[&str]) -> String {
        match parse(arguments) {
            Ok(_) => panic!("{arguments:?} should be refused"),
            Err(error) => format!("{error:#}"),
        }
    }

    #[test]
    fn batch_is_an_execute_action_that_needs_json() {
        let command = parse(&["execute", "ledger", "--batch", "--json", "--bucket", "b"])
            .unwrap()
            .unwrap();
        assert!(matches!(command.action, Action::Batch));
        assert!(refusal(&["execute", "ledger", "--batch", "--bucket", "b"]).contains("--json"));
    }

    #[test]
    fn batch_takes_its_statements_from_stdin_only() {
        for source in [["--command", "select 1"], ["--file", "x.sql"]] {
            let message = refusal(&[
                "execute", "ledger", "--batch", "--json", source[0], source[1],
            ]);
            assert!(message.contains("stdin"), "{message}");
        }
        let message = refusal(&["migrations", "apply", "ledger", "--batch", "--json"]);
        assert!(message.contains("execute"), "{message}");
    }

    #[test]
    fn execute_still_wants_exactly_one_source() {
        let message = refusal(&["execute", "ledger", "--json"]);
        assert!(
            message.contains("exactly one of --command or --file"),
            "{message}"
        );
    }

    #[test]
    fn statements_normalize_to_sql_and_params() {
        let statements = parse_batch(
            r#"["insert into t values (1)", {"sql": "select ?", "params": [7, null, [1, 2]]}, {"sql": "select 2"}]"#,
        )
        .unwrap();
        assert_eq!(
            statements,
            vec![
                serde_json::json!({"sql": "insert into t values (1)", "params": []}),
                serde_json::json!({"sql": "select ?", "params": [7, null, [1, 2]]}),
                serde_json::json!({"sql": "select 2", "params": []}),
            ]
        );
    }

    #[test]
    fn a_malformed_batch_names_the_statement() {
        for (input, needle) in [
            ("not json", "not JSON"),
            (r#"{"sql": "select 1"}"#, "JSON array"),
            ("[1]", "statement 0 must be"),
            (
                r#"["select 1", {"params": []}]"#,
                "statement 1 needs a \"sql\"",
            ),
            (r#"[{"sql": 1}]"#, "statement 0 needs a \"sql\""),
            (
                r#"[{"sql": "select ?", "param": [1]}]"#,
                "unknown key \"param\"",
            ),
            (r#"[{"sql": "select ?", "params": 1}]"#, "must be an array"),
        ] {
            let message = format!("{:#}", parse_batch(input).unwrap_err());
            assert!(message.contains(needle), "{input}: {message}");
        }
    }

    #[test]
    fn an_empty_batch_is_a_batch() {
        assert!(parse_batch("[]").unwrap().is_empty());
    }

    #[test]
    fn an_oversized_batch_is_refused_before_it_is_parsed() {
        let endless = std::io::repeat(b' ');
        let message = format!("{:#}", read_batch(endless).unwrap_err());
        assert!(message.contains("larger than"), "{message}");
    }
}
