// Copyright 2026 Deno Land Inc. Apache-2.0 license.

// The R2 CLI reads operator input and object bodies outside node storage.
#![allow(clippy::disallowed_methods)]

//! `celld r2` — read and write the objects behind an `r2_buckets` binding.
//!
//! A release pipeline publishes a build artifact with
//! `wrangler r2 object put` and the Worker serves it from its `r2_buckets`
//! binding. celld served the binding but had no command, so the only way to
//! seed a bucket was `aws s3 cp` straight into the fleet bucket. That works
//! for the bytes and loses the record: an R2 object carries
//! `customMetadata`, a `cacheExpiry`, and its checksums in a celld-private
//! envelope that a plain copy does not write. The result reads back with no
//! custom metadata, which is a silent difference rather than a failure.
//!
//! So this command does not implement R2. It installs the fleet bucket the
//! same way a node does and calls [`crate::js::r2_ops`] — the functions the
//! binding's ops call. The key scoping, the envelope, the checksum set and
//! the single-request/multipart choice therefore have one implementation,
//! and an object written here is an object the binding could have written.
//!
//! Unlike `celld d1`, `celld kv` and `celld queue`, this command needs no
//! node. A binding reads the fleet bucket directly, so an operator with the
//! bucket credentials can publish into an empty fleet — which is what a
//! release pipeline that runs before a deployment needs.

use crate::cli_options::FLEET_HELP;
use crate::cli_options::LISTING_HELP;
use crate::cli_output::list;
use crate::cli_output::Bounds;
use crate::cli_output::Format;
use crate::cli_output::Output;
use crate::cli_output::Page;
use crate::cli_output::Record;
use crate::cli_output::Resumable;
use crate::cli_output::Resume;
use crate::note;
use anyhow::anyhow;
use anyhow::bail;
use anyhow::Context;
use serde_json::json;
use serde_json::Value;
use std::borrow::Cow;
use std::collections::BTreeMap;
use std::io::Read;
use std::path::PathBuf;

/// How much of a body the command reads at once. The write buffers what it
/// is given and cuts its own parts, so this only bounds the resident copy
/// of a body that can be many gigabytes.
const READ_CHUNK: usize = 1 << 20;

/// One object in a listing.
struct Object {
    key: String,
    size: u64,
    etag: Option<String>,
    uploaded_ms: i64,
}

impl Object {
    fn from_json(object: &Value) -> Self {
        Self {
            key: object
                .get("key")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_string(),
            size: object
                .get("size")
                .and_then(Value::as_u64)
                .unwrap_or_default(),
            etag: object
                .get("etag")
                .and_then(Value::as_str)
                .map(str::to_string),
            uploaded_ms: object
                .get("uploaded")
                .and_then(Value::as_i64)
                .unwrap_or_default(),
        }
    }
}

/// What `--json` prints on stdout for a key that is not there, ahead of the
/// error on stderr, so a script can tell an absent key from a failure.
fn not_found_json(bucket: &str, key: &str) -> Value {
    json!({ "error": "not_found", "bucket": bucket, "key": key })
}

/// One page of a listing in the shape of R2's `R2Objects`: each object as
/// `--json` prints it in a listing, the delimited prefixes, whether the
/// listing was cut off, and the cursor that continues it.
fn page_json(page: &Value) -> anyhow::Result<Value> {
    let objects = page
        .get("objects")
        .and_then(Value::as_array)
        .ok_or_else(|| anyhow!("the listing carried no objects: {page}"))?;
    Ok(json!({
        "objects": objects
            .iter()
            .map(|object| Object::from_json(object).json())
            .collect::<Vec<_>>(),
        "delimitedPrefixes": page.get("prefixes").cloned().unwrap_or_else(|| json!([])),
        "truncated": page.get("truncated").and_then(Value::as_bool).unwrap_or(false),
        "cursor": page.get("cursor").cloned().unwrap_or(Value::Null),
    }))
}

/// The error for a key that is not there. With `--json` the same fact goes to
/// stdout first as one object; the message, and the exit status it brings,
/// are the same either way.
fn missing_object(json: bool, bucket: &str, key: &str) -> anyhow::Error {
    if json {
        let _ = Output::new(Format::Text).line(format_args!("{}", not_found_json(bucket, key)));
    }
    anyhow!("no object {key:?} in r2 bucket {bucket:?}")
}

impl Record for Object {
    fn json(&self) -> Value {
        json!({
            "key": self.key,
            "size": self.size,
            "etag": self.etag,
            "uploaded": self.uploaded_ms,
        })
    }

    fn text(&self) -> Cow<'_, str> {
        Cow::Borrowed(&self.key)
    }
}

impl Resumable for Object {
    fn cursor(&self) -> &str {
        &self.key
    }
}

/// One object's complete record, as `celld r2 head` reports it.
///
/// The JSON carries the binding's object record before the JS conversion.
/// Its `http` and `custom` fields become `httpMetadata` and
/// `customMetadata` in an `R2Object`.
struct Head(Value);

impl Record for Head {
    fn json(&self) -> Value {
        self.0.clone()
    }

    fn text(&self) -> Cow<'_, str> {
        let field = |name: &str| self.0.get(name).cloned().unwrap_or(Value::Null);
        let scalar = |value: &Value| match value {
            Value::Null => "-".to_string(),
            Value::String(text) => text.clone(),
            other => other.to_string(),
        };
        let mut lines = vec![
            format!("key:      {}", scalar(&field("key"))),
            format!("size:     {}", scalar(&field("size"))),
            format!("etag:     {}", scalar(&field("etag"))),
            format!("uploaded: {}", scalar(&field("uploaded"))),
        ];
        // The two maps print only when they hold something. An object
        // written by a plain `aws s3 cp` has neither, and an empty block
        // for each would hide that difference in a wall of dashes.
        for (label, name) in [("http", "http"), ("custom", "custom")] {
            if let Some(map) = field(name).as_object().filter(|map| !map.is_empty()) {
                for (key, value) in map {
                    lines.push(format!("{label}.{key}: {}", scalar(value)));
                }
            }
        }
        Cow::Owned(lines.join("\n"))
    }
}

pub async fn run(arguments: Vec<String>) -> anyhow::Result<()> {
    let Some(command) = Command::parse(arguments)? else {
        print_help();
        return Ok(());
    };
    let storage = command.fleet.clone().resolve("celld r2")?;
    // The binding's store, installed exactly as a node installs it, so
    // every call below reaches the same keys the Worker reads.
    crate::js::set_r2_store(storage.open().await?);
    let bucket = command.bucket.as_str();

    match command.action {
        Action::Get { key } => {
            let read = crate::js::r2_ops::read(
                bucket,
                &key,
                crate::bucket::BlobRange::Whole,
                &crate::bucket::BlobConditions::default(),
            )
            .await
            .map_err(anyhow::Error::msg)?;
            match read {
                crate::bucket::BlobRead::Hit(blob) => {
                    // Streamed rather than collected: a release artifact is
                    // routinely larger than the memory a CI runner gives
                    // this process.
                    Output::new(Format::Text).stream(blob.body).await?;
                }
                // An unconditional read cannot be refused, so the only
                // other answer is an absent key.
                crate::bucket::BlobRead::Missing | crate::bucket::BlobRead::Unmet(_) => {
                    return Err(missing_object(command.json, bucket, &key))
                }
            }
        }
        Action::Head { key } => {
            let meta = crate::js::r2_ops::head(bucket, &key)
                .await
                .map_err(anyhow::Error::msg)?
                .ok_or_else(|| missing_object(command.json, bucket, &key))?;
            let mut out = Output::new(if command.json {
                Format::Json
            } else {
                Format::Text
            });
            out.row(&Head(crate::js::r2_ops::object_json(&key, &meta, None)))?;
            out.finish()?;
        }
        Action::Put {
            key,
            file,
            http,
            custom,
        } => {
            let request = json!({ "http": http, "custom": custom }).to_string();
            let mut put = crate::js::r2_ops::open_put(bucket.to_string(), key.clone(), &request)
                .map_err(anyhow::Error::msg)?;
            let mut source = Source::open(file.as_deref())?;
            let mut buffer = vec![0u8; READ_CHUNK];
            loop {
                let read = source.read(&mut buffer).context("read the object body")?;
                if read == 0 {
                    break;
                }
                put.push(buffer[..read].to_vec())
                    .await
                    .map_err(anyhow::Error::msg)?;
            }
            let answer = put.finish().await.map_err(anyhow::Error::msg)?;
            let answer: Value = serde_json::from_str(&answer).context("read the write's answer")?;
            let size = answer
                .pointer("/object/size")
                .and_then(Value::as_u64)
                .unwrap_or_default();
            note!("wrote {key:?} ({size} bytes)");
        }
        Action::Delete { keys } => {
            crate::js::r2_ops::delete(bucket, &keys)
                .await
                .map_err(anyhow::Error::msg)?;
            note!("deleted {} object(s)", keys.len());
        }
        Action::List {
            prefix,
            bounds,
            delimiter,
            page: true,
        } => {
            let page = crate::js::r2_ops::list(
                bucket,
                &json!({
                    "prefix": prefix,
                    "cursor": bounds.after,
                    "limit": bounds.limit,
                    "delimiter": delimiter,
                })
                .to_string(),
            )
            .await
            .map_err(anyhow::Error::msg)?;
            Output::new(Format::Text).line(format_args!("{}", page_json(&page)?))?;
        }
        Action::List { prefix, bounds, .. } => {
            let mut out = Output::new(if command.json {
                Format::Json
            } else {
                Format::Text
            });
            let listed = list(&mut out, &bounds, |resume, want| {
                let prefix = prefix.clone();
                // An operator's `--after` is a key and the driver's own
                // continuation is the cursor a truncated page answered
                // with. R2 reads the cursor first and both name the same
                // place, so either one resumes strictly after that key.
                let (cursor, after) = match resume {
                    Resume::From(after) => (String::new(), after.unwrap_or_default()),
                    Resume::Token(token) => (token, String::new()),
                };
                async move {
                    let page = crate::js::r2_ops::list(
                        bucket,
                        &json!({
                            "prefix": prefix,
                            "cursor": cursor,
                            "startAfter": after,
                            "limit": want,
                        })
                        .to_string(),
                    )
                    .await
                    .map_err(anyhow::Error::msg)?;
                    let objects = page
                        .get("objects")
                        .and_then(Value::as_array)
                        .ok_or_else(|| anyhow!("the listing carried no objects: {page}"))?;
                    let rows = objects.iter().map(Object::from_json).collect();
                    // The store's own truncation signal, never a row
                    // count: a page filled exactly to its bound is not
                    // evidence that another object exists.
                    let next = page
                        .get("cursor")
                        .and_then(Value::as_str)
                        .map(str::to_string);
                    Ok(Page { rows, next })
                }
            })
            .await?;
            out.finish()?;
            listed.report("object", "--after");
        }
    }
    Ok(())
}

/// Where a `put` reads its body. A named file and standard input are both
/// read in bounded pieces, so neither has to fit in memory.
enum Source {
    File(std::io::BufReader<std::fs::File>),
    Stdin(std::io::Stdin),
}

impl Source {
    fn open(path: Option<&std::path::Path>) -> anyhow::Result<Self> {
        match path {
            None => Ok(Self::Stdin(std::io::stdin())),
            Some(path) => {
                let file = std::fs::File::open(path)
                    .with_context(|| format!("open {}", path.display()))?;
                Ok(Self::File(std::io::BufReader::new(file)))
            }
        }
    }

    fn read(&mut self, buffer: &mut [u8]) -> std::io::Result<usize> {
        match self {
            Self::File(file) => file.read(buffer),
            Self::Stdin(stdin) => stdin.lock().read(buffer),
        }
    }
}

enum Action {
    Get {
        key: String,
    },
    Head {
        key: String,
    },
    Put {
        key: String,
        /// `None` reads the body from standard input.
        file: Option<PathBuf>,
        /// R2's `httpMetadata`, already in the binding's spelling.
        http: BTreeMap<String, Value>,
        /// R2's `customMetadata`.
        custom: BTreeMap<String, String>,
    },
    Delete {
        keys: Vec<String>,
    },
    List {
        prefix: String,
        bounds: Bounds,
        /// Group keys under their first `delimiter`, as R2's `delimiter`
        /// option does. Only a `page` can carry the groups.
        delimiter: Option<String>,
        /// Print one page as a single JSON object, with its cursor.
        page: bool,
    },
}

struct Command {
    action: Action,
    bucket: String,
    fleet: crate::cli_options::FleetFlags,
    json: bool,
}

/// The `httpMetadata` field each flag sets, in R2's spelling.
const HTTP_FLAGS: [(&str, &str); 5] = [
    ("--content-type", "contentType"),
    ("--content-language", "contentLanguage"),
    ("--content-disposition", "contentDisposition"),
    ("--content-encoding", "contentEncoding"),
    ("--cache-control", "cacheControl"),
];

impl Command {
    fn parse(arguments: Vec<String>) -> anyhow::Result<Option<Self>> {
        let mut arguments = arguments.into_iter().peekable();
        let Some(first) = arguments.next() else {
            return Ok(None);
        };
        let verb = match first.as_str() {
            "--help" | "-h" | "help" => return Ok(None),
            verb @ ("get" | "head" | "put" | "delete" | "list") => verb.to_string(),
            other => bail!("unknown `celld r2` subcommand: {other}"),
        };
        // The bucket is the `bucket_name` from `r2_buckets`, not the
        // binding name: the name is what places the keys in the fleet
        // bucket, and it is the one an `env.BUCKET` call resolves to.
        let bucket = arguments
            .next()
            .filter(|value| !value.starts_with('-'))
            .ok_or_else(|| anyhow!("celld r2 needs an r2 bucket name"))?;
        // The same rule the deployment applies to `bucket_name`. Without
        // it a name with a `/` in it would write under another binding's
        // prefix, or outside the reserved `r2/` space entirely.
        if !crate::deploy::valid_resource_name(&bucket) {
            bail!(
                "invalid r2 bucket name {bucket:?}: use letters, digits, `-` and `_` \
                 (not starting with `-`), at most 64 characters"
            );
        }

        let mut positional = Vec::new();
        let mut file = None;
        let mut pipe = false;
        let mut metadata = None;
        let mut prefix = String::new();
        let mut http = BTreeMap::new();
        let mut cache_expiry = None;
        let mut fleet = crate::cli_options::FleetFlags::default();
        let mut bounds = Bounds::default();
        let mut json = false;
        let mut delimiter = None;
        let mut page = false;
        while let Some(argument) = arguments.next() {
            let mut value = |flag: &str| {
                arguments
                    .next()
                    .ok_or_else(|| anyhow!("{flag} requires a value"))
            };
            match argument.as_str() {
                "--path" | "--file" => file = Some(PathBuf::from(value("--path")?)),
                "--pipe" => pipe = true,
                "--metadata" => metadata = Some(value("--metadata")?),
                "--cache-expiry" => {
                    let raw = value("--cache-expiry")?;
                    cache_expiry = Some(raw.parse::<i64>().with_context(|| {
                        format!("--cache-expiry takes unix milliseconds, got {raw:?}")
                    })?);
                }
                "--prefix" => prefix = value("--prefix")?,
                "--json" => json = true,
                "--delimiter" => delimiter = Some(value("--delimiter")?),
                "--page" => page = true,
                // celld has neither concept, and a flag that silently means
                // nothing is the gap the compatibility page forbids.
                flag @ ("--local" | "--remote" | "--jurisdiction") => bail!(
                    "celld r2 does not take {flag}: celld runs neither a local \
                     miniflare store nor a Cloudflare account, and it has no \
                     jurisdiction. An r2 bucket name addresses one key space in \
                     one fleet."
                ),
                "--help" | "-h" => return Ok(None),
                other => {
                    if let Some((_, field)) = HTTP_FLAGS.iter().find(|(flag, _)| *flag == other) {
                        let text = value(other)?;
                        http.insert((*field).to_string(), Value::String(text));
                        continue;
                    }
                    // The shared flags first, so `--bucket`, `--limit` and
                    // their siblings mean here exactly what they mean in
                    // every other command.
                    if fleet.consume(other, &mut value)? || bounds.consume(other, &mut value)? {
                        continue;
                    }
                    if other.starts_with('-') {
                        bail!("unknown option: {other}; run `celld r2 --help` for usage")
                    }
                    positional.push(other.to_string());
                }
            }
        }
        if let Some(expiry) = cache_expiry {
            http.insert("cacheExpiry".to_string(), Value::from(expiry));
        }
        // A flag the verb does not read must be a refusal. Accepting
        // `--content-type` on a `get` would tell an operator they set a
        // header that nothing wrote.
        let refuse = |flag: &str, set: bool| -> anyhow::Result<()> {
            if set {
                bail!("`celld r2 {verb}` takes no {flag}");
            }
            Ok(())
        };
        let write_flags = !http.is_empty() || metadata.is_some();
        let one_key = |what: &str| -> anyhow::Result<String> {
            let [key] = positional.as_slice() else {
                bail!("celld r2 {what} needs exactly one key");
            };
            Ok(key.clone())
        };
        let action = match verb.as_str() {
            "get" | "head" => {
                refuse("--path", file.is_some())?;
                refuse("--pipe", pipe)?;
                refuse("--metadata or a content flag", write_flags)?;
                refuse("--delimiter or --page", delimiter.is_some() || page)?;
                let key = one_key(&verb)?;
                match verb.as_str() {
                    "get" => Action::Get { key },
                    _ => Action::Head { key },
                }
            }
            "put" => {
                refuse("--delimiter or --page", delimiter.is_some() || page)?;
                let key = one_key("put")?;
                // Reading a body from a terminal by default would hang
                // with no output, so standard input is opt-in.
                if file.is_some() == pipe {
                    bail!("celld r2 put requires exactly one of --path FILE or --pipe");
                }
                let custom = match &metadata {
                    None => BTreeMap::new(),
                    Some(text) => parse_metadata(text)?,
                };
                Action::Put {
                    key,
                    file,
                    http,
                    custom,
                }
            }
            "delete" => {
                refuse("--path", file.is_some())?;
                refuse("--pipe", pipe)?;
                refuse("--metadata or a content flag", write_flags)?;
                refuse("--delimiter or --page", delimiter.is_some() || page)?;
                if positional.is_empty() {
                    bail!("celld r2 delete needs at least one key");
                }
                Action::Delete { keys: positional }
            }
            "list" => {
                refuse("--path", file.is_some())?;
                refuse("--pipe", pipe)?;
                refuse("--metadata or a content flag", write_flags)?;
                if !positional.is_empty() {
                    bail!("celld r2 list takes no positional arguments; use --prefix");
                }
                if delimiter.as_deref() == Some("") {
                    bail!("--delimiter needs a non-empty value");
                }
                // The groups ride on a page's `delimitedPrefixes`; a stream
                // of rows has nowhere to put them, and a cursor taken from
                // the middle of one would skip or repeat keys.
                if delimiter.is_some() && !page {
                    bail!("--delimiter needs --page, which carries the prefixes and the cursor");
                }
                if page && bounds.all {
                    bail!("--page prints one page, so it cannot be combined with --all");
                }
                Action::List {
                    prefix,
                    bounds: bounds.clone(),
                    delimiter,
                    page,
                }
            }
            _ => unreachable!("the verb was matched above"),
        };
        bounds.validate()?;
        Ok(Some(Self {
            action,
            bucket,
            fleet: fleet.with_environment(),
            json,
        }))
    }
}

/// `--metadata` as R2's `customMetadata`: a flat object of strings.
///
/// A nested value is refused rather than stringified. R2 stores strings, so
/// accepting an object would write `[object Object]` on one side of the
/// binding and a JSON document on the other.
fn parse_metadata(text: &str) -> anyhow::Result<BTreeMap<String, String>> {
    let parsed: Value = serde_json::from_str(text).context("parse --metadata as JSON")?;
    let object = parsed
        .as_object()
        .ok_or_else(|| anyhow!("--metadata takes a JSON object of strings"))?;
    object
        .iter()
        .map(|(name, value)| match value {
            Value::String(text) => Ok((name.clone(), text.clone())),
            _ => Err(anyhow!(
                "--metadata value for {name:?} must be a string, because R2 \
                 stores customMetadata as strings"
            )),
        })
        .collect()
}

pub fn print_help() {
    // The whole block is laid out here, and the content flags are generated
    // from the table the parser reads, so a flag cannot be accepted and
    // undocumented, or documented and rejected. The column is wide enough
    // for `--content-disposition VALUE`, which is the longest of them.
    let row = |flag: String, text: &str| format!("  {flag:<27} {text}");
    let flags = [
        row("--path FILE".to_string(), "read the body from a file"),
        row("--pipe".to_string(), "read the body from standard input"),
        row(
            "--metadata JSON".to_string(),
            "customMetadata, as a JSON object of strings",
        ),
    ]
    .into_iter()
    .chain(HTTP_FLAGS.iter().map(|(flag, field)| {
        row(
            format!("{flag} VALUE"),
            &format!("set httpMetadata.{field}"),
        )
    }))
    .chain(std::iter::once(row(
        "--cache-expiry MS".to_string(),
        "set httpMetadata.cacheExpiry, in unix milliseconds",
    )))
    .collect::<Vec<_>>()
    .join("\n");
    let text = format!(
        "celld r2 — read and write the objects behind an `r2_buckets` binding

USAGE
  celld r2 get    <bucket-name> <key>    [--json] [fleet options]
  celld r2 head   <bucket-name> <key>    [--json] [fleet options]
  celld r2 put    <bucket-name> <key>    --path FILE | --pipe [put options]
  celld r2 delete <bucket-name> <key>... [fleet options]
  celld r2 list   <bucket-name>          [--prefix P] [--delimiter D --page] [listing options]

The bucket name is the `bucket_name` from the project's `r2_buckets` entry,
not the binding name. `get` writes the object to stdout as bytes, so an
artifact comes back byte for byte.

This command reads the fleet bucket directly and needs no running node, so
a release pipeline can publish an object before it deploys the Worker.

PUT OPTIONS
{flags}

LISTING OPTIONS (list)
  --prefix P          list only the keys under this prefix
  --delimiter D       group keys under their first D after the prefix; needs --page
  --page              print one page as a JSON object with `objects`,
                      `delimitedPrefixes`, `truncated` and `cursor`; --limit
                      sizes it and --after CURSOR continues it
{LISTING_HELP}

With --json, `get` and `head` print {{\"error\":\"not_found\",\"bucket\":...,\"key\":...}}
on stdout for an absent key, before the error on stderr and the failing exit.

FLEET OPTIONS
{FLEET_HELP}

An object written here carries the same record as one a Worker writes:
customMetadata, cacheExpiry and the checksums travel in a celld envelope
beside the five content headers. A copy made with another tool still reads
through the binding, and its user metadata becomes its customMetadata."
    );
    let _ = crate::cli_output::Output::new(crate::cli_output::Format::Text).help(&text);
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(arguments: &[&str]) -> anyhow::Result<Option<Command>> {
        Command::parse(
            arguments
                .iter()
                .map(|argument| (*argument).to_owned())
                .collect(),
        )
    }

    fn list_action(arguments: &[&str]) -> (String, Bounds, Option<String>, bool) {
        let command = parse(arguments).unwrap().unwrap();
        match command.action {
            Action::List {
                prefix,
                bounds,
                delimiter,
                page,
            } => (prefix, bounds, delimiter, page),
            _ => panic!("not a list"),
        }
    }

    #[test]
    fn an_absent_key_is_reported_as_one_json_object() {
        assert_eq!(
            not_found_json("assets", "a/b.txt"),
            json!({"error": "not_found", "bucket": "assets", "key": "a/b.txt"})
        );
    }

    #[test]
    fn list_takes_a_delimiter_with_a_page() {
        let (prefix, bounds, delimiter, page) = list_action(&[
            "list",
            "assets",
            "--prefix",
            "a/",
            "--delimiter",
            "/",
            "--page",
            "--limit",
            "5",
            "--after",
            "a/m",
        ]);
        assert_eq!(prefix, "a/");
        assert_eq!(delimiter.as_deref(), Some("/"));
        assert!(page);
        assert_eq!(bounds.limit, 5);
        assert_eq!(bounds.after.as_deref(), Some("a/m"));
    }

    #[test]
    fn list_without_the_new_flags_is_unchanged() {
        let (_, _, delimiter, page) = list_action(&["list", "assets"]);
        assert_eq!(delimiter, None);
        assert!(!page);
    }

    #[test]
    fn a_delimiter_needs_a_page_to_be_resumable() {
        let error = parse(&["list", "assets", "--delimiter", "/"])
            .err()
            .unwrap();
        assert!(error.to_string().contains("--page"), "{error}");
    }

    #[test]
    fn an_empty_delimiter_is_refused() {
        assert!(parse(&["list", "assets", "--delimiter", "", "--page"]).is_err());
    }

    #[test]
    fn a_page_and_a_full_walk_contradict() {
        let error = parse(&["list", "assets", "--page", "--all"]).err().unwrap();
        assert!(error.to_string().contains("--all"), "{error}");
    }

    #[test]
    fn the_listing_flags_are_refused_on_other_verbs() {
        for verb in ["get", "head"] {
            for flag in [&["--page"][..], &["--delimiter", "/"][..]] {
                let mut arguments = vec![verb, "assets", "key"];
                arguments.extend_from_slice(flag);
                let error = parse(&arguments).err().unwrap();
                assert!(error.to_string().contains(verb), "{error}");
            }
        }
        assert!(parse(&["delete", "assets", "key", "--page"]).is_err());
    }

    #[test]
    fn a_page_carries_objects_prefixes_truncation_and_cursor() {
        let listed = json!({
            "objects": [
                {"key": "a/1", "size": 3, "etag": "e1", "uploaded": 10, "http": {}, "custom": {}},
            ],
            "prefixes": ["a/b/"],
            "truncated": true,
            "cursor": "a/b/",
        });
        assert_eq!(
            page_json(&listed).unwrap(),
            json!({
                "objects": [{"key": "a/1", "size": 3, "etag": "e1", "uploaded": 10}],
                "delimitedPrefixes": ["a/b/"],
                "truncated": true,
                "cursor": "a/b/",
            })
        );
    }

    #[test]
    fn a_final_page_has_no_cursor() {
        let listed = json!({"objects": [], "prefixes": [], "truncated": false, "cursor": null});
        assert_eq!(
            page_json(&listed).unwrap(),
            json!({
                "objects": [],
                "delimitedPrefixes": [],
                "truncated": false,
                "cursor": null,
            })
        );
    }
}
