// Copyright 2026 Deno Land Inc. Apache-2.0 license.

// Operator CLI work executes outside the Actor execution domain.
#![allow(clippy::disallowed_methods)]

//! `celld invoke SCRIPT=VERSION [PATH]`: send one HTTP request to a staged
//! version on one node and print the answer.
//!
//! The command needs `CELLD_OPERATOR_TOKEN`, which the node must also have,
//! and the fleet secret in the bucket. A secret header never goes on the
//! command line: `--header-env NAME=VAR` reads the value of `VAR` from the
//! environment of this process, so it appears in neither argv nor the process
//! list, and an unset variable stops the command without printing anything
//! about it.

use std::ffi::OsString;
use std::io::Read;
use std::time::Duration;

use anyhow::{anyhow, bail, Context};
use axum::http::{HeaderName, HeaderValue};
use base64::engine::general_purpose::STANDARD as BASE64;
use base64::Engine as _;

use crate::bucket::Bucket;
use crate::cli_options::{FleetFlags, FLEET_HELP};
use crate::cli_output::{Format, Output};
use crate::invoke::{InvokeRequest, InvokeResponse};
use crate::note;
use crate::operator_cell::{Reachable, Subject};
use crate::peer_auth::PeerAuth;

/// How long one invocation may take end to end: the node's own budget plus
/// the time to reach it.
const TIMEOUT: Duration = Duration::from_secs(75);

#[derive(Debug, Default)]
pub(crate) struct Options {
    fleet: FleetFlags,
    json: bool,
    unsafe_public_advertise: bool,
    method: Option<String>,
    headers: Vec<(String, String)>,
    /// Header name and the variable that holds its value. The value itself is
    /// read at run time, never parsed from the command line.
    header_env: Vec<(String, String)>,
    data: Option<String>,
    data_file: Option<String>,
    positional: Vec<String>,
}

pub(crate) fn options_from_arguments(
    arguments: impl IntoIterator<Item = String>,
) -> anyhow::Result<Option<Options>> {
    let mut options = Options::default();
    let mut arguments = arguments.into_iter();
    while let Some(argument) = arguments.next() {
        let mut value = |option: &str| {
            arguments
                .next()
                .ok_or_else(|| anyhow!("{option} requires a value"))
        };
        match argument.as_str() {
            "-h" | "--help" => return Ok(None),
            "--json" => options.json = true,
            "--unsafe-public-advertise" => options.unsafe_public_advertise = true,
            "-X" | "--request" => options.method = Some(value("--request")?),
            "-H" | "--header" => {
                let header = value("--header")?;
                let (name, text) = header
                    .split_once(':')
                    .ok_or_else(|| anyhow!("--header expects 'Name: value'"))?;
                options
                    .headers
                    .push((name.trim().to_string(), text.trim().to_string()));
            }
            "--header-env" => {
                let spec = value("--header-env")?;
                // The spelling is not echoed: a value typed here by mistake
                // is a secret on a command line already, and an error must
                // not copy it to a log.
                let (name, variable) = spec
                    .split_once('=')
                    .ok_or_else(|| anyhow!("--header-env expects NAME=VARIABLE"))?;
                if !valid_variable_name(variable) {
                    bail!(
                        "--header-env expects the name of an environment variable after \
                         `=`, not a value"
                    );
                }
                options
                    .header_env
                    .push((name.trim().to_string(), variable.to_string()));
            }
            "--data" | "-d" => options.data = Some(value("--data")?),
            "--data-file" => options.data_file = Some(value("--data-file")?),
            other => {
                if options.fleet.consume(other, &mut value)? {
                    continue;
                }
                if other.starts_with('-') && other != "-" {
                    bail!("unknown celld invoke option: {other}; run `celld invoke --help`");
                }
                options.positional.push(other.to_string());
            }
        }
    }
    Ok(Some(options))
}

fn valid_variable_name(name: &str) -> bool {
    let mut bytes = name.bytes();
    bytes
        .next()
        .is_some_and(|first| first.is_ascii_alphabetic() || first == b'_')
        && bytes.all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
}

/// The header a `--header-env` entry names, with its value read through
/// `lookup`. No error here repeats the variable's name or value; each names
/// only the header.
pub(crate) fn resolve_header_env(
    name: &str,
    variable: &str,
    lookup: impl Fn(&str) -> Option<OsString>,
) -> anyhow::Result<(String, String)> {
    HeaderName::from_bytes(name.as_bytes())
        .map_err(|_| anyhow!("--header-env names a header that is not valid"))?;
    let value = lookup(variable)
        .ok_or_else(|| anyhow!("--header-env: the variable for the {name} header is not set"))?;
    let value = value.into_string().map_err(|_| {
        anyhow!("--header-env: the variable for the {name} header is not valid UTF-8")
    })?;
    HeaderValue::from_str(&value).map_err(|_| {
        anyhow!("--header-env: the variable for the {name} header is not a valid header value")
    })?;
    Ok((name.to_string(), value))
}

impl Options {
    /// The request this command sends. `lookup` reads `--header-env`
    /// variables and `stdin` reads `--data-file -`.
    pub(crate) fn request(
        &self,
        lookup: impl Fn(&str) -> Option<OsString>,
        stdin: impl FnOnce() -> anyhow::Result<Vec<u8>>,
    ) -> anyhow::Result<InvokeRequest> {
        let (target, path) = match self.positional.as_slice() {
            [target] => (target, "/"),
            [target, path] => (target, path.as_str()),
            [] => bail!("name the version to invoke: celld invoke SCRIPT=VERSION [PATH]"),
            _ => bail!("celld invoke takes SCRIPT=VERSION and one path"),
        };
        let (script, version) = target
            .split_once('=')
            .ok_or_else(|| anyhow!("expected SCRIPT=VERSION"))?;
        crate::staged::validate_target(script, version)?;

        let body = match (&self.data, &self.data_file) {
            (Some(_), Some(_)) => bail!("use --data or --data-file, not both"),
            (Some(data), None) => data.clone().into_bytes(),
            (None, Some(path)) if path == "-" => stdin()?,
            (None, Some(path)) => {
                std::fs::read(path).with_context(|| format!("read --data-file {path}"))?
            }
            (None, None) => Vec::new(),
        };
        if body.len() > crate::invoke::MAX_BODY_BYTES {
            bail!(
                "the request body is larger than {} bytes",
                crate::invoke::MAX_BODY_BYTES
            );
        }
        let has_body = self.data.is_some() || self.data_file.is_some();
        let method = self
            .method
            .clone()
            .unwrap_or_else(|| if has_body { "POST" } else { "GET" }.to_string());
        let mut headers = self.headers.clone();
        for (name, variable) in &self.header_env {
            headers.push(resolve_header_env(name, variable, &lookup)?);
        }
        Ok(InvokeRequest {
            script: script.to_string(),
            version: version.to_string(),
            method,
            path: path.to_string(),
            headers,
            body: BASE64.encode(body),
        })
    }
}

/// Send `request` to one live node of the fleet in `bucket`.
///
/// `auth` holds the fleet secret and the operator token; the caller builds it
/// so a test can hold its own. The node refuses the request without both.
pub async fn invoke(
    bucket: &Bucket,
    unsafe_public_advertise: bool,
    auth: PeerAuth,
    request: &InvokeRequest,
) -> anyhow::Result<InvokeResponse> {
    let subject = Subject {
        noun: "node",
        source: "invoke",
        timeout: TIMEOUT,
    };
    let reachable = Reachable::open_at(
        bucket,
        unsafe_public_advertise,
        subject,
        crate::invoke::PATH.to_string(),
        Some(auth),
    )
    .await?;
    let result = reachable
        .call(serde_json::to_value(request).context("encode the request")?)
        .await?;
    serde_json::from_value(result).context("decode the node's answer")
}

pub fn help_text() -> String {
    format!(
        r#"celld invoke — send one request to a staged version on one node

USAGE:
  celld invoke SCRIPT=VERSION [PATH] [OPTIONS] --bucket NAME

Run one HTTP request on a version uploaded with `celld deploy --no-activate`.
The node builds the version into a generation of its own, answers this one
request, and retires the generation. Nothing that serves moves: no pointer,
no traffic and no cron schedule changes.

The version runs with the variables and service bindings its own configuration
declares, against the fleet's real data. Durable Object, D1, KV and queue cells
are shared with the serving version and run the serving version's code, so a
version that adds a class or a first storage binding fails when it reaches the
new one. A request that writes through a cell writes real data.

PATH is the path and query to request; it defaults to /.

The command needs CELLD_OPERATOR_TOKEN in its environment, and the node must
have the same token. Without a token on the node, invoke is refused.

OPTIONS:
  -X, --request METHOD   Request method (default: GET, or POST with a body)
  -H, --header 'N: V'    Add a request header. Visible in the process list;
                         use --header-env for a secret
  --header-env NAME=VAR  Add header NAME with the value of the environment
                         variable VAR. An unset VAR stops the command
  -d, --data TEXT        Request body
  --data-file PATH       Request body from a file, or `-` for standard input
  --unsafe-public-advertise
                         Reach a node that advertises a public address
{FLEET_HELP}
  --json                 Print status, headers and base64 body as one JSON object
  -h, --help             Show this help

The response body goes to standard output; the status and headers go to
standard error."#
    )
}

pub async fn run(arguments: Vec<String>) -> anyhow::Result<()> {
    let Some(mut options) = options_from_arguments(arguments)? else {
        return Output::new(Format::Text).help(&help_text());
    };
    let request = options.request(
        |name| std::env::var_os(name),
        || {
            let mut bytes = Vec::new();
            std::io::stdin()
                .read_to_end(&mut bytes)
                .context("read standard input")?;
            Ok(bytes)
        },
    )?;
    let storage = std::mem::take(&mut options.fleet).resolve("celld invoke")?;
    let bucket = storage.open().await?;
    let auth = PeerAuth::new(crate::peer_auth::load_existing(&bucket).await?, "invoke")?;
    if auth.operator_token().is_none() {
        bail!(
            "celld invoke requires {}, set to the token of the node",
            crate::operator_token::ENV
        );
    }
    let response = invoke(&bucket, options.unsafe_public_advertise, auth, &request).await?;
    let body = BASE64
        .decode(response.body.as_bytes())
        .context("decode the response body")?;
    if options.json {
        let mut out = Output::new(Format::Json);
        out.row(&Answer(&response))?;
        return out.finish();
    }
    note!("HTTP {}", response.status);
    for (name, value) in &response.headers {
        note!("{name}: {value}");
    }
    Output::new(Format::Text).bytes(&body)
}

struct Answer<'a>(&'a InvokeResponse);

impl crate::cli_output::Record for Answer<'_> {
    fn json(&self) -> serde_json::Value {
        serde_json::json!({
            "status": self.0.status,
            "headers": self.0.headers,
            "body": self.0.body,
        })
    }

    fn text(&self) -> std::borrow::Cow<'_, str> {
        std::borrow::Cow::Owned(format!("HTTP {}", self.0.status))
    }
}

#[cfg(test)]
#[path = "invoke_cli_tests.rs"]
mod tests;
