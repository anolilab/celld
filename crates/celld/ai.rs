//! The operator's side of the Workers AI binding: the OpenAI-compatible
//! endpoint a Worker's `env.AI.run()` is served from.
//!
//! The configuration belongs to the node and not to a deployment, so a Worker
//! cannot name a different endpoint, read the key, or reach a model the
//! operator did not map. The host makes the HTTP call; the key never enters
//! an isolate.
//!
//! - `CELLD_AI_BASE_URL`: the API root, such as `https://api.openai.com/v1`.
//!   celld appends `/chat/completions` and `/embeddings`.
//! - `CELLD_AI_API_KEY` or `CELLD_AI_API_KEY_FILE`: the bearer token. Optional,
//!   for a server that takes none.
//! - `CELLD_AI_MODELS`: a JSON object from a Workers AI model name to the model
//!   name the endpoint knows.

use std::collections::BTreeMap;
use std::sync::OnceLock;

use anyhow::{anyhow, bail, Context as _};
use reqwest::header::HeaderValue;
use zeroize::Zeroizing;

pub const BASE_URL: &str = "CELLD_AI_BASE_URL";
pub const API_KEY: &str = "CELLD_AI_API_KEY";
pub const API_KEY_FILE: &str = "CELLD_AI_API_KEY_FILE";
pub const MODELS: &str = "CELLD_AI_MODELS";

/// A key is a short line; a larger file is not one.
const KEY_FILE_MAX_BYTES: u64 = 4096;

/// The most of a request body a Worker may send, and of an upstream answer the
/// host reads. Both are held in host memory, outside the isolate's heap limit.
pub const MAX_BODY_BYTES: usize = 32 << 20;

/// The shortest start of the key that is dropped from the end of an error
/// body that may have been cut inside the key.
const MIN_KEY_PREFIX: usize = 4;

/// Which OpenAI-compatible route a `run()` call goes to.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Route {
    Chat,
    Embeddings,
}

impl Route {
    pub fn parse(name: &str) -> Option<Self> {
        match name {
            "chat" => Some(Self::Chat),
            "embeddings" => Some(Self::Embeddings),
            _ => None,
        }
    }

    fn path(self) -> &'static str {
        match self {
            Self::Chat => "/chat/completions",
            Self::Embeddings => "/embeddings",
        }
    }
}

fn too_large() -> String {
    format!(
        "AI request body is larger than {} MiB",
        MAX_BODY_BYTES >> 20
    )
}

pub struct Config {
    base: String,
    key: Option<Zeroizing<String>>,
    /// `Bearer <key>`, marked sensitive so it never prints.
    bearer: Option<HeaderValue>,
    models: BTreeMap<String, String>,
}

impl std::fmt::Debug for Config {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ai::Config")
            .field("base", &self.base)
            .field("key", &self.key.as_ref().map(|_| ".."))
            .field("models", &self.models)
            .finish()
    }
}

/// One upstream call, ready to send.
pub struct Request {
    pub url: String,
    pub body: Vec<u8>,
    pub bearer: Option<HeaderValue>,
}

impl Config {
    /// The request for `model` (a Workers AI name) with the OpenAI-shaped
    /// `body` the harness built. The upstream model name replaces the
    /// `model` field, whatever the Worker put there.
    pub fn request(&self, route: Route, model: &str, body: &str) -> Result<Request, String> {
        if body.len() > MAX_BODY_BYTES {
            return Err(too_large());
        }
        let upstream = self.models.get(model).ok_or_else(|| {
            format!("Workers AI model {model:?} is not mapped; add it to {MODELS} on this node")
        })?;
        let mut body: serde_json::Value =
            serde_json::from_str(body).map_err(|error| format!("AI request body: {error}"))?;
        let object = body
            .as_object_mut()
            .ok_or("AI request body must be a JSON object")?;
        object.insert("model".into(), upstream.clone().into());
        let body = serde_json::to_vec(&body).map_err(|error| error.to_string())?;
        // The model name makes the body longer than the Worker's.
        if body.len() > MAX_BODY_BYTES {
            return Err(too_large());
        }
        Ok(Request {
            url: format!("{}{}", self.base, route.path()),
            body,
            bearer: self.bearer.clone(),
        })
    }

    /// `text` with every occurrence of the key removed, for an upstream error
    /// that echoes the credential it was sent.
    pub fn redact(&self, text: &str) -> String {
        match &self.key {
            Some(key) => text.replace(key.as_str(), "[redacted]"),
            None => text.to_string(),
        }
    }
}

impl Config {
    fn redact_json(&self, value: &mut serde_json::Value) {
        match value {
            serde_json::Value::String(text) => *text = self.redact(text),
            serde_json::Value::Array(items) => items.iter_mut().for_each(|v| self.redact_json(v)),
            serde_json::Value::Object(map) => {
                let entries = std::mem::take(map);
                for (name, mut item) in entries {
                    self.redact_json(&mut item);
                    map.insert(self.redact(&name), item);
                }
            }
            _ => {}
        }
    }

    /// How many bytes of an upstream error to read so that `cut_detail` can
    /// still see a whole key that starts inside the first `max`.
    fn error_read_limit(&self, max: usize) -> usize {
        max + self.key.as_ref().map_or(0, |key| key.len())
    }

    /// The first `max` bytes of the body of an upstream error `response`, with
    /// the key removed: the body is read far enough to hold a whole key, and
    /// redacted before it is cut, so that no part of the key survives at the
    /// edge.
    pub async fn error_detail(&self, mut response: reqwest::Response, max: usize) -> String {
        let limit = self.error_read_limit(max);
        let mut raw = Vec::new();
        while raw.len() < limit {
            match response.chunk().await {
                Ok(Some(chunk)) => raw.extend_from_slice(&chunk),
                _ => break,
            }
        }
        raw.truncate(limit);
        self.cut_detail(&raw, max)
    }

    fn cut_detail(&self, raw: &[u8], max: usize) -> String {
        let mut raw = raw;
        if let Some(key) = &self.key {
            // A read that stopped at the limit, on an error or on a timeout
            // ends mid-text, so its end may hold a cut key: drop a tail that
            // is the start of the key.
            let cut = (MIN_KEY_PREFIX.min(key.len())..key.len())
                .rev()
                .find(|&n| raw.ends_with(&key.as_bytes()[..n]))
                .unwrap_or(0);
            raw = &raw[..raw.len() - cut];
        }
        let text = String::from_utf8_lossy(raw);
        let mut text = match serde_json::from_str::<serde_json::Value>(text.trim()) {
            // A JSON body may write the key with escapes, which only its
            // decoded strings show.
            Ok(mut value) => {
                self.redact_json(&mut value);
                value.to_string()
            }
            Err(_) => self.redact(text.trim()),
        };
        let mut end = max.min(text.len());
        while !text.is_char_boundary(end) {
            end -= 1;
        }
        text.truncate(end);
        text
    }
}

/// The node's configuration, read once. `None` when `CELLD_AI_BASE_URL` is
/// unset. A malformed setting is an error here and at startup, never a silent
/// default.
pub fn load() -> anyhow::Result<Option<&'static Config>> {
    static CONFIG: OnceLock<Option<Config>> = OnceLock::new();
    cached(&CONFIG, read)
}

/// Checks the settings at startup without keeping them. The log is not set up
/// yet, so a warning about the key file would go nowhere, and a kept result
/// would keep the first real `load` from repeating it.
pub fn validate() -> anyhow::Result<()> {
    validate_with(read)
}

const SHARED_KEY_FILE: crate::credential_file::SharedWarning =
    crate::credential_file::SharedWarning {
        event: "ai_key_file_shared",
        subject: "AI API key file",
    };

fn validate_with(read: impl FnOnce() -> anyhow::Result<Option<Config>>) -> anyhow::Result<()> {
    read().map(drop)
}

fn cached(
    cell: &OnceLock<Option<Config>>,
    read: impl FnOnce() -> anyhow::Result<Option<Config>>,
) -> anyhow::Result<Option<&Config>> {
    if let Some(config) = cell.get() {
        return Ok(config.as_ref());
    }
    let config = read()?;
    Ok(cell.get_or_init(|| config).as_ref())
}

fn read() -> anyhow::Result<Option<Config>> {
    let key = crate::credential_file::variable_or_file(
        API_KEY,
        API_KEY_FILE,
        KEY_FILE_MAX_BYTES,
        &SHARED_KEY_FILE,
    )?;
    parse(
        crate::env_vars::value(BASE_URL)?.as_deref(),
        key.as_ref()
            .map(|key| key.text.trim_end_matches(['\r', '\n'])),
        crate::env_vars::value(MODELS)?.as_deref(),
    )
}

fn parse(
    base: Option<&str>,
    key: Option<&str>,
    models: Option<&str>,
) -> anyhow::Result<Option<Config>> {
    let Some(base) = base else {
        if key.is_some() || models.is_some() {
            bail!("{BASE_URL} is required when {API_KEY}, {API_KEY_FILE} or {MODELS} is set");
        }
        return Ok(None);
    };
    let parsed = url::Url::parse(base).with_context(|| format!("{BASE_URL} is not a valid URL"))?;
    anyhow::ensure!(
        matches!(parsed.scheme(), "http" | "https") && parsed.has_host(),
        "{BASE_URL} must be an http or https URL"
    );
    anyhow::ensure!(
        parsed.username().is_empty()
            && parsed.password().is_none()
            && parsed.query().is_none()
            && parsed.fragment().is_none(),
        "{BASE_URL} must not carry credentials, a query or a fragment; set the key in {API_KEY_FILE}"
    );
    let key = key
        .map(|key| {
            anyhow::ensure!(
                !key.is_empty() && key.bytes().all(|byte| byte.is_ascii_graphic()),
                "the AI API key ({API_KEY} or {API_KEY_FILE}) is empty or has characters a header cannot carry"
            );
            Ok(Zeroizing::new(key.to_string()))
        })
        .transpose()?;
    let bearer = key
        .as_ref()
        .map(|key| {
            let line = Zeroizing::new(format!("Bearer {}", key.as_str()));
            let mut value = HeaderValue::from_str(&line)?;
            value.set_sensitive(true);
            anyhow::Ok(value)
        })
        .transpose()?;
    let models: BTreeMap<String, String> = serde_json::from_str(
        models.ok_or_else(|| anyhow!("{MODELS} is required when {BASE_URL} is set"))?,
    )
    .with_context(|| format!("{MODELS} must be a JSON object of model names to model names"))?;
    anyhow::ensure!(!models.is_empty(), "{MODELS} maps no model");
    anyhow::ensure!(
        models
            .iter()
            .all(|(from, to)| !from.is_empty() && !to.is_empty()),
        "{MODELS} has an empty model name"
    );
    Ok(Some(Config {
        // The parsed URL, so that a stray space around the value is not sent.
        base: parsed.as_str().trim_end_matches('/').to_string(),
        key,
        bearer,
        models,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    const MODELS_JSON: &str = r#"{"@cf/meta/llama-3.1-8b-instruct":"gpt-4o-mini"}"#;

    fn config(key: Option<&str>) -> Config {
        parse(Some("https://api.example.com/v1/"), key, Some(MODELS_JSON))
            .unwrap()
            .unwrap()
    }

    #[derive(Clone, Default)]
    struct Log(std::sync::Arc<std::sync::Mutex<Vec<u8>>>);

    impl std::io::Write for Log {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(bytes);
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    #[cfg(unix)]
    #[test]
    #[allow(clippy::disallowed_methods)] // A fixture file, not node storage.
    fn the_shared_key_file_warning_is_logged_by_the_first_load_after_startup_validation() {
        use std::os::unix::fs::PermissionsExt as _;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("key");
        std::fs::write(&path, "sk-secret\n").unwrap();
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o644)).unwrap();
        let path = path.to_str().unwrap().to_string();
        // What `read` does with `CELLD_AI_API_KEY_FILE`, without the process
        // environment.
        let read = || {
            let key = crate::credential_file::choose(
                (API_KEY, None),
                (API_KEY_FILE, Some(path.clone())),
                KEY_FILE_MAX_BYTES,
                &SHARED_KEY_FILE,
            )?;
            parse(
                Some("https://api.example.com/v1/"),
                key.as_ref().map(|key| key.text.trim()),
                Some(MODELS_JSON),
            )
        };

        let log = Log::default();
        let writer = log.clone();
        let subscriber = tracing_subscriber::fmt()
            .with_writer(move || writer.clone())
            .with_ansi(false)
            .finish();
        // Startup validation runs before any subscriber exists.
        validate_with(read).unwrap();
        assert!(log.0.lock().unwrap().is_empty());
        let cell = OnceLock::new();
        tracing::subscriber::with_default(subscriber, || {
            cached(&cell, read).unwrap();
            cached(&cell, read).unwrap();
        });
        let text = String::from_utf8(log.0.lock().unwrap().clone()).unwrap();
        assert_eq!(text.matches("ai_key_file_shared").count(), 1, "{text}");
    }

    #[test]
    fn nothing_set_is_off() {
        assert!(parse(None, None, None).unwrap().is_none());
    }

    #[test]
    fn other_settings_without_a_base_url_are_refused() {
        for (key, models) in [(Some("k"), None), (None, Some(MODELS_JSON))] {
            let error = parse(None, key, models).unwrap_err().to_string();
            assert!(error.contains(BASE_URL), "{error}");
        }
    }

    #[test]
    fn a_request_names_the_upstream_model_and_route() {
        let config = config(Some("sk-secret"));
        let request = config
            .request(
                Route::Chat,
                "@cf/meta/llama-3.1-8b-instruct",
                r#"{"model":"spoofed","messages":[]}"#,
            )
            .unwrap();
        assert_eq!(request.url, "https://api.example.com/v1/chat/completions");
        let bearer = request.bearer.as_ref().unwrap();
        assert_eq!(bearer, "Bearer sk-secret");
        assert!(bearer.is_sensitive());
        assert!(!format!("{bearer:?}").contains("sk-secret"));
        let body: serde_json::Value = serde_json::from_slice(&request.body).unwrap();
        assert_eq!(body["model"], "gpt-4o-mini");
        let request = config
            .request(Route::Embeddings, "@cf/meta/llama-3.1-8b-instruct", "{}")
            .unwrap();
        assert_eq!(request.url, "https://api.example.com/v1/embeddings");
    }

    #[test]
    fn an_unmapped_model_is_named() {
        let error = config(None)
            .request(Route::Chat, "@cf/other/model", "{}")
            .err()
            .unwrap();
        assert!(error.contains("@cf/other/model"), "{error}");
        assert!(error.contains(MODELS), "{error}");
    }

    #[test]
    fn the_key_never_prints() {
        let config = config(Some("sk-secret"));
        assert!(!format!("{config:?}").contains("sk-secret"));
        assert_eq!(config.redact("bad key sk-secret"), "bad key [redacted]");
    }

    #[test]
    fn a_key_at_the_edge_of_an_error_leaves_no_prefix() {
        let key = "sk-secret-key-0123456789";
        let config = config(Some(key));
        let max = 64;
        for start in max - key.len()..=max + 2 {
            let mut raw = "x".repeat(start).into_bytes();
            raw.extend_from_slice(key.as_bytes());
            raw.extend_from_slice("y".repeat(max * 2).as_bytes());
            raw.truncate(config.error_read_limit(max));
            let detail = config.cut_detail(&raw, max);
            assert!(detail.len() <= max, "{start}");
            for n in 4..=key.len() {
                assert!(!detail.contains(&key[..n]), "{start}: {detail}");
            }
        }
    }

    #[test]
    fn a_body_cut_inside_the_key_leaves_no_prefix() {
        // The read ended early, on a reset or a timeout, not at the limit.
        let key = "sk-secret-key-0123456789";
        let config = config(Some(key));
        let raw = format!("boom, your key {}", &key[..10]);
        let detail = config.cut_detail(raw.as_bytes(), 2048);
        assert_eq!(detail, "boom, your key");
    }

    #[test]
    fn a_json_error_that_writes_the_key_with_escapes_is_redacted() {
        let config = config(Some("sk-a/b-key-0123"));
        let raw = br#"{"error":{"message":"bad key sk-a\/b-key-0123 and \u0073k-a/b-key-0123"}}"#;
        let detail = config.cut_detail(raw, 2048);
        assert!(!detail.contains("key-0123"), "{detail}");
        assert!(detail.contains("[redacted]"), "{detail}");
    }

    #[test]
    fn a_stray_space_around_the_base_url_is_not_sent() {
        let config = parse(
            Some(" https://api.example.com/v1/ "),
            None,
            Some(MODELS_JSON),
        )
        .unwrap()
        .unwrap();
        let request = config
            .request(Route::Chat, "@cf/meta/llama-3.1-8b-instruct", "{}")
            .unwrap();
        assert_eq!(request.url, "https://api.example.com/v1/chat/completions");
    }

    #[test]
    fn an_oversized_request_body_is_refused() {
        let body = format!(r#"{{"prompt":"{}"}}"#, "x".repeat(MAX_BODY_BYTES));
        let error = config(None)
            .request(Route::Chat, "@cf/meta/llama-3.1-8b-instruct", &body)
            .err()
            .unwrap();
        assert!(error.contains("larger than"), "{error}");
    }

    #[test]
    fn a_body_the_model_name_pushes_past_the_limit_is_refused() {
        // Under the limit as the Worker sent it, over it with the model added.
        let head = r#"{"prompt":""#;
        let tail = r#""}"#;
        let fill = MAX_BODY_BYTES - head.len() - tail.len() - 2;
        let body = format!("{head}{}{tail}", "x".repeat(fill));
        assert_eq!(body.len(), MAX_BODY_BYTES - 2);
        let error = config(None)
            .request(Route::Chat, "@cf/meta/llama-3.1-8b-instruct", &body)
            .err()
            .unwrap();
        assert!(error.contains("larger than"), "{error}");
    }

    #[test]
    fn malformed_settings_are_refused() {
        for (base, key, models) in [
            ("ftp://h/v1", None, MODELS_JSON),
            ("https://user:pw@h/v1", None, MODELS_JSON),
            ("https://h/v1?key=1", None, MODELS_JSON),
            ("https://h/v1", Some("two words"), MODELS_JSON),
            ("https://h/v1", Some(""), MODELS_JSON),
            ("https://h/v1", None, "{}"),
            ("https://h/v1", None, r#"{"a":1}"#),
            ("https://h/v1", None, "not json"),
        ] {
            assert!(
                parse(Some(base), key, Some(models)).is_err(),
                "{base} {key:?} {models}"
            );
        }
        assert!(parse(Some("https://h/v1"), None, None).is_err());
    }
}
