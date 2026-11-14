//! Shared by the bench targets: the counting proxy and its key classifier, the
//! end rule that decides when a run has finished, the per-agent request
//! driver, percentiles, and a few environment helpers.
//!
//! Nothing here knows which server is under test. A server only has to speak
//! plain-HTTP, path-style S3 to the proxy and serve `/agent/<name>`.

// Each target uses a different part of this module.
#![allow(dead_code)]

use std::collections::{BTreeMap, HashSet};
use std::convert::Infallible;
use std::io::Write;
use std::net::IpAddr;
use std::path::PathBuf;
use std::process::Command;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use http_body_util::combinators::BoxBody;
use http_body_util::{BodyExt, Full};
use hyper::body::Incoming;
use hyper::service::service_fn;
use hyper::{Method, Request, Response};
use hyper_util::rt::TokioIo;
use regex::Regex;
use serde_json::{json, Value};
use tokio::net::{TcpListener, TcpStream};

/// S3 Standard request prices in dollars, per request.
pub const CLASS_A_PRICE: f64 = 0.005 / 1000.0;
pub const CLASS_B_PRICE: f64 = 0.0004 / 1000.0;
/// The projection: this many agents alive at once, all month.
pub const PROJECTED_CONCURRENT: f64 = 10_000.0;
pub const MONTH_SECONDS: f64 = 730.0 * 3600.0;

/// Requests to this path prefix on the proxy are answered by the proxy itself.
pub const CONTROL: &str = "/.proxy/";

pub type BoxError = Box<dyn std::error::Error + Send + Sync>;

/// How a store bills a request.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Debug)]
pub enum Class {
    /// PUT, COPY, POST and LIST.
    A,
    /// GET and HEAD.
    B,
    /// DELETE.
    Free,
}

pub fn class_of(kind: &str) -> Class {
    match kind {
        "GET" | "HEAD" => Class::B,
        "DELETE" => Class::Free,
        _ => Class::A,
    }
}

/// Whose request it is, by the key it touched.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Debug)]
pub enum Part {
    /// A key that belongs to one agent.
    Agent,
    /// A key that a node shares between agents.
    Shared,
    /// Shared, and steady traffic that goes on without load (leases,
    /// heartbeats). Reported with `Shared`; the end rule can ignore it.
    Background,
}

impl Part {
    fn name(self) -> &'static str {
        match self {
            Part::Agent => "agent",
            Part::Shared => "shared",
            Part::Background => "background",
        }
    }

    fn parse(name: &str) -> Part {
        match name {
            "agent" => Part::Agent,
            "background" => Part::Background,
            _ => Part::Shared,
        }
    }
}

/// A request kind, the shape of the key it touched (digits and ids masked) and
/// whose request it was.
pub type Tally = BTreeMap<(String, String, Part), u64>;

/// Maps a key, relative to the run's root, to a [`Part`].
#[derive(Clone)]
pub struct Classifier {
    agent: Vec<Regex>,
    background: Vec<Regex>,
}

impl Classifier {
    /// The default: everything under `cells/` belongs to one agent.
    pub fn default_layout() -> Classifier {
        Classifier {
            agent: vec![Regex::new("^cells/").expect("default regex")],
            background: Vec::new(),
        }
    }

    /// `BENCH_PER_AGENT_REGEX` (a regex over the key), `BENCH_PER_AGENT_PREFIXES`
    /// (comma-separated key prefixes) and `BENCH_BACKGROUND_REGEX` (steady
    /// shared traffic). Without the first two, the default layout applies.
    pub fn from_env() -> Classifier {
        let mut agent = Vec::new();
        if let Some(pattern) = env_text("BENCH_PER_AGENT_REGEX") {
            agent.push(compile("BENCH_PER_AGENT_REGEX", &pattern));
        }
        if let Some(list) = env_text("BENCH_PER_AGENT_PREFIXES") {
            for prefix in list.split(',').map(str::trim).filter(|p| !p.is_empty()) {
                agent.push(compile(
                    "BENCH_PER_AGENT_PREFIXES",
                    &format!("^{}", regex::escape(prefix)),
                ));
            }
        }
        if agent.is_empty() {
            agent = Self::default_layout().agent;
        }
        let background = env_text("BENCH_BACKGROUND_REGEX")
            .map(|p| vec![compile("BENCH_BACKGROUND_REGEX", &p)])
            .unwrap_or_default();
        Classifier { agent, background }
    }

    pub fn part(&self, key: &str) -> Part {
        if self.background.iter().any(|r| r.is_match(key)) {
            Part::Background
        } else if self.agent.iter().any(|r| r.is_match(key)) {
            Part::Agent
        } else {
            Part::Shared
        }
    }
}

fn compile(name: &str, pattern: &str) -> Regex {
    Regex::new(pattern).unwrap_or_else(|e| panic!("{name}={pattern:?} is not a regex: {e}"))
}

/// Which traffic the end rule watches.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Scope {
    /// Agent keys only. Right when shared keys never go quiet.
    Agent,
    /// Everything except `Background`.
    Foreground,
    /// Every request.
    All,
}

/// When a run has finished: `min_wait` after the last write, then `quiet` with
/// no request in `scope`. Some stores flush to the bucket long after the
/// write, so `min_wait` can be a minute or more.
#[derive(Clone, Debug)]
pub struct EndRule {
    pub min_wait: Duration,
    pub quiet: Duration,
    pub scope: Scope,
    /// A run still making requests this long after `min_wait` fails, rather
    /// than reporting a partial count.
    pub timeout: Duration,
    /// How often the tally is read while waiting for quiet.
    pub poll: Duration,
}

impl EndRule {
    pub fn default_rule() -> EndRule {
        EndRule {
            min_wait: Duration::from_secs(25),
            quiet: Duration::from_secs(10),
            scope: Scope::Agent,
            timeout: Duration::from_secs(300),
            poll: Duration::from_secs(1),
        }
    }

    /// `BENCH_MIN_WAIT_S`, `BENCH_QUIET_S`, `BENCH_QUIET_TIMEOUT_S` and
    /// `BENCH_QUIET_SCOPE` (`agent`, `foreground` or `all`).
    pub fn from_env() -> EndRule {
        let default = EndRule::default_rule();
        let secs =
            |name, fallback: Duration| Duration::from_secs(positive(name, fallback.as_secs()));
        EndRule {
            min_wait: secs("BENCH_MIN_WAIT_S", default.min_wait),
            quiet: secs("BENCH_QUIET_S", default.quiet),
            timeout: secs("BENCH_QUIET_TIMEOUT_S", default.timeout),
            poll: default.poll,
            scope: match env_text("BENCH_QUIET_SCOPE").as_deref() {
                None | Some("agent") => Scope::Agent,
                Some("foreground") => Scope::Foreground,
                Some("all") => Scope::All,
                Some(other) => {
                    panic!("BENCH_QUIET_SCOPE={other:?} must be agent, foreground or all")
                }
            },
        }
    }

    fn watched(&self, tally: &Tally) -> u64 {
        tally
            .iter()
            .filter(|((_, _, part), _)| match self.scope {
                Scope::Agent => *part == Part::Agent,
                Scope::Foreground => *part != Part::Background,
                Scope::All => true,
            })
            .map(|(_, n)| n)
            .sum()
    }

    /// Waits `min_wait` after `last_write`, then until the watched traffic has
    /// been still for `quiet`. Fails when the traffic has not stopped by the
    /// deadline, or the tally cannot be read: a read that stalls is cut off at
    /// the deadline, so the run ends with an error instead of hanging.
    pub async fn wait(&self, source: &TallySource, last_write: Instant) -> Result<(), String> {
        tokio::time::sleep_until((last_write + self.min_wait).into()).await;
        let deadline = Instant::now() + self.timeout;
        let read = || async {
            match tokio::time::timeout_at(deadline.into(), source.take()).await {
                Ok(tally) => tally.map(|tally| self.watched(&tally)),
                Err(_) => Err(format!(
                    "reading the tally was still waiting {:?} after the minimum wait",
                    self.timeout
                )),
            }
        };
        let (mut last, mut since) = (read().await?, Instant::now());
        while since.elapsed() < self.quiet {
            if Instant::now() >= deadline {
                return Err(format!(
                    "{:?} requests still arriving {:?} after the minimum wait",
                    self.scope, self.timeout
                ));
            }
            tokio::time::sleep(self.poll).await;
            let now = read().await?;
            if now != last {
                (last, since) = (now, Instant::now());
            }
        }
        Ok(())
    }
}

/// Where a tally is read from: a proxy in this process or a standalone one.
pub enum TallySource {
    Local(Arc<Mutex<Tally>>),
    Remote {
        client: reqwest::Client,
        url: String,
    },
}

/// The longest a read of a standalone proxy's tally may take.
const TALLY_TIMEOUT: Duration = Duration::from_secs(10);

impl TallySource {
    pub fn remote(proxy_url: &str) -> TallySource {
        TallySource::remote_with_timeout(proxy_url, TALLY_TIMEOUT)
    }

    pub fn remote_with_timeout(proxy_url: &str, timeout: Duration) -> TallySource {
        TallySource::Remote {
            client: reqwest::Client::builder()
                .timeout(timeout)
                .build()
                .expect("build http client"),
            url: format!("{}{CONTROL}tally", proxy_url.trim_end_matches('/')),
        }
    }

    pub async fn take(&self) -> Result<Tally, String> {
        match self {
            TallySource::Local(tally) => Ok(tally.lock().unwrap().clone()),
            TallySource::Remote { client, url } => {
                let response = client
                    .get(url)
                    .send()
                    .await
                    .map_err(|e| format!("could not reach the counting proxy at {url}: {e}"))?;
                let status = response.status();
                if status == reqwest::StatusCode::FORBIDDEN {
                    return Err(format!(
                        "the counting proxy at {url} answered 403: its control routes answer \
                         loopback callers only, so run this on the proxy's host"
                    ));
                }
                if !status.is_success() {
                    return Err(format!("the counting proxy at {url} answered {status}"));
                }
                let body: Value = response
                    .json()
                    .await
                    .map_err(|e| format!("the counting proxy at {url} sent no JSON tally: {e}"))?;
                parse_tally(&body)
            }
        }
    }
}

fn tally_json(tally: &Tally) -> Value {
    json!({
        "requests": tally.iter().map(|((kind, key, part), n)| json!({
            "kind": kind, "key": key, "part": part.name(), "count": n,
        })).collect::<Vec<Value>>(),
    })
}

fn parse_tally(body: &Value) -> Result<Tally, String> {
    let rows = body["requests"]
        .as_array()
        .ok_or("the proxy tally has no `requests` rows")?;
    let mut tally = Tally::new();
    for row in rows {
        let text = |name: &str| {
            row[name]
                .as_str()
                .map(str::to_owned)
                .ok_or_else(|| format!("a tally row has no `{name}`"))
        };
        let count = row["count"].as_u64().ok_or("a tally row has no `count`")?;
        tally.insert(
            (text("kind")?, text("key")?, Part::parse(&text("part")?)),
            count,
        );
    }
    Ok(tally)
}

/// The requests made between two tallies. A tally only grows, so a key that
/// is gone from `after`, or a count that went down, means the proxy's tally
/// was reset between the two: there is no valid difference, and it is an
/// error rather than a wrapped count.
pub fn diff(after: &Tally, before: &Tally) -> Result<Tally, String> {
    for (key, was) in before {
        match after.get(key) {
            Some(now) if now >= was => {}
            _ => {
                return Err(format!(
                    "the tally was reset during the run: {} {} went from {was} to {}",
                    key.0,
                    key.1,
                    after.get(key).copied().unwrap_or(0)
                ))
            }
        }
    }
    Ok(after
        .iter()
        .map(|(key, n)| (key.clone(), n - before.get(key).copied().unwrap_or(0)))
        .filter(|(_, n)| *n > 0)
        .collect())
}

/// A plain-HTTP relay to the store that tallies every request by kind and key
/// shape. Requests pass unchanged, `Host` included, so a SigV4 signature made
/// for the proxy's address still verifies upstream.
///
/// `GET /.proxy/tally` returns the tally as JSON and `POST /.proxy/reset`
/// clears it, so a proxy in another process can be read. Those routes answer
/// callers on a loopback address only, whatever address the proxy listens on:
/// the relay can be reachable from another host, the tally and its reset
/// cannot.
pub struct Proxy {
    pub url: String,
    tally: Arc<Mutex<Tally>>,
    accept: tokio::task::JoinHandle<()>,
}

type ProxyBody = BoxBody<Bytes, hyper::Error>;

impl Proxy {
    /// Listens on `listen` and relays to `upstream` (`host:port`). `root` is
    /// the path every key of the run starts with, which is stripped before a
    /// key is classified and shaped.
    pub async fn start(
        listen: &str,
        upstream: String,
        root: String,
        classifier: Classifier,
    ) -> Proxy {
        let listener = TcpListener::bind(listen).await.expect("bind");
        let url = format!("http://{}", listener.local_addr().expect("address"));
        let tally = Arc::new(Mutex::new(Tally::new()));
        let shared = tally.clone();
        let accept = tokio::spawn(async move {
            while let Ok((stream, peer)) = listener.accept().await {
                let (tally, upstream, root, classifier) = (
                    shared.clone(),
                    upstream.clone(),
                    root.clone(),
                    classifier.clone(),
                );
                let service = service_fn(move |request: Request<Incoming>| {
                    let (tally, upstream) = (tally.clone(), upstream.clone());
                    // The keys of a batch delete are in its body.
                    let kind = kind(&request);
                    let key = relative(&key_of(&request), &root);
                    let (part, shape) = if kind == "BATCH_DELETE" {
                        (Part::Shared, "(batch)".to_owned())
                    } else {
                        (classifier.part(&key), shape(&key))
                    };
                    async move {
                        if request.uri().path().starts_with(CONTROL) {
                            return Ok(control_response(
                                &tally,
                                request.method(),
                                request.uri().path(),
                                peer.ip(),
                            ));
                        }
                        let response = forward(upstream, request).await?;
                        // Counted only once the store answered: a request
                        // that never reached it is not billed.
                        *tally
                            .lock()
                            .unwrap()
                            .entry((kind.to_owned(), shape, part))
                            .or_default() += 1;
                        Ok::<_, BoxError>(response)
                    }
                });
                tokio::spawn(
                    hyper::server::conn::http1::Builder::new()
                        .serve_connection(TokioIo::new(stream), service),
                );
            }
        });
        Proxy { url, tally, accept }
    }

    pub fn source(&self) -> TallySource {
        TallySource::Local(self.tally.clone())
    }

    pub fn take(&self) -> Tally {
        self.tally.lock().unwrap().clone()
    }
}

impl Drop for Proxy {
    fn drop(&mut self) {
        self.accept.abort();
    }
}

fn text_response(status: u16, body: String) -> Response<ProxyBody> {
    let body = Full::new(Bytes::from(body))
        .map_err(|never: Infallible| match never {})
        .boxed();
    Response::builder()
        .status(status)
        .header("content-type", "application/json")
        .body(body)
        .expect("build a response")
}

/// The answer to a request for a control route. Only a loopback `peer` may
/// read or clear the tally.
pub fn control_response(
    tally: &Mutex<Tally>,
    method: &Method,
    path: &str,
    peer: IpAddr,
) -> Response<ProxyBody> {
    if !peer.to_canonical().is_loopback() {
        return text_response(403, "{}".to_owned());
    }
    match (method, path) {
        (&Method::GET, "/.proxy/tally") => {
            text_response(200, tally_json(&tally.lock().unwrap()).to_string())
        }
        (&Method::POST, "/.proxy/reset") => {
            tally.lock().unwrap().clear();
            text_response(200, "{}".to_owned())
        }
        _ => text_response(404, "{}".to_owned()),
    }
}

async fn forward(
    upstream: String,
    request: Request<Incoming>,
) -> Result<Response<ProxyBody>, BoxError> {
    let stream = TcpStream::connect(upstream).await?;
    let (mut sender, connection) =
        hyper::client::conn::http1::handshake(TokioIo::new(stream)).await?;
    tokio::spawn(connection);
    Ok(sender.send_request(request).await?.map(BodyExt::boxed))
}

/// The request kind a store bills: a GET on the bucket itself is a LIST, and
/// a PUT with a copy source is a COPY.
fn kind(request: &Request<Incoming>) -> &'static str {
    let query = request.uri().query().unwrap_or("");
    let on_bucket = request.uri().path().trim_matches('/').split('/').count() <= 1;
    match *request.method() {
        Method::GET if on_bucket || query.contains("list-type") => "LIST",
        Method::GET => "GET",
        Method::HEAD => "HEAD",
        Method::PUT if request.headers().contains_key("x-amz-copy-source") => "COPY",
        Method::PUT => "PUT",
        // DeleteObjects is a POST, billed as class A unlike a single DELETE.
        Method::POST if query.starts_with("delete") => "BATCH_DELETE",
        Method::POST => "POST",
        Method::DELETE => "DELETE",
        _ => "OTHER",
    }
}

/// The object a request names: its path, or for a LIST the bucket and the
/// listed prefix, percent-decoded.
fn key_of(request: &Request<Incoming>) -> String {
    let path = request.uri().path();
    let prefix = request
        .uri()
        .query()
        .unwrap_or("")
        .split('&')
        .find_map(|pair| pair.strip_prefix("prefix="));
    match prefix {
        Some(prefix) => decode(&format!("{}/{prefix}", path.trim_end_matches('/'))),
        None => decode(path),
    }
}

fn decode(text: &str) -> String {
    percent_encoding::percent_decode_str(text)
        .decode_utf8_lossy()
        .into_owned()
}

/// The key without `root` and the leading slash.
fn relative(key: &str, root: &str) -> String {
    let key = key.trim_start_matches('/');
    key.strip_prefix(root.trim_matches('/'))
        .unwrap_or(key)
        .trim_start_matches('/')
        .to_owned()
}

/// The key with long hex ids as `*`, UUIDs as `*` and other numbers as `#`, so
/// requests for different agents and sequence numbers group.
fn shape(key: &str) -> String {
    let mut masked = String::new();
    let mut rest = key;
    while let Some(c) = rest.chars().next() {
        let hex = rest.len()
            - rest
                .trim_start_matches(|c: char| c.is_ascii_hexdigit())
                .len();
        let digits = rest.len() - rest.trim_start_matches(|c: char| c.is_ascii_digit()).len();
        if let Some(len) = uuid_len(rest) {
            masked.push('*');
            rest = &rest[len..];
        } else if hex >= 16 {
            masked.push('*');
            rest = &rest[hex..];
        } else if digits > 0 {
            masked.push('#');
            rest = &rest[digits..];
        } else {
            masked.push(c);
            rest = &rest[c.len_utf8()..];
        }
    }
    masked
}

/// The length of a dashed UUID at the start of `text`, if there is one.
fn uuid_len(text: &str) -> Option<usize> {
    let bytes = text.as_bytes();
    (bytes.len() >= 36
        && [8, 13, 18, 23].iter().all(|&i| bytes[i] == b'-')
        && bytes[..36]
            .iter()
            .enumerate()
            .all(|(i, b)| [8, 13, 18, 23].contains(&i) || b.is_ascii_hexdigit()))
    .then_some(36)
}

/// What a run cost in bucket requests.
pub struct Report {
    pub nodes: usize,
    pub agents: usize,
    /// Requests made while the agents ran.
    pub load: Tally,
    /// How long they ran, settling time excluded.
    pub elapsed: Duration,
    /// Requests made with no load, and over how long: the steady cost of
    /// keeping the system up, which a layout without per-agent keys needs to
    /// subtract.
    pub idle: Option<(Tally, Duration)>,
}

impl Report {
    fn count(&self, class: Class, agent: bool) -> u64 {
        self.load
            .iter()
            .filter(|((kind, _, part), _)| {
                class_of(kind) == class && (*part == Part::Agent) == agent
            })
            .map(|(_, n)| n)
            .sum()
    }

    pub fn per_agent(&self, class: Class) -> f64 {
        self.count(class, true) as f64 / self.agents as f64
    }

    pub fn node_hours(&self) -> f64 {
        self.nodes as f64 * self.elapsed.as_secs_f64() / 3600.0
    }

    pub fn per_node_hour(&self, class: Class) -> f64 {
        self.count(class, false) as f64 / self.node_hours()
    }

    /// Every request of `class` in the run, however it was classified.
    pub fn total(&self, class: Class) -> u64 {
        self.count(class, true) + self.count(class, false)
    }

    /// Requests per second of `class` with no load.
    fn idle_rate(&self, class: Class) -> Option<f64> {
        let (tally, over) = self.idle.as_ref()?;
        let count: u64 = tally
            .iter()
            .filter(|((kind, _, _), _)| class_of(kind) == class)
            .map(|(_, n)| n)
            .sum();
        Some(count as f64 / over.as_secs_f64())
    }

    /// Requests per agent once the idle rate is taken off the total, for a
    /// layout where no key belongs to one agent. `None` without an idle window.
    pub fn net_per_agent(&self, class: Class) -> Option<f64> {
        let rate = self.idle_rate(class)?;
        let net = self.total(class) as f64 - rate * self.elapsed.as_secs_f64();
        Some(net.max(0.0) / self.agents as f64)
    }

    /// Dollars per month of the per-agent requests for `PROJECTED_CONCURRENT`
    /// agents, each living as long as a bench agent does.
    pub fn monthly(&self, lifetime: Duration) -> f64 {
        price(self.per_agent(Class::A), self.per_agent(Class::B), lifetime)
    }

    /// The same projection from the idle-adjusted totals.
    pub fn net_monthly(&self, lifetime: Duration) -> Option<f64> {
        Some(price(
            self.net_per_agent(Class::A)?,
            self.net_per_agent(Class::B)?,
            lifetime,
        ))
    }

    /// Dollars per month of the shared requests of one node under this load.
    pub fn node_monthly(&self) -> f64 {
        MONTH_SECONDS / 3600.0
            * (self.per_node_hour(Class::A) * CLASS_A_PRICE
                + self.per_node_hour(Class::B) * CLASS_B_PRICE)
    }

    pub fn table(&self, errors: usize) -> String {
        let mut text = format!(
            "  {} node(s), {} agents, {} failed requests\n  per 1000 agents: class A {:.0}, class B {:.0}\n",
            self.nodes,
            self.agents,
            errors,
            self.per_agent(Class::A) * 1000.0,
            self.per_agent(Class::B) * 1000.0,
        );
        for ((kind, shape, _), count) in self.load.iter().filter(|((_, _, p), _)| *p == Part::Agent)
        {
            let per_1000 = *count as f64 * 1000.0 / self.agents as f64;
            text += &format!("    {kind:<6} {per_1000:>8.0}  {shape}\n");
        }
        text += &format!(
            "  per node-hour: class A {:.0}, class B {:.0}\n",
            self.per_node_hour(Class::A),
            self.per_node_hour(Class::B),
        );
        for ((kind, shape, _), count) in self.load.iter().filter(|((_, _, p), _)| *p != Part::Agent)
        {
            let rate = *count as f64 / self.node_hours();
            text += &format!("    {kind:<6} {rate:>8.0}  {shape}\n");
        }
        if let (Some(a), Some(b)) = (self.net_per_agent(Class::A), self.net_per_agent(Class::B)) {
            text += &format!(
                "  all requests minus the idle rate, per 1000 agents: class A {:.0}, class B {:.0}\n",
                a * 1000.0,
                b * 1000.0,
            );
        }
        text
    }

    pub fn json(&self, lifetime: Duration) -> Value {
        let idle = self.idle.as_ref().map(|(tally, over)| {
            json!({
                "seconds": over.as_secs_f64(),
                "class_a_per_s": self.idle_rate(Class::A),
                "class_b_per_s": self.idle_rate(Class::B),
                "requests": tally_json(tally)["requests"],
            })
        });
        json!({
            "nodes": self.nodes,
            "agents": self.agents,
            "class_a_per_1000": self.per_agent(Class::A) * 1000.0,
            "class_b_per_1000": self.per_agent(Class::B) * 1000.0,
            "monthly_usd": self.monthly(lifetime),
            "total_class_a": self.total(Class::A),
            "total_class_b": self.total(Class::B),
            "net_class_a_per_1000": self.net_per_agent(Class::A).map(|n| n * 1000.0),
            "net_class_b_per_1000": self.net_per_agent(Class::B).map(|n| n * 1000.0),
            "net_monthly_usd": self.net_monthly(lifetime),
            "elapsed_s": self.elapsed.as_secs_f64(),
            "node_class_a_per_hour": self.per_node_hour(Class::A),
            "node_class_b_per_hour": self.per_node_hour(Class::B),
            "node_monthly_usd": self.node_monthly(),
            "idle": idle,
            "requests": tally_json(&self.load)["requests"],
        })
    }
}

fn price(class_a: f64, class_b: f64, lifetime: Duration) -> f64 {
    let agents = PROJECTED_CONCURRENT * MONTH_SECONDS / lifetime.as_secs_f64();
    agents * (class_a * CLASS_A_PRICE + class_b * CLASS_B_PRICE)
}

/// Runs `agents` agents against `bases` (round-robin): each takes `turns`
/// POSTs `turn` apart and starts evenly over `spread`. Returns the number of
/// failed requests once the last write is done.
pub async fn run_turns(
    client: &reqwest::Client,
    bases: &[String],
    run: &str,
    agents: usize,
    turns: usize,
    turn: Duration,
    spread: Duration,
) -> usize {
    let errors = Arc::new(AtomicUsize::new(0));
    let mut tasks = Vec::new();
    for agent in 0..agents {
        let delay = spread * agent as u32 / agents as u32;
        let url = format!("{}/agent/{run}-{agent}", bases[agent % bases.len()]);
        let (client, errors) = (client.clone(), errors.clone());
        tasks.push(tokio::spawn(async move {
            tokio::time::sleep(delay).await;
            for i in 0..turns {
                let ok = client
                    .post(&url)
                    .send()
                    .await
                    .is_ok_and(|r| r.status().is_success());
                if !ok {
                    errors.fetch_add(1, Ordering::Relaxed);
                }
                if i + 1 < turns {
                    tokio::time::sleep(turn).await;
                }
            }
        }));
    }
    for task in tasks {
        task.await.expect("agent task");
    }
    errors.load(Ordering::Relaxed)
}

/// Nearest-rank percentile of an ascending slice.
pub fn percentile_us(sorted: &[u64], p: f64) -> u64 {
    if sorted.is_empty() {
        return 0;
    }
    let rank = ((p / 100.0) * sorted.len() as f64).ceil() as usize;
    sorted[rank.clamp(1, sorted.len()) - 1]
}

/// The summed resident set, in KiB, of `roots` and every process below them.
/// Fails if `ps` does, or if a listed process is not running: a stale pid must
/// not read as zero memory.
pub fn rss_kib(roots: &[u32]) -> Result<u64, String> {
    let output = Command::new("ps")
        .args(["-A", "-o", "pid=,ppid=,rss="])
        .output()
        .map_err(|e| format!("could not run ps: {e}"))?;
    if !output.status.success() {
        return Err(format!("ps failed: {}", output.status));
    }
    let rows: Vec<(u32, u32, u64)> = String::from_utf8_lossy(&output.stdout)
        .lines()
        .filter_map(|line| {
            let mut cols = line.split_whitespace();
            Some((
                cols.next()?.parse().ok()?,
                cols.next()?.parse().ok()?,
                cols.next()?.parse().ok()?,
            ))
        })
        .collect();
    rss_of_tree(&rows, roots)
}

/// The resident set of `roots` and their descendants in `rows` of
/// `(pid, parent pid, rss)`. Each process counts once, so a root that is also
/// the descendant of another root is not added twice.
pub fn rss_of_tree(rows: &[(u32, u32, u64)], roots: &[u32]) -> Result<u64, String> {
    if let Some(missing) = roots
        .iter()
        .find(|root| !rows.iter().any(|(pid, _, _)| pid == *root))
    {
        return Err(format!("process {missing} is not running"));
    }
    let mut seen = HashSet::new();
    let mut pending = roots.to_vec();
    let mut total = 0;
    while let Some(pid) = pending.pop() {
        if !seen.insert(pid) {
            continue;
        }
        total += rows
            .iter()
            .filter(|(p, _, _)| *p == pid)
            .map(|(_, _, rss)| *rss)
            .sum::<u64>();
        pending.extend(
            rows.iter()
                .filter(|(_, ppid, _)| *ppid == pid)
                .map(|(p, _, _)| *p),
        );
    }
    Ok(total)
}

/// Memory per live agent, in KiB, from the resident set with one agent and
/// with `created` more. `creation` is how long creating them took and `limit`
/// the longest an agent stays resident when idle: past it, early agents may
/// be gone before the sample, so there is no slope to report.
pub fn memory_slope(
    one: u64,
    live: u64,
    created: usize,
    creation: Duration,
    limit: Duration,
) -> Result<f64, String> {
    if created == 0 {
        return Err("no agent was created, so there is nothing to divide by".to_owned());
    }
    if creation >= limit {
        return Err(format!(
            "creating the agents took {:.1} s, not under the {:.1} s they stay resident when idle; \
             lower BENCH_MEM_AGENTS",
            creation.as_secs_f64(),
            limit.as_secs_f64()
        ));
    }
    Ok(live.saturating_sub(one) as f64 / created as f64)
}

/// Where a request that finished `at` after the load started falls against a
/// `warmup` followed by a measured `window`.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum WindowPosition {
    Warmup,
    Measured,
    Over,
}

pub fn window_position(at: Duration, warmup: Duration, window: Duration) -> WindowPosition {
    if at < warmup {
        WindowPosition::Warmup
    } else if at < warmup + window {
        WindowPosition::Measured
    } else {
        WindowPosition::Over
    }
}

/// Whether warm-up requests have established an agent: at least one of the
/// `sent` must have succeeded. Without that, the first measured request may
/// create the agent and its latency would be reported as warm.
pub fn require_warmup(succeeded: usize, sent: usize) -> Result<(), String> {
    if succeeded == 0 {
        return Err(format!(
            "none of {sent} warm-up requests succeeded, so the agent may not exist yet"
        ));
    }
    Ok(())
}

/// The measurement phases of `runtime_latency`.
pub const PHASES: [&str; 5] = ["memory", "cold", "warm", "wake", "throughput"];

/// The phase names in a comma-separated list; an empty list means all. An
/// unknown name is an error, not a skipped phase.
pub fn parse_phases(list: &str) -> Result<Vec<String>, String> {
    let names: Vec<String> = list
        .split(',')
        .map(|name| name.trim().to_owned())
        .filter(|name| !name.is_empty())
        .collect();
    match names.iter().find(|name| !PHASES.contains(&name.as_str())) {
        Some(unknown) => Err(format!(
            "BENCH_PHASES names {unknown:?}, which is not one of {PHASES:?}"
        )),
        None => Ok(names),
    }
}

/// A name for this run that no other run shares: agents named from it are
/// new to the server even when two runs start in the same second.
pub fn run_id(prefix: &str) -> String {
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_nanos());
    format!(
        "{prefix}-{nanos:x}-{:x}-{:x}",
        std::process::id(),
        COUNTER.fetch_add(1, Ordering::Relaxed)
    )
}

pub fn env_text(name: &str) -> Option<String> {
    std::env::var(name).ok().filter(|v| !v.is_empty())
}

/// A setting that must be a whole number above zero: zero seconds or zero
/// connections measure nothing.
pub fn positive<T: std::str::FromStr + Default + PartialOrd>(name: &str, default: T) -> T {
    match std::env::var(name) {
        Ok(value) => value
            .parse()
            .ok()
            .filter(|n| *n > T::default())
            .unwrap_or_else(|| panic!("{name}={value:?} must be a number above zero")),
        Err(_) => default,
    }
}

/// The base URLs in `name`, comma-separated, without a trailing slash.
pub fn base_urls(name: &str) -> Vec<String> {
    let value = env_text(name).unwrap_or_else(|| panic!("{name} must name the server's base URL"));
    value
        .split(',')
        .map(|u| u.trim().trim_end_matches('/').to_owned())
        .filter(|u| !u.is_empty())
        .collect()
}

/// The disallowed-macros policy forbids `println!`, so write to stdout directly.
pub fn out(text: &str) {
    let mut stdout = std::io::stdout().lock();
    let _ = stdout.write_all(text.as_bytes());
    let _ = stdout.flush();
}

/// `target/` of the workspace, following `CARGO_TARGET_DIR` when it is set.
pub fn target_dir() -> PathBuf {
    std::env::var_os("CARGO_TARGET_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../target"))
}

pub fn git(args: &[&str]) -> Option<String> {
    let output = Command::new("git")
        .args(args)
        .current_dir(env!("CARGO_MANIFEST_DIR"))
        .output()
        .ok()?;
    output
        .status
        .success()
        .then(|| String::from_utf8_lossy(&output.stdout).trim().to_owned())
}

pub fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .expect("bind an ephemeral port")
        .local_addr()
        .expect("read local address")
        .port()
}
