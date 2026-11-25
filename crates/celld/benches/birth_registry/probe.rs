//! Reads of the nodes' debug routes, the lookup load, the completeness checks
//! and the gate table.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use serde_json::{json, Value};

use super::{positive, Endpoints};

pub async fn jget(client: &reqwest::Client, url: &str) -> Option<Value> {
    let response = client
        .get(url)
        .timeout(Duration::from_secs(20))
        .send()
        .await
        .ok()?;
    if !response.status().is_success() {
        return None;
    }
    response.json().await.ok()
}

pub async fn jpost(client: &reqwest::Client, url: &str, body: Value) -> Option<Value> {
    let response = client
        .post(url)
        .timeout(Duration::from_secs(20))
        .json(&body)
        .send()
        .await
        .ok()?;
    if !response.status().is_success() {
        return None;
    }
    response.json().await.ok()
}

pub fn nearest(values: &mut [u64], percent: f64) -> u64 {
    values.sort_unstable();
    crate::support::percentile_us(values, percent)
}

pub fn ms_p(values: &[u64], percent: f64) -> f64 {
    let mut values = values.to_vec();
    nearest(&mut values, percent) as f64
}

pub fn us_to_ms_p(values: &[u64], percent: f64) -> f64 {
    ms_p(values, percent) / 1000.0
}

/// A percentile in milliseconds of microsecond samples, or null with none: a
/// gate that reads null fails instead of reading a percentile of nothing as 0.
pub fn us_to_ms_p_json(values: &[u64], percent: f64) -> Value {
    if values.is_empty() {
        Value::Null
    } else {
        json!(us_to_ms_p(values, percent))
    }
}

/// What the aging tool left for the bench: keys that exist and keys that do not.
#[derive(Default, Clone)]
pub struct Samples {
    pub aged: Vec<AgedPartition>,
    pub others: Vec<String>,
}

#[derive(Clone)]
pub struct AgedPartition {
    pub index: u32,
    pub months: u64,
    pub exist: Vec<String>,
    pub absent: Vec<String>,
    pub l1: Vec<String>,
}

pub fn load_samples(dir: &str, run: &str) -> Samples {
    let read = |name: &str| -> Value {
        let path = std::path::Path::new(dir).join(format!("{run}-{name}.json"));
        serde_json::from_str(
            &std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("{path:?}: {e}")),
        )
        .expect("json")
    };
    let (samples, synth) = (read("samples"), read("synth"));
    let list = |value: &Value| -> Vec<String> {
        value
            .as_array()
            .map(|rows| {
                rows.iter()
                    .filter_map(|row| row.as_str().map(str::to_owned))
                    .collect()
            })
            .unwrap_or_default()
    };
    let months: std::collections::HashMap<u64, u64> = synth["aged"]
        .as_array()
        .unwrap()
        .iter()
        .map(|row| {
            (
                row["partition"].as_u64().unwrap(),
                row["months"].as_u64().unwrap(),
            )
        })
        .collect();
    let mut aged: Vec<AgedPartition> = samples["aged"]
        .as_object()
        .unwrap()
        .iter()
        .map(|(index, value)| {
            let index: u64 = index.parse().unwrap();
            AgedPartition {
                index: index as u32,
                months: months[&index],
                exist: list(&value["exist"]),
                absent: list(&value["absent"]),
                l1: list(&value["l1"]),
            }
        })
        .collect();
    aged.sort_by_key(|partition| (partition.months, partition.index));
    Samples {
        aged,
        others: list(&samples["others_exist"]),
    }
}

#[derive(Default)]
pub struct Lookups {
    pub total: AtomicU64,
    pub exists: AtomicU64,
    pub new: AtomicU64,
    pub unknown: AtomicU64,
    pub wrong: AtomicU64,
    pub failed: AtomicU64,
    pub rtt_us: Mutex<Vec<u64>>,
    pub node_us: Mutex<Vec<u64>>,
    pub gets: Mutex<Vec<u64>>,
    pub bytes: Mutex<Vec<u64>>,
    pub wrong_keys: Mutex<Vec<String>>,
    pub stop: AtomicBool,
}

impl Lookups {
    pub fn summary(&self) -> Value {
        let (rtt, node) = (
            self.rtt_us.lock().unwrap().clone(),
            self.node_us.lock().unwrap().clone(),
        );
        let (gets, bytes) = (
            self.gets.lock().unwrap().clone(),
            self.bytes.lock().unwrap().clone(),
        );
        let total = self.total.load(Ordering::Relaxed);
        let failed = self.failed.load(Ordering::Relaxed);
        // The lookups a node answered: a request that failed, timed out or got
        // a status other than 2xx says nothing about `Unknown`.
        let answered = total.saturating_sub(failed);
        // A lookup that read more than the common three GETs or 16 KiB loaded a
        // delta (or a map, or met a changed run): the cause G-a's relative clause
        // would fail on, apart from the common one-round-trip lookup.
        let (mut heavy_us, mut light_us) = (Vec::new(), Vec::new());
        for ((micros, gets), bytes) in node.iter().zip(&gets).zip(&bytes) {
            if *gets > 3 || *bytes > 16 * 1024 {
                heavy_us.push(*micros);
            } else {
                light_us.push(*micros);
            }
        }
        json!({
            "heavy": { "n": heavy_us.len(), "share_pct": heavy_us.len() as f64 * 100.0 / node.len().max(1) as f64,
                       "node_p50_ms": us_to_ms_p(&heavy_us, 50.0), "node_p99_ms": us_to_ms_p(&heavy_us, 99.0) },
            "light": { "n": light_us.len(), "node_p50_ms": us_to_ms_p(&light_us, 50.0), "node_p99_ms": us_to_ms_p(&light_us, 99.0) },
            "total": total,
            "exists": self.exists.load(Ordering::Relaxed),
            "new": self.new.load(Ordering::Relaxed),
            "unknown": self.unknown.load(Ordering::Relaxed),
            "answered": answered,
            "unknown_pct": self.unknown.load(Ordering::Relaxed) as f64 * 100.0 / answered.max(1) as f64,
            // Every request got an answer, and there was at least one.
            "complete": failed == 0 && answered > 0,
            "wrong": self.wrong.load(Ordering::Relaxed),
            "failed": self.failed.load(Ordering::Relaxed),
            "node_p50_ms": us_to_ms_p_json(&node, 50.0),
            "node_p99_ms": us_to_ms_p_json(&node, 99.0),
            "node_max_ms": us_to_ms_p_json(&node, 100.0),
            "rtt_p99_ms": us_to_ms_p_json(&rtt, 99.0),
            "gets_mean": gets.iter().sum::<u64>() as f64 / gets.len().max(1) as f64,
            "gets_max": gets.iter().max().copied().unwrap_or(0),
            "bytes_mean": bytes.iter().sum::<u64>() as f64 / bytes.len().max(1) as f64,
            "bytes_max": bytes.iter().max().copied().unwrap_or(0),
            "wrong_keys": self.wrong_keys.lock().unwrap().iter().take(10).cloned().collect::<Vec<_>>(),
        })
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub enum Expect {
    Exists,
    New,
}

/// One lookup on `node`. `None` if the node did not answer.
pub async fn lookup(
    ep: &Endpoints,
    node: usize,
    key: &str,
) -> Option<(String, Value, u64, u64, u64)> {
    let value = jget(
        &ep.client,
        &format!("{}/__registry/lookup?key={key}", ep.internal(node)),
    )
    .await?;
    Some((
        value["answer"].as_str()?.to_owned(),
        // An unknown answer carries its doubt where a known one carries its record.
        if value["answer"] == "unknown" {
            json!({ "doubt": value["doubt"] })
        } else {
            value["record"].clone()
        },
        value["gets"].as_u64()?,
        value["bytes"].as_u64()?,
        value["micros"].as_u64()?,
    ))
}

/// Record one lookup into `stats`. Returns whether the answer was wrong.
pub fn record(
    stats: &Lookups,
    key: &str,
    expect: Expect,
    rtt: u64,
    got: Option<(String, Value, u64, u64, u64)>,
) -> bool {
    stats.total.fetch_add(1, Ordering::Relaxed);
    let Some((answer, _, gets, bytes, micros)) = got else {
        stats.failed.fetch_add(1, Ordering::Relaxed);
        return false;
    };
    stats.rtt_us.lock().unwrap().push(rtt);
    stats.node_us.lock().unwrap().push(micros);
    stats.gets.lock().unwrap().push(gets);
    stats.bytes.lock().unwrap().push(bytes);
    let wrong = match answer.as_str() {
        "exists" => {
            stats.exists.fetch_add(1, Ordering::Relaxed);
            expect == Expect::New
        }
        "new" => {
            stats.new.fetch_add(1, Ordering::Relaxed);
            expect == Expect::Exists
        }
        _ => {
            stats.unknown.fetch_add(1, Ordering::Relaxed);
            false
        }
    };
    if wrong {
        stats.wrong.fetch_add(1, Ordering::Relaxed);
        stats.wrong_keys.lock().unwrap().push(format!(
            "{key} expected {} got {answer}",
            if expect == Expect::New {
                "new"
            } else {
                "exists"
            }
        ));
    }
    wrong
}

/// A steady lookup load over the aged partitions: keys that exist and keys
/// that do not, `rate` a second spread over the nodes that answer.
pub async fn lookup_load(
    ep: Endpoints,
    samples: Samples,
    rate: f64,
    stats: Arc<Lookups>,
    extra: Arc<Mutex<Vec<String>>>,
) {
    let mut at = 0_usize;
    let mut tick = tokio::time::interval(Duration::from_secs_f64(1.0 / rate));
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    while !stats.stop.load(Ordering::Relaxed) {
        tick.tick().await;
        at += 1;
        let (key, expect) = pick(&samples, at, &extra);
        let node = at % ep.count();
        let began = Instant::now();
        let got = lookup(&ep, node, &key).await;
        record(
            &stats,
            &key,
            expect,
            began.elapsed().as_micros() as u64,
            got,
        );
    }
}

fn pick(samples: &Samples, at: usize, extra: &Arc<Mutex<Vec<String>>>) -> (String, Expect) {
    let aged = &samples.aged[at % samples.aged.len().max(1)];
    match at % 5 {
        0 | 1 => (
            aged.exist[(at / 5) % aged.exist.len()].clone(),
            Expect::Exists,
        ),
        2 => (
            aged.absent[(at / 5) % aged.absent.len()].clone(),
            Expect::New,
        ),
        3 if !aged.l1.is_empty() => (aged.l1[(at / 5) % aged.l1.len()].clone(), Expect::Exists),
        _ => {
            let extra = extra.lock().unwrap();
            if extra.is_empty() || at.is_multiple_of(2) {
                (
                    samples.others[at % samples.others.len().max(1)].clone(),
                    Expect::Exists,
                )
            } else {
                (extra[at % extra.len()].clone(), Expect::Exists)
            }
        }
    }
}

pub struct Check {
    pub total: u64,
    pub exists: u64,
    pub new: u64,
    pub unknown: u64,
    /// Not the expected answer after one retry.
    pub misses: u64,
    pub first_misses: Vec<String>,
    pub bases: u64,
    /// The unknown answers by the doubt the node gave; `no_answer` when the
    /// debug request itself failed or ran out of time.
    pub doubts: std::collections::BTreeMap<String, u64>,
}

impl Check {
    pub fn json(&self) -> Value {
        json!({
            "total": self.total, "exists": self.exists, "new": self.new, "unknown": self.unknown,
            "misses": self.misses, "first_misses": self.first_misses, "bases": self.bases,
            "doubts": self.doubts,
        })
    }
}

/// What one lookup of a node's debug route answered.
pub type Looked = Option<(String, Value, u64, u64, u64)>;

/// Lookups a check sends per second, all workers together. The store a node
/// reads may close every connection (a gateway that answers `Connection:
/// close`), and each lookup is three reads: unlimited, 32 workers use up the
/// host's ephemeral ports within seconds, every later connect fails, the nodes
/// miss their lease renewals and halt, and the check then waits on dead nodes.
const CHECK_PER_S: f64 = 100.0;

/// Look up every key, `parallel` at a time across the live nodes, at most
/// `CELLD_REG_CHECK_PER_S` lookups a second (default 100). A miss is retried
/// once after 2 s, then counted.
pub async fn check_keys(
    ep: &Endpoints,
    keys: Vec<(String, Expect)>,
    parallel: usize,
    nodes: &[usize],
) -> Check {
    let per_s = positive("CELLD_REG_CHECK_PER_S", CHECK_PER_S);
    let ep = ep.clone();
    check_with(
        keys,
        parallel,
        nodes,
        per_s,
        Duration::from_secs(60),
        move |node, key| {
            let ep = ep.clone();
            async move { lookup(&ep, node, &key).await }
        },
    )
    .await
}

/// `check_keys` over any lookup. The whole check has a deadline, three times
/// the time its rate needs plus `slack`; keys not answered by then are counted
/// as misses, so a store or a node that is gone ends the check instead of
/// holding it.
pub async fn check_with<F, Fut>(
    keys: Vec<(String, Expect)>,
    parallel: usize,
    nodes: &[usize],
    per_s: f64,
    slack: Duration,
    lookup: F,
) -> Check
where
    F: Fn(usize, String) -> Fut + Clone + Send + Sync + 'static,
    Fut: std::future::Future<Output = Looked> + Send,
{
    let total = keys.len() as u64;
    let deadline =
        tokio::time::Instant::now() + Duration::from_secs_f64(total as f64 / per_s * 3.0) + slack;
    let gap = Duration::from_secs_f64(1.0 / per_s);
    let next_slot = Arc::new(Mutex::new(tokio::time::Instant::now()));
    let queue = Arc::new(Mutex::new(keys));
    let check = Arc::new(Mutex::new(Check {
        total,
        exists: 0,
        new: 0,
        unknown: 0,
        misses: 0,
        first_misses: Vec::new(),
        bases: 0,
        doubts: std::collections::BTreeMap::new(),
    }));
    let mut tasks = Vec::new();
    for worker in 0..parallel {
        let (queue, check, next_slot, lookup) = (
            queue.clone(),
            check.clone(),
            next_slot.clone(),
            lookup.clone(),
        );
        let nodes = nodes.to_vec();
        tasks.push(tokio::spawn(async move {
            let mut at = worker;
            while tokio::time::Instant::now() < deadline {
                let Some((key, expect)) = queue.lock().unwrap().pop() else {
                    return;
                };
                at += 1;
                let mut got = None;
                for attempt in 0..2 {
                    let slot = {
                        let mut next = next_slot.lock().unwrap();
                        let slot = (*next).max(tokio::time::Instant::now());
                        *next = slot + gap;
                        slot
                    };
                    tokio::time::sleep_until(slot).await;
                    got = tokio::time::timeout_at(
                        deadline,
                        lookup(nodes[(at + attempt) % nodes.len()], key.clone()),
                    )
                    .await
                    .unwrap_or(None);
                    let ok = got.as_ref().is_some_and(|(answer, ..)| match expect {
                        Expect::Exists => answer == "exists",
                        Expect::New => answer == "new",
                    });
                    if ok {
                        break;
                    }
                    if attempt == 0 {
                        tokio::time::sleep(Duration::from_secs(2)).await;
                    }
                }
                let mut check = check.lock().unwrap();
                match got.as_ref().map(|(answer, ..)| answer.as_str()) {
                    Some("exists") => check.exists += 1,
                    Some("new") => check.new += 1,
                    other => {
                        check.unknown += 1;
                        let doubt = match (other, &got) {
                            (Some(_), Some((_, record, ..))) => {
                                record["doubt"].as_str().unwrap_or("unnamed").to_owned()
                            }
                            _ => "no_answer".to_owned(),
                        };
                        *check.doubts.entry(doubt).or_default() += 1;
                    }
                }
                if got
                    .as_ref()
                    .is_some_and(|(_, record, ..)| record["base"] == true)
                {
                    check.bases += 1;
                }
                let ok = got.as_ref().is_some_and(|(answer, ..)| match expect {
                    Expect::Exists => answer == "exists",
                    Expect::New => answer == "new",
                });
                if !ok {
                    check.misses += 1;
                    if check.first_misses.len() < 10 {
                        check.first_misses.push(key);
                    }
                }
            }
        }));
    }
    for task in tasks {
        let _ = task.await;
    }
    let mut check = Arc::try_unwrap(check).ok().unwrap().into_inner().unwrap();
    let left = queue.lock().unwrap().len() as u64;
    if left > 0 {
        say!(
            "check: deadline reached with {left} of {total} keys not looked up; counted as unknown"
        );
        check.unknown += left;
        check.misses += left;
        *check.doubts.entry("not_looked_up".to_owned()).or_default() += left;
    }
    check
}

/// A record of what a gate measured, its threshold and the verdict.
pub struct Gates(pub Vec<Value>);

impl Gates {
    pub fn new() -> Self {
        Gates(Vec::new())
    }

    pub fn add(&mut self, gate: &str, measured: Value, threshold: &str, pass: Option<bool>) {
        let verdict = match pass {
            Some(true) => "PASS",
            Some(false) => "FAIL",
            None => "not run",
        };
        say!("{gate:<6} {verdict:<8} {measured}  [{threshold}]");
        self.0.push(
            json!({ "gate": gate, "measured": measured, "threshold": threshold, "pass": pass }),
        );
    }

    pub fn json(&self) -> Value {
        Value::Array(self.0.clone())
    }
}

/// Look up each key and count the answers that are not the base `(epoch, txid)`
/// the merge rule must keep.
pub async fn wrong_bases(
    ep: &Endpoints,
    expected: Vec<(String, u64, u64)>,
    nodes: &[usize],
) -> u64 {
    let queue = Arc::new(Mutex::new(expected));
    let wrong = Arc::new(AtomicU64::new(0));
    let mut tasks = Vec::new();
    for worker in 0..16 {
        let (ep, queue, wrong, nodes) = (ep.clone(), queue.clone(), wrong.clone(), nodes.to_vec());
        tasks.push(tokio::spawn(async move {
            let mut at = worker;
            loop {
                let Some((key, epoch, txid)) = queue.lock().unwrap().pop() else {
                    return;
                };
                at += 1;
                let got = lookup(&ep, nodes[at % nodes.len()], &key).await;
                let right = got.is_some_and(|(answer, record, ..)| {
                    answer == "exists"
                        && record["base"] == true
                        && record["epoch"] == epoch
                        && record["max_txid"] == txid
                });
                if !right {
                    wrong.fetch_add(1, Ordering::Relaxed);
                }
            }
        }));
    }
    for task in tasks {
        let _ = task.await;
    }
    wrong.load(Ordering::Relaxed)
}
