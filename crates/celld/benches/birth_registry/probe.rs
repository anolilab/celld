//! Reads of the nodes' debug routes, the lookup load, the completeness checks
//! and the gate table.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use serde_json::{json, Value};

use super::Endpoints;

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
        json!({
            "total": total,
            "exists": self.exists.load(Ordering::Relaxed),
            "new": self.new.load(Ordering::Relaxed),
            "unknown": self.unknown.load(Ordering::Relaxed),
            "unknown_pct": self.unknown.load(Ordering::Relaxed) as f64 * 100.0 / total.max(1) as f64,
            "wrong": self.wrong.load(Ordering::Relaxed),
            "failed": self.failed.load(Ordering::Relaxed),
            "node_p50_ms": us_to_ms_p(&node, 50.0),
            "node_p99_ms": us_to_ms_p(&node, 99.0),
            "node_max_ms": us_to_ms_p(&node, 100.0),
            "rtt_p99_ms": us_to_ms_p(&rtt, 99.0),
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
        value["record"].clone(),
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
}

impl Check {
    pub fn json(&self) -> Value {
        json!({
            "total": self.total, "exists": self.exists, "new": self.new, "unknown": self.unknown,
            "misses": self.misses, "first_misses": self.first_misses, "bases": self.bases,
        })
    }
}

/// Look up every key, `parallel` at a time across the live nodes. A miss is
/// retried once after 2 s, then counted.
pub async fn check_keys(
    ep: &Endpoints,
    keys: Vec<(String, Expect)>,
    parallel: usize,
    nodes: &[usize],
) -> Check {
    let total = keys.len() as u64;
    let queue = Arc::new(Mutex::new(keys));
    let check = Arc::new(Mutex::new(Check {
        total,
        exists: 0,
        new: 0,
        unknown: 0,
        misses: 0,
        first_misses: Vec::new(),
        bases: 0,
    }));
    let mut tasks = Vec::new();
    for worker in 0..parallel {
        let (ep, queue, check) = (ep.clone(), queue.clone(), check.clone());
        let nodes = nodes.to_vec();
        tasks.push(tokio::spawn(async move {
            let mut at = worker;
            loop {
                let Some((key, expect)) = queue.lock().unwrap().pop() else {
                    return;
                };
                at += 1;
                let mut got = None;
                for attempt in 0..2 {
                    got = lookup(&ep, nodes[(at + attempt) % nodes.len()], &key).await;
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
                    _ => check.unknown += 1,
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
    Arc::try_unwrap(check).ok().unwrap().into_inner().unwrap()
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
