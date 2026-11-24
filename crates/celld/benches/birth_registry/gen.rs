//! The segment generator: 100 streams that write segments straight into the
//! bucket with the registry's own encoders, so the real nodes fold fleet-rate
//! traffic without creating real cells, and a monitor that watches the root and
//! the delta headers to time "durable segment to covered".

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use celld_logic::registry::{self as reg, DeltaId, Key, Record};
use object_store::path::Path as OPath;
use object_store::{ObjectStore, ObjectStoreExt as _, PutMode, PutOptions, PutPayload};
use sha2::{Digest as _, Sha256};

pub type Store = Arc<dyn ObjectStore>;

const ROOT: &str = "fleet/reg/v1";

/// A uniform key from a hash of the run, the stream and a counter.
pub fn gen_key(run: &str, stream: u64, counter: u64) -> Key {
    let mut hasher = Sha256::new();
    hasher.update(run.as_bytes());
    hasher.update(stream.to_le_bytes());
    hasher.update(counter.to_le_bytes());
    hasher.finalize()[..16].try_into().unwrap()
}

pub fn hex(key: &Key) -> String {
    key.iter().map(|byte| format!("{byte:02x}")).collect()
}

/// A key that falls in partition `index` of `count`, drawn by trial.
pub fn key_in(run: &str, stream: u64, counter: &mut u64, index: u32, count: u32) -> Key {
    loop {
        let key = gen_key(run, stream, *counter);
        *counter += 1;
        if reg::partition_of(&key, count) == index {
            return key;
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub enum Shape {
    /// Births only, streams spread over the window.
    Births,
    /// Births only, every stream writes at the same instant.
    Aligned,
    /// Births and bases: birth, base, higher base, birth at the next epoch.
    Bases,
}

pub struct Generator {
    pub streams: u64,
    pub window: Duration,
    /// Entries a second in all.
    pub rate: f64,
    pub shape: Shape,
    pub run: String,
    pub store: Store,
    pub prefix: String,
    pub stop: Arc<AtomicBool>,
    pub segments: AtomicU64,
    pub entries: AtomicU64,
    pub failed: AtomicU64,
    /// Full segment key to the instant its write returned.
    pub durable: Mutex<HashMap<String, Instant>>,
    pub covered_ms: Mutex<Vec<u64>>,
    /// Keys that went through the whole cycle, with the record they must read.
    pub finished: Mutex<Vec<(Key, Record)>>,
}

impl Generator {
    pub fn new(
        store: Store,
        prefix: &str,
        run: &str,
        rate: f64,
        shape: Shape,
        streams: u64,
        window: Duration,
    ) -> Arc<Generator> {
        Arc::new(Generator {
            streams,
            window,
            rate,
            shape,
            run: run.to_owned(),
            store,
            prefix: prefix.to_owned(),
            stop: Arc::new(AtomicBool::new(false)),
            segments: AtomicU64::new(0),
            entries: AtomicU64::new(0),
            failed: AtomicU64::new(0),
            durable: Mutex::default(),
            covered_ms: Mutex::default(),
            finished: Mutex::default(),
        })
    }

    pub fn start(self: &Arc<Self>) -> Vec<tokio::task::JoinHandle<()>> {
        let mut tasks: Vec<_> = (0..self.streams)
            .map(|stream| {
                let this = self.clone();
                tokio::spawn(async move { this.stream(stream).await })
            })
            .collect();
        let this = self.clone();
        tasks.push(tokio::spawn(async move { this.monitor().await }));
        tasks
    }

    pub fn stop(&self) {
        self.stop.store(true, Ordering::Relaxed);
    }

    fn entries_per_segment(&self) -> usize {
        let per_stream = self.rate / self.streams as f64 * self.window.as_secs_f64();
        per_stream.round().max(1.0) as usize
    }

    async fn stream(self: Arc<Self>, stream: u64) {
        let window = self.window;
        let offset = match self.shape {
            Shape::Aligned => {
                let now = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_millis() as u64;
                let to_next = window.as_millis() as u64 - now % window.as_millis() as u64;
                Duration::from_millis(to_next)
            }
            _ => window.mul_f64(stream as f64 / self.streams as f64),
        };
        tokio::time::sleep(offset).await;
        let mut tick = tokio::time::interval(window);
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        let per = self.entries_per_segment();
        let (mut counter, mut n) = (0_u64, 0_u64);
        let mut history: [Vec<Key>; 3] = Default::default();
        while !self.stop.load(Ordering::Relaxed) {
            tick.tick().await;
            let records = match self.shape {
                Shape::Births | Shape::Aligned => (0..per)
                    .map(|_| {
                        counter += 1;
                        Record::birth(gen_key(&self.run, stream, counter), 1)
                    })
                    .collect(),
                Shape::Bases => {
                    let quarter = per.div_ceil(4).max(1);
                    let fresh: Vec<Key> = (0..quarter)
                        .map(|_| {
                            counter += 1;
                            gen_key(&self.run, stream, counter)
                        })
                        .collect();
                    let mut records: Vec<Record> =
                        fresh.iter().map(|key| Record::birth(*key, 1)).collect();
                    let pack = |key: &Key| -> [u8; 12] {
                        let mut pack = [0xaa; 12];
                        pack[..8].copy_from_slice(&key[..8]);
                        pack
                    };
                    records.extend(
                        history[0]
                            .iter()
                            .map(|key| Record::base(*key, pack(key), 1, 10)),
                    );
                    records.extend(
                        history[1]
                            .iter()
                            .map(|key| Record::base(*key, pack(key), 1, 20)),
                    );
                    let done: Vec<Key> = std::mem::take(&mut history[2]);
                    records.extend(done.iter().map(|key| Record::birth(*key, 2)));
                    if !done.is_empty() {
                        let mut finished = self.finished.lock().unwrap();
                        if finished.len() < 20_000 {
                            finished.extend(
                                done.iter()
                                    .take(8)
                                    .map(|key| (*key, Record::base(*key, pack(key), 1, 20))),
                            );
                        }
                    }
                    history[2] = std::mem::take(&mut history[1]);
                    history[1] = std::mem::take(&mut history[0]);
                    history[0] = fresh;
                    records
                }
            };
            let count = records.len() as u64;
            let body = reg::encode_segment(&reg::sorted(records));
            let key = format!(
                "{}{ROOT}/seg/gen-{stream}/{}/{n:010}",
                self.prefix, self.run
            );
            n += 1;
            let put = self
                .store
                .put_opts(
                    &OPath::from(key.clone()),
                    PutPayload::from(body),
                    PutOptions {
                        mode: PutMode::Create,
                        ..Default::default()
                    },
                )
                .await;
            match put {
                Ok(_) => {
                    self.segments.fetch_add(1, Ordering::Relaxed);
                    self.entries.fetch_add(count, Ordering::Relaxed);
                    self.durable.lock().unwrap().insert(key, Instant::now());
                }
                Err(_) => {
                    self.failed.fetch_add(1, Ordering::Relaxed);
                }
            }
        }
    }

    /// Reads the root twice a second and every delta it newly names, and
    /// records how long each generated segment took to be covered.
    async fn monitor(self: Arc<Self>) {
        let mut seen: HashSet<DeltaId> = HashSet::new();
        let mut tick = tokio::time::interval(Duration::from_millis(500));
        while !self.stop.load(Ordering::Relaxed) {
            tick.tick().await;
            let Some(root) = read_root(&self.store, &self.prefix).await else {
                continue;
            };
            let fresh: Vec<DeltaId> = root
                .deltas
                .iter()
                .filter(|id| !seen.contains(id))
                .copied()
                .collect();
            for id in &fresh {
                let key = format!("{}{ROOT}/delta/{}-{:012}", self.prefix, id.epoch, id.n);
                let Ok(got) = self.store.get(&OPath::from(key)).await else {
                    continue;
                };
                let Ok(bytes) = got.bytes().await else {
                    continue;
                };
                let Ok(delta) = reg::decode_delta(&bytes) else {
                    continue;
                };
                seen.insert(*id);
                let now = Instant::now();
                let mut durable = self.durable.lock().unwrap();
                let mut covered = self.covered_ms.lock().unwrap();
                for segment in &delta.segments {
                    if let Some(at) = durable.remove(segment) {
                        covered.push(now.saturating_duration_since(at).as_millis() as u64);
                    }
                }
            }
        }
    }
}

pub async fn read_root(store: &Store, prefix: &str) -> Option<reg::Root> {
    let got = store
        .get(&OPath::from(format!("{prefix}{ROOT}/root")))
        .await
        .ok()?;
    reg::decode_root(&got.bytes().await.ok()?).ok()
}

/// Write one segment of `records` by hand, as a node would.
pub async fn put_segment(store: &Store, prefix: &str, name: &str, records: Vec<Record>) -> bool {
    let body = reg::encode_segment(&reg::sorted(records));
    store
        .put_opts(
            &OPath::from(format!("{prefix}{ROOT}/seg/{name}")),
            PutPayload::from(body),
            PutOptions {
                mode: PutMode::Create,
                ..Default::default()
            },
        )
        .await
        .is_ok()
}
