//! Session-granted births on a three-node fleet over one bucket.
//!
//! Needs a real S3-compatible bucket, as the object-store contract suite does:
//! without `CELLD_STORE_CONTRACT_BUCKET` each test passes after saying it was
//! skipped. The credentials and `AWS_ENDPOINT_URL` come from the environment.
//! `crates/celld/tests/stores/compose.yml` brings up a store to point at:
//! `docker compose -f crates/celld/tests/stores/compose.yml -p celld-store-rustfs
//! up -d rustfs`, then create the bucket `contract` and use
//! `AWS_ACCESS_KEY_ID=rustfsadmin AWS_SECRET_ACCESS_KEY=rustfsadmin
//! AWS_ENDPOINT_URL=http://127.0.0.1:9000 CELLD_STORE_CONTRACT_BUCKET=s3://contract`.

// The test supervises real node processes, outside the execution boundary the
// disallowed-methods policy protects.
#![allow(clippy::disallowed_methods)]

use std::os::unix::process::CommandExt as _;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use celld::bucket::Bucket;
use futures_util::{FutureExt as _, StreamExt as _};
use serde_json::Value;

use super::harness::{client, fixture_dir, signal_group, wait_until};

// The counting proxy of the request bench: the nodes of a counted fleet talk to
// the bucket through it, and its tally is the requests that each key got.
#[path = "../../benches/support/mod.rs"]
#[allow(clippy::pedantic)]
mod support;

const CELLD: &str = env!("CARGO_BIN_EXE_celld");
const TOKEN: &str = "lazy-birth-fleet-token";

fn region() -> String {
    std::env::var("AWS_REGION").unwrap_or_else(|_| "us-east-1".into())
}

pub struct Node {
    child: Option<Child>,
    pub public: String,
    internal: String,
    home: PathBuf,
}

impl Node {
    /// Start node `index` in its own directory, which survives a respawn: the
    /// follower disks and the node's log stay.
    pub fn spawn(
        home: &Path,
        index: usize,
        bucket: &str,
        endpoint: &str,
        env: &[(&str, &str)],
    ) -> Node {
        let (public, internal) = (free_port(), free_port());
        let home = home.join(format!("node-{index}"));
        std::fs::create_dir_all(&home).expect("create the node directory");
        let log = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(home.join("celld.log"))
            .expect("open the node log");
        let child = Command::new(CELLD)
            .args(["--bucket", bucket, "--endpoint", endpoint])
            .args(["--region", &region()])
            .args(["--listen", &format!("127.0.0.1:{public}")])
            .args(["--internal-listen", &format!("127.0.0.1:{internal}")])
            .args(["--advertise", &format!("127.0.0.1:{internal}")])
            .env("CELLD_NODE", format!("lazy-{index}"))
            .env("CELLD_WATCH", home.join("state"))
            .env("CELLD_OPERATOR_TOKEN", TOKEN)
            .env("CELLD_CONFIG_DIR", home.join("config"))
            .envs(env.iter().copied())
            .current_dir(&home)
            .stdin(Stdio::null())
            .stdout(log.try_clone().expect("clone the log handle"))
            .stderr(log)
            .process_group(0)
            .spawn()
            .expect("spawn celld");
        Node {
            child: Some(child),
            public: format!("http://127.0.0.1:{public}"),
            internal: format!("http://127.0.0.1:{internal}"),
            home,
        }
    }

    pub fn log(&self) -> String {
        std::fs::read_to_string(self.home.join("celld.log")).unwrap_or_default()
    }

    /// Kill the node's process group at once: no drain, no seal.
    pub fn kill(&mut self) {
        if let Some(mut child) = self.child.take() {
            signal_group(child.id(), "KILL");
            let _ = child.wait();
        }
    }

    pub async fn wait_ready(&self, client: &reqwest::Client) {
        wait_until(
            &format!("the node at {} to be ready", self.public),
            Duration::from_secs(120),
            || async {
                for path in ["/.well-known/celld/health", "/__ready"] {
                    let response = client.get(format!("{}{path}", self.public)).send().await;
                    if !response.is_ok_and(|response| response.status().is_success()) {
                        return None;
                    }
                }
                Some(())
            },
        )
        .await;
    }
}

impl Node {
    /// A preserved restart: ask the node to stop and keep its cells for the
    /// next process of this directory, then wait for it to exit.
    pub async fn preserve(&mut self, client: &reqwest::Client) {
        let response = client
            .post(format!("{}/shutdown?handoff=preserve", self.internal))
            .bearer_auth(TOKEN)
            .send()
            .await
            .expect("ask for a preserved stop");
        assert!(response.status().is_success(), "{}", response.status());
        let mut child = self.child.take().expect("a running node");
        wait_until("the node to exit", Duration::from_secs(90), || {
            let exited = matches!(child.try_wait(), Ok(Some(_)));
            async move { exited.then_some(()) }
        })
        .await;
    }

    /// Stop the node as an orchestrator does: TERM, then wait for its drain.
    pub fn stop(&mut self) {
        drop_child(self.child.take());
    }
}

impl Drop for Node {
    fn drop(&mut self) {
        drop_child(self.child.take());
    }
}

fn drop_child(child: Option<Child>) {
    {
        let Some(mut child) = child else {
            return;
        };
        signal_group(child.id(), "TERM");
        let deadline = Instant::now() + Duration::from_secs(45);
        while Instant::now() < deadline {
            if matches!(child.try_wait(), Ok(Some(_))) {
                return;
            }
            std::thread::sleep(Duration::from_millis(100));
        }
        signal_group(child.id(), "KILL");
        let _ = child.wait();
    }
}

fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .expect("bind an ephemeral port")
        .local_addr()
        .expect("read the local address")
        .port()
}

/// A fleet of three nodes on a bucket of its own, with the fixture deployed.
pub struct Fleet {
    pub spec: String,
    pub endpoint: String,
    pub home: tempfile::TempDir,
    pub nodes: Vec<Node>,
    pub client: reqwest::Client,
    pub bucket: Bucket,
    /// The proxy that counts the requests of the nodes, for a counted fleet.
    pub proxy: Option<support::Proxy>,
}

impl Fleet {
    /// `None` when no store is configured.
    pub async fn start(fixture: &str, nodes: usize, env: &[(&str, &str)]) -> Option<Fleet> {
        Fleet::start_each(fixture, &vec![env; nodes]).await
    }

    /// One environment per node.
    pub async fn start_each(fixture: &str, envs: &[&[(&str, &str)]]) -> Option<Fleet> {
        Fleet::start_each_counted(fixture, envs, false).await
    }

    /// A fleet whose nodes reach the bucket through a counting proxy.
    pub async fn start_counted(fixture: &str, nodes: usize, env: &[(&str, &str)]) -> Option<Fleet> {
        Fleet::start_each_counted(fixture, &vec![env; nodes], true).await
    }

    async fn start_each_counted(
        fixture: &str,
        envs: &[&[(&str, &str)]],
        counted: bool,
    ) -> Option<Fleet> {
        let Ok(bucket) = std::env::var("CELLD_STORE_CONTRACT_BUCKET") else {
            eprintln!("skipped: CELLD_STORE_CONTRACT_BUCKET is not set");
            return None;
        };
        let endpoint = std::env::var("AWS_ENDPOINT_URL").expect("AWS_ENDPOINT_URL names the store");
        let run = format!(
            "lazy-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        );
        let spec = format!("{}/{run}", bucket.trim_end_matches('/'));
        let home = tempfile::TempDir::new().expect("create a temp dir");
        let deployed = Command::new(CELLD)
            .arg("deploy")
            .arg(fixture_dir(fixture))
            .args(["--bucket", &spec, "--endpoint", &endpoint])
            .args(["--region", &region()])
            .env("CELLD_CONFIG_DIR", home.path().join("deploy"))
            .current_dir(home.path())
            .output()
            .expect("run celld deploy");
        assert!(
            deployed.status.success(),
            "celld deploy failed: {}",
            String::from_utf8_lossy(&deployed.stderr)
        );
        let client = client();
        let proxy = if counted {
            let upstream = endpoint
                .trim_start_matches("http://")
                .trim_end_matches('/')
                .to_string();
            let root = spec.trim_start_matches("s3://").to_string();
            Some(
                support::Proxy::start(
                    "127.0.0.1:0",
                    upstream,
                    root,
                    support::Classifier::default_layout(),
                )
                .await,
            )
        } else {
            None
        };
        let node_endpoint = proxy.as_ref().map_or(endpoint.clone(), |p| p.url.clone());
        let nodes: Vec<Node> = envs
            .iter()
            .enumerate()
            .map(|(index, env)| Node::spawn(home.path(), index, &spec, &node_endpoint, env))
            .collect();
        for node in &nodes {
            node.wait_ready(&client).await;
        }
        let bucket = Bucket::open(&spec, Some(&endpoint), &region(), None, Some("lazy-fleet"))
            .unwrap_or_else(|error| panic!("open {spec}: {error:#}"));
        Some(Fleet {
            spec,
            endpoint,
            home,
            nodes,
            client,
            bucket,
            proxy,
        })
    }

    /// Replace node `index` by a new process on the same directory.
    pub async fn respawn(&mut self, index: usize, env: &[(&str, &str)]) {
        self.nodes[index].kill();
        let endpoint = self.proxy.as_ref().map_or(&self.endpoint, |p| &p.url);
        let node = Node::spawn(self.home.path(), index, &self.spec, endpoint, env);
        node.wait_ready(&self.client).await;
        self.nodes[index] = node;
    }

    pub async fn get_json(&self, node: usize, path: &str) -> Option<Value> {
        let response = self
            .client
            .get(format!("{}{path}", self.nodes[node].public))
            .send()
            .await
            .ok()?;
        if !response.status().is_success() {
            return None;
        }
        response.json().await.ok()
    }

    pub async fn object(&self, key: &str) -> Option<Value> {
        let (bytes, _) = self.bucket.get(key).await.expect("read an object")?;
        serde_json::from_slice(&bytes).ok()
    }

    pub async fn keys(&self, prefix: &str) -> Vec<String> {
        self.bucket
            .list(prefix)
            .await
            .expect("list")
            .into_iter()
            .map(|meta| meta.location.to_string())
            .collect()
    }

    /// Delete every object the run wrote.
    pub async fn clean(&self) {
        let keys = self.keys("").await;
        let deleted = self.bucket.delete_many(&keys).await;
        assert_eq!(deleted.len(), keys.len(), "cleanup left objects");
    }
}

/// Run `body`, then stop the nodes and delete the run's objects, also when an
/// assertion in `body` fails.
async fn with_fleet<F, Fut>(fleet: Fleet, body: F)
where
    F: FnOnce(Fleet) -> Fut,
    Fut: std::future::Future<Output = Fleet>,
{
    let outcome = std::panic::AssertUnwindSafe(async { body(fleet).await })
        .catch_unwind()
        .await;
    match outcome {
        Ok(mut fleet) => {
            fleet.nodes.clear();
            fleet.clean().await;
        }
        Err(panic) => std::panic::resume_unwind(panic),
    }
}

fn counter_of(header: &Value, id: &str) -> u64 {
    let unhex = |text: &str| -> Vec<u8> {
        (0..text.len() / 2)
            .map(|i| u8::from_str_radix(&text[2 * i..2 * i + 2], 16).expect("hex"))
            .collect()
    };
    let key: [u8; 32] = unhex(header["key"].as_str().expect("a key"))
        .try_into()
        .expect("32 bytes");
    celld::session::decrypt_counter(&key, unhex(&id[16..32]).try_into().expect("8 bytes"))
}

/// The ensemble of node 0 forms by maintenance, a few seconds after it is
/// ready; a grant before that is waived.
async fn wait_for_ensemble(fleet: &Fleet) {
    wait_until(
        "node 0 to have an ensemble",
        Duration::from_secs(90),
        || async {
            let lease = fleet.object("nodes/lazy-0.json").await?;
            (lease["log"]["state"] == "open"
                && !lease["log"]["ensemble"].as_array()?.is_empty()
                && !lease["session"].is_null())
            .then_some(())
        },
    )
    .await;
}

const SHADOW: &[(&str, &str)] = &[
    ("CELLD_LAZY_BIRTH", "shadow"),
    ("CELLD_LAZY_BIRTH_CHECKPOINT_MS", "500"),
];

/// The minting node is killed. Every id answers again from another node; its
/// session is sealed by the takeover's recovery, and the sealed chunk holds
/// exactly the ids that got a record.
#[tokio::test]
async fn killing_the_minting_node_seals_its_session_with_its_grants() {
    let Some(fleet) = Fleet::start("do-ids", 3, SHADOW).await else {
        return;
    };
    with_fleet(fleet, |mut fleet| async move {
        wait_for_ensemble(&fleet).await;
        let minted = fleet.get_json(0, "/mint").await.expect("mint");
        let ids: Vec<String> = minted["ids"]
            .as_array()
            .unwrap()
            .iter()
            .map(|id| id.as_str().unwrap().to_string())
            .collect();
        let tag = ids[0][..16].to_string();
        // Two of the three ids get a record; the middle one never does.
        let third = fleet
            .get_json(0, &format!("/bump?id={}", ids[2]))
            .await
            .expect("bump");
        assert_eq!(third["n"], 1);
        let first = fleet
            .get_json(0, &format!("/bump?id={}", ids[0]))
            .await
            .expect("bump");
        assert_eq!(first["n"], 1);
        // Wait for the checkpoint to write the grant to the chunk.
        let header = fleet
            .object(&format!("fleet/sessions/{tag}.json"))
            .await
            .expect("the session header");
        let granted = [counter_of(&header, &ids[0]), counter_of(&header, &ids[2])];
        wait_until("the checkpoint", Duration::from_secs(30), || async {
            let chunk = fleet.object(&format!("fleet/sessions/{tag}/c/0")).await?;
            (chunk["granted"] == serde_json::json!(granted)).then_some(())
        })
        .await;
        fleet.nodes[0].kill();
        // Another node serves the id: its takeover recovers the dead node.
        wait_until(
            "another node to serve the id",
            Duration::from_secs(180),
            || async {
                for node in [1, 2] {
                    if let Some(out) = fleet.get_json(node, &format!("/bump?id={}", ids[0])).await {
                        return Some(out);
                    }
                }
                None
            },
        )
        .await;
        let header = fleet
            .object(&format!("fleet/sessions/{tag}.json"))
            .await
            .expect("the session header");
        assert_eq!(header["state"], "sealed", "{header}");
        let chunk = fleet
            .object(&format!("fleet/sessions/{tag}/c/0"))
            .await
            .expect("the chunk");
        assert_eq!(chunk["state"], "sealed", "{chunk}");
        assert_eq!(chunk["granted"], serde_json::json!(granted), "{chunk}");
        // Nobody wrote epoch 1 for the ids of the dead session but itself.
        for node in [1, 2] {
            let log = fleet.nodes[node].log();
            assert!(!log.contains("panicked"), "{log}");
        }
        // The killed node comes back with a new session, and the old one stays
        // sealed.
        fleet.respawn(0, SHADOW).await;
        let header = fleet
            .object(&format!("fleet/sessions/{tag}.json"))
            .await
            .unwrap();
        assert_eq!(header["state"], "sealed");
        fleet
    })
    .await;
}

/// No checkpoint runs before the kill, so the only trace of the grant is the
/// `Grant` mark on the followers: the recoverer reads it from their tails and
/// the sealed chunk holds the counter.
#[tokio::test]
async fn a_grant_that_no_checkpoint_wrote_survives_the_loss_of_the_minting_node() {
    let env: &[(&str, &str)] = &[
        ("CELLD_LAZY_BIRTH", "shadow"),
        ("CELLD_LAZY_BIRTH_CHECKPOINT_MS", "3600000"),
    ];
    let Some(fleet) = Fleet::start("do-ids", 3, env).await else {
        return;
    };
    with_fleet(fleet, |mut fleet| async move {
        wait_for_ensemble(&fleet).await;
        let minted = fleet.get_json(0, "/mint").await.expect("mint");
        let ids: Vec<String> = minted["ids"]
            .as_array()
            .unwrap()
            .iter()
            .map(|id| id.as_str().unwrap().to_string())
            .collect();
        let tag = ids[0][..16].to_string();
        let out = fleet
            .get_json(0, &format!("/bump?id={}", ids[0]))
            .await
            .expect("bump");
        assert_eq!(out["n"], 1);
        let header = fleet
            .object(&format!("fleet/sessions/{tag}.json"))
            .await
            .expect("the header");
        let counter = counter_of(&header, &ids[0]);
        let chunk = fleet
            .object(&format!("fleet/sessions/{tag}/c/0"))
            .await
            .expect("the chunk");
        assert_eq!(
            chunk["granted"],
            serde_json::json!([]),
            "no checkpoint ran: {chunk}"
        );
        fleet.nodes[0].kill();
        wait_until(
            "another node to serve the id",
            Duration::from_secs(180),
            || async {
                for node in [1, 2] {
                    if let Some(out) = fleet.get_json(node, &format!("/bump?id={}", ids[0])).await {
                        return Some(out);
                    }
                }
                None
            },
        )
        .await;
        let chunk = fleet
            .object(&format!("fleet/sessions/{tag}/c/0"))
            .await
            .expect("the chunk");
        assert_eq!(chunk["state"], "sealed", "{chunk}");
        assert_eq!(chunk["granted"], serde_json::json!([counter]), "{chunk}");
        fleet
    })
    .await;
}

/// A node without the flag in the ensemble means no `Grant` can be appended:
/// the id is waived, takes the standard path with no bit, and the recoverer of
/// the dead session finds no mark, so the sealed chunk holds no bit.
#[tokio::test]
async fn an_ensemble_member_without_the_capability_waives_the_id() {
    let off: &[(&str, &str)] = &[];
    let Some(fleet) = Fleet::start_each("do-ids", &[SHADOW, off]).await else {
        return;
    };
    with_fleet(fleet, |mut fleet| async move {
        wait_until(
            "node 0 to have an ensemble",
            Duration::from_secs(90),
            || async {
                let lease = fleet.object("nodes/lazy-0.json").await?;
                (lease["log"]["state"] == "open"
                    && !lease["log"]["ensemble"].as_array()?.is_empty())
                .then_some(())
            },
        )
        .await;
        let minted = fleet.get_json(0, "/mint").await.expect("mint");
        let id = minted["ids"][0].as_str().unwrap().to_string();
        let tag = id[..16].to_string();
        let out = fleet
            .get_json(0, &format!("/bump?id={id}"))
            .await
            .expect("bump");
        assert_eq!(out["n"], 1, "the waived id is served on the standard path");
        fleet.nodes[0].kill();
        wait_until(
            "the other node to serve the id",
            Duration::from_secs(180),
            || async { fleet.get_json(1, &format!("/bump?id={id}")).await },
        )
        .await;
        let chunk = fleet
            .object(&format!("fleet/sessions/{tag}/c/0"))
            .await
            .expect("the chunk");
        assert_eq!(chunk["state"], "sealed", "{chunk}");
        assert!(
            chunk["granted"]
                .as_array()
                .is_none_or(|bits| bits.is_empty()),
            "a waived id has no bit: {chunk}"
        );
        fleet
    })
    .await;
}

/// The first per-cell object of a granted cell waits for its bit to be in a
/// chunk: with no checkpoint due for an hour, the held write asks for one, and
/// the chunk holds the bit by the time the object exists.
#[tokio::test]
async fn the_first_object_of_a_granted_cell_follows_its_bit_in_the_chunk() {
    let env: &[(&str, &str)] = &[
        ("CELLD_LAZY_BIRTH", "shadow"),
        ("CELLD_LAZY_BIRTH_CHECKPOINT_MS", "3600000"),
        ("CELLD_IDLE_EVICT_S", "2"),
    ];
    let Some(fleet) = Fleet::start("do-ids", 3, env).await else {
        return;
    };
    with_fleet(fleet, |fleet| async move {
        wait_for_ensemble(&fleet).await;
        let minted = fleet.get_json(0, "/mint").await.expect("mint");
        let id = minted["ids"][0].as_str().unwrap().to_string();
        let tag = id[..16].to_string();
        let header = fleet
            .object(&format!("fleet/sessions/{tag}.json"))
            .await
            .expect("the header");
        let counter = counter_of(&header, &id);
        let out = fleet
            .get_json(0, &format!("/bump?id={id}"))
            .await
            .expect("bump");
        assert_eq!(out["n"], 1);
        wait_until(
            "the first object of the cell",
            Duration::from_secs(60),
            || async {
                let keys = fleet.keys("").await;
                keys.iter()
                    .any(|key| key.contains("/ltx/e1/"))
                    .then_some(())
            },
        )
        .await;
        let chunk = fleet
            .object(&format!("fleet/sessions/{tag}/c/0"))
            .await
            .expect("the chunk");
        assert_eq!(
            chunk["granted"],
            serde_json::json!([counter]),
            "the object exists and the chunk does not hold its bit: {chunk}"
        );
        fleet
    })
    .await;
}

/// The `Grant` of a granted cell rides the bundle that carries the cell's
/// rows, so a recoverer that finds the bundle finds the mark even when the
/// followers' tails are gone.
#[tokio::test]
async fn the_bundle_of_a_granted_cell_carries_its_mark() {
    let env: &[(&str, &str)] = &[
        ("CELLD_LAZY_BIRTH", "shadow"),
        ("CELLD_LAZY_BIRTH_CHECKPOINT_MS", "3600000"),
    ];
    let Some(fleet) = Fleet::start("do-ids", 3, env).await else {
        return;
    };
    with_fleet(fleet, |fleet| async move {
        wait_for_ensemble(&fleet).await;
        let minted = fleet.get_json(0, "/mint").await.expect("mint");
        let id = minted["ids"][0].as_str().unwrap().to_string();
        let out = fleet
            .get_json(0, &format!("/bump?id={id}"))
            .await
            .expect("bump");
        assert_eq!(out["n"], 1);
        wait_until(
            "a bundle with the mark",
            Duration::from_secs(60),
            || async {
                for key in fleet.keys("log/").await {
                    if !key.ends_with(".ltxb") {
                        continue;
                    }
                    let (bytes, _) = fleet.bucket.get(&key).await.ok()??;
                    let marks = celld_ltx::bundle::decode_marks(&bytes).ok()?;
                    if marks.iter().any(|cell| cell.ends_with(&id)) {
                        return Some(());
                    }
                }
                None
            },
        )
        .await;
        fleet
    })
    .await;
}

/// A request that gives up before the fleet proof of its `Grant` fails with an
/// error that may be retried, the cell does not activate, and the grant is
/// pending: the id is granted (its bit is set at the append), it never takes the
/// standard path in its session, and every later request for it fails the same
/// way, also when the proof comes late.
#[tokio::test]
async fn a_grant_without_a_timely_proof_is_pending_and_never_waived() {
    let env: &[(&str, &str)] = &[
        ("CELLD_LAZY_BIRTH", "shadow"),
        ("CELLD_LAZY_BIRTH_CHECKPOINT_MS", "500"),
        ("CELLD_LAZY_BIRTH_PROOF_WAIT_MS", "0"),
    ];
    let Some(fleet) = Fleet::start("do-ids", 3, env).await else {
        return;
    };
    with_fleet(fleet, |fleet| async move {
        wait_for_ensemble(&fleet).await;
        let minted = fleet.get_json(0, "/mint").await.expect("mint");
        let id = minted["ids"][0].as_str().unwrap().to_string();
        let tag = id[..16].to_string();
        let header = fleet
            .object(&format!("fleet/sessions/{tag}.json"))
            .await
            .expect("the header");
        let counter = counter_of(&header, &id);
        assert!(
            fleet.get_json(0, &format!("/bump?id={id}")).await.is_none(),
            "the request gave up before a proof"
        );
        // The proof comes late and changes nothing: the cell stays refused.
        tokio::time::sleep(Duration::from_secs(3)).await;
        assert!(fleet.get_json(0, &format!("/bump?id={id}")).await.is_none());
        assert!(fleet.nodes[0].log().contains("pending_grant"));
        // The record exists and the id is granted in the chunk.
        let records: Vec<_> = fleet
            .keys("cells/")
            .await
            .into_iter()
            .filter(|key| key.ends_with("/own.json"))
            .collect();
        assert_eq!(records.len(), 1, "{records:?}");
        wait_until("the bit in the chunk", Duration::from_secs(30), || async {
            let chunk = fleet.object(&format!("fleet/sessions/{tag}/c/0")).await?;
            (chunk["granted"] == serde_json::json!([counter])).then_some(())
        })
        .await;
        fleet
    })
    .await;
}

/// B3: a request for an id with no record, reaching a node other than the
/// minting one while the session is open, is forwarded to the minting node,
/// which makes the first claim at epoch 1. Without the forward the other node
/// would claim epoch 2.
#[tokio::test]
async fn an_id_without_a_record_is_forwarded_to_the_live_minting_node() {
    let Some(fleet) = Fleet::start("do-ids", 3, SHADOW).await else {
        return;
    };
    with_fleet(fleet, |fleet| async move {
        let minted = fleet.get_json(0, "/mint").await.expect("mint");
        let id = minted["ids"][0].as_str().unwrap().to_string();
        let tag = id[..16].to_string();
        let out = fleet
            .get_json(1, &format!("/bump?id={id}"))
            .await
            .expect("bump through another node");
        assert_eq!(out["n"], 1);
        let records: Vec<_> = fleet
            .keys("cells/")
            .await
            .into_iter()
            .filter(|key| key.ends_with("/own.json"))
            .collect();
        assert_eq!(records.len(), 1, "{records:?}");
        let record = fleet.object(&records[0]).await.expect("the record");
        assert_eq!(record["epoch"], 1, "the minting node claimed: {record}");
        assert_eq!(record["node"], "lazy-0", "{record}");
        assert_eq!(record["by"], tag.as_str(), "{record}");
        assert!(fleet.nodes[1].log().contains("b3_forward"));
        fleet
    })
    .await;
}

/// B3: with the minting node dead and its session not sealed, a request through
/// another node recovers the dead node first, which seals the header, and the id
/// is then claimed by the sealed chunk's rules.
#[tokio::test]
async fn an_id_without_a_record_of_a_dead_minting_node_waits_for_its_seal() {
    let Some(fleet) = Fleet::start("do-ids", 3, SHADOW).await else {
        return;
    };
    with_fleet(fleet, |mut fleet| async move {
        let minted = fleet.get_json(0, "/mint").await.expect("mint");
        let id = minted["ids"][0].as_str().unwrap().to_string();
        let tag = id[..16].to_string();
        fleet.nodes[0].kill();
        wait_until(
            "another node to serve the id",
            Duration::from_secs(240),
            || async {
                for node in [1, 2] {
                    if let Some(out) = fleet.get_json(node, &format!("/bump?id={id}")).await {
                        return Some(out);
                    }
                }
                None
            },
        )
        .await;
        let header = fleet
            .object(&format!("fleet/sessions/{tag}.json"))
            .await
            .expect("the header");
        assert_eq!(header["state"], "sealed", "{header}");
        let records: Vec<_> = fleet
            .keys("cells/")
            .await
            .into_iter()
            .filter(|key| key.ends_with("/own.json"))
            .collect();
        let record = fleet.object(&records[0]).await.expect("the record");
        assert_eq!(
            record["epoch"], 2,
            "a new cell of a sealed session: {record}"
        );
        assert!(record["nonce"].is_string(), "{record}");
        fleet
    })
    .await;
}

/// First-form records only behind the marker: while a live lease lacks the
/// capability, a node that takes a cell of a sealed session writes a plain
/// record, which an older build reads correctly, and no nonce.
#[tokio::test]
async fn a_first_form_record_is_not_written_while_a_lease_lacks_the_capability() {
    let off: &[(&str, &str)] = &[];
    let Some(fleet) = Fleet::start_each("do-ids", &[SHADOW, SHADOW, off]).await else {
        return;
    };
    with_fleet(fleet, |mut fleet| async move {
        let minted = fleet.get_json(0, "/mint").await.expect("mint");
        let id = minted["ids"][0].as_str().unwrap().to_string();
        fleet.nodes[0].kill();
        wait_until(
            "node 1 to serve the id",
            Duration::from_secs(240),
            || async { fleet.get_json(1, &format!("/bump?id={id}")).await },
        )
        .await;
        let records: Vec<_> = fleet
            .keys("cells/")
            .await
            .into_iter()
            .filter(|key| key.ends_with("/own.json"))
            .collect();
        let record = fleet.object(&records[0]).await.expect("the record");
        assert!(record["nonce"].is_null(), "no first-form record: {record}");
        assert!(fleet.nodes[1].log().contains("marker_gate"));
        fleet
    })
    .await;
}

/// How many agents a request count covers (`LAZY_FLEET_AGENTS`).
fn agents() -> usize {
    std::env::var("LAZY_FLEET_AGENTS")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(600)
}

/// The sum of the requests of each class in a tally, and the requests of the
/// keys that match `shape`.
fn class_totals(tally: &support::Tally) -> (u64, u64) {
    let mut totals = (0, 0);
    for ((kind, _, _), n) in tally {
        match support::class_of(kind) {
            support::Class::A => totals.0 += n,
            support::Class::B => totals.1 += n,
            support::Class::Free => {}
        }
    }
    totals
}

fn requests_on(tally: &support::Tally, kind: &str, needle: &str) -> u64 {
    tally
        .iter()
        .filter(|((k, shape, _), _)| (kind.is_empty() || k == kind) && shape.contains(needle))
        .map(|(_, n)| n)
        .sum()
}

/// Mint `count` agents on node 0, each called in the request that creates it,
/// in requests of at most 20, and return their ids.
async fn spawn_agents(fleet: &Fleet, count: usize) -> Vec<String> {
    let mut ids = Vec::new();
    while ids.len() < count {
        let batch = (count - ids.len()).min(20);
        let spawned = fleet
            .get_json(0, &format!("/spawn?n={batch}"))
            .await
            .expect("spawn");
        ids.extend(
            spawned["ids"]
                .as_array()
                .unwrap()
                .iter()
                .map(|id| id.as_str().unwrap().to_string()),
        );
    }
    ids
}

/// The requests a minted agent costs, measured at the bucket through the
/// counting proxy: the window of the run minus an idle window of the same
/// length, divided by the agents. Prints the numbers; the assertions are the
/// ones that need no baseline.
async fn requests_per_minted_agent(mode: &'static str) -> (f64, f64, u64, u64) {
    // The checkpoint interval is the default of 30 s, as in production.
    let env: &[(&str, &str)] = &[("CELLD_LAZY_BIRTH", mode)];
    let Some(fleet) = Fleet::start_counted("do-ids", 3, env).await else {
        return (0.0, 0.0, 0, 0);
    };
    let result = std::sync::Arc::new(std::sync::Mutex::new((0.0, 0.0, 0, 0)));
    let out = result.clone();
    with_fleet(fleet, |fleet| async move {
        wait_for_ensemble(&fleet).await;
        // A warm-up agent settles the session's own writes.
        let warm = spawn_agents(&fleet, 1).await;
        assert_eq!(warm.len(), 1);
        let proxy = fleet.proxy.as_ref().expect("a counted fleet");
        let n = agents();
        // The idle window: the same time without agents.
        let idle_from = proxy.take();
        let idle_started = Instant::now();
        tokio::time::sleep(Duration::from_secs(40)).await;
        let idle = support::diff(&proxy.take(), &idle_from).expect("diff");
        let idle_secs = idle_started.elapsed().as_secs_f64();
        let before = proxy.take();
        let started = Instant::now();
        let ids = spawn_agents(&fleet, n).await;
        assert_eq!(ids.len(), n);
        // The run ends after a checkpoint of the session has fallen in the window.
        tokio::time::sleep(Duration::from_secs(35)).await;
        let after = proxy.take();
        let secs = started.elapsed().as_secs_f64();
        let load = support::diff(&after, &before).expect("diff");
        let (a, b) = class_totals(&load);
        let (idle_a, idle_b) = class_totals(&idle);
        let scale = secs / idle_secs;
        let net_a = a as f64 - idle_a as f64 * scale;
        let net_b = b as f64 - idle_b as f64 * scale;
        let own_get = requests_on(&load, "GET", "/own.json");
        let own_all = requests_on(&load, "", "/own.json");
        let lists = requests_on(&load, "LIST", "cells/");
        eprintln!(
            "S4 mode={mode} agents={n} window={secs:.0}s: all-in A={a} B={b}; idle-scaled A={:.1} B={:.1}; \
             net per agent A={:.3} B={:.3}; own.json requests={own_all} (GET {own_get}); cells LIST={lists}",
            idle_a as f64 * scale,
            idle_b as f64 * scale,
            net_a / n as f64,
            net_b / n as f64,
        );
        for ((kind, shape, part), count) in &load {
            eprintln!("  {kind} {shape} {part:?} {count} (idle {})", idle.get(&(kind.clone(), shape.clone(), *part)).copied().unwrap_or(0));
        }
        *out.lock().unwrap() = (net_a / n as f64, net_b / n as f64, own_all, own_get);
        fleet
    })
    .await;
    let value = *result.lock().unwrap();
    value
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn s4_requests_per_minted_agent_in_shadow() {
    requests_per_minted_agent("shadow").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn s4_requests_per_minted_agent_in_read() {
    let (a, b, own_all, own_get) = requests_per_minted_agent("read").await;
    if a == 0.0 && b == 0.0 {
        return;
    }
    let n = agents() as u64;
    assert_eq!(own_get, 0, "read skips the read of own.json");
    assert_eq!(
        own_all, n,
        "one create-only write of own.json for each agent"
    );
    eprintln!(
        "S4 read within the limit 1.17/0.4: {}",
        a <= 1.17 && b <= 0.4
    );
    assert!(a <= 1.17 && b <= 0.4, "per agent A {a:.3} B {b:.3}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn s4_requests_per_minted_agent_in_on() {
    let (a, b, own_all, own_get) = requests_per_minted_agent("on").await;
    if a == 0.0 && b == 0.0 {
        return;
    }
    assert_eq!(
        own_all, 0,
        "a lazy cell has no request on own.json (GET {own_get})"
    );
    eprintln!("S4 on within the limit 0.17/0.4: {}", a <= 0.17 && b <= 0.4);
    assert!(a <= 0.17 && b <= 0.4, "per agent A {a:.3} B {b:.3}");
}

const ON: &[(&str, &str)] = &[
    ("CELLD_LAZY_BIRTH", "on"),
    ("CELLD_LAZY_BIRTH_CHECKPOINT_MS", "500"),
];

/// A lazy cell has no record. When its minting node dies, a peer takes it
/// after the seal as a takeover that restores the acknowledged write.
#[tokio::test]
async fn a_lazy_cell_is_taken_after_its_minting_node_dies() {
    let Some(fleet) = Fleet::start("do-ids", 3, ON).await else {
        return;
    };
    with_fleet(fleet, |mut fleet| async move {
        wait_for_ensemble(&fleet).await;
        let spawned = fleet.get_json(0, "/spawn?n=2").await.expect("spawn");
        let id = spawned["ids"][0].as_str().unwrap().to_string();
        let tag = id[..16].to_string();
        let header = fleet
            .object(&format!("fleet/sessions/{tag}.json"))
            .await
            .expect("the header");
        let counter = counter_of(&header, &id);
        assert!(
            fleet
                .keys("cells/")
                .await
                .iter()
                .all(|key| !key.ends_with("/own.json")),
            "a lazy cell has no record"
        );
        wait_until("the checkpoint", Duration::from_secs(30), || async {
            let chunk = fleet.object(&format!("fleet/sessions/{tag}/c/0")).await?;
            chunk["granted"]
                .as_array()?
                .contains(&serde_json::json!(counter))
                .then_some(())
        })
        .await;
        fleet.nodes[0].kill();
        let out = wait_until(
            "another node to take the cell",
            Duration::from_secs(180),
            || async {
                for node in [1, 2] {
                    if let Some(out) = fleet.get_json(node, &format!("/bump?id={id}")).await {
                        return Some(out);
                    }
                }
                None
            },
        )
        .await;
        assert_eq!(out["n"], 2, "the acknowledged write was restored");
        let record = fleet
            .object(&format!("cells/Cell:{id}/own.json"))
            .await
            .expect("the taker's record");
        assert_eq!(record["epoch"], 2, "{record}");
        assert!(record["nonce"].is_string(), "a first-form record: {record}");
        let chunk = fleet
            .object(&format!("fleet/sessions/{tag}/c/0"))
            .await
            .unwrap();
        assert_eq!(
            chunk["taken"][counter.to_string()],
            record["nonce"],
            "{chunk}"
        );
        fleet
    })
    .await;
}

/// A grant whose request gave up is pending in every mode. In mode `on` the
/// next request wakes the cell at epoch 2 by the leave: a leave-form record at
/// that epoch and a confirmed left entry. The first request after the give-up
/// never activates the cell at epoch 1.
#[tokio::test]
async fn a_pending_grant_of_a_lazy_cell_wakes_at_epoch_two() {
    let env: &[(&str, &str)] = &[
        ("CELLD_LAZY_BIRTH", "on"),
        ("CELLD_LAZY_BIRTH_CHECKPOINT_MS", "500"),
        ("CELLD_LAZY_BIRTH_PROOF_WAIT_MS", "0"),
    ];
    let Some(fleet) = Fleet::start("do-ids", 3, env).await else {
        return;
    };
    with_fleet(fleet, |fleet| async move {
        wait_for_ensemble(&fleet).await;
        let minted = fleet.get_json(0, "/mint").await.expect("mint");
        let id = minted["ids"][0].as_str().unwrap().to_string();
        let tag = id[..16].to_string();
        let header = fleet
            .object(&format!("fleet/sessions/{tag}.json"))
            .await
            .expect("the header");
        let counter = counter_of(&header, &id);
        assert!(fleet.get_json(0, &format!("/bump?id={id}")).await.is_none());
        assert!(
            fleet
                .object(&format!("cells/Cell:{id}/own.json"))
                .await
                .is_none(),
            "nothing activated the cell at epoch 1"
        );
        let woken = wait_until("the cell to wake", Duration::from_secs(30), || async {
            fleet.get_json(0, &format!("/bump?id={id}")).await
        })
        .await;
        assert_eq!(woken["n"], 1, "the first request never activated the cell");
        let record = fleet
            .object(&format!("cells/Cell:{id}/own.json"))
            .await
            .expect("the leave-form record");
        assert_eq!(record["epoch"], 2, "{record}");
        assert!(record["leave"].is_string(), "{record}");
        assert_eq!(record["by"], tag);
        // The marks reach the chunk by the checkpoint.
        wait_until("the left entry", Duration::from_secs(30), || async {
            let chunk = fleet.object(&format!("fleet/sessions/{tag}/c/0")).await?;
            (chunk["left"][counter.to_string()]["done"] == record["leave"]).then_some(())
        })
        .await;
        fleet
    })
    .await;
}

/// A drain does not hand a dormant lazy cell on: nothing is written for it, and
/// the seal of the session lets a peer take it by a first-form record at the
/// next epoch, with the acknowledged write.
#[tokio::test]
async fn a_drain_leaves_a_dormant_lazy_cell_to_the_seal() {
    let Some(fleet) = Fleet::start("do-ids", 3, ON).await else {
        return;
    };
    with_fleet(fleet, |mut fleet| async move {
        wait_for_ensemble(&fleet).await;
        let spawned = fleet.get_json(0, "/spawn?n=1").await.expect("spawn");
        let id = spawned["ids"][0].as_str().unwrap().to_string();
        let tag = id[..16].to_string();
        fleet.nodes[0].stop();
        let out = wait_until(
            "a peer to serve the cell",
            Duration::from_secs(120),
            || async {
                for node in [1, 2] {
                    if let Some(out) = fleet.get_json(node, &format!("/bump?id={id}")).await {
                        return Some(out);
                    }
                }
                None
            },
        )
        .await;
        assert_eq!(out["n"], 2);
        let record = fleet
            .object(&format!("cells/Cell:{id}/own.json"))
            .await
            .expect("a record");
        assert!(
            record["leave"].is_null(),
            "the drain wrote no leave: {record}"
        );
        assert!(
            record["nonce"].is_string(),
            "a taker's first-form record: {record}"
        );
        let header = fleet
            .object(&format!("fleet/sessions/{tag}.json"))
            .await
            .unwrap();
        assert_eq!(header["state"], "sealed");
        fleet
    })
    .await;
}

/// A leave that only the followers' log and the bundles know (no checkpoint has
/// written it) is sealed into the chunk by the recoverer: the taker finds the
/// confirmed left entry that matches the leave-form record, and takes the cell
/// at the next epoch with the acknowledged write.
#[tokio::test]
async fn a_leave_that_only_the_followers_hold_is_sealed_into_the_chunk() {
    let env: &[(&str, &str)] = &[
        ("CELLD_LAZY_BIRTH", "on"),
        ("CELLD_LAZY_BIRTH_CHECKPOINT_MS", "600000"),
        ("CELLD_LAZY_BIRTH_PROOF_WAIT_MS", "0"),
    ];
    let Some(fleet) = Fleet::start("do-ids", 3, env).await else {
        return;
    };
    with_fleet(fleet, |mut fleet| async move {
        wait_for_ensemble(&fleet).await;
        let minted = fleet.get_json(0, "/mint").await.expect("mint");
        let id = minted["ids"][0].as_str().unwrap().to_string();
        let tag = id[..16].to_string();
        let header = fleet
            .object(&format!("fleet/sessions/{tag}.json"))
            .await
            .expect("the header");
        let counter = counter_of(&header, &id);
        assert!(fleet.get_json(0, &format!("/bump?id={id}")).await.is_none());
        let woken = wait_until("the cell to wake", Duration::from_secs(30), || async {
            fleet.get_json(0, &format!("/bump?id={id}")).await
        })
        .await;
        assert_eq!(woken["n"], 1);
        let record = fleet
            .object(&format!("cells/Cell:{id}/own.json"))
            .await
            .expect("the leave-form record");
        let chunk = fleet
            .object(&format!("fleet/sessions/{tag}/c/0"))
            .await
            .unwrap();
        assert!(
            chunk["left"][counter.to_string()].is_null(),
            "no checkpoint ran: {chunk}"
        );
        fleet.nodes[0].kill();
        let out = wait_until(
            "a peer to take the cell",
            Duration::from_secs(180),
            || async {
                for node in [1, 2] {
                    if let Some(out) = fleet.get_json(node, &format!("/bump?id={id}")).await {
                        return Some(out);
                    }
                }
                None
            },
        )
        .await;
        assert_eq!(out["n"], 2);
        let chunk = fleet
            .object(&format!("fleet/sessions/{tag}/c/0"))
            .await
            .unwrap();
        let left = &chunk["left"][counter.to_string()];
        assert_eq!(left["done"], record["leave"], "{chunk}");
        assert_eq!(chunk["state"], "sealed");
        fleet
    })
    .await;
}

/// An alarm on a lazy cell fires: the cell is found without a record and its
/// retirement rests on a record that the leave wrote.
#[tokio::test]
async fn an_alarm_on_a_lazy_cell_fires() {
    let Some(fleet) = Fleet::start("do-ids", 3, ON).await else {
        return;
    };
    with_fleet(fleet, |fleet| async move {
        wait_for_ensemble(&fleet).await;
        let spawned = fleet.get_json(0, "/spawn?n=1").await.expect("spawn");
        let id = spawned["ids"][0].as_str().unwrap().to_string();
        fleet
            .get_json(0, &format!("/arm?id={id}"))
            .await
            .expect("arm");
        wait_until("the alarm to fire", Duration::from_secs(60), || async {
            let state = fleet.get_json(0, &format!("/state?id={id}")).await?;
            (state["alarmed"] == true).then_some(())
        })
        .await;
        let record = fleet
            .object(&format!("cells/Cell:{id}/own.json"))
            .await
            .expect("the retirement needed a record");
        assert!(record["leave"].is_string(), "{record}");
        fleet
    })
    .await;
}

/// How many cells a drill covers in each window (`LAZY_FLEET_CELLS`).
fn cells() -> usize {
    std::env::var("LAZY_FLEET_CELLS")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(12)
}

/// Ids from `/mint`, which makes three at a time and calls none of them.
async fn mint(fleet: &Fleet, count: usize) -> Vec<String> {
    let mut ids = Vec::new();
    while ids.len() < count {
        let minted = fleet.get_json(0, "/mint").await.expect("mint");
        ids.extend(
            minted["ids"]
                .as_array()
                .unwrap()
                .iter()
                .map(|id| id.as_str().unwrap().to_string()),
        );
    }
    ids.truncate(count);
    ids
}

/// Change the chunk of a session in the bucket by a compare-and-swap, the way
/// a fault would.
async fn edit_chunk(fleet: &Fleet, tag: &str, edit: impl Fn(&mut Value)) {
    let key = format!("fleet/sessions/{tag}/c/0");
    for _ in 0..20 {
        let (bytes, token) = fleet
            .bucket
            .get(&key)
            .await
            .expect("read the chunk")
            .expect("a chunk");
        let mut chunk: Value = serde_json::from_slice(&bytes).expect("a chunk");
        edit(&mut chunk);
        chunk["rev"] = (chunk["rev"].as_u64().unwrap() + 1).into();
        let body = serde_json::to_vec(&chunk).unwrap();
        if fleet
            .bucket
            .put_cas(&key, body, Some(&token))
            .await
            .expect("write the chunk")
            .is_some()
        {
            return;
        }
    }
    panic!("the chunk kept changing under the edit");
}

async fn delete_records(fleet: &Fleet, ids: &[String]) {
    let keys: Vec<String> = ids
        .iter()
        .map(|id| format!("cells/Cell:{id}/own.json"))
        .collect();
    let deleted = fleet.bucket.delete_many(&keys).await;
    assert_eq!(deleted.len(), keys.len());
}

/// The answers of the peers (nodes 1 and 2) to a call of the cell: how many
/// answered, and their counters.
async fn peer_answers(fleet: &Fleet, id: &str) -> Vec<u64> {
    let mut answers = Vec::new();
    for node in [1, 2] {
        if let Some(out) = fleet.get_json(node, &format!("/bump?id={id}")).await {
            answers.push(out["n"].as_u64().unwrap());
        }
    }
    answers
}

const PENDING: &[(&str, &str)] = &[
    ("CELLD_LAZY_BIRTH", "on"),
    ("CELLD_LAZY_BIRTH_CHECKPOINT_MS", "500"),
    ("CELLD_LAZY_BIRTH_PROOF_WAIT_MS", "0"),
];

/// S7. Delete `own.json` in each window of the record-loss table: after a
/// confirmed leave, over an announced leave whose record was never written, over a
/// cell that a taker marked, and under a leave that lands late. Every cell is
/// refused or has one acting writer; none has two.
#[tokio::test]
async fn s7_record_loss_in_each_window_refuses_or_keeps_one_writer() {
    let Some(fleet) = Fleet::start("do-ids", 3, PENDING).await else {
        return;
    };
    with_fleet(fleet, |mut fleet| async move {
        wait_for_ensemble(&fleet).await;
        let n = cells();
        // One chunk holds 1,024 counters: a drill of 4n cells and a little slack
        // stays in the first.
        assert!(4 * n + 32 <= 1024, "at most 248 cells per window in one chunk");
        let minted = mint(&fleet, 4 * n + 32).await;
        let tag = minted[0][..16].to_string();
        let header = fleet
            .object(&format!("fleet/sessions/{tag}.json"))
            .await
            .expect("the header");
        let counter = |id: &String| counter_of(&header, id);
        // The grants are pending: the first call of a cell fails (a call that
        // succeeds anyway raced the proof and is left out). A: the second call
        // wakes the cell at epoch 2 by the leave. The others stay lazy.
        let mut a = Vec::new();
        let mut lazy = Vec::new();
        for id in &minted {
            if a.len() == n && lazy.len() == 3 * n {
                break;
            }
            if fleet.get_json(0, &format!("/bump?id={id}")).await.is_some() {
                continue;
            }
            if a.len() < n {
                wait_until("the cell to wake", Duration::from_secs(30), || async {
                    fleet.get_json(0, &format!("/bump?id={id}")).await
                })
                .await;
                a.push(id.clone());
            } else {
                lazy.push(id.clone());
            }
        }
        assert_eq!((a.len(), lazy.len()), (n, 3 * n), "enough cells were pending");
        let ids: Vec<String> = a.iter().chain(&lazy).cloned().collect();
        let (a, rest) = ids.split_at(n);
        let (b, rest) = rest.split_at(n);
        let (c, d) = rest.split_at(n);
        let everything: Vec<u64> = ids.iter().map(counter).collect();
        wait_until("the checkpoint", Duration::from_secs(60), || async {
            let chunk = fleet.object(&format!("fleet/sessions/{tag}/c/0")).await?;
            let granted: Vec<u64> = chunk["granted"]
                .as_array()?
                .iter()
                .filter_map(Value::as_u64)
                .collect();
            let left_done = a.iter().all(|id| {
                chunk["left"][counter(id).to_string()]["done"].is_string()
            });
            (everything.iter().all(|c| granted.contains(c)) && left_done).then_some(())
        })
        .await;
        // C and D: a leave was announced and its record never written.
        let planted: Vec<(u64, String)> = c
            .iter()
            .chain(d)
            .enumerate()
            .map(|(i, id)| (counter(id), format!("{:016x}", i + 1)))
            .collect();
        edit_chunk(&fleet, &tag, |chunk| {
            for (counter, nonce) in &planted {
                chunk["left"][counter.to_string()] =
                    serde_json::json!({"epoch": 1, "nonces": [nonce], "done": null});
            }
        })
        .await;
        // A: the record is lost after the leave was confirmed.
        delete_records(&fleet, a).await;
        fleet.nodes[0].kill();
        // The seal and the first takers: B has no left entry (K5 from nothing at
        // epoch 2), C and D an open one (K5 at epoch 2).
        // A call goes through the peer that acts, or is forwarded to it, so a
        // writer answers with the next counter every time.
        let mut last = std::collections::BTreeMap::new();
        for id in b.iter().chain(c).chain(d) {
            let answers = wait_until("a peer to take the cell", Duration::from_secs(180), || async {
                let answers = peer_answers(&fleet, id).await;
                (!answers.is_empty()).then_some(answers)
            })
            .await;
            assert_eq!(answers[0], 1, "no write was acknowledged before, and none is lost");
            last.insert(id.clone(), *answers.last().unwrap());
        }
        let mut refused = 0;
        let mut one_writer = 0;
        // A: confirmed leave, no record: refused everywhere.
        for id in a {
            assert!(peer_answers(&fleet, id).await.is_empty(), "{id}");
            refused += 1;
        }
        // B and C: the taker's record is lost.
        delete_records(&fleet, b).await;
        delete_records(&fleet, c).await;
        // D: the taker's record is lost and the stalled leave lands over it.
        delete_records(&fleet, d).await;
        for (id, (_, nonce)) in d.iter().zip(&planted[n..]) {
            let body = serde_json::json!({
                "node": "lazy-0", "epoch": 1, "by": tag, "leave": nonce
            });
            let key = format!("cells/Cell:{id}/own.json");
            let written = fleet
                .bucket
                .put_cas(&key, serde_json::to_vec(&body).unwrap(), None)
                .await
                .expect("the late leave");
            assert!(written.is_some());
        }
        for id in b.iter().chain(c).chain(d) {
            let answers = peer_answers(&fleet, id).await;
            if answers.is_empty() {
                refused += 1;
                continue;
            }
            // One acting writer: its counter goes on from where it was. A second
            // writer would start again from its own state.
            for (next, answer) in (last[id] + 1..).zip(&answers) {
                assert_eq!(*answer, next, "two acting writers for {id}: {answers:?}");
            }
            one_writer += 1;
        }
        // A fresh reader judges each record by itself: a new process of node 0
        // refuses every cell whose taker's record is lost, and writes nothing.
        fleet.respawn(0, PENDING).await;
        for id in b.iter().chain(c).chain(d) {
            assert!(
                fleet.get_json(0, &format!("/bump?id={id}")).await.is_none(),
                "a fresh reader must refuse {id}"
            );
        }
        let keys = fleet.keys("cells/").await;
        for id in b.iter().chain(c) {
            assert!(
                !keys.contains(&format!("cells/Cell:{id}/own.json")),
                "the refusal wrote no record"
            );
        }
        for id in d {
            let record = fleet.object(&format!("cells/Cell:{id}/own.json")).await.unwrap();
            assert!(record["leave"].is_string(), "still the late leave: {record}");
        }
        eprintln!("S7 cells per window {n}: refused {refused}, one acting writer {one_writer}, two writers 0");
        fleet
    })
    .await;
}

/// S12. A node with live lazy cells loses its followers. Each cell's next write
/// is acknowledged on a bucket proof, which leaves the cell first: one
/// create-only write of its record, and the left entries of all the cells in a few
/// chunk writes.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn s12_a_node_that_loses_its_followers_leaves_each_lazy_cell_once() {
    let count: usize = std::env::var("LAZY_FLEET_S12_CELLS")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(100);
    // `LAZY_FLEET_S12_UNCOUNTED` runs the nodes without the counting proxy, to tell
    // the load of the harness from the load of the node.
    let uncounted = std::env::var("LAZY_FLEET_S12_UNCOUNTED").is_ok();
    // `LAZY_FLEET_S12_MODE=off` is the control: the same drill with the flag off.
    let mode = std::env::var("LAZY_FLEET_S12_MODE").unwrap_or_else(|_| "on".into());
    let control = mode == "off";
    let mode: &'static str = Box::leak(mode.into_boxed_str());
    let env: &'static [(&str, &str)] = Box::leak(Box::new([
        ("CELLD_LAZY_BIRTH", mode),
        ("CELLD_LAZY_BIRTH_CHECKPOINT_MS", "500"),
    ]));
    let started_fleet = if uncounted {
        Fleet::start("do-ids", 3, env).await
    } else {
        Fleet::start_counted("do-ids", 3, env).await
    };
    let Some(fleet) = started_fleet else {
        return;
    };
    with_fleet(fleet, |mut fleet| async move {
        if control {
            wait_until("an ensemble", Duration::from_secs(90), || async {
                let lease = fleet.object("nodes/lazy-0.json").await?;
                (lease["log"]["state"] == "open"
                    && !lease["log"]["ensemble"].as_array()?.is_empty())
                .then_some(())
            })
            .await;
        } else {
            wait_for_ensemble(&fleet).await;
        }
        let ids = spawn_agents(&fleet, count).await;
        let tag = ids[0][..16].to_string();
        // Every grant is in a chunk before the followers go.
        if !control {
            wait_until("the checkpoint", Duration::from_secs(60), || async {
                let chunk = fleet.object(&format!("fleet/sessions/{tag}/c/0")).await?;
                (chunk["granted"].as_array()?.len() >= count).then_some(())
            })
            .await;
        }
        let before = fleet.proxy.as_ref().map(|proxy| proxy.take());
        fleet.nodes[1].kill();
        fleet.nodes[2].kill();
        // A probe that writes a small object to the store every 100 ms, from the
        // test's own client: if the store is slow, it is slow for the probe too.
        let probe_bucket = fleet.bucket.clone();
        let probe_stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let probe_flag = probe_stop.clone();
        let probe = tokio::spawn(async move {
            let mut worst = Duration::ZERO;
            while !probe_flag.load(std::sync::atomic::Ordering::SeqCst) {
                let t = Instant::now();
                let _ = probe_bucket.put("probe/latency", b"x".to_vec()).await;
                worst = worst.max(t.elapsed());
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            worst
        });
        // `LAZY_FLEET_S12_BURST` calls at a time: the whole set at once starves the
        // node's runtime on this machine with the flag off as well.
        let burst: usize = std::env::var("LAZY_FLEET_S12_BURST")
            .ok()
            .and_then(|value| value.parse().ok())
            .unwrap_or(count);
        let calls = futures_util::stream::iter(ids.iter().map(|id| {
            let url = format!("{}/bump?id={id}", fleet.nodes[0].public);
            let client = fleet.client.clone();
            async move {
                // One request each: it waits for its acknowledgement, and a failure
                // is a failure.
                let asked = Instant::now();
                let response = client
                    .get(&url)
                    .timeout(Duration::from_secs(100))
                    .send()
                    .await
                    .expect("the request is answered");
                (response.status().is_success(), asked.elapsed())
            }
        }))
        .buffer_unordered(burst)
        .collect::<Vec<_>>();
        let Ok(times) =
            tokio::time::timeout(Duration::from_secs(120), calls).await
        else {
            probe_stop.store(true, std::sync::atomic::Ordering::SeqCst);
            eprintln!("S12 the store's slowest small write for the probe: {:?}", probe.await.unwrap());
            let log = fleet.nodes[0].log();
            let leases: Vec<&str> = log
                .lines()
                .filter(|line| line.contains("node_lease_attempt") || line.contains("FENCE"))
                .rev()
                .take(20)
                .collect();
            eprintln!("lease lines:\n{}", leases.join("\n"));
            let tail: Vec<&str> = log.lines().rev().take(40).collect();
            panic!("a cell did not acknowledge; the node log ends:\n{}", tail.join("\n"));
        };
        let failed = times.iter().filter(|(ok, _)| !ok).count();
        let times: Vec<Duration> = times.into_iter().map(|(_, took)| took).collect();
        if failed > 0 {
            let log = fleet.nodes[0].log();
            let sample: Vec<&str> = log
                .lines()
                .filter(|line| !line.contains(" INFO "))
                .rev()
                .take(15)
                .collect();
            eprintln!("S12 {failed} requests failed; the node log:\n{}", sample.join("\n"));
        }
        probe_stop.store(true, std::sync::atomic::Ordering::SeqCst);
        let store_worst = probe.await.unwrap();
        eprintln!("S12 the store's slowest small write for the probe: {store_worst:?}");
        let slowest = times.iter().max().copied().unwrap();
        let (records, chunks) = match (&fleet.proxy, &before) {
            (Some(proxy), Some(before)) => {
                let load = support::diff(&proxy.take(), before).expect("diff");
                (
                    requests_on(&load, "PUT", "/own.json"),
                    requests_on(&load, "PUT", "fleet/sessions/*/c/#"),
                )
            }
            _ => (count as u64, 0),
        };
        eprintln!(
            "S12 cells {count}: slowest acknowledgement {:.1}s after its request, in bursts of the configured size; own.json PUTs {records}; chunk PUTs {chunks}",
            slowest.as_secs_f64()
        );
        assert!(
            slowest < Duration::from_secs(5),
            "every cell acknowledges within 5 s"
        );
        if control {
            eprintln!("S12 control (flag off): done");
            return fleet;
        }
        assert_eq!(records as usize, count, "one create-only write for each cell");
        assert!(chunks <= 20, "{chunks} chunk writes");
        // The bucket-proof acknowledgement left each cell: a leave-form record and
        // a confirmed left entry.
        let sample = &ids[0];
        let record = fleet
            .object(&format!("cells/Cell:{sample}/own.json"))
            .await
            .expect("a record");
        assert!(record["leave"].is_string(), "{record}");
        let chunk = fleet.object(&format!("fleet/sessions/{tag}/c/0")).await.unwrap();
        let left = chunk["left"].as_object().map_or(0, |left| left.len());
        assert_eq!(left, count, "a left entry for each cell");
        fleet
    })
    .await;
}

fn with_mode(mode: &'static str) -> [(&'static str, &'static str); 2] {
    [
        ("CELLD_LAZY_BIRTH", mode),
        ("CELLD_LAZY_BIRTH_CHECKPOINT_MS", "500"),
    ]
}

/// S14. Roll the flag back from `on` to `read` to `shadow`, restarting the nodes
/// one by one while lazy cells exist. Every lazy cell is still served, whatever
/// the flag of the node that asks, and a new session writes the record again.
#[tokio::test]
async fn s14_a_rollback_from_on_to_read_to_shadow_keeps_every_lazy_cell() {
    let on = with_mode("on");
    let Some(mut fleet) = Fleet::start("do-ids", 3, &on).await else {
        return;
    };
    let count = cells();
    let body = async {
        wait_for_ensemble(&fleet).await;
        let spawned = spawn_agents(&fleet, count).await;
        let tag = spawned[0][..16].to_string();
        assert!(
            fleet
                .keys("cells/")
                .await
                .iter()
                .all(|key| !key.ends_with("/own.json")),
            "the cells are lazy"
        );
        // The followers go down in the flag first: node 1 to `read`, node 2 to
        // `shadow`. The lazy cells are still served by their session on node 0
        // (a read of the state writes nothing and moves no counter), whatever
        // node asks.
        for (index, mode) in [(1_usize, "read"), (2, "shadow")] {
            fleet.respawn(index, &with_mode(mode)).await;
            wait_for_ensemble(&fleet).await;
            for node in 0..3 {
                for id in &spawned {
                    let state = wait_until(
                        "the cell to be served",
                        Duration::from_secs(120),
                        || async { fleet.get_json(node, &format!("/state?id={id}")).await },
                    )
                    .await;
                    assert_eq!(state["n"], 1, "served by its session through node {node}");
                }
            }
        }
        assert!(
            fleet
                .keys("cells/")
                .await
                .iter()
                .all(|key| !key.ends_with("/own.json")),
            "no cell left the lazy state"
        );
        // The minting node goes down and comes back in `read`. The `shadow` node is
        // asked first: the header of the dead session says `on`, so it takes each
        // lazy cell by a first-form claim and restores the acknowledged write.
        fleet.respawn(0, &with_mode("read")).await;
        let mut expected = vec![1_u64; count];
        for (slot, id) in spawned.iter().enumerate() {
            let out = wait_until(
                "the shadow node to take the cell",
                Duration::from_secs(180),
                || async { fleet.get_json(2, &format!("/bump?id={id}")).await },
            )
            .await;
            expected[slot] += 1;
            assert_eq!(out["n"].as_u64().unwrap(), expected[slot], "a lost write");
            let record = fleet
                .object(&format!("cells/Cell:{id}/own.json"))
                .await
                .expect("the taker's record");
            assert!(record["nonce"].is_string(), "a first-form claim: {record}");
        }
        let chunk = fleet
            .object(&format!("fleet/sessions/{tag}/c/0"))
            .await
            .unwrap();
        assert_eq!(
            chunk["taken"].as_object().map_or(0, |t| t.len()),
            count,
            "{chunk}"
        );
        for node in 0..3 {
            for (slot, id) in spawned.iter().enumerate() {
                let out = wait_until(
                    "the cell to be served",
                    Duration::from_secs(120),
                    || async { fleet.get_json(node, &format!("/bump?id={id}")).await },
                )
                .await;
                expected[slot] += 1;
                assert_eq!(out["n"].as_u64().unwrap(), expected[slot], "node {node}");
            }
        }
        // A new session of the flag in force writes its record again: node 0 now
        // runs `read`, node 2 `shadow`.
        let fresh_read = fleet.get_json(0, "/spawn?n=1").await.expect("spawn");
        let id = fresh_read["ids"][0].as_str().unwrap();
        let record = fleet
            .object(&format!("cells/Cell:{id}/own.json"))
            .await
            .expect("a read session writes the record");
        assert_eq!(record["birth"], true, "{record}");
        let fresh_shadow = fleet.get_json(2, "/spawn?n=1").await.expect("spawn");
        let id = fresh_shadow["ids"][0].as_str().unwrap();
        let record = fleet
            .object(&format!("cells/Cell:{id}/own.json"))
            .await
            .expect("a shadow session writes the record");
        assert!(record["birth"].is_null(), "{record}");
    };
    let outcome = std::panic::AssertUnwindSafe(body).catch_unwind().await;
    fleet.nodes.clear();
    fleet.clean().await;
    if let Err(panic) = outcome {
        std::panic::resume_unwind(panic);
    }
}

/// S15. Sessions in `shadow`, then `read`, then `on` hand their cells to peers in
/// a drain. Delete `own.json` of every handed-off cell: each is refused, and no
/// granted id of a `shadow` or `read` session is taken by a first-form claim over
/// nothing, while a lazy cell of the `on` session still is.
#[tokio::test]
async fn s15_mixed_modes_refuse_handed_off_cells_and_take_only_lazy_ones() {
    let shadow = with_mode("shadow");
    let peers = with_mode("on");
    let Some(mut fleet) = Fleet::start_each("do-ids", &[&shadow, &peers, &peers]).await else {
        return;
    };
    let count = cells();
    let body = async {
        wait_for_ensemble(&fleet).await;
        let mut handed: Vec<(&str, String, Vec<String>)> = Vec::new();
        let mut lazy: Vec<String> = Vec::new();
        for mode in ["shadow", "read", "on"] {
            let env: Vec<(&str, &str)> = if mode == "on" {
                vec![
                    ("CELLD_LAZY_BIRTH", "on"),
                    ("CELLD_LAZY_BIRTH_CHECKPOINT_MS", "500"),
                    ("CELLD_LAZY_BIRTH_PROOF_WAIT_MS", "0"),
                ]
            } else {
                with_mode(mode).to_vec()
            };
            if mode != "shadow" {
                fleet.respawn(0, &env).await;
                wait_for_ensemble(&fleet).await;
            }
            let ids = if mode == "on" {
                // The grants are pending; the second request wakes each cell by the
                // leave, so the cells that the drain hands on are not lazy.
                let ids = mint(&fleet, count + count).await;
                for id in &ids[..count] {
                    assert!(fleet.get_json(0, &format!("/bump?id={id}")).await.is_none());
                    wait_until("the cell to wake", Duration::from_secs(30), || async {
                        fleet.get_json(0, &format!("/bump?id={id}")).await
                    })
                    .await;
                }
                // The rest stay lazy; ask once so that they are granted.
                for id in &ids[count..] {
                    assert!(fleet.get_json(0, &format!("/bump?id={id}")).await.is_none());
                }
                lazy = ids[count..].to_vec();
                ids[..count].to_vec()
            } else {
                spawn_agents(&fleet, count).await
            };
            let tag = ids[0][..16].to_string();
            let header = wait_until("the header", Duration::from_secs(30), || async {
                fleet.object(&format!("fleet/sessions/{tag}.json")).await
            })
            .await;
            let _ = header;
            fleet.nodes[0].stop();
            // The drain handed every cell to a peer: each has a record again.
            wait_until("the handoff", Duration::from_secs(120), || async {
                for id in &ids {
                    let record = fleet.object(&format!("cells/Cell:{id}/own.json")).await?;
                    // Adopted by a peer: its record names a later session.
                    (record["by"] != tag.as_str() && record["epoch"].as_u64()? >= 2)
                        .then_some(())?;
                }
                Some(())
            })
            .await;
            handed.push((mode, tag, ids));
        }
        for (mode, _, ids) in &handed {
            delete_records(&fleet, ids).await;
            for id in ids {
                assert!(
                    peer_answers(&fleet, id).await.is_empty(),
                    "a handed-off cell of a {mode} session must be refused"
                );
            }
        }
        // No granted id of `shadow` or `read` was taken over nothing.
        for (mode, tag, ids) in &handed {
            let header = fleet
                .object(&format!("fleet/sessions/{tag}.json"))
                .await
                .unwrap();
            let chunk = fleet
                .object(&format!("fleet/sessions/{tag}/c/0"))
                .await
                .unwrap();
            let taken = chunk["taken"].as_object().cloned().unwrap_or_default();
            for id in ids {
                let counter = counter_of(&header, id).to_string();
                assert!(
                    !taken.contains_key(&counter),
                    "{mode}: a takeover of a granted id"
                );
            }
        }
        // A lazy cell of the `on` session is still taken.
        for id in &lazy {
            let answers = wait_until(
                "a peer to take the lazy cell",
                Duration::from_secs(120),
                || async {
                    let answers = peer_answers(&fleet, id).await;
                    (!answers.is_empty()).then_some(answers)
                },
            )
            .await;
            assert_eq!(answers[0], 1);
        }
        let (_, tag, _) = handed.last().unwrap();
        let header = fleet
            .object(&format!("fleet/sessions/{tag}.json"))
            .await
            .unwrap();
        let chunk = fleet
            .object(&format!("fleet/sessions/{tag}/c/0"))
            .await
            .unwrap();
        for id in &lazy {
            let counter = counter_of(&header, id).to_string();
            assert!(chunk["taken"][&counter].is_string(), "{chunk}");
        }
    };
    let outcome = std::panic::AssertUnwindSafe(body).catch_unwind().await;
    fleet.nodes.clear();
    fleet.clean().await;
    if let Err(panic) = outcome {
        std::panic::resume_unwind(panic);
    }
}

/// B5. A preserved restart with a resident lazy cell: before the log is sealed
/// the cell leaves at epoch 1 (a leave-form record that names the first session
/// and a confirmed left entry), the successor resumes it without a claim and
/// answers with the acknowledged write, and a peer finds the record.
#[tokio::test]
async fn a_preserved_restart_leaves_each_resident_lazy_cell_first() {
    let Some(mut fleet) = Fleet::start("do-ids", 3, ON).await else {
        return;
    };
    let body = async {
        wait_for_ensemble(&fleet).await;
        let spawned = fleet.get_json(0, "/spawn?n=2").await.expect("spawn");
        let ids: Vec<String> = spawned["ids"]
            .as_array()
            .unwrap()
            .iter()
            .map(|id| id.as_str().unwrap().to_string())
            .collect();
        let tag = ids[0][..16].to_string();
        let header = fleet
            .object(&format!("fleet/sessions/{tag}.json"))
            .await
            .unwrap();
        assert!(fleet
            .object(&format!("cells/Cell:{}/own.json", ids[0]))
            .await
            .is_none());
        let client = fleet.client.clone();
        fleet.nodes[0].preserve(&client).await;
        for id in &ids {
            let record = fleet
                .object(&format!("cells/Cell:{id}/own.json"))
                .await
                .expect("the leave wrote a record before the restart");
            assert_eq!(record["epoch"], 1, "{record}");
            assert_eq!(record["by"], tag, "{record}");
            assert!(record["leave"].is_string(), "{record}");
            let chunk = fleet
                .object(&format!("fleet/sessions/{tag}/c/0"))
                .await
                .unwrap();
            let left = &chunk["left"][counter_of(&header, id).to_string()];
            assert_eq!(left["done"], record["leave"], "{chunk}");
        }
        fleet.respawn(0, ON).await;
        for id in &ids {
            let out = wait_until(
                "the successor to answer",
                Duration::from_secs(120),
                || async { fleet.get_json(0, &format!("/bump?id={id}")).await },
            )
            .await;
            assert_eq!(out["n"], 2, "the acknowledged write survived");
            let record = fleet
                .object(&format!("cells/Cell:{id}/own.json"))
                .await
                .unwrap();
            assert_eq!(record["epoch"], 1, "resumed without a claim: {record}");
        }
    };
    let outcome = std::panic::AssertUnwindSafe(body).catch_unwind().await;
    fleet.nodes.clear();
    fleet.clean().await;
    if let Err(panic) = outcome {
        std::panic::resume_unwind(panic);
    }
}
