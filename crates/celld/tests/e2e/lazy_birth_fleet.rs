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
use futures_util::FutureExt as _;
use serde_json::Value;

use super::harness::{client, fixture_dir, signal_group, wait_until};

const CELLD: &str = env!("CARGO_BIN_EXE_celld");
const TOKEN: &str = "lazy-birth-fleet-token";

fn region() -> String {
    std::env::var("AWS_REGION").unwrap_or_else(|_| "us-east-1".into())
}

pub struct Node {
    child: Option<Child>,
    pub public: String,
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

    /// Stop the node as an orchestrator does: TERM, then wait for its drain.
    pub fn stop(&mut self) {
        let Some(mut child) = self.child.take() else {
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

impl Drop for Node {
    fn drop(&mut self) {
        let Some(mut child) = self.child.take() else {
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
}

impl Fleet {
    /// `None` when no store is configured.
    pub async fn start(fixture: &str, nodes: usize, env: &[(&str, &str)]) -> Option<Fleet> {
        Fleet::start_each(fixture, &vec![env; nodes]).await
    }

    /// One environment per node.
    pub async fn start_each(fixture: &str, envs: &[&[(&str, &str)]]) -> Option<Fleet> {
        Fleet::start_each_at(fixture, envs, None).await
    }

    /// [`Self::start_each`] with the nodes reaching the store at `node_endpoint`
    /// (a proxy), while the test itself reads and cleans through the store.
    pub async fn start_each_at(
        fixture: &str,
        envs: &[&[(&str, &str)]],
        node_endpoint: Option<String>,
    ) -> Option<Fleet> {
        let Ok(bucket) = std::env::var("CELLD_STORE_CONTRACT_BUCKET") else {
            eprintln!("skipped: CELLD_STORE_CONTRACT_BUCKET is not set");
            return None;
        };
        let store = std::env::var("AWS_ENDPOINT_URL").expect("AWS_ENDPOINT_URL names the store");
        let endpoint = node_endpoint.unwrap_or_else(|| store.clone());
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
            .args(["--bucket", &spec, "--endpoint", &store])
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
        let nodes: Vec<Node> = envs
            .iter()
            .enumerate()
            .map(|(index, env)| Node::spawn(home.path(), index, &spec, &endpoint, env))
            .collect();
        for node in &nodes {
            node.wait_ready(&client).await;
        }
        let bucket = Bucket::open(&spec, Some(&store), &region(), None, Some("lazy-fleet"))
            .unwrap_or_else(|error| panic!("open {spec}: {error:#}"));
        Some(Fleet {
            spec,
            endpoint,
            home,
            nodes,
            client,
            bucket,
        })
    }

    /// Kill every node and start all of them again at once, as an orchestrator
    /// does after a whole-fleet stop, and wait for each to be ready.
    pub async fn restart_all(&mut self, env: &[(&str, &str)]) {
        for node in &mut self.nodes {
            node.kill();
        }
        let count = self.nodes.len();
        for index in 0..count {
            self.nodes[index] =
                Node::spawn(self.home.path(), index, &self.spec, &self.endpoint, env);
        }
        for node in &self.nodes {
            node.wait_ready(&self.client).await;
        }
    }

    /// Replace node `index` by a new process on the same directory.
    pub async fn respawn(&mut self, index: usize, env: &[(&str, &str)]) {
        self.nodes[index].kill();
        let node = Node::spawn(self.home.path(), index, &self.spec, &self.endpoint, env);
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
    // A node grants ids only into an ensemble that has been active for a moment
    // (an ensemble that names a member that is gone fails its first append).
    tokio::time::sleep(Duration::from_secs(2)).await;
}

const SHADOW: &[(&str, &str)] = &[
    ("CELLD_LAZY_BIRTH", "shadow"),
    ("CELLD_TEST_TAGGED_IDS", "1"),
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
        ("CELLD_TEST_TAGGED_IDS", "1"),
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
        ("CELLD_TEST_TAGGED_IDS", "1"),
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
        ("CELLD_TEST_TAGGED_IDS", "1"),
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

/// In `shadow` nothing rests on a grant: a request that gives up waiting for the
/// fleet proof of its `Grant` does not fail, the cell is not left pending, and
/// every later request for it is answered. The failure is logged and the id is
/// still granted in the chunk.
#[tokio::test]
async fn a_grant_without_a_timely_proof_does_not_fail_or_wedge_a_request_in_shadow() {
    let env: &[(&str, &str)] = &[
        ("CELLD_LAZY_BIRTH", "shadow"),
        ("CELLD_TEST_TAGGED_IDS", "1"),
        ("CELLD_LAZY_BIRTH_CHECKPOINT_MS", "500"),
        ("CELLD_LAZY_BIRTH_PROOF_WAIT_MS", "1"),
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
        let answered = fleet.get_json(0, &format!("/bump?id={id}")).await;
        assert_eq!(answered.expect("the request is answered")["n"], 1);
        tokio::time::sleep(Duration::from_secs(3)).await;
        assert_eq!(
            fleet
                .get_json(0, &format!("/bump?id={id}"))
                .await
                .expect("not wedged")["n"],
            2
        );
        assert!(fleet.nodes[0].log().contains("shadow_failure"));
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

fn agents_in_drills() -> usize {
    std::env::var("LAZY_FLEET_DRILL_AGENTS")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(60)
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

/// S11. Every node is killed at once and restarted; one is killed twice more
/// during its start. Every node serves again, every header of an ended session is
/// sealed, and no cell is stuck.
#[tokio::test]
async fn s11_a_fleet_killed_at_once_and_a_node_killed_during_its_start_come_back() {
    let Some(mut fleet) = Fleet::start("do-ids", 3, SHADOW).await else {
        return;
    };
    let body = async {
        wait_for_ensemble(&fleet).await;
        let ids = spawn_agents(&fleet, agents_in_drills().min(30)).await;
        tokio::time::sleep(Duration::from_secs(4)).await;
        for node in &mut fleet.nodes {
            node.kill();
        }
        let restarted = Instant::now();
        for index in [1, 2] {
            let node = Node::spawn(
                fleet.home.path(),
                index,
                &fleet.spec,
                &fleet.endpoint,
                SHADOW,
            );
            fleet.nodes[index] = node;
        }
        // Node 0 is killed twice during its start.
        for _ in 0..2 {
            let node = Node::spawn(fleet.home.path(), 0, &fleet.spec, &fleet.endpoint, SHADOW);
            fleet.nodes[0] = node;
            tokio::time::sleep(Duration::from_millis(1500)).await;
            fleet.nodes[0].kill();
        }
        fleet.respawn(0, SHADOW).await;
        for node in &fleet.nodes {
            node.wait_ready(&fleet.client).await;
        }
        let ready = restarted.elapsed();
        for id in &ids {
            let n = wait_until("an agent to answer", Duration::from_secs(240), || async {
                fleet.get_json(0, &format!("/bump?id={id}")).await
            })
            .await;
            assert_eq!(n["n"], 2, "a write was lost");
        }
        // No header of an ended session stays open.
        wait_until(
            "the old headers to be sealed",
            Duration::from_secs(60),
            || async {
                let keys = fleet.keys("fleet/sessions/").await;
                let mut open = 0;
                for key in keys.iter().filter(|key| key.ends_with(".json")) {
                    let header = fleet.object(key).await?;
                    if header["state"] != "sealed" {
                        open += 1;
                    }
                }
                // The three live sessions are open.
                (open <= 3).then_some(())
            },
        )
        .await;
        eprintln!(
            "S11 {} agents: every node ready {:.1}s after the kill, every agent answers with its last write",
            ids.len(),
            ready.as_secs_f64()
        );
    };
    let outcome = std::panic::AssertUnwindSafe(body).catch_unwind().await;
    fleet.nodes.clear();
    fleet.clean().await;
    if let Err(panic) = outcome {
        std::panic::resume_unwind(panic);
    }
}

/// S13. Lost session objects: a sealed header and chunk are deleted, and a live
/// session's header and chunk are deleted before its node is killed and restarted.
/// Every affected id is refused and none is claimed; the node starts again with the
/// header made again as lost.
#[tokio::test]
async fn s13_lost_session_objects_refuse_their_ids_and_the_node_starts() {
    let Some(mut fleet) = Fleet::start("do-ids", 3, SHADOW).await else {
        return;
    };
    let body = async {
        wait_for_ensemble(&fleet).await;
        let spawned = spawn_agents(&fleet, 6).await;
        let tag = spawned[0][..16].to_string();
        tokio::time::sleep(Duration::from_secs(3)).await;
        fleet.nodes[0].stop();
        // The sealed header and chunk are lost.
        wait_until("the seal", Duration::from_secs(60), || async {
            let header = fleet.object(&format!("fleet/sessions/{tag}.json")).await?;
            (header["state"] == "sealed").then_some(())
        })
        .await;
        let keys = vec![
            format!("fleet/sessions/{tag}.json"),
            format!("fleet/sessions/{tag}/c/0"),
        ];
        assert_eq!(fleet.bucket.delete_many(&keys).await.len(), 2);
        for id in &spawned {
            assert!(peer_answers(&fleet, id).await.is_empty(), "refused: {id}");
        }
        assert!(
            fleet
                .keys("cells/")
                .await
                .iter()
                .all(|key| !key.contains("/ltx/e2/")),
            "no peer claimed one"
        );
        // A live session loses its header and chunk, and its node is killed.
        fleet.respawn(0, SHADOW).await;
        wait_for_ensemble(&fleet).await;
        let live = fleet.get_json(0, "/spawn?n=2").await.expect("spawn");
        let live_tag = live["ids"][0].as_str().unwrap()[..16].to_string();
        let keys = vec![
            format!("fleet/sessions/{live_tag}.json"),
            format!("fleet/sessions/{live_tag}/c/0"),
        ];
        assert_eq!(fleet.bucket.delete_many(&keys).await.len(), 2);
        fleet.nodes[0].kill();
        fleet.respawn(0, SHADOW).await;
        let header = wait_until(
            "the header made again",
            Duration::from_secs(120),
            || async {
                fleet
                    .object(&format!("fleet/sessions/{live_tag}.json"))
                    .await
            },
        )
        .await;
        assert_eq!(header["lost"], true, "{header}");
        assert_eq!(header["state"], "sealed", "{header}");
        for id in live["ids"].as_array().unwrap() {
            assert!(peer_answers(&fleet, id.as_str().unwrap()).await.is_empty());
        }
    };
    let outcome = std::panic::AssertUnwindSafe(body).catch_unwind().await;
    fleet.nodes.clear();
    fleet.clean().await;
    if let Err(panic) = outcome {
        std::panic::resume_unwind(panic);
    }
}

/// S16, a declared loss. Every node is killed within one flush interval of the
/// writes, and the fleet restarts. Each agent answers or is refused; none has two
/// writers (its counter never goes back). The agents that lost their rows are
/// counted, with the flag on and, as the control, off.
async fn s16_run(mode: &'static str) -> (usize, usize, usize) {
    let env: &'static [(&str, &str)] = Box::leak(Box::new([
        ("CELLD_LAZY_BIRTH", mode),
        ("CELLD_TEST_TAGGED_IDS", "1"),
        ("CELLD_LAZY_BIRTH_CHECKPOINT_MS", "500"),
    ]));
    let Some(fleet) = Fleet::start("do-ids", 3, env).await else {
        return (0, 0, 0);
    };
    let out = std::sync::Arc::new(std::sync::Mutex::new((0, 0, 0)));
    let result = out.clone();
    with_fleet(fleet, |mut fleet| async move {
        if mode == "off" {
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
        let ids = spawn_agents(&fleet, agents_in_drills()).await;
        // Inside the flush interval: nothing waits for a bundle.
        for node in &mut fleet.nodes {
            node.kill();
        }
        for index in 0..3 {
            fleet.respawn(index, env).await;
        }
        let (mut kept, mut lost, mut refused) = (0, 0, 0);
        for id in &ids {
            let answer = wait_until(
                "an agent to be judged",
                Duration::from_secs(240),
                || async {
                    for node in 0..3 {
                        if let Some(out) = fleet.get_json(node, &format!("/bump?id={id}")).await {
                            return Some(Some(out["n"].as_u64().unwrap()));
                        }
                    }
                    // A refusal is held for 10 s; after two rounds it is final.
                    tokio::time::sleep(Duration::from_secs(12)).await;
                    for node in 0..3 {
                        if let Some(out) = fleet.get_json(node, &format!("/bump?id={id}")).await {
                            return Some(Some(out["n"].as_u64().unwrap()));
                        }
                    }
                    Some(None)
                },
            )
            .await;
            match answer {
                Some(2) => kept += 1,
                Some(1) => lost += 1,
                Some(other) => panic!("two writers or a counter that went wrong: {other}"),
                None => refused += 1,
            }
        }
        *result.lock().unwrap() = (kept, lost, refused);
        fleet
    })
    .await;
    let value = *out.lock().unwrap();
    value
}

#[tokio::test]
async fn s16_a_declared_loss_leaves_no_agent_with_two_writers() {
    let (kept, lost, refused) = s16_run("shadow").await;
    let (kept_off, lost_off, refused_off) = s16_run("off").await;
    eprintln!(
        "S16 agents {}: flag on kept {kept}, rows lost {lost}, refused {refused}; flag off kept {kept_off}, rows lost {lost_off}, refused {refused_off}",
        agents_in_drills()
    );
}

/// A proxy to the store that can refuse: while it refuses, every open
/// connection is cut and every new one is closed at once.
struct Refusing {
    refusing: tokio::sync::watch::Sender<bool>,
    url: String,
}

impl Refusing {
    async fn start(upstream: &str) -> Refusing {
        let upstream = upstream
            .trim_start_matches("http://")
            .trim_end_matches('/')
            .to_string();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind the proxy");
        let url = format!("http://{}", listener.local_addr().expect("proxy address"));
        let (refusing, watch) = tokio::sync::watch::channel(false);
        tokio::spawn(async move {
            loop {
                let Ok((mut inbound, _)) = listener.accept().await else {
                    return;
                };
                if *watch.borrow() {
                    continue;
                }
                let upstream = upstream.clone();
                let mut watch = watch.clone();
                tokio::spawn(async move {
                    let Ok(mut outbound) = tokio::net::TcpStream::connect(&upstream).await else {
                        return;
                    };
                    celld::asyncrt::select! {
                        _ = tokio::io::copy_bidirectional(&mut inbound, &mut outbound) => {}
                        _ = watch.wait_for(|refusing| *refusing) => {}
                    }
                });
            }
        });
        Refusing { refusing, url }
    }

    fn refuse(&self, on: bool) {
        self.refusing.send_replace(on);
    }
}

/// The bundles of the node logs that the bucket holds.
async fn bundles(fleet: &Fleet) -> usize {
    fleet
        .keys("log/")
        .await
        .iter()
        .filter(|key| key.contains("/bundle/"))
        .count()
}

/// S10. The bucket refuses every request for 60 s while a node grants ids (every
/// node halts when its lease cannot be renewed); then the fleet is started again
/// at once. The bits live only in marks (the checkpoint is an hour away), and the
/// cut falls right after the record of a grant applied, so the acknowledged grant
/// has its proof only in the followers' tails. Meanwhile no object of a cell is
/// written, and afterwards no loss is declared, every acknowledged id is in the
/// sealed chunk of its session, and the retained bundles are back to an idle
/// count within two minutes.
#[tokio::test]
async fn s10_a_bucket_that_refuses_for_a_minute_loses_no_acknowledged_grant() {
    const MARKS_ONLY: &[(&str, &str)] = &[
        ("CELLD_LAZY_BIRTH", "shadow"),
        ("CELLD_TEST_TAGGED_IDS", "1"),
        ("CELLD_LAZY_BIRTH_CHECKPOINT_MS", "3600000"),
    ];
    let Ok(upstream) = std::env::var("AWS_ENDPOINT_URL") else {
        eprintln!("skipped: AWS_ENDPOINT_URL is not set");
        return;
    };
    let proxy = Refusing::start(&upstream).await;
    let Some(mut fleet) =
        Fleet::start_each_at("do-ids", &[MARKS_ONLY; 3], Some(proxy.url.clone())).await
    else {
        return;
    };
    let body = async {
        wait_for_ensemble(&fleet).await;
        // Agents are made one after the other, and the bucket is cut the moment
        // the last one is acknowledged: each has its record, and its mark was
        // proven by the followers, and nothing else holds its grant. The
        // checkpoint is an hour away.
        let mut acknowledged: Vec<String> = Vec::new();
        for _ in 0..5 {
            let spawned = fleet
                .get_json(0, "/spawn?n=1")
                .await
                .expect("the agent is acknowledged");
            acknowledged.extend(
                spawned["ids"]
                    .as_array()
                    .expect("ids")
                    .iter()
                    .map(|id| id.as_str().expect("an id").to_string()),
            );
        }
        proxy.refuse(true);
        let cut_at = Instant::now();
        assert_eq!(acknowledged.len(), 5);
        tokio::time::sleep(Duration::from_secs(60).saturating_sub(cut_at.elapsed())).await;
        // No object of a cell was written while the bucket refused, and no
        // record was released.
        let cells = fleet.keys("cells/").await;
        assert!(
            cells.iter().all(|key| !key.contains("/ltx/")),
            "an object of a cell was written: {cells:?}"
        );
        for key in cells.iter().filter(|key| key.ends_with("/own.json")) {
            let record = fleet.object(key).await.expect("a record");
            assert_ne!(
                record["node"], "",
                "released while the bucket refused: {key}"
            );
        }
        // A node that cannot renew its lease halts. An orchestrator starts the
        // fleet again, all nodes at once, once the store is back.
        proxy.refuse(false);
        fleet.restart_all(MARKS_ONLY).await;
        let tag = acknowledged[0][..16].to_string();
        let header = wait_until("the seal", Duration::from_secs(120), || async {
            let header = fleet.object(&format!("fleet/sessions/{tag}.json")).await?;
            (header["state"] == "sealed").then_some(header)
        })
        .await;
        let losses: Vec<String> = fleet
            .keys("log/")
            .await
            .into_iter()
            .filter(|key| key.ends_with(".loss.json"))
            .collect();
        assert!(losses.is_empty(), "a loss was declared: {losses:?}");
        let chunk = fleet
            .object(&format!("fleet/sessions/{tag}/c/0"))
            .await
            .expect("the chunk");
        assert_eq!(chunk["state"], "sealed", "{chunk}");
        let granted: Vec<u64> = chunk["granted"]
            .as_array()
            .expect("granted")
            .iter()
            .map(|counter| counter.as_u64().expect("a counter"))
            .collect();
        for id in &acknowledged {
            assert!(
                granted.contains(&counter_of(&header, id)),
                "an acknowledged grant is not in the sealed chunk: {id} {chunk}"
            );
        }
        // The count of an idle fleet (the retained bundles of a quiet run).
        wait_until(
            "the retained bundles to return to an idle count",
            Duration::from_secs(120),
            || async { (bundles(&fleet).await <= 10).then_some(()) },
        )
        .await;
    };
    let outcome = std::panic::AssertUnwindSafe(body).catch_unwind().await;
    if outcome.is_err() {
        for (index, node) in fleet.nodes.iter().enumerate() {
            let log = node.log();
            let tail: Vec<&str> = log
                .lines()
                .filter(|line| {
                    ["FENCE", "seal", "recover", "session", "loss", "declared"]
                        .iter()
                        .any(|word| line.contains(word))
                })
                .collect();
            eprintln!(
                "node {index}:\n{}",
                tail[tail.len().saturating_sub(25)..].join("\n")
            );
        }
    }
    proxy.refuse(false);
    fleet.nodes.clear();
    fleet.clean().await;
    if let Err(panic) = outcome {
        std::panic::resume_unwind(panic);
    }
}
