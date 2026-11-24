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
        let nodes: Vec<Node> = envs
            .iter()
            .enumerate()
            .map(|(index, env)| Node::spawn(home.path(), index, &spec, &endpoint, env))
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
        })
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
