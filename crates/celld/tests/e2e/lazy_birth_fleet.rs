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

use super::harness::{client, fixture_dir, run_id, signal_group, wait_until};

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

    /// Send a signal to the node's process group.
    pub fn signal(&self, signal: &str) {
        if let Some(child) = &self.child {
            signal_group(child.id(), signal);
        }
    }

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
        Fleet::start_each_at(fixture, envs, None).await
    }

    /// [`Self::start_each`] with the nodes reaching the store at `node_endpoint`
    /// (a proxy), while the test itself reads and cleans through the store.
    pub async fn start_each_at(
        fixture: &str,
        envs: &[&[(&str, &str)]],
        node_endpoint: Option<String>,
    ) -> Option<Fleet> {
        Fleet::start_inner(fixture, envs, node_endpoint, false).await
    }

    /// A fleet whose nodes reach the bucket through a counting proxy.
    pub async fn start_counted(fixture: &str, nodes: usize, env: &[(&str, &str)]) -> Option<Fleet> {
        Fleet::start_inner(fixture, &vec![env; nodes], None, true).await
    }

    async fn start_inner(
        fixture: &str,
        envs: &[&[(&str, &str)]],
        node_endpoint: Option<String>,
        counted: bool,
    ) -> Option<Fleet> {
        let Ok(bucket) = std::env::var("CELLD_STORE_CONTRACT_BUCKET") else {
            eprintln!("skipped: CELLD_STORE_CONTRACT_BUCKET is not set");
            return None;
        };
        let store = std::env::var("AWS_ENDPOINT_URL").expect("AWS_ENDPOINT_URL names the store");
        let run = run_id("lazy");
        let spec = format!("{}/{run}", bucket.trim_end_matches('/'));
        let proxy = if counted {
            let upstream = store
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
        let endpoint = proxy
            .as_ref()
            .map(|proxy| proxy.url.clone())
            .or(node_endpoint)
            .unwrap_or_else(|| store.clone());
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
            proxy,
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

/// Rolling back from `shadow` to `off`: marks that `shadow` wrote into the
/// bundles and the followers' tails are read or skipped by a node with the flag
/// off, which recovers the log of the killed minting node cleanly, and every
/// agent still answers with its last write.
#[tokio::test]
async fn a_node_with_the_flag_off_recovers_the_marks_that_shadow_wrote() {
    let shadow: &[(&str, &str)] = &[
        ("CELLD_LAZY_BIRTH", "shadow"),
        ("CELLD_TEST_TAGGED_IDS", "1"),
        ("CELLD_LAZY_BIRTH_CHECKPOINT_MS", "3600000"),
    ];
    let off: &[(&str, &str)] = &[("CELLD_LAZY_BIRTH", "off")];
    let Some(fleet) = Fleet::start("do-ids", 3, shadow).await else {
        return;
    };
    with_fleet(fleet, |mut fleet| async move {
        wait_for_ensemble(&fleet).await;
        let ids = spawn_agents(&fleet, 6).await;
        // A bundle carries a mark, and so do the followers' tails.
        wait_until("a bundle with a mark", Duration::from_secs(60), || async {
            for key in fleet.keys("log/").await {
                if !key.ends_with(".ltxb") {
                    continue;
                }
                let (bytes, _) = fleet.bucket.get(&key).await.ok()??;
                if !celld_ltx::bundle::decode_marks(&bytes).ok()?.is_empty() {
                    return Some(());
                }
            }
            None
        })
        .await;
        // The minting node is killed and comes back with the flag off, as a
        // node does in a rollback: its start recovers the log that holds marks.
        fleet.nodes[0].kill();
        fleet.respawn(0, off).await;
        wait_until(
            "the predecessor log to be recovered and sealed",
            Duration::from_secs(120),
            || async {
                fleet.nodes[0]
                    .log()
                    .contains("node log recovered and sealed")
                    .then_some(())
            },
        )
        .await;
        let losses: Vec<String> = fleet
            .keys("log/")
            .await
            .into_iter()
            .filter(|key| key.ends_with(".loss.json"))
            .collect();
        assert!(losses.is_empty(), "a loss was declared: {losses:?}");
        for id in &ids {
            let n = wait_until("an agent to answer", Duration::from_secs(120), || async {
                fleet.get_json(1, &format!("/bump?id={id}")).await
            })
            .await;
            assert_eq!(n["n"], 2, "a write was lost");
        }
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

fn agents() -> usize {
    std::env::var("LAZY_FLEET_AGENTS")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(200)
}

/// The sum of the requests of each class in a tally.
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

/// The requests of one kind (all kinds when empty) whose key shape contains
/// `needle`.
fn requests_on(tally: &support::Tally, kind: &str, needle: &str) -> u64 {
    tally
        .iter()
        .filter(|((k, shape, _), _)| (kind.is_empty() || k == kind) && shape.contains(needle))
        .map(|(_, n)| n)
        .sum()
}

/// The requests a minted agent costs, measured at the bucket through the counting
/// proxy: the window of the run minus an idle window of the same length, divided
/// by the agents. Returns the class A and class B per agent, and the requests on
/// `own.json` (all, and the GETs).
async fn requests_per_minted_agent(mode: &'static str) -> (f64, f64, u64, u64) {
    // The checkpoint interval is the default of 30 s, as in production.
    let env: &[(&str, &str)] = &[("CELLD_LAZY_BIRTH", mode), ("CELLD_TEST_TAGGED_IDS", "1")];
    let Some(fleet) = Fleet::start_counted("do-ids", 3, env).await else {
        return (0.0, 0.0, 0, 0);
    };
    let result = std::sync::Arc::new(std::sync::Mutex::new((0.0, 0.0, 0, 0)));
    let out = result.clone();
    with_fleet(fleet, |fleet| async move {
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
        // A warm-up agent settles the session's own writes.
        assert_eq!(spawn_agents(&fleet, 1).await.len(), 1);
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
        assert_eq!(spawn_agents(&fleet, n).await.len(), n);
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
        eprintln!(
            "S4 mode={mode} agents={n} window={secs:.0}s: all-in A={a} B={b}; idle-scaled A={:.1} B={:.1}; \
             net per agent A={:.3} B={:.3}; own.json requests={own_all} (GET {own_get})",
            idle_a as f64 * scale,
            idle_b as f64 * scale,
            net_a / n as f64,
            net_b / n as f64,
        );
        for ((kind, shape, part), count) in &load {
            eprintln!("  {kind} {shape} {part:?} {count}");
        }
        *out.lock().unwrap() = (net_a / n as f64, net_b / n as f64, own_all, own_get);
        fleet
    })
    .await;
    let value = *result.lock().unwrap();
    value
}

/// S4: requests per minted agent, `read` against the same run with the flag off
/// (random ids). `read` skips the read and the listing of the cell: at most 2.05
/// class A and 1.05 class B per agent, and no GET of `own.json`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn s4_a_minted_agent_in_read_costs_no_read_and_no_listing() {
    let (off_a, off_b, off_own, off_get) = requests_per_minted_agent("off").await;
    let (a, b, own_all, own_get) = requests_per_minted_agent("read").await;
    if a == 0.0 && b == 0.0 {
        return;
    }
    let n = agents() as u64;
    eprintln!("S4 off:  A {off_a:.3} B {off_b:.3}, own.json {off_own} (GET {off_get})");
    eprintln!("S4 read: A {a:.3} B {b:.3}, own.json {own_all} (GET {own_get})");
    assert_eq!(own_get, 0, "read skips the read of own.json");
    assert_eq!(
        own_all, n,
        "one create-only write of own.json for each agent"
    );
    assert!(a <= 2.05 && b <= 1.05, "per agent A {a:.3} B {b:.3}");
    assert!(a < off_a && b < off_b, "read costs less than off");
}

const READ: &[(&str, &str)] = &[
    ("CELLD_LAZY_BIRTH", "read"),
    ("CELLD_TEST_TAGGED_IDS", "1"),
    ("CELLD_LAZY_BIRTH_CHECKPOINT_MS", "500"),
];

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

async fn delete_records(fleet: &Fleet, ids: &[String]) {
    let keys: Vec<String> = ids
        .iter()
        .map(|id| format!("cells/Cell:{id}/own.json"))
        .collect();
    let deleted = fleet.bucket.delete_many(&keys).await;
    assert_eq!(deleted.len(), keys.len());
}

/// Every call of every cell goes through a peer until it answers; the answer is
/// the counter.
async fn answer_through_peers(fleet: &Fleet, id: &str, wait: Duration) -> u64 {
    wait_until("a peer to answer", wait, || async {
        peer_answers(fleet, id).await.first().copied()
    })
    .await
}

/// S5. The minting node is killed under a set of cells that each acknowledged a
/// write. Every cell answers again on a peer with its last acknowledged write.
/// The time until all answer is reported for the flag on and, as the control, off.
async fn s5_run(mode: &'static str) -> (Duration, usize) {
    let env: &'static [(&str, &str)] = Box::leak(Box::new([
        ("CELLD_LAZY_BIRTH", mode),
        ("CELLD_TEST_TAGGED_IDS", "1"),
        ("CELLD_LAZY_BIRTH_CHECKPOINT_MS", "500"),
    ]));
    let Some(fleet) = Fleet::start("do-ids", 3, env).await else {
        return (Duration::ZERO, 0);
    };
    let out = std::sync::Arc::new(std::sync::Mutex::new((Duration::ZERO, 0)));
    let result = out.clone();
    with_fleet(fleet, |mut fleet| async move {
        let lease_ready = mode == "off";
        if lease_ready {
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
        // Let the writes reach a bundle, as they do in a moment of a real run.
        tokio::time::sleep(Duration::from_secs(4)).await;
        // The lease of the minting node renews every few seconds and a peer takes its
        // cells only after it ran out: kill right after a renewal, so that both runs
        // wait for the same lease and the times compare.
        let first = fleet
            .object("nodes/lazy-0.json")
            .await
            .map(|lease| lease["expires_ms"].clone());
        wait_until(
            "a renewal of the lease",
            Duration::from_secs(30),
            || async {
                let now = fleet.object("nodes/lazy-0.json").await?["expires_ms"].clone();
                (Some(&now) != first.as_ref()).then_some(())
            },
        )
        .await;
        fleet.nodes[0].kill();
        let started = Instant::now();
        // All the agents ask at once, as clients do.
        let timed = futures_util::future::join_all(ids.iter().map(|id| async {
            let n = answer_through_peers(&fleet, id, Duration::from_secs(240)).await;
            (n, started.elapsed())
        }))
        .await;
        let wrong = timed.iter().filter(|(n, _)| *n != 2).count();
        let mut times: Vec<Duration> = timed.iter().map(|(_, t)| *t).collect();
        times.sort();
        eprintln!(
            "S5 {mode}: first answer {:.2}s, median {:.2}s, last {:.2}s",
            times[0].as_secs_f64(),
            times[times.len() / 2].as_secs_f64(),
            times[times.len() - 1].as_secs_f64()
        );
        if let Ok(dir) = std::env::var("LAZY_FLEET_KEEP_LOGS") {
            for (index, node) in fleet.nodes.iter().enumerate() {
                let _ = std::fs::write(format!("{dir}/s5-{mode}-node-{index}.log"), node.log());
            }
        }
        *result.lock().unwrap() = (started.elapsed(), wrong);
        fleet
    })
    .await;
    let value = *out.lock().unwrap();
    value
}

#[tokio::test]
async fn s5_killing_the_minting_node_loses_no_write_and_answers_again() {
    let (read, wrong_read) = s5_run("read").await;
    let (off, wrong_off) = s5_run("off").await;
    eprintln!(
        "S5 agents {}: every agent answers {:.1}s after the kill in read (wrong answers {wrong_read}), {:.1}s with it off (wrong {wrong_off})",
        agents_in_drills(),
        read.as_secs_f64(),
        off.as_secs_f64()
    );
    assert_eq!(wrong_read, 0, "a write was lost");
    assert!(
        read <= off + Duration::from_secs(1),
        "the plan allows the control plus one second"
    );
}

/// S6, the paused minting process. The process is stopped for three lease
/// lifetimes and continued. Peers take its cells meanwhile; afterwards no cell has
/// two writers: every answer goes on from the counter of the writer that took it.
#[tokio::test]
async fn s6_a_paused_minting_process_acknowledges_nothing_after_its_cells_were_taken() {
    let Some(fleet) = Fleet::start("do-ids", 3, READ).await else {
        return;
    };
    with_fleet(fleet, |fleet| async move {
        wait_for_ensemble(&fleet).await;
        let ids = spawn_agents(&fleet, agents_in_drills().min(20)).await;
        tokio::time::sleep(Duration::from_secs(3)).await;
        fleet.nodes[0].signal("STOP");
        tokio::time::sleep(Duration::from_secs(30)).await;
        let mut last = Vec::new();
        for id in &ids {
            let n = answer_through_peers(&fleet, id, Duration::from_secs(240)).await;
            assert_eq!(n, 2, "the peer took the cell with its acknowledged write");
            last.push(n);
        }
        fleet.nodes[0].signal("CONT");
        tokio::time::sleep(Duration::from_secs(5)).await;
        // The paused process must not answer from a state of its own: whatever
        // it answers (or forwards) goes on from the peer's counter.
        for (id, before) in ids.iter().zip(&last) {
            if let Some(out) = fleet.get_json(0, &format!("/bump?id={id}")).await {
                assert!(out["n"].as_u64().unwrap() > *before, "{out}");
            }
            let peer = answer_through_peers(&fleet, id, Duration::from_secs(60)).await;
            assert!(peer > *before);
        }
        fleet
    })
    .await;
}

/// S9. Every node's disk is wiped and the fleet starts again from the bucket. The
/// agents answer with the state that the same drill gives with the flag off.
async fn s9_run(mode: &'static str) -> Vec<u64> {
    let env: &'static [(&str, &str)] = Box::leak(Box::new([
        ("CELLD_LAZY_BIRTH", mode),
        ("CELLD_TEST_TAGGED_IDS", "1"),
        ("CELLD_LAZY_BIRTH_CHECKPOINT_MS", "500"),
    ]));
    let Some(fleet) = Fleet::start("do-ids", 3, env).await else {
        return Vec::new();
    };
    let out = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
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
        // Past the flush of the bundles and the checkpoint.
        tokio::time::sleep(Duration::from_secs(6)).await;
        for node in &mut fleet.nodes {
            node.kill();
        }
        for index in 0..3 {
            let _ = std::fs::remove_dir_all(fleet.home.path().join(format!("node-{index}")));
        }
        for index in 0..3 {
            fleet.respawn(index, env).await;
        }
        let mut answers = Vec::new();
        for id in &ids {
            let n = wait_until("an agent to answer", Duration::from_secs(240), || async {
                fleet.get_json(0, &format!("/bump?id={id}")).await
            })
            .await;
            answers.push(n["n"].as_u64().unwrap());
        }
        *result.lock().unwrap() = answers;
        fleet
    })
    .await;
    let value = out.lock().unwrap().clone();
    value
}

#[tokio::test]
async fn s9_wiped_disks_restart_from_the_bucket_with_the_answers_of_the_flag_off_run() {
    let read = s9_run("read").await;
    let off = s9_run("off").await;
    eprintln!(
        "S9 agents {}: answers equal {}, read {:?}, off {:?}",
        read.len(),
        read == off,
        read.iter().filter(|n| **n == 2).count(),
        off.iter().filter(|n| **n == 2).count()
    );
    assert_eq!(read, off);
}

/// New agents are made at a steady pace by a few clients through all three
/// nodes while the nodes are restarted one at a time: first by a drain, then twice
/// by a preserved restart with resident cells. A call that fails is tried on the
/// next node. Reports the success of new ids and the time of each restart.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn s8_rolling_and_preserved_restarts_under_a_stream_of_new_agents() {
    let mode: &'static str = Box::leak(
        std::env::var("LAZY_FLEET_S8_MODE")
            .unwrap_or_else(|_| "read".into())
            .into_boxed_str(),
    );
    let env: &'static [(&str, &str)] = Box::leak(Box::new([
        ("CELLD_LAZY_BIRTH", mode),
        ("CELLD_TEST_TAGGED_IDS", "1"),
        ("CELLD_LAZY_BIRTH_CHECKPOINT_MS", "500"),
    ]));
    let Some(mut fleet) = Fleet::start("do-ids", 3, env).await else {
        return;
    };
    let body = async {
        if mode != "off" {
            wait_for_ensemble(&fleet).await;
        } else {
            tokio::time::sleep(Duration::from_secs(20)).await;
        }
        let resident = spawn_agents(&fleet, 20).await;
        let urls: Vec<String> = fleet.nodes.iter().map(|node| node.public.clone()).collect();
        let _ = urls;
        let stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let ok = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0));
        let bad = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0));
        // The nodes change address on a restart: the clients read the current
        // addresses from a shared list.
        let addresses = std::sync::Arc::new(std::sync::Mutex::new(urls));
        let mut clients = Vec::new();
        for worker in 0..6 {
            let (stop, ok, bad, addresses, client) = (
                stop.clone(),
                ok.clone(),
                bad.clone(),
                addresses.clone(),
                fleet.client.clone(),
            );
            clients.push(tokio::spawn(async move {
                let mut turn = worker;
                while !stop.load(std::sync::atomic::Ordering::SeqCst) {
                    let mut done = false;
                    for attempt in 0..3 {
                        let url = {
                            let addresses = addresses.lock().unwrap();
                            addresses[(turn + attempt) % addresses.len()].clone()
                        };
                        let response = client
                            .get(format!("{url}/spawn?n=1"))
                            .timeout(Duration::from_secs(20))
                            .send()
                            .await;
                        match response {
                            Ok(r) if r.status().is_success() => {
                                done = true;
                                break;
                            }
                            Ok(r) => {
                                let status = r.status();
                                let body = r.text().await.unwrap_or_default();
                                eprintln!(
                                    "S8 failed call: {status} {}",
                                    &body[..body.len().min(160)]
                                );
                            }
                            Err(error) => eprintln!("S8 failed call: {error}"),
                        }
                    }
                    let counter = if done { &ok } else { &bad };
                    counter.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    turn += 1;
                    tokio::time::sleep(Duration::from_millis(60)).await;
                }
            }));
        }
        let started = Instant::now();
        let mut times = Vec::new();
        let publish = |fleet: &Fleet| {
            *addresses.lock().unwrap() = fleet.nodes.iter().map(|n| n.public.clone()).collect();
        };
        // A rolling restart by a drain.
        for index in 0..3 {
            let t = Instant::now();
            fleet.nodes[index].stop();
            fleet.respawn(index, env).await;
            publish(&fleet);
            times.push(("drain", t.elapsed()));
        }
        // Two deploys in a row, each a preserved restart of every node.
        for _ in 0..2 {
            for index in 0..3 {
                let t = Instant::now();
                let client = fleet.client.clone();
                fleet.nodes[index].preserve(&client).await;
                let node = Node::spawn(fleet.home.path(), index, &fleet.spec, &fleet.endpoint, env);
                node.wait_ready(&fleet.client).await;
                fleet.nodes[index] = node;
                publish(&fleet);
                times.push(("preserved", t.elapsed()));
            }
        }
        stop.store(true, std::sync::atomic::Ordering::SeqCst);
        for client in clients {
            let _ = client.await;
        }
        for (index, node) in fleet.nodes.iter().enumerate() {
            let log = node.log();
            for needle in [
                "SELF-FENCE",
                "clean_reload_abandoned",
                "pending_grant",
                "NodeFenced",
            ] {
                let lines: Vec<&str> = log.lines().filter(|line| line.contains(needle)).collect();
                if let Some(first) = lines.first() {
                    eprintln!(
                        "S8 node {index} {needle}: {} lines, first: {}",
                        lines.len(),
                        &first[..first.len().min(300)]
                    );
                }
            }
        }
        if let Ok(dir) = std::env::var("LAZY_FLEET_KEEP_LOGS") {
            for (index, node) in fleet.nodes.iter().enumerate() {
                let _ = std::fs::write(format!("{dir}/s8-node-{index}.log"), node.log());
            }
        }
        let (ok, bad) = (
            ok.load(std::sync::atomic::Ordering::SeqCst),
            bad.load(std::sync::atomic::Ordering::SeqCst),
        );
        let secs = started.elapsed().as_secs_f64();
        let slowest = times.iter().map(|(_, t)| *t).max().unwrap();
        eprintln!(
            "S8 {:.0}s, {ok} new agents ok, {bad} failed after three attempts: {:.2}% success, {:.1} ids/s; slowest restart {:.1}s",
            secs,
            100.0 * ok as f64 / (ok + bad).max(1) as f64,
            ok as f64 / secs,
            slowest.as_secs_f64()
        );
        assert!(
            ok as f64 / (ok + bad).max(1) as f64 >= 0.999,
            "new agents succeed at least 99.9% of the time"
        );
        // The resident cells answer after the second deploy and keep counting.
        for id in &resident {
            let n = wait_until(
                "a resident cell to answer",
                Duration::from_secs(120),
                || async { fleet.get_json(0, &format!("/bump?id={id}")).await },
            )
            .await;
            assert_eq!(n["n"], 2, "the acknowledged write survived two deploys");
        }
    };
    let outcome = std::panic::AssertUnwindSafe(body).catch_unwind().await;
    fleet.nodes.clear();
    fleet.clean().await;
    if let Err(panic) = outcome {
        std::panic::resume_unwind(panic);
    }
}

/// S15, the `shadow` and `read` halves. Sessions in `shadow`, then in `read`,
/// hand their cells to peers in a drain. Delete `own.json` of every handed-off cell:
/// each is refused, and no granted id of a session of either mode is taken by a
/// first-form claim over nothing.
#[tokio::test]
async fn s15_shadow_and_read_cells_are_refused_when_their_record_is_lost() {
    let Some(mut fleet) = Fleet::start_each("do-ids", &[&MODE_SHADOW, READ, READ]).await else {
        return;
    };
    let count = cells();
    let body = async {
        wait_for_ensemble(&fleet).await;
        let mut handed: Vec<(&str, String, Vec<String>)> = Vec::new();
        for mode in ["shadow", "read"] {
            if mode == "read" {
                fleet.respawn(0, READ).await;
                wait_for_ensemble(&fleet).await;
            }
            let ids = spawn_agents(&fleet, count).await;
            let tag = ids[0][..16].to_string();
            fleet.nodes[0].stop();
            // The drain handed every cell to a peer: each has a record that names a
            // later session at a later epoch.
            wait_until("the handoff", Duration::from_secs(120), || async {
                for id in &ids {
                    let record = fleet.object(&format!("cells/Cell:{id}/own.json")).await?;
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
    };
    let outcome = std::panic::AssertUnwindSafe(body).catch_unwind().await;
    fleet.nodes.clear();
    fleet.clean().await;
    if let Err(panic) = outcome {
        std::panic::resume_unwind(panic);
    }
}

const MODE_SHADOW: [(&str, &str); 3] = [
    ("CELLD_LAZY_BIRTH", "shadow"),
    ("CELLD_TEST_TAGGED_IDS", "1"),
    ("CELLD_LAZY_BIRTH_CHECKPOINT_MS", "500"),
];

/// The time of the first request of a new minted agent, over `count` agents, in
/// milliseconds sorted.
async fn first_request_times(nodes: usize, mode: &'static str, count: usize) -> Vec<f64> {
    let env: &'static [(&str, &str)] = Box::leak(Box::new([
        ("CELLD_LAZY_BIRTH", mode),
        ("CELLD_TEST_TAGGED_IDS", "1"),
        ("CELLD_LAZY_BIRTH_CHECKPOINT_MS", "500"),
    ]));
    let Some(fleet) = Fleet::start("do-ids", nodes, env).await else {
        return Vec::new();
    };
    let out = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
    let result = out.clone();
    with_fleet(fleet, |fleet| async move {
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
        assert_eq!(spawn_agents(&fleet, 3).await.len(), 3, "a warm-up");
        let mut times = Vec::new();
        for _ in 0..count {
            let began = Instant::now();
            let out = fleet.get_json(0, "/spawn?n=1").await;
            assert!(out.is_some(), "an agent was created");
            times.push(began.elapsed().as_secs_f64() * 1_000.0);
        }
        times.sort_by(|a, b| a.partial_cmp(b).unwrap());
        *result.lock().unwrap() = times;
        fleet
    })
    .await;
    let value = out.lock().unwrap().clone();
    value
}

/// S3, the cold path of a new minted agent: the time of its first response in
/// `read` is not worse than with the flag off, at 2 and at 4 nodes.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn s3_the_first_response_of_a_minted_agent_in_read_is_not_slower_than_off() {
    for nodes in [2, 4] {
        let off = first_request_times(nodes, "off", 100).await;
        let read = first_request_times(nodes, "read", 100).await;
        if off.is_empty() || read.is_empty() {
            return;
        }
        let p =
            |times: &[f64], q: f64| times[((times.len() as f64 * q) as usize).min(times.len() - 1)];
        eprintln!(
            "S3 {nodes} nodes: off p50 {:.1} ms p99 {:.1} ms; read p50 {:.1} ms p99 {:.1} ms",
            p(&off, 0.5),
            p(&off, 0.99),
            p(&read, 0.5),
            p(&read, 0.99)
        );
        assert!(
            p(&read, 0.5) <= p(&off, 0.5) + 5.0,
            "{nodes} nodes: the median of read is not worse than off"
        );
        assert!(
            p(&read, 0.99) <= p(&off, 0.99) + 25.0,
            "{nodes} nodes: the p99 of read is not worse than off, within noise"
        );
    }
}

/// "Why P3 is needed". S minted an id and never granted it. The fleet claimed the
/// id on its own (it ran the gate on S, sealed S's header, and claimed it at epoch
/// 2), and that record was then lost. S's create-only write of a birth-form record
/// now succeeds over the lost one. The record must not hide the loss: a reader
/// finds that no grant covers it, finds LTX under the cell, and refuses it, as the
/// standard path does. A cell that no node ever claimed (no LTX) is still taken.
#[tokio::test]
async fn a_birth_form_record_without_a_grant_is_refused_over_a_lost_claim() {
    let Some(mut fleet) = Fleet::start("do-ids", 3, READ).await else {
        return;
    };
    let body = async {
        wait_for_ensemble(&fleet).await;
        let ids = mint(&fleet, 3).await;
        let tag = ids[0][..16].to_string();
        let (claimed, fresh) = (ids[0].clone(), ids[1].clone());
        // S is gone before it decides anything. The fleet runs the gate on it,
        // which seals its header, and claims the id as a new cell at epoch 2.
        fleet.nodes[0].kill();
        let n = wait_until(
            "a peer to claim the id",
            Duration::from_secs(180),
            || async { peer_answers(&fleet, &claimed).await.first().copied() },
        )
        .await;
        assert_eq!(n, 1);
        wait_until(
            "the first object of the claim",
            Duration::from_secs(90),
            || async {
                fleet
                    .keys(&format!("cells/Cell:{claimed}/ltx/"))
                    .await
                    .iter()
                    .any(|key| key.contains("/e2/"))
                    .then_some(())
            },
        )
        .await;
        // The record of the claim is lost, and S's late write of its birth-form
        // record applies over it.
        delete_records(&fleet, std::slice::from_ref(&claimed)).await;
        let late = serde_json::json!({
            "node": "lazy-0", "epoch": 1, "by": tag, "birth": true,
        });
        for id in [&claimed, &fresh] {
            fleet
                .bucket
                .put_cas(
                    &format!("cells/Cell:{id}/own.json"),
                    serde_json::to_vec(&late).unwrap(),
                    None,
                )
                .await
                .unwrap()
                .expect("the late write applies");
        }
        // With LTX the record hides a lost claim: refused.
        assert!(
            peer_answers(&fleet, &claimed).await.is_empty(),
            "the cell with LTX must be refused"
        );
        // Without LTX nothing was ever claimed: the record is replaced at epoch 2.
        let n = wait_until(
            "a peer to take the unclaimed cell",
            Duration::from_secs(120),
            || async { peer_answers(&fleet, &fresh).await.first().copied() },
        )
        .await;
        assert_eq!(n, 1);
        let record = fleet
            .object(&format!("cells/Cell:{fresh}/own.json"))
            .await
            .unwrap();
        assert_eq!(record["epoch"], 2, "{record}");
    };
    let outcome = std::panic::AssertUnwindSafe(body).catch_unwind().await;
    fleet.nodes.clear();
    fleet.clean().await;
    if let Err(panic) = outcome {
        std::panic::resume_unwind(panic);
    }
}
