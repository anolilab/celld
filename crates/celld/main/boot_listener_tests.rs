//! The boot serves its follower disks while it publishes its address.

use super::*;
use celld::operator_token::OperatorToken;

/// The execution domain the binary's tests share, bound once: the drain's
/// timer and the follower store run on it. The handler budget is short so a
/// drain test does not wait for a minute.
pub(crate) fn host() {
    static HOST: std::sync::OnceLock<tokio::runtime::Runtime> = std::sync::OnceLock::new();
    HOST.get_or_init(|| {
        std::env::set_var("CELLD_HANDLER_BUDGET_S", "1");
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .expect("host runtime");
        celld::asyncrt::set_host_handle(runtime.handle().clone());
        runtime
    });
}

fn token() -> OperatorToken {
    OperatorToken::parse(std::ffi::OsStr::new("s3cret")).unwrap()
}

fn app(follower: Arc<celld::node_log::FollowerStore>) -> AppHandle {
    AppHandle {
        tx: mpsc::unbounded_channel().0,
        runtime: None,
        reload: celld::generation::reload_channel().0,
        peer_http: reqwest::Client::new(),
        peer_auth: Arc::new(
            PeerAuth::new([5; 32], "node-b")
                .unwrap()
                .with_operator_token(Some(token())),
        ),
        operator_token: token(),
        email_inbound_token: None,
        advertise: "127.0.0.1:1".to_string(),
        websockets: mpsc::unbounded_channel().0,
        max_outbound_websockets: 1,
        draining: Arc::new(std::sync::atomic::AtomicBool::new(false)),
        fleet_ready: Arc::new(std::sync::atomic::AtomicBool::new(false)),
        public_in_flight: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
        drain_pins: Default::default(),
        follower: Some(follower),
        trust_forwarded_headers: false,
        max_request_body_bytes: 1 << 20,
        operation_deadline_ms: 1_000,
    }
}

// A publish that is stalled (a slow bucket, retrying) must not stop a peer's
// seal and tail requests from being answered.
#[test]
fn a_stalled_publish_does_not_stop_the_follower_listener_answering() {
    host();
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(async {
            let dir = tempfile::TempDir::new().unwrap();
            let follower = Arc::new(celld::node_log::FollowerStore::new(
                dir.path(),
                None,
                "node-b",
            ));
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let addr = listener.local_addr().unwrap();
            let boot = publish_then_recover(
                &listener,
                app(follower),
                std::future::pending::<anyhow::Result<()>>(),
                async { Ok(()) },
            );
            let probe = async {
                let client = PeerAuth::new([5; 32], "node-a")
                    .unwrap()
                    .with_operator_token(Some(token()));
                let body = br#"{"leader":"node-a/g1","epoch":1}"#.to_vec();
                let request = client
                    .sign(
                        reqwest::Client::new().post(format!("http://{addr}/peer/log/seal")),
                        "POST",
                        "/peer/log/seal",
                        &body,
                        "node-b",
                    )
                    .unwrap();
                tokio::time::timeout(
                    std::time::Duration::from_secs(10),
                    request.body(body).send(),
                )
                .await
                .expect("the seal request was never answered")
                .unwrap()
            };
            futures_util::pin_mut!(boot, probe);
            match futures_util::future::select(boot, probe).await {
                futures_util::future::Either::Right((response, _)) => {
                    assert!(
                        response.status().is_success(),
                        "the seal request got {}",
                        response.status()
                    );
                }
                futures_util::future::Either::Left(_) => {
                    panic!("the boot ended although its publish never finished")
                }
            }
        });
}
