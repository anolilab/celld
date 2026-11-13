//! The fence rules a node gives its bridges, against a fake container engine.

use std::time::Duration;

use super::fake_engine::FakeEngine;
use super::harness::{run_expecting_exit, wait_until, Server};

async fn rules(extra: &[(&str, &str)]) -> String {
    let engine = FakeEngine::start().await;
    let mut env = engine.env();
    env.extend(extra.iter().map(|(k, v)| (k.to_string(), v.to_string())));
    let env: Vec<(&str, &str)> = env.iter().map(|(k, v)| (k.as_str(), v.as_str())).collect();
    let _server = Server::start("container-exec", &env).await;
    // The node fences its bridges when it starts, before any cell needs them.
    wait_until("the fence to run", Duration::from_secs(30), || async {
        engine.fence_rules()
    })
    .await
}

#[tokio::test]
async fn the_default_fence_has_no_allowance_for_replies_to_the_node() {
    let rules = rules(&[]).await;
    // Only the input chain admits established connections.
    assert_eq!(rules.matches("established,related").count(), 1, "{rules}");
}

#[tokio::test]
async fn auto_with_pinned_addresses_admits_replies_to_them() {
    let rules = rules(&[
        ("CELLD_FENCE_NODE_NETNS", "auto"),
        ("CELLD_FENCE_NODE_ADDRS", "172.17.0.2"),
    ])
    .await;
    assert!(
        rules.contains(
            r#"iifname { "celld0", "celld1" } ct state established,related ip daddr { 172.17.0.2 } accept"#
        ),
        "{rules}"
    );
    // The allowance comes before the rejects of the private ranges.
    assert!(
        rules.find("172.17.0.2").unwrap() < rules.find("169.254.0.0/16").unwrap(),
        "{rules}"
    );
}

#[test]
fn a_bad_setting_stops_the_node() {
    for (name, value) in [
        ("CELLD_FENCE_NODE_NETNS", "maybe"),
        ("CELLD_FENCE_NODE_ADDRS", "172.17.0.2"),
    ] {
        let exited = run_expecting_exit("container-exec", &[(name, value)]);
        assert!(!exited.success, "{name}={value} was accepted");
        assert!(exited.output.contains(name), "{}", exited.output);
    }
}
