use std::collections::HashSet;

use std::time::Duration;

use super::harness::{run_expecting_exit, wait_until, Server};

async fn isolate_ids(server: &Server) -> Vec<String> {
    let mut ids = Vec::new();
    for cell in ["a", "b", "c"] {
        ids.push(server.get_text(&format!("/{cell}")).await);
    }
    ids
}

#[tokio::test]
async fn cells_share_an_isolate_by_default() {
    let server = Server::start("isolate-packing", &[]).await;
    let ids = isolate_ids(&server).await;
    assert_eq!(ids.iter().collect::<HashSet<_>>().len(), 1, "{ids:?}");
}

#[tokio::test]
async fn one_cell_per_isolate_gives_each_cell_its_own() {
    let server = Server::start("isolate-packing", &[("CELLD_MAX_CELLS_PER_ISOLATE", "1")]).await;
    let ids = isolate_ids(&server).await;
    assert_eq!(ids.iter().collect::<HashSet<_>>().len(), 3, "{ids:?}");
}

#[test]
fn zero_cells_per_isolate_is_refused() {
    let exited = run_expecting_exit("isolate-packing", &[("CELLD_MAX_CELLS_PER_ISOLATE", "0")]);
    assert!(!exited.success, "celld started: {}", exited.output);
    assert!(
        exited.output.contains("must be greater than zero"),
        "{}",
        exited.output
    );
}

#[derive(serde::Deserialize)]
struct Parked {
    isolate: String,
    own: bool,
}

#[derive(serde::Deserialize)]
struct Released {
    isolate: String,
    foreign: bool,
}

/// Cell `a` waits on a promise that cell `b` settles, so `a` continues while
/// `b`'s request is the one running. What `a` writes to its request's `/tmp`
/// must stay with `a`.
#[tokio::test]
async fn a_cell_that_resumes_during_another_keeps_its_own_tmp() {
    let server = std::sync::Arc::new(Server::start("isolate-sharing", &[]).await);
    let parking = tokio::spawn({
        let server = server.clone();
        async move { server.get_json::<Parked>("/a/park").await }
    });
    // Only a parked cell makes the release meaningful.
    wait_until("cell a parks", Duration::from_secs(10), || async {
        (server.get_text("/a/state").await == "true").then_some(())
    })
    .await;
    let released: Released = server.get_json("/b/release").await;
    let parked = tokio::time::timeout(Duration::from_secs(10), parking)
        .await
        .expect("the parked cell never answered")
        .expect("the request task panicked");
    assert_eq!(
        parked.isolate, released.isolate,
        "the cells ran in different isolates, so the test proves nothing"
    );
    assert!(parked.own, "the parked cell lost the directory it created");
    assert!(
        !released.foreign,
        "the releasing cell sees a directory the parked cell created"
    );
}
