use std::collections::HashSet;

use super::harness::{run_expecting_exit, Server};

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
