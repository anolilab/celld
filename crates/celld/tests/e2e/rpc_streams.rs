use super::harness::Server;

/// A cell keeps a stream that another cell sent it over RPC, and reads it in a
/// later call. The sender feeds the stream after the sending call has
/// returned, so the stream must outlive that call.
#[tokio::test]
async fn a_stream_received_over_rpc_outlives_the_call_that_carried_it() {
    let server = Server::start("rpc-streams", &[]).await;
    assert_eq!(server.get_text("/").await, "sent after the call");
}
