//! `PeerAuth::new` reads `CELLD_OPERATOR_TOKEN`, so every client built on it
//! sends the token. This is the only test in its binary, because it sets a
//! process-wide variable.

use celld::peer_auth::PeerAuth;

#[test]
fn peer_auth_reads_the_operator_token_from_the_environment() {
    std::env::set_var("CELLD_OPERATOR_TOKEN", "from-the-environment");
    let auth = PeerAuth::new([9; 32], "node-a").expect("a valid token is accepted");
    let headers = auth
        .signed_headers("GET", "/state", b"", "node-b")
        .expect("sign");
    assert_eq!(
        headers.get("authorization").expect("authorization header"),
        "Bearer from-the-environment"
    );

    std::env::set_var("CELLD_OPERATOR_TOKEN", "");
    assert!(
        PeerAuth::new([9; 32], "node-a").is_err(),
        "an empty token is an error, not an unset one"
    );

    std::env::remove_var("CELLD_OPERATOR_TOKEN");
    let headers = PeerAuth::new([9; 32], "node-a")
        .unwrap()
        .signed_headers("GET", "/state", b"", "node-b")
        .unwrap();
    assert!(headers.get("authorization").is_none());
}
