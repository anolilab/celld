//! `PeerAuth::new` reads `CELLD_OPERATOR_TOKEN`, so every client built on it
//! sends the token. This is the only test in its binary, because it sets a
//! process-wide variable.

#![allow(clippy::disallowed_methods)]

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

    // With no variable, a client reads the file a node on this machine wrote,
    // and sends no credential when there is none.
    let dir = tempfile::tempdir().unwrap();
    std::env::set_var("CELLD_CONFIG_DIR", dir.path());
    std::env::remove_var("CELLD_OPERATOR_TOKEN");
    let headers = PeerAuth::new([9; 32], "node-a")
        .unwrap()
        .signed_headers("GET", "/state", b"", "node-b")
        .unwrap();
    assert!(headers.get("authorization").is_none());

    let file = dir.path().join("operator-token");
    std::fs::write(&file, "from-the-file\n").unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        std::fs::set_permissions(&file, std::fs::Permissions::from_mode(0o600)).unwrap();
    }
    let headers = PeerAuth::new([9; 32], "node-a")
        .unwrap()
        .signed_headers("GET", "/state", b"", "node-b")
        .unwrap();
    assert_eq!(
        headers.get("authorization").expect("authorization header"),
        "Bearer from-the-file"
    );

    // The variable wins over the file.
    std::env::set_var("CELLD_OPERATOR_TOKEN", "from-the-environment");
    let headers = PeerAuth::new([9; 32], "node-a")
        .unwrap()
        .signed_headers("GET", "/state", b"", "node-b")
        .unwrap();
    assert_eq!(
        headers.get("authorization").expect("authorization header"),
        "Bearer from-the-environment"
    );
}
