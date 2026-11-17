//! `CELLD_EXTRA_CA_CERTS` reaches a Worker's outbound `wss://` and not celld's
//! own WebSocket, so a private authority an operator trusts for Workers cannot
//! vouch for the control plane. This is the only test in its binary, because
//! it sets process-wide variables.

#![allow(clippy::disallowed_methods)]

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use rustls::pki_types::{PrivateKeyDer, PrivatePkcs8KeyDer};
use tokio::net::TcpListener;

#[tokio::test]
async fn extra_roots_reach_the_worker_dial_and_not_the_control_plane() {
    let _ = rustls::crypto::ring::default_provider().install_default();
    use rcgen::{BasicConstraints, CertificateParams, DnType, IsCa, KeyPair};
    let ca_key = KeyPair::generate().unwrap();
    let mut ca_params = CertificateParams::new(Vec::new()).unwrap();
    ca_params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    ca_params
        .distinguished_name
        .push(DnType::CommonName, "celld test authority");
    let ca = ca_params.self_signed(&ca_key).unwrap();
    let leaf_key = KeyPair::generate().unwrap();
    let leaf = CertificateParams::new(vec!["localhost".to_string()])
        .unwrap()
        .signed_by(&leaf_key, &ca, &ca_key)
        .unwrap();
    let config = rustls::ServerConfig::builder_with_provider(Arc::new(
        rustls::crypto::ring::default_provider(),
    ))
    .with_safe_default_protocol_versions()
    .unwrap()
    .with_no_client_auth()
    .with_single_cert(
        vec![leaf.der().clone()],
        PrivateKeyDer::from(PrivatePkcs8KeyDer::from(leaf_key.serialize_der())),
    )
    .unwrap();
    let acceptor = tokio_rustls::TlsAcceptor::from(Arc::new(config));
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let handshakes = Arc::new(AtomicUsize::new(0));
    let counted = handshakes.clone();
    tokio::spawn(async move {
        while let Ok((stream, _)) = listener.accept().await {
            if acceptor.accept(stream).await.is_ok() {
                counted.fetch_add(1, Ordering::SeqCst);
            }
        }
    });

    let dir = tempfile::tempdir().unwrap();
    let bundle = dir.path().join("extra-ca.pem");
    std::fs::write(&bundle, ca.pem()).unwrap();
    std::env::set_var("CELLD_EXTRA_CA_CERTS", &bundle);
    std::env::set_var("CELLD_EGRESS", "open");
    let url = format!("wss://localhost:{port}/");

    // The Worker's dial trusts the private authority: the TLS handshake
    // completes. The server then closes without an upgrade, which is not what
    // this test is about.
    let worker = celld::ws_client::connect_for_worker(&url, Default::default()).await;
    assert!(
        !matches!(&worker, Err(error) if error.to_string().contains("TLS handshake")),
        "{:?}",
        worker.err().map(|error| error.to_string())
    );
    assert_eq!(handshakes.load(Ordering::SeqCst), 1);

    // celld's own connection does not.
    let own = celld::ws_client::connect(&url, Default::default()).await;
    let error = own.err().expect("the private authority is not trusted");
    assert!(error.to_string().contains("TLS handshake"), "{error}");
    assert_eq!(handshakes.load(Ordering::SeqCst), 1);
}
