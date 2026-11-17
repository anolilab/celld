//! The trust roots for outbound TLS.
//!
//! The bundled public roots are always trusted. `CELLD_EXTRA_CA_CERTS` names a
//! PEM file whose certificates are trusted in addition, for an origin behind a
//! private authority. They apply to what a Worker dials: `fetch()`,
//! `connect()` with `secureTransport`, `startTls()` and outbound `wss://`. One
//! setting covers all of them, so a Worker cannot reach an origin through
//! `connect()` that `fetch()` refuses, or the reverse. celld's own connections,
//! the control-plane WebSocket included, trust the bundled roots only, so a
//! private authority an operator adds for Workers cannot vouch for celld's
//! control plane.
//!
//! The file is read once. An unreadable file, a file with no certificate, and
//! a certificate that cannot be a trust anchor all refuse the start, so a typo
//! cannot leave a node silently trusting less than the operator meant.

// The bundle is an operator file read at start-up, not node storage.
#![allow(clippy::disallowed_methods)]

use anyhow::{bail, Context as _};
use rustls::pki_types::{pem::PemObject as _, CertificateDer};
use std::sync::OnceLock;

pub const ENV: &str = "CELLD_EXTRA_CA_CERTS";

/// The operator's extra roots, as DER. Empty when the variable is unset.
pub fn extra() -> anyhow::Result<&'static [CertificateDer<'static>]> {
    static EXTRA: OnceLock<Result<Vec<CertificateDer<'static>>, String>> = OnceLock::new();
    EXTRA
        .get_or_init(|| load().map_err(|error| format!("{error:#}")))
        .as_deref()
        .map_err(|message| anyhow::anyhow!("{message}"))
}

fn load() -> anyhow::Result<Vec<CertificateDer<'static>>> {
    load_from(crate::env_vars::value(ENV)?.as_deref())
}

/// The certificates in the PEM file at `path`, none when `path` is `None`.
fn load_from(path: Option<&str>) -> anyhow::Result<Vec<CertificateDer<'static>>> {
    let Some(path) = path else {
        return Ok(Vec::new());
    };
    let pem = std::fs::read(path).with_context(|| format!("{ENV}: read {path}"))?;
    let certs = CertificateDer::pem_slice_iter(&pem)
        .collect::<Result<Vec<_>, _>>()
        .with_context(|| format!("{ENV}: {path} is not a PEM certificate bundle"))?;
    if certs.is_empty() {
        bail!("{ENV}: {path} holds no certificate");
    }
    let mut check = rustls::RootCertStore::empty();
    for (index, cert) in certs.iter().enumerate() {
        check
            .add(cert.clone())
            .with_context(|| format!("{ENV}: certificate {} in {path} is not usable", index + 1))?;
    }
    Ok(certs)
}

fn bundled() -> rustls::RootCertStore {
    rustls::RootCertStore {
        roots: webpki_roots::TLS_SERVER_ROOTS.to_vec(),
    }
}

fn connector_for(roots: rustls::RootCertStore) -> tokio_rustls::TlsConnector {
    let config = rustls::ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    tokio_rustls::TlsConnector::from(std::sync::Arc::new(config))
}

/// The bundled roots plus the operator's. The start-up check has validated
/// the operator's, so a failure here is a bug.
pub fn root_store() -> rustls::RootCertStore {
    let mut roots = bundled();
    for cert in extra().expect("validated CELLD_EXTRA_CA_CERTS") {
        roots
            .add(cert.clone())
            .expect("validated CELLD_EXTRA_CA_CERTS");
    }
    roots
}

/// The TLS connector for what a Worker dials: the bundled roots plus the
/// operator's.
pub fn connector() -> tokio_rustls::TlsConnector {
    static CONNECTOR: OnceLock<tokio_rustls::TlsConnector> = OnceLock::new();
    CONNECTOR
        .get_or_init(|| connector_for(root_store()))
        .clone()
}

/// The TLS connector for celld's own connections: the bundled roots only.
/// Mozilla's roots, deliberately not the platform store, so a downloaded celld
/// reaches `wss://` on a host that has no `/etc/ssl/certs`.
pub fn bundled_connector() -> tokio_rustls::TlsConnector {
    static CONNECTOR: OnceLock<tokio_rustls::TlsConnector> = OnceLock::new();
    CONNECTOR.get_or_init(|| connector_for(bundled())).clone()
}

/// Add the operator's roots to a Worker's HTTP client. The start-up check has
/// validated them, so a failure here is a bug.
pub fn apply(mut builder: reqwest::ClientBuilder) -> reqwest::ClientBuilder {
    for cert in extra().expect("validated CELLD_EXTRA_CA_CERTS") {
        builder = builder.add_root_certificate(
            reqwest::Certificate::from_der(cert).expect("validated CELLD_EXTRA_CA_CERTS"),
        );
    }
    builder
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unset_adds_nothing() {
        assert!(load_from(None).unwrap().is_empty());
    }

    #[test]
    fn unreadable_file_is_refused() {
        let error = load_from(Some("/nonexistent/celld-ca.pem")).unwrap_err();
        assert!(error.to_string().contains(ENV));
    }

    #[test]
    fn a_file_without_a_certificate_is_refused() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("empty.pem");
        std::fs::write(&path, "not a certificate\n").unwrap();
        let error = load_from(path.to_str()).unwrap_err();
        assert!(error.to_string().contains("no certificate"));
    }

    #[test]
    fn garbage_inside_a_certificate_block_is_refused() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("bad.pem");
        std::fs::write(
            &path,
            "-----BEGIN CERTIFICATE-----\n!!!!\n-----END CERTIFICATE-----\n",
        )
        .unwrap();
        assert!(load_from(path.to_str()).is_err());
    }
}
