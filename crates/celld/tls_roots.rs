//! The trust roots for outbound TLS: a Worker's `fetch()`, `connect()` with
//! `secureTransport`, `startTls()` and outbound `wss://`.
//!
//! The bundled public roots are always trusted. `CELLD_EXTRA_CA_CERTS` names a
//! PEM file whose certificates are trusted in addition, for an origin behind a
//! private authority. One setting covers every path, so a Worker cannot reach
//! an origin through `connect()` that `fetch()` refuses, or the reverse.
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
    let Some(path) = crate::env_vars::value(ENV)? else {
        return Ok(Vec::new());
    };
    let pem = std::fs::read(&path).with_context(|| format!("{ENV}: read {path}"))?;
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

/// The bundled roots plus the operator's.
pub fn root_store() -> anyhow::Result<rustls::RootCertStore> {
    let mut roots = rustls::RootCertStore {
        roots: webpki_roots::TLS_SERVER_ROOTS.to_vec(),
    };
    for cert in extra()? {
        roots.add(cert.clone())?;
    }
    Ok(roots)
}

/// Add the operator's roots to an HTTP client. The start-up check has
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

    fn with_env<T>(value: Option<&str>, run: impl FnOnce() -> T) -> T {
        // The variable is process-wide; only this module's tests read it.
        static LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());
        let _guard = LOCK.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        match value {
            Some(value) => std::env::set_var(ENV, value),
            None => std::env::remove_var(ENV),
        }
        let out = run();
        std::env::remove_var(ENV);
        out
    }

    #[test]
    fn unset_adds_nothing() {
        with_env(None, || assert!(load().unwrap().is_empty()));
    }

    #[test]
    fn unreadable_file_is_refused() {
        with_env(Some("/nonexistent/celld-ca.pem"), || {
            assert!(load().unwrap_err().to_string().contains(ENV));
        });
    }

    #[test]
    fn a_file_without_a_certificate_is_refused() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("empty.pem");
        std::fs::write(&path, "not a certificate\n").unwrap();
        with_env(Some(path.to_str().unwrap()), || {
            assert!(load().unwrap_err().to_string().contains("no certificate"));
        });
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
        with_env(Some(path.to_str().unwrap()), || {
            assert!(load().is_err());
        });
    }
}
