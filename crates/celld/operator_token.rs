//! The shared operator token for the internal listener.
//!
//! Every request to the internal listener must carry
//! `Authorization: Bearer <token>`, whatever its path. The check sits in front
//! of routing, so an unknown path answers 401 like a known one and an
//! unauthenticated caller reaches no handler and no signature check.
//!
//! The token is on by default. A node takes it from `CELLD_OPERATOR_TOKEN`.
//! Without that, a node whose internal listener binds only a loopback address
//! generates one, keeps it in a file readable by its owner alone and logs the
//! file's path. A node that binds any other address refuses to start without
//! `CELLD_OPERATOR_TOKEN`, because the generated file is only reachable from
//! the machine the node runs on.
//!
//! The same value is read on the client side, from the variable or from that
//! file. [`PeerAuth`](crate::peer_auth::PeerAuth) attaches it to every request
//! it signs, which covers peer forwarding, handoff, abort, the log tier, the
//! signed probe, `diagnose` and the `d1`, `kv` and `queue` commands without
//! each caller knowing about it.

use anyhow::{anyhow, bail, Context as _};
use axum::http::{header::AUTHORIZATION, HeaderMap, HeaderValue};
use base64::Engine as _;
use rand::RngCore as _;
use sha2::{Digest, Sha256};
use std::ffi::OsStr;
use std::net::IpAddr;
use std::path::{Path, PathBuf};
use subtle::ConstantTimeEq;

use crate::startup::Advertise;

/// The variable that holds the token.
pub const ENV: &str = "CELLD_OPERATOR_TOKEN";

/// The variable that names the file a generated token is kept in and read
/// from. Without it the file is [`FILE_NAME`] in celld's per-user
/// configuration directory.
pub const FILE_ENV: &str = "CELLD_OPERATOR_TOKEN_FILE";

/// The name of the token file inside a configuration or state directory.
pub const FILE_NAME: &str = "operator-token";

const SCHEME: &str = "Bearer";

/// Where the generated token lives, and where a client looks for it.
fn token_file() -> anyhow::Result<PathBuf> {
    match std::env::var_os(FILE_ENV) {
        Some(path) if !path.is_empty() => Ok(PathBuf::from(path)),
        Some(_) => bail!("{FILE_ENV} is set but empty"),
        None => Ok(crate::config_dir::path()?.join(FILE_NAME)),
    }
}

/// A validated operator token. It prints nothing in `Debug`.
#[derive(Clone)]
pub struct OperatorToken {
    /// `Bearer <token>`, marked sensitive so HTTP stacks keep it out of logs.
    bearer: HeaderValue,
    digest: [u8; 32],
}

impl std::fmt::Debug for OperatorToken {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("OperatorToken(..)")
    }
}

impl OperatorToken {
    /// The token a client sends: `CELLD_OPERATOR_TOKEN`, else the file a
    /// node on this machine generated, else none. A set variable that is
    /// empty, has surrounding whitespace or is not a usable header value is an
    /// error and never reads as "unset", and so is a file that holds one: a
    /// typo must not quietly send no credential. The file's location is
    /// resolved only when the variable is not set.
    pub fn from_env() -> anyhow::Result<Option<Self>> {
        Self::resolve(token_file)
    }

    /// Like [`from_env`](Self::from_env), with the token file at `path`.
    /// `celld dev` keeps its node's file in the project's state directory.
    pub fn from_env_or_file(path: &Path) -> anyhow::Result<Option<Self>> {
        Self::resolve(|| Ok(path.to_path_buf()))
    }

    fn resolve(path: impl FnOnce() -> anyhow::Result<PathBuf>) -> anyhow::Result<Option<Self>> {
        match Self::from_variable()? {
            Some(token) => Ok(Some(token)),
            None => Self::from_file(&path()?),
        }
    }

    /// `CELLD_OPERATOR_TOKEN` alone, without reading any file. Startup
    /// validation uses it, so a damaged token file stops only the commands
    /// that need the file.
    pub(crate) fn from_variable() -> anyhow::Result<Option<Self>> {
        match std::env::var_os(ENV) {
            None => Ok(None),
            Some(value) => Self::parse(&value)
                .with_context(|| format!("{ENV} is not usable"))
                .map(Some),
        }
    }

    /// The token a node enforces. `CELLD_OPERATOR_TOKEN` wins. Without it, a
    /// node that binds and advertises only loopback addresses loads the token
    /// file, creating it with a random token when it is missing. Any other
    /// node refuses to start: the generated file is only reachable from the
    /// machine the node runs on, so a peer or a client elsewhere could never
    /// learn it.
    pub fn for_node(internal_bind: IpAddr, advertise: &Advertise) -> anyhow::Result<Self> {
        if let Some(token) = Self::from_variable()? {
            return Ok(token);
        }
        if !internal_bind.is_loopback() {
            bail!(
                "the internal listener is bound to {internal_bind}, which is not a loopback address, \
                 so {ENV} must be set to the shared token of the fleet; every internal request \
                 must carry it as `Authorization: Bearer <token>`"
            );
        }
        if !advertise.is_loopback() {
            bail!(
                "this node advertises {advertise}, which is not a loopback address, so peers and \
                 clients on other machines dial it and {ENV} must be set to the shared token of \
                 the fleet; every internal request must carry it as \
                 `Authorization: Bearer <token>`"
            );
        }
        let path = token_file()?;
        let token = Self::load_or_create(&path)?;
        tracing::info!(
            event = "operator_token_file",
            path = %path.display(),
            "the internal listener requires the operator token kept in this file; \
             set {ENV} to use another one"
        );
        Ok(token)
    }

    /// Read a token file. A missing file is `None`. A symbolic link, a file
    /// another user owns or can read, and one that holds an unusable value are
    /// errors: the file is a credential, so it must be the node owner's own.
    #[allow(clippy::disallowed_methods)] // A per-user credential file, not node storage.
    pub fn from_file(path: &Path) -> anyhow::Result<Option<Self>> {
        use std::io::Read as _;
        let mut options = std::fs::OpenOptions::new();
        options.read(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt as _;
            options.custom_flags(libc::O_NOFOLLOW);
        }
        let mut file = match options.open(path) {
            Ok(file) => file,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            #[cfg(unix)]
            Err(error) if error.raw_os_error() == Some(libc::ELOOP) => {
                bail!(
                    "{} is a symbolic link; replace it with the token file",
                    path.display()
                );
            }
            Err(error) => {
                return Err(error).with_context(|| format!("read {}", path.display()));
            }
        };
        // Checked on the open file, so the file cannot change between the
        // check and the read.
        let metadata = file
            .metadata()
            .with_context(|| format!("stat {}", path.display()))?;
        #[cfg(unix)]
        {
            // SAFETY: `geteuid` takes no arguments and cannot fail.
            check_private(path, &metadata, unsafe { libc::geteuid() })?;
        }
        #[cfg(not(unix))]
        anyhow::ensure!(
            metadata.is_file(),
            "{} is not a regular file",
            path.display()
        );
        let mut text = String::new();
        file.read_to_string(&mut text)
            .with_context(|| format!("read {}", path.display()))?;
        Self::parse(OsStr::new(text.trim_end_matches(['\r', '\n'])))
            .with_context(|| format!("{} does not hold a usable token", path.display()))
            .map(Some)
    }

    /// Load the token file, or write one holding a random token. The file
    /// appears complete or not at all: it is written and flushed under a
    /// random name and linked into place, so two nodes starting together agree
    /// on one token, and a crash leaves at most an unused staging file that no
    /// later start trips over.
    #[allow(clippy::disallowed_methods)] // A per-user credential file, not node storage.
    fn load_or_create(path: &Path) -> anyhow::Result<Self> {
        if let Some(token) = Self::from_file(path)? {
            return Ok(token);
        }
        let parent = path
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty());
        if let Some(parent) = parent {
            std::fs::create_dir_all(parent)
                .with_context(|| format!("create {}", parent.display()))?;
        }
        let mut rng = crate::asyncrt::rng("operator_token");
        let mut random = [0_u8; 32];
        rng.fill_bytes(&mut random);
        let value = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(random);
        let name = path.file_name().unwrap_or(OsStr::new(FILE_NAME));
        let staging = path.with_file_name(format!(
            "{}.{:016x}.new",
            name.to_string_lossy(),
            rng.next_u64()
        ));
        {
            use std::io::Write as _;
            let mut options = std::fs::OpenOptions::new();
            options.write(true).create_new(true);
            #[cfg(unix)]
            {
                use std::os::unix::fs::OpenOptionsExt as _;
                options.mode(0o600);
            }
            let mut file = options
                .open(&staging)
                .with_context(|| format!("create {}", staging.display()))?;
            file.write_all(value.as_bytes())
                .and_then(|()| file.sync_all())
                .with_context(|| format!("write {}", staging.display()))?;
        }
        let linked = std::fs::hard_link(&staging, path);
        let _ = std::fs::remove_file(&staging);
        match linked {
            Ok(()) => {
                // Best effort: the file is complete either way.
                #[cfg(unix)]
                if let Some(Ok(directory)) = parent.map(std::fs::File::open) {
                    let _ = directory.sync_all();
                }
                Self::parse(OsStr::new(&value))
            }
            // Another node won the race; its token is the one in the file.
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
                Self::from_file(path)?
                    .ok_or_else(|| anyhow!("{} vanished while it was created", path.display()))
            }
            Err(error) => Err(error).with_context(|| format!("create {}", path.display())),
        }
    }

    /// Validate a token. It must be non-empty printable ASCII with no
    /// whitespace, so it is a valid bearer credential as written. The errors
    /// do not name the source; the caller adds it.
    pub fn parse(raw: &OsStr) -> anyhow::Result<Self> {
        let Some(value) = raw.to_str() else {
            bail!("the token must be valid UTF-8");
        };
        if value.is_empty() {
            bail!("the token is empty; remove it to use the generated token");
        }
        if value.trim() != value {
            bail!("the token must not have leading or trailing whitespace");
        }
        if !value.bytes().all(|byte| byte.is_ascii_graphic()) {
            bail!("the token must contain only printable ASCII characters and no whitespace");
        }
        let mut bearer = HeaderValue::from_str(&format!("{SCHEME} {value}"))
            .map_err(|_| anyhow!("the token is not a valid header value"))?;
        bearer.set_sensitive(true);
        Ok(Self {
            bearer,
            digest: Sha256::digest(value.as_bytes()).into(),
        })
    }

    /// The `Authorization` value a client sends.
    pub fn bearer(&self) -> HeaderValue {
        self.bearer.clone()
    }

    /// Whether `headers` carry exactly one `Authorization: Bearer` header
    /// that holds this token. Digests are compared in constant time, so
    /// neither the token's content nor its length leaks through timing.
    pub fn authorizes(&self, headers: &HeaderMap) -> bool {
        let mut values = headers.get_all(AUTHORIZATION).iter();
        let (Some(value), None) = (values.next(), values.next()) else {
            return false;
        };
        let Ok(value) = value.to_str() else {
            return false;
        };
        let Some((scheme, presented)) = value.split_once(' ') else {
            return false;
        };
        if !scheme.eq_ignore_ascii_case(SCHEME) {
            return false;
        }
        let presented: [u8; 32] = Sha256::digest(presented.as_bytes()).into();
        presented.ct_eq(&self.digest).into()
    }
}

/// Refuse a token file that is not the node owner's own private file.
#[cfg(unix)]
fn check_private(path: &Path, metadata: &std::fs::Metadata, euid: u32) -> anyhow::Result<()> {
    use std::os::unix::fs::{MetadataExt as _, PermissionsExt as _};
    anyhow::ensure!(
        metadata.is_file(),
        "{} is not a regular file",
        path.display()
    );
    if metadata.uid() != euid {
        bail!(
            "{} is owned by user {}, not by the user running this command ({euid}); \
             delete it or take it over",
            path.display(),
            metadata.uid()
        );
    }
    let mode = metadata.permissions().mode();
    if mode & 0o077 != 0 {
        bail!(
            "{} is readable by other users (mode {:o}); run `chmod 600` on it or delete it",
            path.display(),
            mode & 0o777
        );
    }
    Ok(())
}

#[cfg(test)]
#[allow(clippy::disallowed_methods)] // Tests write real files in a temp directory.
mod tests {
    use super::*;

    fn token(value: &str) -> OperatorToken {
        OperatorToken::parse(OsStr::new(value)).expect("valid token")
    }

    fn headers(values: &[&str]) -> HeaderMap {
        let mut headers = HeaderMap::new();
        for value in values {
            headers.append(AUTHORIZATION, HeaderValue::from_str(value).unwrap());
        }
        headers
    }

    #[test]
    fn refuses_unusable_values() {
        for bad in ["", " ", " abc", "abc ", "a b", "a\tb", "tok\u{e9}n", "a\nb"] {
            assert!(
                OperatorToken::parse(OsStr::new(bad)).is_err(),
                "{bad:?} should be refused"
            );
        }
        #[cfg(unix)]
        {
            use std::os::unix::ffi::OsStrExt;
            assert!(OperatorToken::parse(OsStr::from_bytes(&[0xff, 0xfe])).is_err());
        }
    }

    #[test]
    fn accepts_printable_ascii() {
        for good in ["secret", "a-B_c.d~e+f/g=", "0123456789abcdef!@#$%^&*()"] {
            token(good);
        }
    }

    #[test]
    fn authorizes_only_the_exact_bearer_token() {
        let token = token("s3cret");
        assert!(token.authorizes(&headers(&["Bearer s3cret"])));
        assert!(token.authorizes(&headers(&["bearer s3cret"])));
        assert!(!token.authorizes(&HeaderMap::new()));
        assert!(!token.authorizes(&headers(&["Bearer s3cre"])));
        assert!(!token.authorizes(&headers(&["Bearer s3crett"])));
        assert!(!token.authorizes(&headers(&["Bearer S3CRET"])));
        assert!(!token.authorizes(&headers(&["Bearer  s3cret"])));
        assert!(!token.authorizes(&headers(&["Bearer s3cret "])));
        assert!(!token.authorizes(&headers(&["Basic s3cret"])));
        assert!(!token.authorizes(&headers(&["s3cret"])));
        assert!(!token.authorizes(&headers(&["Bearer"])));
        // A second Authorization header is ambiguous, even when one is right.
        assert!(!token.authorizes(&headers(&["Bearer s3cret", "Bearer s3cret"])));
        assert!(!token.authorizes(&headers(&["Bearer nope", "Bearer s3cret"])));
    }

    #[test]
    fn debug_and_header_hide_the_value() {
        let token = token("s3cret");
        assert!(!format!("{token:?}").contains("s3cret"));
        assert!(token.bearer().is_sensitive());
    }

    #[test]
    fn a_node_creates_the_file_once_and_reuses_it() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("state").join(FILE_NAME);
        let first = OperatorToken::load_or_create(&path).unwrap();
        let text = std::fs::read_to_string(&path).unwrap();
        assert!(text.len() >= 40, "a random token, not a placeholder");
        assert!(first.authorizes(&headers(&[&format!("Bearer {text}")])));
        let second = OperatorToken::load_or_create(&path).unwrap();
        assert!(second.authorizes(&headers(&[&format!("Bearer {text}")])));
        assert!(OperatorToken::from_file(&path).unwrap().is_some());
        assert_eq!(
            std::fs::read_dir(path.parent().unwrap()).unwrap().count(),
            1
        );
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt as _;
            assert_eq!(
                std::fs::metadata(&path).unwrap().permissions().mode() & 0o777,
                0o600
            );
        }
    }

    #[test]
    fn a_missing_file_is_no_token_and_a_bad_one_is_an_error() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join(FILE_NAME);
        assert!(OperatorToken::from_file(&path).unwrap().is_none());
        std::fs::write(&path, "has a space").unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt as _;
            std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600)).unwrap();
            assert!(OperatorToken::from_file(&path).is_err());
            std::fs::write(&path, "fine-token\n").unwrap();
            assert!(OperatorToken::from_file(&path).unwrap().is_some());
            std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o644)).unwrap();
            let error = OperatorToken::from_file(&path).unwrap_err();
            assert!(format!("{error:#}").contains("readable by other users"));
        }
    }

    #[test]
    fn a_stale_staging_file_does_not_block_creation() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join(FILE_NAME);
        // What a crashed start left behind, under the name every start in a
        // container (always pid 1) would have used.
        std::fs::write(
            dir.path()
                .join(format!("{FILE_NAME}.{}.new", std::process::id())),
            "half",
        )
        .unwrap();
        std::fs::write(dir.path().join(format!("{FILE_NAME}.1.new")), "half").unwrap();
        let token = OperatorToken::load_or_create(&path).unwrap();
        let text = std::fs::read_to_string(&path).unwrap();
        assert!(token.authorizes(&headers(&[&format!("Bearer {text}")])));
    }

    #[cfg(unix)]
    #[test]
    fn a_symbolic_link_is_refused() {
        use std::os::unix::fs::PermissionsExt as _;
        let dir = tempfile::tempdir().unwrap();
        let real = dir.path().join("real");
        std::fs::write(&real, "fine-token").unwrap();
        std::fs::set_permissions(&real, std::fs::Permissions::from_mode(0o600)).unwrap();
        let link = dir.path().join(FILE_NAME);
        std::os::unix::fs::symlink(&real, &link).unwrap();
        let error = OperatorToken::from_file(&link).unwrap_err();
        assert!(format!("{error:#}").contains("symbolic link"), "{error:#}");
        // Creating a node's file never writes through a link either.
        assert!(OperatorToken::load_or_create(&link).is_err());
    }

    #[cfg(unix)]
    #[test]
    fn a_file_another_user_owns_is_refused() {
        use std::os::unix::fs::{MetadataExt as _, PermissionsExt as _};
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join(FILE_NAME);
        std::fs::write(&path, "fine-token").unwrap();
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600)).unwrap();
        let metadata = std::fs::metadata(&path).unwrap();
        check_private(&path, &metadata, metadata.uid()).unwrap();
        let error = check_private(&path, &metadata, metadata.uid() + 1).unwrap_err();
        assert!(format!("{error:#}").contains("owned by user"), "{error:#}");
    }

    #[test]
    fn a_bad_token_names_its_source_not_the_variable_for_a_file() {
        let error = OperatorToken::parse(OsStr::new("a b")).unwrap_err();
        assert!(!format!("{error}").contains(ENV), "{error}");
    }
}
