//! The shared operator token for the internal listener.
//!
//! When `CELLD_OPERATOR_TOKEN` is set, every request to the internal listener
//! must carry `Authorization: Bearer <token>`, whatever its path. The check
//! sits in front of routing, so an unknown path answers 401 like a known one
//! and an unauthenticated caller reaches no handler and no signature check.
//!
//! The same value is read on the client side. [`PeerAuth`](crate::peer_auth::PeerAuth)
//! attaches it to every request it signs, which covers peer forwarding,
//! handoff, abort, the log tier, the signed probe, `diagnose` and the `d1`,
//! `kv` and `queue` commands without each caller knowing about it.

use anyhow::{anyhow, bail};
use axum::http::{header::AUTHORIZATION, HeaderMap, HeaderValue};
use sha2::{Digest, Sha256};
use std::ffi::OsStr;
use subtle::ConstantTimeEq;

/// The variable that turns the check on.
pub const ENV: &str = "CELLD_OPERATOR_TOKEN";

const SCHEME: &str = "Bearer";

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
    /// Read `CELLD_OPERATOR_TOKEN`. An unset variable means no token. A set
    /// variable that is empty, has surrounding whitespace or is not a usable
    /// header value is an error and never reads as "unset": a typo must not
    /// leave the listener open.
    pub fn from_env() -> anyhow::Result<Option<Self>> {
        match std::env::var_os(ENV) {
            None => Ok(None),
            Some(value) => Self::parse(&value).map(Some),
        }
    }

    /// Validate a token. It must be non-empty printable ASCII with no
    /// whitespace, so it is a valid bearer credential as written.
    pub fn parse(raw: &OsStr) -> anyhow::Result<Self> {
        let Some(value) = raw.to_str() else {
            bail!("{ENV} must be valid UTF-8");
        };
        if value.is_empty() {
            bail!("{ENV} is set but empty; unset it to leave the internal listener open");
        }
        if value.trim() != value {
            bail!("{ENV} must not have leading or trailing whitespace");
        }
        if !value.bytes().all(|byte| byte.is_ascii_graphic()) {
            bail!("{ENV} must contain only printable ASCII characters and no whitespace");
        }
        let mut bearer = HeaderValue::from_str(&format!("{SCHEME} {value}"))
            .map_err(|_| anyhow!("{ENV} is not a valid header value"))?;
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

#[cfg(test)]
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
}
