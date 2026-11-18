// Copyright 2026 Deno Land Inc. Apache-2.0 license.

//! The node's SMTP settings, read from the environment; see the table in
//! [`super`].

use anyhow::{anyhow, bail};
use std::net::IpAddr;
use std::sync::OnceLock;
use zeroize::Zeroizing;

pub const HOST: &str = "CELLD_SMTP_HOST";
pub const PORT: &str = "CELLD_SMTP_PORT";
pub const TLS: &str = "CELLD_SMTP_TLS";
pub const USERNAME: &str = "CELLD_SMTP_USERNAME";
pub const PASSWORD: &str = "CELLD_SMTP_PASSWORD";
pub const PASSWORD_FILE: &str = "CELLD_SMTP_PASSWORD_FILE";
pub const HELO_NAME: &str = "CELLD_SMTP_HELO_NAME";

/// How a password file that other users can read is reported.
const SHARED_FILE: crate::credential_file::SharedWarning = crate::credential_file::SharedWarning {
    event: "smtp_password_file_shared",
    subject: "SMTP password file",
};
const PASSWORD_FILE_MAX_BYTES: u64 = 1024;
const MAX_CREDENTIAL_BYTES: usize = 255;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TlsMode {
    /// Connect in the clear, then upgrade with `STARTTLS`.
    StartTls,
    /// TLS from the first byte.
    Tls,
    /// No TLS at all. The operator chose it by name.
    Off,
}

impl TlsMode {
    fn default_port(self) -> u16 {
        match self {
            Self::StartTls => 587,
            Self::Tls => 465,
            Self::Off => 25,
        }
    }
}

/// The relay's settings, with the password in hand.
pub struct Settings {
    pub(super) host: String,
    pub(super) port: u16,
    pub(super) tls: TlsMode,
    pub(super) helo: String,
    pub(super) credentials: Option<Credentials>,
}

pub(super) struct Credentials {
    pub(super) username: String,
    pub(super) password: Zeroizing<String>,
}

// The password never appears in `Debug`, so a log of the settings cannot leak it.
impl std::fmt::Debug for Settings {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Settings")
            .field("host", &self.host)
            .field("port", &self.port)
            .field("tls", &self.tls)
            .field("helo", &self.helo)
            .field(
                "credentials",
                &self
                    .credentials
                    .as_ref()
                    .map(|c| format!("{} / <redacted>", c.username)),
            )
            .finish()
    }
}

fn valid_name(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= 253
        && name.split('.').all(|label| {
            !label.is_empty()
                && label.len() <= 63
                && !label.starts_with('-')
                && !label.ends_with('-')
                && label
                    .chars()
                    .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_')
        })
}

fn parse_host(variable: &str, text: &str) -> anyhow::Result<String> {
    let bare = text.trim_start_matches('[').trim_end_matches(']');
    anyhow::ensure!(
        bare.parse::<IpAddr>().is_ok() || valid_name(bare),
        "{variable} is not a host name or an IP address"
    );
    Ok(bare.to_string())
}

fn credential_text(variable: &str, text: &str) -> anyhow::Result<()> {
    anyhow::ensure!(
        !text.is_empty()
            && text.len() <= MAX_CREDENTIAL_BYTES
            && !text.contains(['\r', '\n', '\0']),
        "{variable} must be 1 to {MAX_CREDENTIAL_BYTES} bytes without a line break"
    );
    Ok(())
}

/// The settings in `get`, checked, with the password read. Nothing here caches
/// or logs, so startup can call it before logging exists. A variable that is
/// set while `CELLD_SMTP_HOST` is not is an error, since it would otherwise be
/// ignored without a word.
fn parse(get: &dyn Fn(&str) -> anyhow::Result<Option<String>>) -> anyhow::Result<Option<Settings>> {
    let set = |name: &str| -> anyhow::Result<Option<String>> {
        Ok(get(name)?.filter(|value| !value.trim().is_empty()))
    };
    let Some(host) = set(HOST)? else {
        for name in [PORT, TLS, USERNAME, PASSWORD, PASSWORD_FILE, HELO_NAME] {
            if set(name)?.is_some() {
                bail!("{name} is set but {HOST} is not");
            }
        }
        return Ok(None);
    };
    let host = parse_host(HOST, host.trim())?;
    let tls = match set(TLS)?.as_deref().map(str::trim) {
        None | Some("starttls") => TlsMode::StartTls,
        Some("tls") => TlsMode::Tls,
        Some("off") => TlsMode::Off,
        Some(other) => bail!("{TLS} must be starttls, tls or off, not {other:?}"),
    };
    let port = match set(PORT)? {
        None => tls.default_port(),
        Some(text) => text
            .trim()
            .parse::<u16>()
            .ok()
            .filter(|port| *port != 0)
            .ok_or_else(|| anyhow!("{PORT} must be a port number from 1 to 65535"))?,
    };
    let helo = match set(HELO_NAME)? {
        None => "localhost".to_string(),
        Some(text) => {
            let name = parse_host(HELO_NAME, text.trim())?;
            // An address literal is written in brackets in `EHLO`.
            match name.parse::<IpAddr>() {
                Ok(IpAddr::V6(_)) => format!("[IPv6:{name}]"),
                Ok(IpAddr::V4(_)) => format!("[{name}]"),
                Err(_) => name,
            }
        }
    };
    let username = set(USERNAME)?;
    let password = crate::credential_file::choose(
        (PASSWORD, get(PASSWORD)?),
        (PASSWORD_FILE, get(PASSWORD_FILE)?),
        PASSWORD_FILE_MAX_BYTES,
        &SHARED_FILE,
    )?;
    let credentials = match (username, password) {
        (None, None) => None,
        (Some(username), Some(password)) => {
            credential_text(USERNAME, &username)?;
            // A file ends in a line end that its editor wrote; a variable is
            // taken as it is, so one with a line break is refused.
            let text = if password.origin == PASSWORD {
                password.text.as_str()
            } else {
                password.text.trim_end_matches(['\r', '\n'])
            };
            credential_text(&password.origin, text)?;
            Some(Credentials {
                username,
                password: Zeroizing::new(text.to_string()),
            })
        }
        (Some(_), None) => bail!("{USERNAME} is set without {PASSWORD} or {PASSWORD_FILE}"),
        (None, Some(_)) => bail!("{PASSWORD} or {PASSWORD_FILE} is set without {USERNAME}"),
    };
    Ok(Some(Settings {
        host,
        port,
        tls,
        helo,
        credentials,
    }))
}

/// Check the node's SMTP settings, for startup. The log is not set up yet, so a
/// warning about the password file goes nowhere; the first real `settings` call
/// repeats it.
pub fn validate() -> anyhow::Result<()> {
    load().map(|_| ())
}

/// The node's relay, or `None` when `CELLD_SMTP_HOST` is unset. A good result,
/// the password file's text included, is kept for the process. A failure is
/// not: it is a file that could not be read this time, and the next send reads
/// it again.
pub fn settings() -> anyhow::Result<Option<&'static Settings>> {
    static SETTINGS: OnceLock<Option<Settings>> = OnceLock::new();
    cached(&SETTINGS, load)
}

fn cached(
    cell: &OnceLock<Option<Settings>>,
    read: impl FnOnce() -> anyhow::Result<Option<Settings>>,
) -> anyhow::Result<Option<&Settings>> {
    if let Some(settings) = cell.get() {
        return Ok(settings.as_ref());
    }
    let settings = read()?;
    Ok(cell.get_or_init(|| settings).as_ref())
}

fn load() -> anyhow::Result<Option<Settings>> {
    parse(&|name| crate::env_vars::value(name))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn env(pairs: &[(&str, &str)]) -> HashMap<String, String> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    fn parsed(pairs: &[(&str, &str)]) -> anyhow::Result<Option<Settings>> {
        let map = env(pairs);
        parse(&|name| Ok(map.get(name).cloned()))
    }

    fn refusal(pairs: &[(&str, &str)]) -> String {
        format!("{:#}", parsed(pairs).expect_err("the settings are refused"))
    }

    #[test]
    fn no_host_means_no_relay_and_a_stray_setting_is_refused() {
        assert!(parsed(&[]).unwrap().is_none());
        assert!(parsed(&[(HOST, "  ")]).unwrap().is_none());
        for name in [PORT, TLS, USERNAME, PASSWORD, PASSWORD_FILE, HELO_NAME] {
            let error = refusal(&[(name, "x")]);
            assert!(error.contains(name) && error.contains(HOST), "{error}");
        }
    }

    #[test]
    fn tls_is_starttls_unless_the_operator_says_otherwise() {
        let relay = parsed(&[(HOST, "mail.example.com")]).unwrap().unwrap();
        assert_eq!(
            (relay.tls, relay.port, relay.helo.as_str()),
            (TlsMode::StartTls, 587, "localhost")
        );
        for (mode, tls, port) in [
            ("tls", TlsMode::Tls, 465),
            ("off", TlsMode::Off, 25),
            ("starttls", TlsMode::StartTls, 587),
        ] {
            let relay = parsed(&[(HOST, "mail.example.com"), (TLS, mode)])
                .unwrap()
                .unwrap();
            assert_eq!((relay.tls, relay.port), (tls, port), "{mode}");
        }
        let relay = parsed(&[(HOST, "127.0.0.1"), (PORT, "2525"), (TLS, "off")])
            .unwrap()
            .unwrap();
        assert_eq!(relay.port, 2525);
        for bad in ["TLS", "plain", "true", "no"] {
            assert!(refusal(&[(HOST, "h"), (TLS, bad)]).contains(TLS), "{bad}");
        }
    }

    #[test]
    fn host_port_and_helo_are_validated() {
        for bad in [
            "bad host",
            "a/b",
            "mail.example.com:25",
            "-x",
            "a..b",
            "héllo.com",
        ] {
            assert!(refusal(&[(HOST, bad)]).contains(HOST), "{bad}");
        }
        for bad in ["0", "65536", "smtp", "-1", "25 26"] {
            assert!(refusal(&[(HOST, "h"), (PORT, bad)]).contains(PORT), "{bad}");
        }
        let relay = parsed(&[(HOST, "[::1]"), (HELO_NAME, "10.0.0.1")])
            .unwrap()
            .unwrap();
        assert_eq!(
            (relay.host.as_str(), relay.helo.as_str()),
            ("::1", "[10.0.0.1]")
        );
        assert!(refusal(&[(HOST, "h"), (HELO_NAME, "a b")]).contains(HELO_NAME));
    }

    #[test]
    fn credentials_need_both_halves_and_one_password_source() {
        assert!(refusal(&[(HOST, "h"), (USERNAME, "u")]).contains("without"));
        assert!(refusal(&[(HOST, "h"), (PASSWORD, "p")]).contains("without"));
        let both = refusal(&[
            (HOST, "h"),
            (USERNAME, "u"),
            (PASSWORD, "p"),
            (PASSWORD_FILE, "/x"),
        ]);
        assert!(
            both.contains(PASSWORD) && both.contains(PASSWORD_FILE),
            "{both}"
        );
        for bad in ["a\nb", "a\r", &"x".repeat(256)] {
            assert!(refusal(&[(HOST, "h"), (USERNAME, bad), (PASSWORD, "p")]).contains(USERNAME));
            assert!(refusal(&[(HOST, "h"), (USERNAME, "u"), (PASSWORD, bad)]).contains(PASSWORD));
        }
        let ok = parsed(&[(HOST, "h"), (USERNAME, "u"), (PASSWORD, "p")])
            .unwrap()
            .unwrap();
        assert!(ok.credentials.is_some());
        // A password file is read with the settings, and a line end is dropped.
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("pw");
        #[allow(clippy::disallowed_methods)] // The test writes a credential file.
        std::fs::write(&path, "from-file\r\n").unwrap();
        let file = parsed(&[
            (HOST, "h"),
            (USERNAME, "u"),
            (PASSWORD_FILE, path.to_str().unwrap()),
        ])
        .unwrap()
        .unwrap();
        assert_eq!(file.credentials.unwrap().password.as_str(), "from-file");
        let missing = refusal(&[
            (HOST, "h"),
            (USERNAME, "u"),
            (PASSWORD_FILE, "/nonexistent/pw"),
        ]);
        assert!(missing.contains(PASSWORD_FILE), "{missing}");
    }

    #[test]
    fn the_password_is_absent_from_debug_output_and_errors() {
        let relay = Settings {
            host: "h".into(),
            port: 25,
            tls: TlsMode::Off,
            helo: "localhost".into(),
            credentials: Some(Credentials {
                username: "user".into(),
                password: Zeroizing::new("hunter2-secret".into()),
            }),
        };
        let debug = format!("{relay:?}");
        assert!(
            !debug.contains("hunter2-secret") && debug.contains("<redacted>"),
            "{debug}"
        );
        let error = refusal(&[(HOST, "h"), (USERNAME, "u"), (PASSWORD, "hunter2-secret\n")]);
        assert!(!error.contains("hunter2-secret"), "{error}");
    }

    #[test]
    fn a_failed_read_is_not_kept_and_a_good_one_is() {
        let cell = OnceLock::new();
        let failing = || -> anyhow::Result<Option<Settings>> { bail!("the file is not there") };
        assert!(cached(&cell, failing).is_err());
        assert!(cached(&cell, failing).is_err());
        let good = || parsed(&[(HOST, "mail.example.com")]);
        assert!(cached(&cell, good).unwrap().is_some());
        // Read once: a later failing reader is not asked.
        assert!(cached(&cell, failing).unwrap().is_some());
    }
}
