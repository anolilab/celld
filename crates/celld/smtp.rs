// Copyright 2026 Deno Land Inc. Apache-2.0 license.

//! The SMTP relay of a node, and the client that hands it one message.
//!
//! The relay belongs to the operator, not to a Worker. Its address and
//! credentials are node settings and nothing else: a Worker never sees them,
//! they are not in `env`, a log line, an error a Worker reads, or `Debug`
//! output.
//!
//! | Variable | Meaning |
//! | --- | --- |
//! | `CELLD_SMTP_HOST` | The relay's name or address. Unset means no relay. |
//! | `CELLD_SMTP_PORT` | Its port. Defaults to 587 (`starttls`), 465 (`tls`) or 25 (`off`). |
//! | `CELLD_SMTP_TLS` | `starttls` (the default), `tls`, or `off`. |
//! | `CELLD_SMTP_USERNAME` | The account, with a password. |
//! | `CELLD_SMTP_PASSWORD` | The password. |
//! | `CELLD_SMTP_PASSWORD_FILE` | A file that holds the password, in place of the variable. |
//! | `CELLD_SMTP_HELO_NAME` | The name given in `EHLO`. Defaults to `localhost`. |
//!
//! TLS is required unless `CELLD_SMTP_TLS=off` says otherwise. With
//! `starttls`, a server that does not offer it is an error: the client never
//! continues in the clear, and never authenticates before TLS. The server's
//! certificate is checked against the bundled public roots, not
//! `CELLD_EXTRA_CA_CERTS`, which is for what Workers dial.
//!
//! The connection is made by the host. The relay is operator-chosen, so a
//! private or loopback address is fine for it, but a link-local, metadata,
//! unspecified, multicast or reserved address, and a listener of this node,
//! are refused; see [`crate::egress_filter::smtp_addresses`].
//!
//! One message is one transaction on one connection: `EHLO`, optional
//! `STARTTLS`, optional `AUTH`, `MAIL FROM`, `RCPT TO` for each recipient,
//! `DATA`, `QUIT`. A server's reply text is never passed on, because a server
//! may echo what it was sent; a failure carries the stage and the reply code.

use anyhow::{anyhow, bail};
use base64::engine::general_purpose::STANDARD;
use base64::Engine as _;
use std::net::IpAddr;
use std::sync::OnceLock;
use std::time::Duration;
use tokio::io::{
    AsyncBufReadExt as _, AsyncRead, AsyncReadExt as _, AsyncWrite, AsyncWriteExt as _,
};
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

/// How long one connection attempt may take.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(15);
/// How long the server may take to answer one command.
const REPLY_TIMEOUT: Duration = Duration::from_secs(60);
/// How long one whole transaction may take, `DATA` included.
const TRANSACTION_TIMEOUT: Duration = Duration::from_secs(300);
/// The longest reply line read; RFC 5321 allows 512.
const MAX_REPLY_LINE: u64 = 2048;
/// The most lines one reply may have.
const MAX_REPLY_LINES: usize = 100;
/// The most transactions a node runs at once, so a Worker cannot open an
/// unbounded number of connections to the relay.
const MAX_CONCURRENT: usize = 16;

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
    host: String,
    port: u16,
    tls: TlsMode,
    helo: String,
    credentials: Option<Credentials>,
}

struct Credentials {
    username: String,
    password: Zeroizing<String>,
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

impl Settings {
    pub fn host(&self) -> &str {
        &self.host
    }

    pub fn port(&self) -> u16 {
        self.port
    }

    pub fn tls(&self) -> TlsMode {
        self.tls
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
    parse(&|name| crate::env_vars::value(name)).map(|_| ())
}

/// The node's relay, or `None` when `CELLD_SMTP_HOST` is unset. The result,
/// the password file's text included, is kept for the process.
pub fn settings() -> Result<Option<&'static Settings>, &'static str> {
    static SETTINGS: OnceLock<Result<Option<Settings>, String>> = OnceLock::new();
    SETTINGS
        .get_or_init(|| {
            load().map_err(|error| {
                // Once, so the operator can read the cause: it names a
                // variable or a path, never a value.
                let cause = format!("{error:#}");
                tracing::error!(event = "smtp_settings_invalid", error = %cause);
                cause
            })
        })
        .as_ref()
        .map(Option::as_ref)
        .map_err(String::as_str)
}

fn load() -> anyhow::Result<Option<Settings>> {
    parse(&|name| crate::env_vars::value(name))
}

// ---- The client ------------------------------------------------------------

/// Why a transaction failed, as a Worker may read it.
#[derive(Debug, PartialEq, Eq)]
pub struct SendError {
    pub code: &'static str,
    pub message: String,
}

impl SendError {
    fn failed(stage: &str, detail: impl std::fmt::Display) -> Self {
        Self {
            code: "E_DELIVERY_FAILED",
            message: format!("the SMTP transaction failed at {stage}: {detail}"),
        }
    }

    fn refused(stage: &str, reply: &Reply) -> Self {
        let code = if reply.code == 552 {
            "E_CONTENT_TOO_LARGE"
        } else {
            "E_DELIVERY_FAILED"
        };
        Self {
            code,
            message: format!("the SMTP server answered {} at {stage}", reply.code),
        }
    }
}

trait Duplex: AsyncRead + AsyncWrite + Send + Unpin {}
impl<T: AsyncRead + AsyncWrite + Send + Unpin> Duplex for T {}

struct Reply {
    code: u16,
    /// The text of each line after the code, upper-cased: it is only read for
    /// extension keywords, and never leaves this module.
    lines: Vec<String>,
}

struct Connection {
    stream: tokio::io::BufReader<Box<dyn Duplex>>,
}

impl Connection {
    fn new(stream: Box<dyn Duplex>) -> Self {
        Self {
            stream: tokio::io::BufReader::new(stream),
        }
    }

    async fn send(&mut self, stage: &str, bytes: &[u8]) -> Result<(), SendError> {
        let write = async {
            self.stream.get_mut().write_all(bytes).await?;
            self.stream.get_mut().flush().await
        };
        match crate::asyncrt::timeout(REPLY_TIMEOUT, write).await {
            Ok(Ok(())) => Ok(()),
            Ok(Err(error)) => Err(SendError::failed(
                stage,
                format!("write failed ({})", error.kind()),
            )),
            Err(_) => Err(SendError::failed(stage, "write timed out")),
        }
    }

    async fn command(&mut self, stage: &str, line: &str) -> Result<Reply, SendError> {
        debug_assert!(!line.contains(['\r', '\n']));
        self.send(stage, format!("{line}\r\n").as_bytes()).await?;
        self.reply(stage).await
    }

    async fn reply(&mut self, stage: &str) -> Result<Reply, SendError> {
        match crate::asyncrt::timeout(REPLY_TIMEOUT, self.read_reply(stage)).await {
            Ok(reply) => reply,
            Err(_) => Err(SendError::failed(
                stage,
                "the server did not answer in time",
            )),
        }
    }

    async fn read_reply(&mut self, stage: &str) -> Result<Reply, SendError> {
        let mut lines = Vec::new();
        loop {
            let mut line = Vec::new();
            let read = (&mut self.stream)
                .take(MAX_REPLY_LINE)
                .read_until(b'\n', &mut line)
                .await
                .map_err(|error| {
                    SendError::failed(stage, format!("read failed ({})", error.kind()))
                })?;
            if read == 0 {
                return Err(SendError::failed(stage, "the server closed the connection"));
            }
            if !line.ends_with(b"\n") {
                return Err(SendError::failed(
                    stage,
                    "the server sent an overlong reply line",
                ));
            }
            let malformed = || SendError::failed(stage, "the server sent a malformed reply");
            // The code is read from bytes: the line is the server's, and a
            // multi-byte character in its first three bytes is not a code.
            let text = line.strip_suffix(b"\n").unwrap_or(&line);
            let text = text.strip_suffix(b"\r").unwrap_or(text);
            let digits = text
                .get(..3)
                .filter(|digits| digits.iter().all(u8::is_ascii_digit));
            let code = digits
                .and_then(|digits| std::str::from_utf8(digits).ok()?.parse::<u16>().ok())
                .filter(|code| (200..600).contains(code))
                .ok_or_else(malformed)?;
            let (more, body) = match text.get(3) {
                Some(b'-') => (true, &text[4..]),
                Some(b' ') => (false, &text[4..]),
                None => (false, &text[3..]),
                Some(_) => return Err(malformed()),
            };
            let body = String::from_utf8_lossy(body);
            lines.push(body.to_ascii_uppercase());
            if lines.len() > MAX_REPLY_LINES {
                return Err(SendError::failed(stage, "the server sent too long a reply"));
            }
            if !more {
                return Ok(Reply { code, lines });
            }
        }
    }

    /// Upgrade to TLS in place. Bytes the server sent after its `220` would
    /// have been read as the first of the TLS session, so they are refused.
    async fn upgrade(
        self,
        host: &str,
        connector: &tokio_rustls::TlsConnector,
    ) -> Result<Self, SendError> {
        if !self.stream.buffer().is_empty() {
            return Err(SendError::failed(
                "starttls",
                "the server sent data before the TLS handshake",
            ));
        }
        let stream = self.stream.into_inner();
        Ok(Self::new(handshake(stream, host, connector).await?))
    }
}

async fn handshake(
    stream: Box<dyn Duplex>,
    host: &str,
    connector: &tokio_rustls::TlsConnector,
) -> Result<Box<dyn Duplex>, SendError> {
    let name = rustls::pki_types::ServerName::try_from(host.to_string())
        .map_err(|_| SendError::failed("tls", "the relay's name is not valid for TLS"))?;
    let tls = connector.connect(name, stream);
    match crate::asyncrt::timeout(REPLY_TIMEOUT, tls).await {
        Ok(Ok(stream)) => Ok(Box::new(stream)),
        Ok(Err(_)) => Err(SendError::failed(
            "tls",
            "the TLS handshake failed or the relay's certificate was not accepted",
        )),
        Err(_) => Err(SendError::failed("tls", "the TLS handshake timed out")),
    }
}

/// Connect to `host:port` on the addresses the egress policy for this binding
/// allows, and to nothing else.
async fn dial(host: &str, port: u16) -> Result<tokio::net::TcpStream, SendError> {
    let addresses = crate::egress_filter::smtp_addresses(host, port)
        .await
        .map_err(|refused| SendError::failed("connect", refused))?;
    match crate::asyncrt::timeout(
        CONNECT_TIMEOUT,
        tokio::net::TcpStream::connect(&addresses[..]),
    )
    .await
    {
        Ok(Ok(stream)) => Ok(stream),
        Ok(Err(error)) => Err(SendError::failed(
            "connect",
            format!("could not connect to the relay ({})", error.kind()),
        )),
        Err(_) => Err(SendError::failed(
            "connect",
            "the connection to the relay timed out",
        )),
    }
}

/// `data` for `DATA`: every line that starts with `.` gets another, and the
/// body ends with `.` on a line of its own. `data` has CRLF line ends.
pub fn dot_stuff(data: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(data.len() + data.len() / 64 + 8);
    let mut line_start = true;
    for &byte in data {
        if line_start && byte == b'.' {
            out.push(b'.');
        }
        out.push(byte);
        line_start = byte == b'\n';
    }
    if !out.ends_with(b"\r\n") {
        out.extend_from_slice(b"\r\n");
    }
    out.extend_from_slice(b".\r\n");
    out
}

fn semaphore() -> &'static tokio::sync::Semaphore {
    static SEMAPHORE: OnceLock<tokio::sync::Semaphore> = OnceLock::new();
    SEMAPHORE.get_or_init(|| tokio::sync::Semaphore::new(MAX_CONCURRENT))
}

/// Hand one message to the relay: one transaction on one connection.
pub async fn deliver(
    settings: &Settings,
    from: &str,
    recipients: &[String],
    data: &[u8],
) -> Result<(), SendError> {
    deliver_with(
        settings,
        &crate::tls_roots::bundled_connector(),
        from,
        recipients,
        data,
    )
    .await
}

/// [`deliver`] with the TLS trust the caller chooses; a test trusts its own
/// authority.
async fn deliver_with(
    settings: &Settings,
    connector: &tokio_rustls::TlsConnector,
    from: &str,
    recipients: &[String],
    data: &[u8],
) -> Result<(), SendError> {
    // A send that finds every slot taken waits a bounded time, so a burst
    // cannot leave messages queued for good.
    let _permit = match crate::asyncrt::timeout(REPLY_TIMEOUT, semaphore().acquire()).await {
        Ok(Ok(permit)) => permit,
        Ok(Err(_)) => return Err(SendError::failed("connect", "the node is shutting down")),
        Err(_) => {
            return Err(SendError::failed(
                "connect",
                "the node is sending too many messages",
            ))
        }
    };
    match crate::asyncrt::timeout(
        TRANSACTION_TIMEOUT,
        transaction(settings, connector, from, recipients, data),
    )
    .await
    {
        Ok(result) => result,
        Err(_) => Err(SendError::failed(
            "transaction",
            "it did not finish in time",
        )),
    }
}

async fn transaction(
    settings: &Settings,
    connector: &tokio_rustls::TlsConnector,
    from: &str,
    recipients: &[String],
    data: &[u8],
) -> Result<(), SendError> {
    let tcp = dial(&settings.host, settings.port).await?;
    let mut connection = Connection::new(Box::new(tcp));
    if settings.tls == TlsMode::Tls {
        let stream = connection.stream.into_inner();
        connection = Connection::new(handshake(stream, &settings.host, connector).await?);
    }
    let greeting = connection.reply("greeting").await?;
    if greeting.code != 220 {
        return Err(SendError::refused("greeting", &greeting));
    }
    let ehlo = format!("EHLO {}", settings.helo);
    let mut capabilities = expect(&mut connection, "ehlo", &ehlo, &[250]).await?;
    if settings.tls == TlsMode::StartTls {
        if !has_extension(&capabilities, "STARTTLS") {
            return Err(SendError::failed(
                "starttls",
                "the relay does not offer STARTTLS, and CELLD_SMTP_TLS requires TLS",
            ));
        }
        expect(&mut connection, "starttls", "STARTTLS", &[220]).await?;
        connection = connection.upgrade(&settings.host, connector).await?;
        // What the server offered before TLS is not to be trusted after it.
        capabilities = expect(&mut connection, "ehlo", &ehlo, &[250]).await?;
    }
    if let Some(credentials) = &settings.credentials {
        authenticate(&mut connection, &capabilities, credentials).await?;
    }
    let size = extension_value(&capabilities, "SIZE").and_then(|value| value.parse::<usize>().ok());
    if size.is_some_and(|limit| limit != 0 && data.len() > limit) {
        return Err(SendError {
            code: "E_CONTENT_TOO_LARGE",
            message: "the message is larger than the relay accepts".to_string(),
        });
    }
    let mail = match size {
        Some(_) => format!("MAIL FROM:<{from}> SIZE={}", data.len()),
        None => format!("MAIL FROM:<{from}>"),
    };
    expect(&mut connection, "mail", &mail, &[250]).await?;
    for recipient in recipients {
        expect(
            &mut connection,
            "rcpt",
            &format!("RCPT TO:<{recipient}>"),
            &[250, 251],
        )
        .await?;
    }
    expect(&mut connection, "data", "DATA", &[354]).await?;
    connection.send("body", &dot_stuff(data)).await?;
    let accepted = connection.reply("body").await?;
    if accepted.code != 250 {
        return Err(SendError::refused("body", &accepted));
    }
    // The message is the relay's now; whether it says goodbye is of no matter.
    let _ = connection.command("quit", "QUIT").await;
    Ok(())
}

/// Send `line` and require one of `codes`.
async fn expect(
    connection: &mut Connection,
    stage: &str,
    line: &str,
    codes: &[u16],
) -> Result<Reply, SendError> {
    let reply = connection.command(stage, line).await?;
    if codes.contains(&reply.code) {
        Ok(reply)
    } else {
        Err(SendError::refused(stage, &reply))
    }
}

fn has_extension(reply: &Reply, name: &str) -> bool {
    // The first line is the greeting, the rest are extensions.
    reply
        .lines
        .iter()
        .skip(1)
        .any(|line| line.split(' ').next() == Some(name))
}

fn extension_value<'a>(reply: &'a Reply, name: &str) -> Option<&'a str> {
    reply.lines.iter().skip(1).find_map(|line| {
        let (keyword, value) = line.split_once(' ')?;
        (keyword == name).then_some(value)
    })
}

/// `AUTH PLAIN` when the relay offers it, `AUTH LOGIN` otherwise. Only called
/// once the connection is as encrypted as the operator asked.
async fn authenticate(
    connection: &mut Connection,
    capabilities: &Reply,
    credentials: &Credentials,
) -> Result<(), SendError> {
    let mechanisms: Vec<&str> = capabilities
        .lines
        .iter()
        .skip(1)
        .filter_map(|line| {
            line.strip_prefix("AUTH ")
                .or_else(|| line.strip_prefix("AUTH="))
        })
        .flat_map(|list| list.split(' '))
        .collect();
    let encode = |text: &[u8]| Zeroizing::new(STANDARD.encode(text));
    if mechanisms.contains(&"PLAIN") {
        let mut plain = Zeroizing::new(Vec::new());
        plain.push(0);
        plain.extend_from_slice(credentials.username.as_bytes());
        plain.push(0);
        plain.extend_from_slice(credentials.password.as_bytes());
        let line = Zeroizing::new(format!("AUTH PLAIN {}", encode(&plain).as_str()));
        expect(connection, "auth", &line, &[235]).await?;
    } else if mechanisms.contains(&"LOGIN") {
        expect(connection, "auth", "AUTH LOGIN", &[334]).await?;
        expect(
            connection,
            "auth",
            encode(credentials.username.as_bytes()).as_str(),
            &[334],
        )
        .await?;
        expect(
            connection,
            "auth",
            encode(credentials.password.as_bytes()).as_str(),
            &[235],
        )
        .await?;
    } else {
        return Err(SendError::failed(
            "auth",
            "the relay offers neither AUTH PLAIN nor AUTH LOGIN",
        ));
    }
    Ok(())
}

/// The node's relay, or the error a Worker reads when there is none.
pub fn relay() -> Result<&'static Settings, SendError> {
    match settings() {
        Ok(Some(relay)) => Ok(relay),
        Ok(None) => Err(SendError {
            code: "E_INTERNAL_SERVER_ERROR",
            message: format!("this node has no SMTP relay configured ({HOST})"),
        }),
        Err(_) => Err(SendError {
            code: "E_INTERNAL_SERVER_ERROR",
            message: "this node's SMTP relay settings are invalid".to_string(),
        }),
    }
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

    #[tokio::test]
    async fn a_reply_with_a_multibyte_character_in_its_code_is_malformed_not_a_panic() {
        crate::asyncrt::install_test_host_runtime();
        for reply in [
            "2\u{e9}0 ok\r\n",
            "\u{e9}50 ok\r\n",
            "25\u{20ac}\r\n",
            "\u{1f4a5}\r\n",
            "25\r\n",
            "250\u{e9}ok\r\n",
            "abc ok\r\n",
            "+50 ok\r\n",
        ] {
            let (client, mut server) = tokio::io::duplex(1024);
            tokio::io::AsyncWriteExt::write_all(&mut server, reply.as_bytes())
                .await
                .unwrap();
            let mut connection = Connection::new(Box::new(client));
            let error = match connection.read_reply("greeting").await {
                Ok(_) => panic!("{reply:?} was accepted"),
                Err(error) => error,
            };
            assert!(
                error.message.contains("malformed reply"),
                "{reply:?}: {error:?}"
            );
        }
        let (client, mut server) = tokio::io::duplex(1024);
        tokio::io::AsyncWriteExt::write_all(&mut server, b"250-first\r\n250 second\r\n")
            .await
            .unwrap();
        let reply = Connection::new(Box::new(client))
            .read_reply("greeting")
            .await
            .unwrap();
        assert_eq!((reply.code, reply.lines.len()), (250, 2));
    }

    #[test]
    fn dot_stuffing_doubles_a_leading_dot_and_ends_the_body() {
        assert_eq!(
            dot_stuff(b"a\r\n.b\r\n..\r\n.\r\nc\r\n"),
            b"a\r\n..b\r\n...\r\n..\r\nc\r\n.\r\n"
        );
        assert_eq!(dot_stuff(b".x"), b"..x\r\n.\r\n");
        assert_eq!(dot_stuff(b""), b"\r\n.\r\n");
        assert_eq!(dot_stuff(b"x\r\n"), b"x\r\n.\r\n");
    }

    // ---- STARTTLS against a sink with a certificate of its own ----

    use std::sync::{Arc, Mutex};

    struct Authority {
        connector: tokio_rustls::TlsConnector,
        acceptor: tokio_rustls::TlsAcceptor,
    }

    fn authority() -> Authority {
        use rcgen::{BasicConstraints, CertificateParams, DnType, IsCa, KeyPair};
        use rustls::pki_types::{PrivateKeyDer, PrivatePkcs8KeyDer};
        let provider = Arc::new(rustls::crypto::ring::default_provider());
        let ca_key = KeyPair::generate().unwrap();
        let mut ca_params = CertificateParams::new(Vec::new()).unwrap();
        ca_params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
        ca_params
            .distinguished_name
            .push(DnType::CommonName, "celld smtp test authority");
        let ca = ca_params.self_signed(&ca_key).unwrap();
        let leaf_key = KeyPair::generate().unwrap();
        let leaf = CertificateParams::new(vec!["localhost".to_string()])
            .unwrap()
            .signed_by(&leaf_key, &ca, &ca_key)
            .unwrap();
        let server = rustls::ServerConfig::builder_with_provider(provider.clone())
            .with_safe_default_protocol_versions()
            .unwrap()
            .with_no_client_auth()
            .with_single_cert(
                vec![leaf.der().clone()],
                PrivateKeyDer::from(PrivatePkcs8KeyDer::from(leaf_key.serialize_der())),
            )
            .unwrap();
        let mut roots = rustls::RootCertStore::empty();
        roots.add(ca.der().clone()).unwrap();
        let client = rustls::ClientConfig::builder_with_provider(provider)
            .with_safe_default_protocol_versions()
            .unwrap()
            .with_root_certificates(roots)
            .with_no_client_auth();
        Authority {
            connector: tokio_rustls::TlsConnector::from(Arc::new(client)),
            acceptor: tokio_rustls::TlsAcceptor::from(Arc::new(server)),
        }
    }

    #[derive(Default)]
    struct Seen {
        /// The commands read before the TLS handshake.
        plain: Vec<String>,
        /// The commands read inside TLS.
        secure: Vec<String>,
    }

    /// A relay that offers STARTTLS, and `AUTH` only inside TLS. With
    /// `smuggle` it sends a line right behind its `220` to `STARTTLS`.
    #[allow(clippy::disallowed_methods)] // The test drives a real loopback socket.
    async fn relay(acceptor: tokio_rustls::TlsAcceptor, smuggle: bool) -> (u16, Arc<Mutex<Seen>>) {
        use tokio::io::{AsyncBufReadExt as _, AsyncWriteExt as _, BufReader};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let seen = Arc::new(Mutex::new(Seen::default()));
        let record = seen.clone();
        tokio::spawn(async move {
            let Ok((stream, _)) = listener.accept().await else {
                return;
            };
            let mut stream = BufReader::new(stream);
            stream.get_mut().write_all(b"220 sink\r\n").await.unwrap();
            let mut line = String::new();
            loop {
                line.clear();
                if stream.read_line(&mut line).await.unwrap_or(0) == 0 {
                    return;
                }
                let verb = line
                    .split_whitespace()
                    .next()
                    .unwrap_or("")
                    .to_ascii_uppercase();
                record.lock().unwrap().plain.push(verb.clone());
                match verb.as_str() {
                    "EHLO" => stream
                        .get_mut()
                        .write_all(b"250-sink\r\n250 STARTTLS\r\n")
                        .await
                        .unwrap(),
                    "STARTTLS" => {
                        let reply: &[u8] = if smuggle {
                            b"220 go\r\n250 smuggled\r\n"
                        } else {
                            b"220 go\r\n"
                        };
                        stream.get_mut().write_all(reply).await.unwrap();
                        break;
                    }
                    _ => return,
                }
            }
            let Ok(tls) = acceptor.accept(stream.into_inner()).await else {
                return;
            };
            let mut tls = BufReader::new(tls);
            loop {
                line.clear();
                if tls.read_line(&mut line).await.unwrap_or(0) == 0 {
                    return;
                }
                let verb = line
                    .split_whitespace()
                    .next()
                    .unwrap_or("")
                    .to_ascii_uppercase();
                record.lock().unwrap().secure.push(verb.clone());
                let reply: &[u8] = match verb.as_str() {
                    "EHLO" => b"250-sink\r\n250 AUTH PLAIN\r\n",
                    "AUTH" => b"235 ok\r\n",
                    "DATA" => {
                        tls.get_mut().write_all(b"354 go\r\n").await.unwrap();
                        loop {
                            line.clear();
                            if tls.read_line(&mut line).await.unwrap_or(0) == 0 || line == ".\r\n" {
                                break;
                            }
                        }
                        b"250 queued\r\n"
                    }
                    "QUIT" => {
                        let _ = tls.get_mut().write_all(b"221 bye\r\n").await;
                        return;
                    }
                    _ => b"250 ok\r\n",
                };
                tls.get_mut().write_all(reply).await.unwrap();
            }
        });
        (port, seen)
    }

    fn secure_settings(port: u16) -> Settings {
        Settings {
            host: "localhost".into(),
            port,
            tls: TlsMode::StartTls,
            helo: "localhost".into(),
            credentials: Some(Credentials {
                username: "user".into(),
                password: Zeroizing::new("secret".into()),
            }),
        }
    }

    const MESSAGE: &[u8] = b"Subject: x\r\n\r\nbody\r\n";

    #[tokio::test]
    async fn starttls_upgrades_before_it_authenticates_and_sends() {
        crate::asyncrt::install_test_host_runtime();
        let authority = authority();
        let (port, seen) = relay(authority.acceptor, false).await;
        deliver_with(
            &secure_settings(port),
            &authority.connector,
            "a@example.com",
            &["b@example.com".to_string()],
            MESSAGE,
        )
        .await
        .unwrap();
        let seen = seen.lock().unwrap();
        // Nothing but the greeting and the upgrade ran in the clear, and the
        // capabilities were asked for again inside TLS, where AUTH is offered.
        assert_eq!(seen.plain, ["EHLO", "STARTTLS"]);
        assert_eq!(
            seen.secure,
            ["EHLO", "AUTH", "MAIL", "RCPT", "DATA", "QUIT"]
        );
    }

    #[tokio::test]
    async fn bytes_that_follow_the_220_of_starttls_are_refused() {
        crate::asyncrt::install_test_host_runtime();
        let authority = authority();
        let (port, seen) = relay(authority.acceptor, true).await;
        let error = deliver_with(
            &secure_settings(port),
            &authority.connector,
            "a@example.com",
            &["b@example.com".to_string()],
            MESSAGE,
        )
        .await
        .unwrap_err();
        assert!(
            error.message.contains("before the TLS handshake"),
            "{error:?}"
        );
        assert!(seen.lock().unwrap().secure.is_empty());
    }

    #[tokio::test]
    async fn a_certificate_of_an_unknown_authority_is_not_accepted() {
        crate::asyncrt::install_test_host_runtime();
        let authority = authority();
        let (port, seen) = relay(authority.acceptor, false).await;
        // The bundled public roots do not know the test authority. The node
        // installs the process-wide TLS provider at start.
        rustls::crypto::ring::default_provider()
            .install_default()
            .ok();
        let error = deliver(
            &secure_settings(port),
            "a@example.com",
            &["b@example.com".to_string()],
            MESSAGE,
        )
        .await
        .unwrap_err();
        assert!(error.message.contains("TLS"), "{error:?}");
        assert!(seen.lock().unwrap().secure.is_empty());
    }
}
