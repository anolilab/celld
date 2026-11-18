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

use crate::email::{EmailError, Prepared};
use base64::engine::general_purpose::STANDARD;
use base64::Engine as _;
use std::sync::OnceLock;
use std::time::Duration;
use tokio::io::{
    AsyncBufReadExt as _, AsyncRead, AsyncReadExt as _, AsyncWrite, AsyncWriteExt as _,
};
use zeroize::Zeroizing;

mod settings;

pub use settings::{
    settings, validate, Settings, TlsMode, HELO_NAME, HOST, PASSWORD, PASSWORD_FILE, PORT, TLS,
    USERNAME,
};

use settings::Credentials;

/// How long one connection attempt may take.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(15);
/// How long the server may take to answer one command.
const REPLY_TIMEOUT: Duration = Duration::from_secs(60);
/// How long one chunk of the body may take to be written.
const WRITE_TIMEOUT: Duration = Duration::from_secs(60);
/// How long the server may take to answer the end of the body: it may scan or
/// queue the message first. RFC 5321 section 4.5.3.2.
const FINAL_REPLY_TIMEOUT: Duration = Duration::from_secs(600);
/// How long everything but the body and the answer to it may take.
const SETUP_TIMEOUT: Duration = Duration::from_secs(300);
/// The slowest write of the body that is still allowed to finish, in bytes per
/// second. It lengthens the time one transaction may take with the message.
const MIN_BODY_RATE: usize = 64 * 1024;
/// The size of one write of the body.
const BODY_CHUNK: usize = 64 * 1024;
/// How long the `QUIT` that ends a delivered message may take to be written.
const QUIT_TIMEOUT: Duration = Duration::from_secs(5);
/// The longest command line, its CRLF included (RFC 5321).
const MAX_COMMAND_LINE: usize = 512;

/// The longest reply line read; RFC 5321 allows 512.
const MAX_REPLY_LINE: u64 = 2048;
/// The most lines one reply may have.
const MAX_REPLY_LINES: usize = 100;
/// The most transactions a node runs at once, so a Worker cannot open an
/// unbounded number of connections to the relay.
const MAX_CONCURRENT: usize = 16;
/// The most of those one script may hold, so that one Worker cannot take them
/// all.
const MAX_PER_SCRIPT: usize = 4;
/// The most messages one isolate may have in flight.
pub const MAX_PER_ISOLATE: usize = 4;

/// The limits on the time a transaction takes.
#[derive(Clone, Copy)]
struct Timeouts {
    /// One command's reply.
    reply: Duration,
    /// One chunk of the body.
    write: Duration,
    /// The reply to the end of the body.
    final_reply: Duration,
    /// Everything else, which the deadline of a transaction adds the body to.
    setup: Duration,
}

impl Timeouts {
    const NODE: Self = Self {
        reply: REPLY_TIMEOUT,
        write: WRITE_TIMEOUT,
        final_reply: FINAL_REPLY_TIMEOUT,
        setup: SETUP_TIMEOUT,
    };

    /// How long a transaction of `size` bytes may take in all: the setup, the
    /// body at the slowest allowed rate, and the answer to it.
    fn transaction(&self, size: usize) -> Duration {
        self.setup + self.final_reply + Duration::from_secs((size / MIN_BODY_RATE) as u64)
    }
}

// ---- The client ------------------------------------------------------------

fn failed(stage: &str, detail: impl std::fmt::Display) -> EmailError {
    EmailError::new(
        "E_DELIVERY_FAILED",
        format!("the SMTP transaction failed at {stage}: {detail}"),
    )
}

fn refused(stage: &str, reply: &Reply) -> EmailError {
    let code = if reply.code == 552 {
        "E_CONTENT_TOO_LARGE"
    } else {
        "E_DELIVERY_FAILED"
    };
    EmailError::new(
        code,
        format!("the SMTP server answered {} at {stage}", reply.code),
    )
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
    timeouts: Timeouts,
}

impl Connection {
    fn new(stream: Box<dyn Duplex>, timeouts: Timeouts) -> Self {
        Self {
            stream: tokio::io::BufReader::new(stream),
            timeouts,
        }
    }

    async fn write(
        &mut self,
        stage: &str,
        bytes: &[u8],
        limit: Duration,
    ) -> Result<(), EmailError> {
        let write = async {
            self.stream.get_mut().write_all(bytes).await?;
            self.stream.get_mut().flush().await
        };
        match crate::asyncrt::timeout(limit, write).await {
            Ok(Ok(())) => Ok(()),
            Ok(Err(error)) => Err(failed(stage, format!("write failed ({})", error.kind()))),
            Err(_) => Err(failed(stage, "write timed out")),
        }
    }

    async fn send(&mut self, stage: &str, bytes: &[u8]) -> Result<(), EmailError> {
        self.write(stage, bytes, self.timeouts.reply).await
    }

    /// The body, a chunk at a time: a relay that takes it slowly is fine as
    /// long as it keeps taking it.
    async fn send_body(&mut self, body: &[u8]) -> Result<(), EmailError> {
        for chunk in body.chunks(BODY_CHUNK) {
            self.write("body", chunk, self.timeouts.write).await?;
        }
        Ok(())
    }

    async fn command(&mut self, stage: &str, line: &str) -> Result<Reply, EmailError> {
        debug_assert!(!line.contains(['\r', '\n']));
        self.send(stage, format!("{line}\r\n").as_bytes()).await?;
        self.reply(stage).await
    }

    async fn reply(&mut self, stage: &str) -> Result<Reply, EmailError> {
        self.reply_within(stage, self.timeouts.reply).await
    }

    async fn reply_within(&mut self, stage: &str, limit: Duration) -> Result<Reply, EmailError> {
        match crate::asyncrt::timeout(limit, self.read_reply(stage)).await {
            Ok(reply) => reply,
            Err(_) => Err(failed(stage, "the server did not answer in time")),
        }
    }

    async fn read_reply(&mut self, stage: &str) -> Result<Reply, EmailError> {
        let mut lines = Vec::new();
        loop {
            let mut line = Vec::new();
            let read = (&mut self.stream)
                .take(MAX_REPLY_LINE)
                .read_until(b'\n', &mut line)
                .await
                .map_err(|error| failed(stage, format!("read failed ({})", error.kind())))?;
            if read == 0 {
                return Err(failed(stage, "the server closed the connection"));
            }
            if !line.ends_with(b"\n") {
                return Err(failed(stage, "the server sent an overlong reply line"));
            }
            let malformed = || failed(stage, "the server sent a malformed reply");
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
                return Err(failed(stage, "the server sent too long a reply"));
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
    ) -> Result<Self, EmailError> {
        if !self.stream.buffer().is_empty() {
            return Err(failed(
                "starttls",
                "the server sent data before the TLS handshake",
            ));
        }
        let stream = self.stream.into_inner();
        let timeouts = self.timeouts;
        Ok(Self::new(
            handshake(stream, host, connector, timeouts.reply).await?,
            timeouts,
        ))
    }
}

async fn handshake(
    stream: Box<dyn Duplex>,
    host: &str,
    connector: &tokio_rustls::TlsConnector,
    limit: Duration,
) -> Result<Box<dyn Duplex>, EmailError> {
    let name = rustls::pki_types::ServerName::try_from(host.to_string())
        .map_err(|_| failed("tls", "the relay's name is not valid for TLS"))?;
    let tls = connector.connect(name, stream);
    match crate::asyncrt::timeout(limit, tls).await {
        Ok(Ok(stream)) => Ok(Box::new(stream)),
        Ok(Err(_)) => Err(failed(
            "tls",
            "the TLS handshake failed or the relay's certificate was not accepted",
        )),
        Err(_) => Err(failed("tls", "the TLS handshake timed out")),
    }
}

/// Connect to `host:port` on the addresses the egress policy for this binding
/// allows, and to nothing else.
async fn dial(host: &str, port: u16) -> Result<tokio::net::TcpStream, EmailError> {
    let addresses = crate::egress_filter::smtp_addresses(host, port)
        .await
        .map_err(|refused| failed("connect", refused))?;
    match crate::asyncrt::timeout(
        CONNECT_TIMEOUT,
        tokio::net::TcpStream::connect(&addresses[..]),
    )
    .await
    {
        Ok(Ok(stream)) => Ok(stream),
        Ok(Err(error)) => Err(failed(
            "connect",
            format!("could not connect to the relay ({})", error.kind()),
        )),
        Err(_) => Err(failed("connect", "the connection to the relay timed out")),
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

/// The transactions each script has running, which `MAX_PER_SCRIPT` bounds
/// ahead of the node-wide limit.
struct ScriptLimits {
    cap: usize,
    wait: Duration,
    scripts:
        std::sync::Mutex<std::collections::HashMap<String, std::sync::Arc<tokio::sync::Semaphore>>>,
}

impl ScriptLimits {
    fn new(cap: usize, wait: Duration) -> Self {
        Self {
            cap,
            wait,
            scripts: Default::default(),
        }
    }

    /// A slot for `script`, waited for a bounded time.
    async fn acquire(&self, script: &str) -> Result<tokio::sync::OwnedSemaphorePermit, EmailError> {
        let semaphore = {
            let mut scripts = self.scripts.lock().unwrap_or_else(|e| e.into_inner());
            // A script that sends nothing now keeps no entry.
            if scripts.len() >= 1024 {
                scripts.retain(|_, held| {
                    std::sync::Arc::strong_count(held) > 1 || held.available_permits() < self.cap
                });
            }
            scripts
                .entry(script.to_string())
                .or_insert_with(|| std::sync::Arc::new(tokio::sync::Semaphore::new(self.cap)))
                .clone()
        };
        match crate::asyncrt::timeout(self.wait, semaphore.acquire_owned()).await {
            Ok(Ok(permit)) => Ok(permit),
            Ok(Err(_)) => Err(failed("connect", "the node is shutting down")),
            Err(_) => Err(EmailError::new(
                "E_RATE_LIMIT_EXCEEDED",
                "this Worker has too many messages being sent",
            )),
        }
    }
}

fn script_limits() -> &'static ScriptLimits {
    static LIMITS: OnceLock<ScriptLimits> = OnceLock::new();
    LIMITS.get_or_init(|| ScriptLimits::new(MAX_PER_SCRIPT, REPLY_TIMEOUT))
}

/// One message of an isolate in flight. The count falls when this is dropped,
/// whichever way the send ends.
pub struct InFlight(std::sync::Arc<std::sync::atomic::AtomicUsize>);

impl InFlight {
    /// A slot, unless `counter` already holds `MAX_PER_ISOLATE`.
    pub fn take(counter: &std::sync::Arc<std::sync::atomic::AtomicUsize>) -> Option<Self> {
        use std::sync::atomic::Ordering::Relaxed;
        if counter.fetch_add(1, Relaxed) >= MAX_PER_ISOLATE {
            counter.fetch_sub(1, Relaxed);
            return None;
        }
        Some(Self(counter.clone()))
    }
}

impl Drop for InFlight {
    fn drop(&mut self) {
        self.0.fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
    }
}

/// Hand one message to the relay: one transaction on one connection. `script`
/// is the Worker that sends it.
pub async fn deliver(
    settings: &Settings,
    script: &str,
    message: &Prepared,
) -> Result<(), EmailError> {
    let _slot = script_limits().acquire(script).await?;
    deliver_with(
        settings,
        &crate::tls_roots::bundled_connector(),
        Timeouts::NODE,
        message,
    )
    .await
}

/// [`deliver`] with the TLS trust and the time limits the caller chooses; a
/// test trusts its own authority and waits for little.
async fn deliver_with(
    settings: &Settings,
    connector: &tokio_rustls::TlsConnector,
    timeouts: Timeouts,
    message: &Prepared,
) -> Result<(), EmailError> {
    // A send that finds every slot taken waits a bounded time, so a burst
    // cannot leave messages queued for good.
    let _permit = match crate::asyncrt::timeout(timeouts.reply, semaphore().acquire()).await {
        Ok(Ok(permit)) => permit,
        Ok(Err(_)) => return Err(failed("connect", "the node is shutting down")),
        Err(_) => return Err(failed("connect", "the node is sending too many messages")),
    };
    let deadline = timeouts.transaction(message.data.len());
    let mut connection = match crate::asyncrt::timeout(
        deadline,
        transaction(settings, connector, timeouts, message),
    )
    .await
    {
        Ok(result) => result?,
        Err(_) => return Err(failed("transaction", "it did not finish in time")),
    };
    // The message is the relay's now. `QUIT` is a courtesy, written within a
    // short time of its own and not answered for: the Worker is not kept
    // waiting on a relay that is slow to say goodbye.
    let _ = connection.write("quit", b"QUIT\r\n", QUIT_TIMEOUT).await;
    Ok(())
}

/// The relay's refusal of what it does not offer.
fn unsupported(what: &str, extension: &str) -> EmailError {
    EmailError::new(
        "E_8BIT_NOT_SUPPORTED",
        format!("the message has {what} and the relay does not offer {extension}"),
    )
}

/// One transaction up to the relay's answer to the end of the body, with the
/// connection left open for `QUIT`.
async fn transaction(
    settings: &Settings,
    connector: &tokio_rustls::TlsConnector,
    timeouts: Timeouts,
    message: &Prepared,
) -> Result<Connection, EmailError> {
    let (from, recipients, data) = (&message.from, &message.recipients, &message.data);
    let tcp = dial(&settings.host, settings.port).await?;
    let mut connection = Connection::new(Box::new(tcp), timeouts);
    if settings.tls == TlsMode::Tls {
        let stream = connection.stream.into_inner();
        connection = Connection::new(
            handshake(stream, &settings.host, connector, timeouts.reply).await?,
            timeouts,
        );
    }
    let greeting = connection.reply("greeting").await?;
    if greeting.code != 220 {
        return Err(refused("greeting", &greeting));
    }
    let ehlo = format!("EHLO {}", settings.helo);
    let mut capabilities = expect(&mut connection, "ehlo", &ehlo, &[250]).await?;
    if settings.tls == TlsMode::StartTls {
        if !has_extension(&capabilities, "STARTTLS") {
            return Err(failed(
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
        return Err(EmailError::new(
            "E_CONTENT_TOO_LARGE",
            "the message is larger than the relay accepts",
        ));
    }
    let mut mail = format!("MAIL FROM:<{from}>");
    if size.is_some() {
        mail.push_str(&format!(" SIZE={}", data.len()));
    }
    // A relay that does not offer what the message needs would otherwise get
    // bytes it may mangle or drop, so the message is refused instead.
    if message.eight_bit {
        if !has_extension(&capabilities, "8BITMIME") {
            return Err(unsupported("8-bit content", "8BITMIME"));
        }
        mail.push_str(" BODY=8BITMIME");
    }
    if message.smtputf8 {
        if !has_extension(&capabilities, "SMTPUTF8") {
            return Err(unsupported("non-ASCII headers", "SMTPUTF8"));
        }
        mail.push_str(" SMTPUTF8");
    }
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
    connection.send_body(&dot_stuff(data)).await?;
    let accepted = connection
        .reply_within("body", timeouts.final_reply)
        .await?;
    if accepted.code != 250 {
        return Err(refused("body", &accepted));
    }
    Ok(connection)
}

/// Send `line` and require one of `codes`.
async fn expect(
    connection: &mut Connection,
    stage: &str,
    line: &str,
    codes: &[u16],
) -> Result<Reply, EmailError> {
    let reply = connection.command(stage, line).await?;
    if codes.contains(&reply.code) {
        Ok(reply)
    } else {
        Err(refused(stage, &reply))
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
) -> Result<(), EmailError> {
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
        let encoded = encode(&plain);
        let line = Zeroizing::new(format!("AUTH PLAIN {}", encoded.as_str()));
        if line.len() + 2 <= MAX_COMMAND_LINE {
            expect(connection, "auth", &line, &[235]).await?;
        } else {
            // Long credentials do not fit the command line. The response
            // goes on its own line after the server's `334`.
            expect(connection, "auth", "AUTH PLAIN", &[334]).await?;
            expect(connection, "auth", encoded.as_str(), &[235]).await?;
        }
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
        return Err(failed(
            "auth",
            "the relay offers neither AUTH PLAIN nor AUTH LOGIN",
        ));
    }
    Ok(())
}

/// The node's relay, or the error a Worker reads when there is none.
pub fn relay() -> Result<&'static Settings, EmailError> {
    match settings() {
        Ok(Some(relay)) => Ok(relay),
        Ok(None) => Err(EmailError::new(
            "E_INTERNAL_SERVER_ERROR",
            format!("this node has no SMTP relay configured ({HOST})"),
        )),
        Err(error) => {
            // The cause names a variable or a path, never a value.
            tracing::error!(event = "smtp_settings_invalid", error = %format!("{error:#}"));
            Err(EmailError::new(
                "E_INTERNAL_SERVER_ERROR",
                "this node's SMTP relay settings are invalid",
            ))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
            let mut connection = Connection::new(Box::new(client), Timeouts::NODE);
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
        let reply = Connection::new(Box::new(client), Timeouts::NODE)
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

    fn message() -> Prepared {
        Prepared::new(
            "a@example.com".into(),
            vec!["b@example.com".into()],
            MESSAGE.to_vec(),
            "id@example.com".into(),
        )
    }

    #[tokio::test]
    async fn starttls_upgrades_before_it_authenticates_and_sends() {
        crate::asyncrt::install_test_host_runtime();
        let authority = authority();
        let (port, seen) = relay(authority.acceptor, false).await;
        deliver_with(
            &secure_settings(port),
            &authority.connector,
            Timeouts::NODE,
            &message(),
        )
        .await
        .unwrap();
        let seen = seen.lock().unwrap();
        // Nothing but the greeting and the upgrade ran in the clear, and the
        // capabilities were asked for again inside TLS, where AUTH is offered.
        assert_eq!(seen.plain, ["EHLO", "STARTTLS"]);
        // `QUIT` is written as the transaction ends, and the relay may not
        // have read it yet.
        assert_eq!(seen.secure[..5], ["EHLO", "AUTH", "MAIL", "RCPT", "DATA"]);
        assert!(seen.secure.len() <= 6, "{:?}", seen.secure);
    }

    #[tokio::test]
    async fn bytes_that_follow_the_220_of_starttls_are_refused() {
        crate::asyncrt::install_test_host_runtime();
        let authority = authority();
        let (port, seen) = relay(authority.acceptor, true).await;
        let error = deliver_with(
            &secure_settings(port),
            &authority.connector,
            Timeouts::NODE,
            &message(),
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
        let error = deliver(&secure_settings(port), "script", &message())
            .await
            .unwrap_err();
        assert!(error.message.contains("TLS"), "{error:?}");
        assert!(seen.lock().unwrap().secure.is_empty());
    }

    // ---- time limits, slots and what the relay must offer ----

    /// A relay in the clear that serves one connection. `offers` is its last
    /// `EHLO` line; `final_delay` is how long it waits before it answers the
    /// end of the body; with `stall`, it reads no body at all. The `MAIL` line
    /// it saw is returned.
    #[allow(clippy::disallowed_methods)] // The test drives a real loopback socket.
    async fn plain_relay(
        offers: &'static [&'static str],
        final_delay: Duration,
        stall: bool,
    ) -> (u16, Arc<Mutex<Vec<String>>>) {
        use tokio::io::{AsyncBufReadExt as _, AsyncWriteExt as _, BufReader};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let mails = Arc::new(Mutex::new(Vec::new()));
        let record = mails.clone();
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
                let reply = match verb.as_str() {
                    "EHLO" => {
                        let mut reply = String::from("250-sink\r\n");
                        for (index, offer) in offers.iter().enumerate() {
                            let separator = if index + 1 < offers.len() { '-' } else { ' ' };
                            reply.push_str(&format!("250{separator}{offer}\r\n"));
                        }
                        reply
                    }
                    "MAIL" => {
                        record.lock().unwrap().push(line.trim_end().to_string());
                        "250 ok\r\n".to_string()
                    }
                    "DATA" => {
                        stream.get_mut().write_all(b"354 go\r\n").await.unwrap();
                        if stall {
                            tokio::time::sleep(Duration::from_secs(30)).await;
                            return;
                        }
                        loop {
                            line.clear();
                            if stream.read_line(&mut line).await.unwrap_or(0) == 0
                                || line == ".\r\n"
                            {
                                break;
                            }
                        }
                        tokio::time::sleep(final_delay).await;
                        "250 queued\r\n".to_string()
                    }
                    "QUIT" => {
                        // Slow to say goodbye.
                        tokio::time::sleep(Duration::from_secs(3)).await;
                        return;
                    }
                    _ => "250 ok\r\n".to_string(),
                };
                stream.get_mut().write_all(reply.as_bytes()).await.unwrap();
            }
        });
        (port, mails)
    }

    fn open_settings(port: u16) -> Settings {
        Settings {
            host: "127.0.0.1".into(),
            port,
            tls: TlsMode::Off,
            helo: "localhost".into(),
            credentials: None,
        }
    }

    fn quick(final_reply: Duration, write: Duration) -> Timeouts {
        Timeouts {
            reply: Duration::from_millis(150),
            write,
            final_reply,
            setup: Duration::from_secs(30),
        }
    }

    async fn deliver_open(
        port: u16,
        timeouts: Timeouts,
        message: &Prepared,
    ) -> Result<(), EmailError> {
        let authority = authority();
        deliver_with(
            &open_settings(port),
            &authority.connector,
            timeouts,
            message,
        )
        .await
    }

    /// A relay that accepts `AUTH PLAIN` only as the RFC 5321 command line
    /// allows: a longer line is a `500`, and an initial response may follow a
    /// bare `AUTH PLAIN` after a `334`. Returns what authenticated.
    #[allow(clippy::disallowed_methods)] // The test drives a real loopback socket.
    async fn auth_relay() -> (u16, Arc<Mutex<Vec<String>>>) {
        use tokio::io::{AsyncBufReadExt as _, AsyncWriteExt as _, BufReader};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let accepted = Arc::new(Mutex::new(Vec::new()));
        let record = accepted.clone();
        tokio::spawn(async move {
            let Ok((stream, _)) = listener.accept().await else {
                return;
            };
            let mut stream = BufReader::new(stream);
            stream.get_mut().write_all(b"220 sink\r\n").await.unwrap();
            let mut line = String::new();
            let mut awaiting_response = false;
            loop {
                line.clear();
                if stream.read_line(&mut line).await.unwrap_or(0) == 0 {
                    return;
                }
                let reply: &[u8] = if awaiting_response {
                    awaiting_response = false;
                    record.lock().unwrap().push(line.trim_end().to_string());
                    b"235 ok\r\n"
                } else if line.len() > 512 {
                    b"500 line too long\r\n"
                } else if line.trim_end().eq_ignore_ascii_case("AUTH PLAIN") {
                    awaiting_response = true;
                    b"334 \r\n"
                } else if line.to_ascii_uppercase().starts_with("AUTH PLAIN ") {
                    record
                        .lock()
                        .unwrap()
                        .push(line.trim_end()[11..].to_string());
                    b"235 ok\r\n"
                } else if line.to_ascii_uppercase().starts_with("EHLO") {
                    b"250-sink\r\n250 AUTH PLAIN\r\n"
                } else if line.to_ascii_uppercase().starts_with("DATA") {
                    stream.get_mut().write_all(b"354 go\r\n").await.unwrap();
                    loop {
                        line.clear();
                        if stream.read_line(&mut line).await.unwrap_or(0) == 0 || line == ".\r\n" {
                            break;
                        }
                    }
                    b"250 queued\r\n"
                } else if line.to_ascii_uppercase().starts_with("QUIT") {
                    return;
                } else {
                    b"250 ok\r\n"
                };
                stream.get_mut().write_all(reply).await.unwrap();
            }
        });
        (port, accepted)
    }

    #[tokio::test]
    async fn credentials_too_long_for_one_command_line_go_after_a_334() {
        crate::asyncrt::install_test_host_runtime();
        for (user, password) in [
            ("user".to_string(), "pass".to_string()),
            ("u".repeat(255), "p".repeat(255)),
        ] {
            let (port, accepted) = auth_relay().await;
            let mut settings = open_settings(port);
            settings.credentials = Some(Credentials {
                username: user.clone(),
                password: password.clone().into(),
            });
            let authority = authority();
            let timeouts = Timeouts {
                reply: Duration::from_secs(5),
                ..quick(Duration::from_secs(5), Duration::from_secs(5))
            };
            deliver_with(&settings, &authority.connector, timeouts, &message())
                .await
                .unwrap_or_else(|error| panic!("{} bytes: {error}", user.len()));
            let expected = STANDARD.encode(format!("\0{user}\0{password}"));
            assert_eq!(*accepted.lock().unwrap(), [expected]);
        }
    }

    #[tokio::test]
    async fn the_answer_to_the_end_of_the_body_may_take_longer_than_any_other_reply() {
        crate::asyncrt::install_test_host_runtime();
        let (port, _) = plain_relay(&["8BITMIME"], Duration::from_millis(500), false).await;
        let timeouts = quick(Duration::from_secs(10), Duration::from_secs(10));
        deliver_open(port, timeouts, &message()).await.unwrap();
        // A reply to a command is still held to the short limit.
        let (port, _) = plain_relay(&["8BITMIME"], Duration::from_millis(500), false).await;
        let same = quick(Duration::from_millis(150), Duration::from_secs(10));
        let error = deliver_open(port, same, &message()).await.unwrap_err();
        assert!(
            error.message.contains("did not answer in time"),
            "{error:?}"
        );
    }

    #[tokio::test]
    async fn a_relay_that_stops_reading_the_body_fails_the_write_not_the_whole_wait() {
        crate::asyncrt::install_test_host_runtime();
        let (port, _) = plain_relay(&["8BITMIME"], Duration::ZERO, true).await;
        let big = Prepared::new(
            "a@example.com".into(),
            vec!["b@example.com".into()],
            format!("{}\r\n", "x".repeat(70))
                .repeat(300_000)
                .into_bytes(),
            "id@example.com".into(),
        );
        let timeouts = quick(Duration::from_secs(10), Duration::from_millis(400));
        let started = crate::asyncrt::mono_ms();
        let error = deliver_open(port, timeouts, &big).await.unwrap_err();
        assert!(
            error.message.contains("body") && error.message.contains("write timed out"),
            "{error:?}"
        );
        assert!(crate::asyncrt::mono_ms() - started < 10_000);
    }

    #[test]
    fn the_deadline_of_a_transaction_covers_a_long_body_and_the_answer_to_it() {
        let node = Timeouts::NODE;
        let largest = node.transaction(25 * 1024 * 1024);
        // The setup, the answer to the body, and the body at the slowest
        // rate: none of them may cut the others short.
        assert!(largest >= node.setup + node.final_reply + Duration::from_secs(400));
        assert!(node.transaction(0) >= node.setup + node.final_reply);
        assert!(node.final_reply >= Duration::from_secs(600));
    }

    #[tokio::test]
    async fn a_relay_that_is_slow_to_say_goodbye_does_not_hold_the_worker() {
        crate::asyncrt::install_test_host_runtime();
        // This relay takes three seconds over `QUIT`.
        let (port, _) = plain_relay(&["8BITMIME"], Duration::ZERO, false).await;
        let started = crate::asyncrt::mono_ms();
        deliver_open(port, Timeouts::NODE, &message())
            .await
            .unwrap();
        assert!(
            crate::asyncrt::mono_ms() - started < 2_000,
            "{} ms",
            crate::asyncrt::mono_ms() - started
        );
    }

    #[tokio::test]
    async fn eight_bit_content_needs_8bitmime_and_is_declared() {
        crate::asyncrt::install_test_host_runtime();
        let eight = Prepared::new(
            "a@example.com".into(),
            vec!["b@example.com".into()],
            "Subject: x\r\n\r\ncaf\u{e9}\r\n".as_bytes().to_vec(),
            "id@example.com".into(),
        );
        assert!(eight.eight_bit && !eight.smtputf8);
        let (port, mails) = plain_relay(&["8BITMIME"], Duration::ZERO, false).await;
        deliver_open(port, Timeouts::NODE, &eight).await.unwrap();
        assert_eq!(
            mails.lock().unwrap()[0],
            "MAIL FROM:<a@example.com> BODY=8BITMIME"
        );
        let (port, mails) = plain_relay(&["PIPELINING"], Duration::ZERO, false).await;
        let error = deliver_open(port, Timeouts::NODE, &eight)
            .await
            .unwrap_err();
        assert_eq!(error.code, "E_8BIT_NOT_SUPPORTED", "{error:?}");
        assert!(
            mails.lock().unwrap().is_empty(),
            "the relay was told of a message"
        );
        // Plain ASCII needs neither.
        let (port, mails) = plain_relay(&["PIPELINING"], Duration::ZERO, false).await;
        deliver_open(port, Timeouts::NODE, &message())
            .await
            .unwrap();
        assert_eq!(mails.lock().unwrap()[0], "MAIL FROM:<a@example.com>");
    }

    #[tokio::test]
    async fn non_ascii_headers_need_smtputf8() {
        crate::asyncrt::install_test_host_runtime();
        let utf8 = Prepared::new(
            "a@example.com".into(),
            vec!["b@example.com".into()],
            "Subject: caf\u{e9}\r\n\r\nbody\r\n".as_bytes().to_vec(),
            "id@example.com".into(),
        );
        assert!(utf8.smtputf8);
        let (port, mails) = plain_relay(&["8BITMIME", "SMTPUTF8"], Duration::ZERO, false).await;
        deliver_open(port, Timeouts::NODE, &utf8).await.unwrap();
        assert_eq!(
            mails.lock().unwrap()[0],
            "MAIL FROM:<a@example.com> BODY=8BITMIME SMTPUTF8"
        );
        let (port, _) = plain_relay(&["8BITMIME"], Duration::ZERO, false).await;
        let error = deliver_open(port, Timeouts::NODE, &utf8).await.unwrap_err();
        assert_eq!(error.code, "E_8BIT_NOT_SUPPORTED", "{error:?}");
    }

    #[test]
    fn an_isolate_holds_a_few_messages_and_gives_each_slot_back() {
        let counter = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let held: Vec<_> = (0..MAX_PER_ISOLATE)
            .map(|_| InFlight::take(&counter).expect("a free slot"))
            .collect();
        assert!(InFlight::take(&counter).is_none());
        assert!(
            InFlight::take(&counter).is_none(),
            "a refusal keeps no slot"
        );
        drop(held);
        let _again = InFlight::take(&counter).expect("the slots came back");
    }

    #[tokio::test]
    async fn one_script_cannot_take_every_slot_of_the_node() {
        crate::asyncrt::install_test_host_runtime();
        let limits = ScriptLimits::new(2, Duration::from_millis(100));
        let first = limits.acquire("busy").await.unwrap();
        let _second = limits.acquire("busy").await.unwrap();
        let error = limits.acquire("busy").await.unwrap_err();
        assert_eq!(error.code, "E_RATE_LIMIT_EXCEEDED");
        assert!(error.message.contains("this Worker"), "{error:?}");
        // Another script is not held up by it.
        let _other = limits.acquire("quiet").await.unwrap();
        drop(first);
        let _slot = limits.acquire("busy").await.expect("a slot came back");
    }
}
