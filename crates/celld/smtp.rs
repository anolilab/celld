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

use crate::email::EmailError;
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
/// How long one whole transaction may take, `DATA` included.
const TRANSACTION_TIMEOUT: Duration = Duration::from_secs(300);
/// The longest reply line read; RFC 5321 allows 512.
const MAX_REPLY_LINE: u64 = 2048;
/// The most lines one reply may have.
const MAX_REPLY_LINES: usize = 100;
/// The most transactions a node runs at once, so a Worker cannot open an
/// unbounded number of connections to the relay.
const MAX_CONCURRENT: usize = 16;

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
}

impl Connection {
    fn new(stream: Box<dyn Duplex>) -> Self {
        Self {
            stream: tokio::io::BufReader::new(stream),
        }
    }

    async fn send(&mut self, stage: &str, bytes: &[u8]) -> Result<(), EmailError> {
        let write = async {
            self.stream.get_mut().write_all(bytes).await?;
            self.stream.get_mut().flush().await
        };
        match crate::asyncrt::timeout(REPLY_TIMEOUT, write).await {
            Ok(Ok(())) => Ok(()),
            Ok(Err(error)) => Err(failed(stage, format!("write failed ({})", error.kind()))),
            Err(_) => Err(failed(stage, "write timed out")),
        }
    }

    async fn command(&mut self, stage: &str, line: &str) -> Result<Reply, EmailError> {
        debug_assert!(!line.contains(['\r', '\n']));
        self.send(stage, format!("{line}\r\n").as_bytes()).await?;
        self.reply(stage).await
    }

    async fn reply(&mut self, stage: &str) -> Result<Reply, EmailError> {
        match crate::asyncrt::timeout(REPLY_TIMEOUT, self.read_reply(stage)).await {
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
        Ok(Self::new(handshake(stream, host, connector).await?))
    }
}

async fn handshake(
    stream: Box<dyn Duplex>,
    host: &str,
    connector: &tokio_rustls::TlsConnector,
) -> Result<Box<dyn Duplex>, EmailError> {
    let name = rustls::pki_types::ServerName::try_from(host.to_string())
        .map_err(|_| failed("tls", "the relay's name is not valid for TLS"))?;
    let tls = connector.connect(name, stream);
    match crate::asyncrt::timeout(REPLY_TIMEOUT, tls).await {
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

/// Hand one message to the relay: one transaction on one connection.
pub async fn deliver(
    settings: &Settings,
    from: &str,
    recipients: &[String],
    data: &[u8],
) -> Result<(), EmailError> {
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
) -> Result<(), EmailError> {
    // A send that finds every slot taken waits a bounded time, so a burst
    // cannot leave messages queued for good.
    let _permit = match crate::asyncrt::timeout(REPLY_TIMEOUT, semaphore().acquire()).await {
        Ok(Ok(permit)) => permit,
        Ok(Err(_)) => return Err(failed("connect", "the node is shutting down")),
        Err(_) => return Err(failed("connect", "the node is sending too many messages")),
    };
    match crate::asyncrt::timeout(
        TRANSACTION_TIMEOUT,
        transaction(settings, connector, from, recipients, data),
    )
    .await
    {
        Ok(result) => result,
        Err(_) => Err(failed("transaction", "it did not finish in time")),
    }
}

async fn transaction(
    settings: &Settings,
    connector: &tokio_rustls::TlsConnector,
    from: &str,
    recipients: &[String],
    data: &[u8],
) -> Result<(), EmailError> {
    let tcp = dial(&settings.host, settings.port).await?;
    let mut connection = Connection::new(Box::new(tcp));
    if settings.tls == TlsMode::Tls {
        let stream = connection.stream.into_inner();
        connection = Connection::new(handshake(stream, &settings.host, connector).await?);
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
        return Err(refused("body", &accepted));
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
        Err(_) => Err(EmailError::new(
            "E_INTERNAL_SERVER_ERROR",
            "this node's SMTP relay settings are invalid",
        )),
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
