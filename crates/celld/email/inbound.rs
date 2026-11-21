// Copyright 2026 Deno Land Inc. Apache-2.0 license.

//! Inbound mail: a raw RFC 5322 message posted by a relay becomes the
//! `message` of an `email()` event, and what the handler does with it
//! (`forward()`, `reply()`, `setReject()`) is checked here.
//!
//! The route answers on the internal listener, behind the operator token or
//! the route's own token (`CELLD_EMAIL_INBOUND_TOKEN`), so the poster is the
//! operator's own relay or webhook. A message is accepted
//! as it came: the headers are parsed for `message.headers`, but a `Received:`
//! header, a foreign `From:` and a `Message-ID` are all normal in mail that
//! arrives, and the bytes handed to the Worker are never rewritten. Only what
//! the node itself sends on (`forward`, `reply`) is held to the rules of
//! [`prepare_raw`](super::prepare_raw): CRLF line ends, no `Bcc:`, bounded
//! lines and size, and an envelope that is a plain address.
//!
//! What a handler may do is bounded on the host. The envelope, the bytes and
//! the `Message-ID` live in [`Event`], which the event's context holds; the
//! JavaScript side names an action and never supplies the original.

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, OnceLock};

use bytes::Bytes;
use serde_json::Value;

use super::raw::{normalise_line_ends, parse_headers, split_head, Header};
use super::{
    new_message_id, prepare_builder, prepare_raw, too_large, valid_address, EmailError, Prepared,
    SendEmailBinding, MAX_HEADER_SECTION_BYTES, MAX_LINE_BYTES, MAX_MESSAGE_BYTES,
    MAX_RESTRICTION_ADDRESSES,
};

/// The most headers an inbound message may carry.
pub const MAX_HEADERS: usize = 1000;
/// The most `X-` headers one `forward()` may add.
pub const MAX_FORWARD_HEADERS: usize = 20;
/// RFC 5322 `References` entries past which a reply is refused, so two
/// auto-responders cannot keep each other going.
pub const MAX_REFERENCES: usize = 100;
/// The most characters of a `setReject()` reason that reach the relay.
pub const MAX_REJECT_REASON_CHARS: usize = 200;
/// Messages the node holds at once, each on the host and again in the
/// isolate. A relay past this is told to retry.
pub const MAX_CONCURRENT: usize = 4;
/// The variable that sets how many seconds the route waits to read a message
/// and, separately, for its handler.
pub const DEADLINE_ENV: &str = "CELLD_EMAIL_INBOUND_DEADLINE_S";
/// The variable that lists the destinations `forward()` may send to.
pub const FORWARD_ALLOW_ENV: &str = "CELLD_EMAIL_FORWARD_ALLOW";

/// How long the route waits to read a message, and then for its handler,
/// before it answers 408 or 504: 60 seconds unless [`DEADLINE_ENV`] says so.
pub fn deadline() -> std::time::Duration {
    static DEADLINE: OnceLock<std::time::Duration> = OnceLock::new();
    *DEADLINE.get_or_init(|| {
        std::time::Duration::from_secs(
            crate::env_vars::positive_or(DEADLINE_ENV, 60).expect("validated at start"),
        )
    })
}

/// The destinations `forward()` may send to: whole addresses, compared exactly
/// as written like the lists of a binding, and `@domain` entries, which match
/// that domain alone, not its subdomains, and ignore case.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct ForwardAllow {
    addresses: Vec<String>,
    domains: Vec<String>,
}

impl ForwardAllow {
    /// A comma-separated list. A blank list allows nothing; an empty entry or
    /// an entry that is neither an address nor `@domain` is an error.
    pub fn parse(text: &str) -> Result<Self, String> {
        let mut allow = Self::default();
        if text.trim().is_empty() {
            return Ok(allow);
        }
        for entry in text.split(',').map(str::trim) {
            if allow.addresses.len() + allow.domains.len() >= MAX_RESTRICTION_ADDRESSES {
                return Err(format!("has more than {MAX_RESTRICTION_ADDRESSES} entries"));
            }
            match entry.strip_prefix('@') {
                Some(domain) if valid_address(&format!("a@{domain}")) => {
                    allow.domains.push(domain.to_ascii_lowercase());
                }
                None if valid_address(entry) => allow.addresses.push(entry.to_string()),
                _ => return Err(format!("has {entry:?}, which is not an address or @domain")),
            }
        }
        Ok(allow)
    }

    pub fn allows(&self, address: &str) -> bool {
        self.addresses.iter().any(|allowed| allowed == address)
            || address.rsplit_once('@').is_some_and(|(_, domain)| {
                self.domains
                    .iter()
                    .any(|allowed| allowed.eq_ignore_ascii_case(domain))
            })
    }
}

fn load_forward_allow() -> anyhow::Result<ForwardAllow> {
    match crate::env_vars::value(FORWARD_ALLOW_ENV)? {
        None => Ok(ForwardAllow::default()),
        Some(text) => ForwardAllow::parse(&text)
            .map_err(|error| anyhow::anyhow!("{FORWARD_ALLOW_ENV} {error}")),
    }
}

/// The node's forward list. It is checked at start, so one that cannot be
/// read here allows nothing.
pub fn forward_allow() -> &'static ForwardAllow {
    static ALLOW: OnceLock<ForwardAllow> = OnceLock::new();
    ALLOW.get_or_init(|| load_forward_allow().unwrap_or_default())
}

/// Check the node's inbound mail settings, for startup.
pub fn validate() -> anyhow::Result<()> {
    crate::env_vars::positive::<u64>(DEADLINE_ENV)?;
    load_forward_allow().map(drop)
}

/// A refusal of a posted message, with the HTTP status the relay is told.
#[derive(Debug, PartialEq, Eq)]
pub struct InboundError {
    pub status: u16,
    pub message: String,
}

impl InboundError {
    fn new(status: u16, message: impl Into<String>) -> Self {
        Self {
            status,
            message: message.into(),
        }
    }
}

/// A message that was posted, parsed.
#[derive(Debug)]
pub struct Inbound {
    /// The envelope sender; empty for a bounce (`MAIL FROM:<>`).
    pub from: String,
    pub to: String,
    pub raw: Bytes,
    /// Every header, in order, with its folds removed.
    pub headers: Vec<(String, String)>,
    /// The `Message-ID` header, with its angle brackets.
    pub message_id: Option<String>,
    /// The entries of every `References` header.
    pub references: Vec<String>,
    /// The place among [`MAX_CONCURRENT`] this message holds. It moves to the
    /// [`Event`], so it is kept until the handler's event has ended.
    pub permit: Option<Permit>,
    /// Set when the route gives up on the handler, so what it does from then
    /// on is refused.
    pub cancelled: Arc<AtomicBool>,
    /// What the event's `forward()` and `reply()` sends have done so far.
    pub sends: Arc<Sends>,
}

/// The sends of one event: whether the relay has accepted one, and how many
/// are still talking to it. The route reads both at the deadline.
#[derive(Debug, Default)]
pub struct Sends {
    /// Set once a `forward()` or `reply()` has been accepted by the relay.
    delivered: AtomicBool,
    active: AtomicUsize,
    idle: tokio::sync::Notify,
}

impl Sends {
    /// Wait for the sends in flight to end. Each is bounded by its own SMTP
    /// transaction deadline.
    async fn settle(&self) {
        loop {
            let idle = self.idle.notified();
            tokio::pin!(idle);
            idle.as_mut().enable();
            if self.active.load(Ordering::SeqCst) == 0 {
                return;
            }
            idle.await;
        }
    }
}

/// One send in flight: counted from the call until it ends or is dropped.
#[derive(Debug)]
pub struct SendGuard(Arc<Sends>);

impl SendGuard {
    /// The relay accepted the message.
    pub fn delivered(&self) {
        self.0.delivered.store(true, Ordering::Release);
    }
}

impl Drop for SendGuard {
    fn drop(&mut self) {
        if self.0.active.fetch_sub(1, Ordering::SeqCst) == 1 {
            self.0.idle.notify_waiters();
        }
    }
}

/// Parse a posted message. `from` and `to` are the envelope as the relay gave
/// it, and both are required: the recipient decides which domain a reply may be
/// sent from, so it is never read from a header the sender wrote.
pub fn parse(
    raw: impl Into<Bytes>,
    from: Option<&str>,
    to: Option<&str>,
) -> Result<Inbound, InboundError> {
    let raw = raw.into();
    if raw.is_empty() {
        return Err(InboundError::new(400, "the message is empty"));
    }
    if raw.len() > MAX_MESSAGE_BYTES {
        return Err(InboundError::new(
            413,
            format!("the message exceeds {MAX_MESSAGE_BYTES} bytes"),
        ));
    }
    if raw.contains(&0) {
        return Err(InboundError::new(400, "the message holds a NUL byte"));
    }
    // Only the start is normalised and parsed: a header section is small, and
    // a message with no end to it inside this window has headers that are too
    // large.
    let window = &raw[..raw.len().min(2 * MAX_HEADER_SECTION_BYTES)];
    let normalised = normalise_line_ends(window);
    let (head, _) = split_head(&normalised);
    let ended = normalised.starts_with(b"\r\n")
        || normalised.windows(4).any(|part| part == b"\r\n\r\n")
        || window.len() == raw.len();
    if head.len() > MAX_HEADER_SECTION_BYTES || !ended {
        return Err(InboundError::new(
            431,
            format!("the message headers exceed {MAX_HEADER_SECTION_BYTES} bytes"),
        ));
    }
    let parsed = parse_headers(head).map_err(|error| InboundError::new(400, error.message))?;
    if parsed.len() > MAX_HEADERS {
        return Err(InboundError::new(
            431,
            format!("the message has more than {MAX_HEADERS} headers"),
        ));
    }
    let find = |name: &str| parsed.iter().find(|header| header.name_is(name));
    let from = match from {
        Some("") => String::new(),
        Some(address) if valid_address(address) => address.to_string(),
        Some(_) => {
            return Err(InboundError::new(
                400,
                "`from` is not a valid email address",
            ))
        }
        None => {
            return Err(InboundError::new(
                400,
                "the envelope sender is missing: pass `from`",
            ))
        }
    };
    let to = match to {
        Some(address) if valid_address(address) => address.to_string(),
        Some(_) => return Err(InboundError::new(400, "`to` is not a valid email address")),
        None => {
            return Err(InboundError::new(
                400,
                "the envelope recipient is missing: pass `to`",
            ))
        }
    };
    let message_id = find("message-id")
        .map(|header| header.value.clone())
        .filter(|id| valid_message_id(id));
    let references = parsed
        .iter()
        .filter(|header| header.name_is("references"))
        .flat_map(|header| header.value.split_whitespace())
        .filter(|id| valid_message_id(id))
        .map(str::to_string)
        .collect();
    let headers = parsed
        .into_iter()
        .map(|Header { name, value, .. }| (name, value))
        .collect();
    Ok(Inbound {
        from,
        to,
        raw,
        headers,
        message_id,
        references,
        permit: None,
        cancelled: Arc::default(),
        sends: Arc::default(),
    })
}

/// `<id>`: printable ASCII with no space or second bracket, so it can be put
/// into a header as written. An id with other bytes (8-bit, or U+FFFD from a
/// lossy decode) cannot be threaded on, so it counts as absent.
fn valid_message_id(id: &str) -> bool {
    id.len() <= 512
        && id.len() > 2
        && id.starts_with('<')
        && id.ends_with('>')
        && !id[1..id.len() - 1].contains(|c: char| !c.is_ascii_graphic() || "<>".contains(c))
}

/// What the node keeps of one inbound message for the event that handles it.
/// Everything a handler may do with the message is decided from this.
pub struct Event {
    pub from: String,
    pub to: String,
    /// The one copy the host keeps, shared with the request that carried it.
    raw: Bytes,
    message_id: Option<String>,
    references: Vec<String>,
    /// Why a reply would be unwelcome, when the message is automatic mail.
    automatic: Option<&'static str>,
    replied: AtomicBool,
    cancelled: Arc<AtomicBool>,
    sends: Arc<Sends>,
    _permit: Option<Permit>,
}

/// Whether the headers or the envelope sender mark the message as one a
/// machine sent, which a reply could only bounce back and forth.
fn automatic(from: &str, headers: &[(String, String)]) -> Option<&'static str> {
    fn values<'a>(headers: &'a [(String, String)], name: &'a str) -> impl Iterator<Item = &'a str> {
        headers
            .iter()
            .filter(move |(key, _)| key.eq_ignore_ascii_case(name))
            .map(|(_, value)| value.trim())
    }
    let first_word = |value: &str| {
        value
            .split(|c: char| c == ';' || c.is_whitespace())
            .next()
            .unwrap_or("")
            .to_ascii_lowercase()
    };
    if values(headers, "auto-submitted").any(|value| first_word(value) != "no") {
        return Some("it is automatic mail (Auto-Submitted)");
    }
    if values(headers, "precedence")
        .any(|value| matches!(first_word(value).as_str(), "bulk" | "list" | "junk"))
    {
        return Some("it is bulk mail (Precedence)");
    }
    let local = from.rsplit_once('@').map_or(from, |(local, _)| local);
    if ["mailer-daemon", "postmaster"]
        .iter()
        .any(|name| local.eq_ignore_ascii_case(name))
    {
        return Some("it comes from a mail system");
    }
    None
}

impl Event {
    pub fn new(inbound: &mut Inbound) -> Self {
        Self {
            from: inbound.from.clone(),
            to: inbound.to.clone(),
            raw: inbound.raw.clone(),
            message_id: inbound.message_id.clone(),
            references: inbound.references.clone(),
            automatic: automatic(&inbound.from, &inbound.headers),
            replied: AtomicBool::new(false),
            cancelled: inbound.cancelled.clone(),
            sends: inbound.sends.clone(),
            _permit: inbound.permit.take(),
        }
    }

    /// Refuse what a handler tries once the route has given up on it.
    pub fn check_open(&self) -> Result<(), EmailError> {
        if self.cancelled.load(Ordering::SeqCst) {
            return Err(EmailError::new(
                "E_TIMEOUT",
                "the email event was cancelled because the handler did not finish in time",
            ));
        }
        Ok(())
    }

    /// Count a send from now until it ends, or refuse it once the route has
    /// given up. The count comes first and the flag is read after it, so a
    /// send is either refused or seen by the route when it waits for the sends
    /// in flight.
    pub fn track_send(&self) -> Result<SendGuard, EmailError> {
        self.sends.active.fetch_add(1, Ordering::SeqCst);
        let guard = SendGuard(self.sends.clone());
        self.check_open()?;
        Ok(guard)
    }

    /// The original message plus the `X-` headers the handler adds, for
    /// `rcpt_to`, which must be a destination `allow` lists. It goes out with
    /// the receiving address as its envelope sender, since the sender is
    /// someone else's and the relay would be sending as them; the `From:`
    /// header stays. The bytes are the received ones apart from CRLF line ends
    /// and the loss of `Bcc:`; `Received:`, `Message-ID` and any signature
    /// headers stay as they were, so a signature over them still holds. A
    /// message with no usable `Message-ID` gets one, so the id this returns is
    /// the one in the bytes.
    pub fn prepare_forward(
        &self,
        allow: &ForwardAllow,
        rcpt_to: &str,
        headers: &[(String, String)],
    ) -> Result<Prepared, EmailError> {
        if !valid_address(rcpt_to) {
            return Err(EmailError::new(
                "E_VALIDATION_ERROR",
                "the recipient is not a valid email address",
            ));
        }
        if !allow.allows(rcpt_to) {
            return Err(EmailError::new(
                "E_RECIPIENT_NOT_ALLOWED",
                "the recipient is not a destination the operator allows forwarding to",
            ));
        }
        let extra = forward_headers(headers)?;
        let message = normalise_line_ends(&self.raw);
        let (head, body) = split_head(&message);
        if head.len() > MAX_HEADER_SECTION_BYTES {
            return Err(EmailError::new(
                "E_HEADERS_TOO_LARGE",
                format!("the message headers exceed {MAX_HEADER_SECTION_BYTES} bytes"),
            ));
        }
        if body
            .split(|byte| *byte == b'\n')
            .any(|line| line.strip_suffix(b"\r").unwrap_or(line).len() > MAX_LINE_BYTES)
        {
            return Err(EmailError::new(
                "E_LINE_TOO_LONG",
                format!("a line of the message body exceeds {MAX_LINE_BYTES} bytes"),
            ));
        }
        let parsed = parse_headers(head)?;
        let message_id = self.message_id.as_deref().map_or_else(
            || new_message_id(&self.to),
            |id| id[1..id.len() - 1].to_string(),
        );
        let mut data = Vec::with_capacity(message.len() + extra.len() + 80);
        data.extend_from_slice(extra.as_bytes());
        if self.message_id.is_none() {
            data.extend_from_slice(format!("Message-ID: <{message_id}>\r\n").as_bytes());
        }
        for header in &parsed {
            let dropped = header.name_is("bcc")
                || header.name_is("resent-bcc")
                || (self.message_id.is_none() && header.name_is("message-id"));
            if !dropped {
                data.extend_from_slice(&head[header.start..header.end]);
            }
        }
        data.extend_from_slice(b"\r\n");
        data.extend_from_slice(body);
        if !data.ends_with(b"\r\n") {
            data.extend_from_slice(b"\r\n");
        }
        if data.len() > MAX_MESSAGE_BYTES {
            return Err(too_large());
        }
        Ok(Prepared::new(
            self.to.clone(),
            vec![rcpt_to.to_string()],
            data,
            message_id,
        ))
    }

    /// Whether the one reply this event may make is still to be made.
    pub fn check_reply_open(&self) -> Result<(), EmailError> {
        if self.replied.load(Ordering::Acquire) {
            return Err(EmailError::new(
                "E_VALIDATION_ERROR",
                "this message has been replied to already",
            ));
        }
        Ok(())
    }

    /// Take the one reply this event may make, once the send is admitted. A
    /// reply that is then refused or fails counts, so a handler cannot try one
    /// sender after another; one that was turned away for lack of a slot or a
    /// subrequest does not, as it can be made again.
    pub fn take_reply(&self) -> Result<(), EmailError> {
        if self.replied.swap(true, Ordering::AcqRel) {
            return self.check_reply_open();
        }
        Ok(())
    }

    /// The `In-Reply-To` and `References` values a reply to this message
    /// carries, or why there is no reply to make.
    fn threading(&self) -> Result<(String, String), EmailError> {
        let Some(id) = &self.message_id else {
            return Err(EmailError::new(
                "E_FIELD_MISSING",
                "the incoming message has no Message-ID, so a reply cannot be threaded",
            ));
        };
        if self.from.is_empty() {
            return Err(EmailError::new(
                "E_VALIDATION_ERROR",
                "the incoming message has no sender to reply to",
            ));
        }
        if let Some(why) = self.automatic {
            return Err(EmailError::new(
                "E_RECIPIENT_NOT_ALLOWED",
                format!("a reply is not sent because {why}"),
            ));
        }
        if self.references.len() > MAX_REFERENCES {
            return Err(EmailError::new(
                "E_VALIDATION_ERROR",
                format!("the incoming message has more than {MAX_REFERENCES} References"),
            ));
        }
        let mut references = self.references.clone();
        references.push(id.clone());
        Ok((id.clone(), references.join(" ")))
    }

    /// The domain a reply must be sent from: the one that received the message.
    fn check_reply_sender(&self, from: &str) -> Result<(), EmailError> {
        let domain = |address: &str| {
            address
                .rsplit_once('@')
                .map(|(_, domain)| domain.to_ascii_lowercase())
        };
        if domain(from).is_none() || domain(from) != domain(&self.to) {
            return Err(EmailError::new(
                "E_SENDER_NOT_VERIFIED",
                "the reply must be sent from the domain that received the message",
            ));
        }
        Ok(())
    }

    /// A reply made from a finished message. Its recipient must be the sender
    /// of the original; `In-Reply-To` and `References` are added, and a value
    /// the message carries must be the one this computes.
    pub fn prepare_reply_raw(
        &self,
        from: &str,
        to: &str,
        raw: &[u8],
    ) -> Result<Prepared, EmailError> {
        let (in_reply_to, full) = self.threading()?;
        let references = trim_references(&full);
        if to != self.from {
            return Err(EmailError::new(
                "E_RECIPIENT_NOT_ALLOWED",
                "the reply must be addressed to the sender of the incoming message",
            ));
        }
        self.check_reply_sender(from)?;
        if raw.len() > MAX_MESSAGE_BYTES {
            return Err(too_large());
        }
        let message = normalise_line_ends(raw);
        let (head, body) = split_head(&message);
        let parsed = parse_headers(head)?;
        let mut data = Vec::with_capacity(message.len() + references.len() + 64);
        for (name, wanted) in [("In-Reply-To", &in_reply_to), ("References", &references)] {
            let mut present = false;
            for header in parsed.iter().filter(|header| header.name_is(name)) {
                present = true;
                let tokens = header.value.split_whitespace();
                let full = if name == "References" { &full } else { wanted };
                if !tokens.clone().eq(wanted.split_whitespace())
                    && !tokens.eq(full.split_whitespace())
                {
                    return Err(EmailError::new(
                        "E_VALIDATION_ERROR",
                        format!("{name}: is not the value for this reply"),
                    ));
                }
            }
            if !present {
                data.extend_from_slice(fold_header(name, wanted).as_bytes());
            }
        }
        if !parsed.iter().any(|header| header.name_is("auto-submitted")) {
            data.extend_from_slice(AUTO_REPLIED.as_bytes());
        }
        data.extend_from_slice(head);
        data.extend_from_slice(b"\r\n");
        data.extend_from_slice(body);
        prepare_raw(&SendEmailBinding::default(), from, to, &data)
    }

    /// A reply made from fields. It goes to the sender of the original, so the
    /// fields have no `to`, `cc` or `bcc`.
    pub fn prepare_reply_builder(&self, builder: Value) -> Result<Prepared, EmailError> {
        let invalid = |message: &str| EmailError::new("E_VALIDATION_ERROR", message);
        let (in_reply_to, references) = self.threading()?;
        let Value::Object(mut fields) = builder else {
            return Err(invalid("the reply is not an object"));
        };
        if ["to", "cc", "bcc"]
            .iter()
            .any(|key| fields.contains_key(*key))
        {
            return Err(invalid(
                "a reply goes to the sender: it has no `to`, `cc` or `bcc`",
            ));
        }
        let from = match fields.get("from") {
            Some(Value::String(address)) => Some(address.as_str()),
            Some(Value::Object(named)) => named.get("email").and_then(Value::as_str),
            _ => None,
        };
        let Some(from) = from else {
            return Err(EmailError::new("E_FIELD_MISSING", "`from` is required"));
        };
        self.check_reply_sender(from)?;
        let mut headers = match fields.remove("headers") {
            None | Some(Value::Null) => serde_json::Map::new(),
            Some(Value::Object(headers)) => headers,
            Some(_) => return Err(invalid("`headers` is not an object")),
        };
        for (name, wanted) in [("In-Reply-To", &in_reply_to), ("References", &references)] {
            let found: Vec<_> = headers
                .iter()
                .filter(|(key, _)| key.eq_ignore_ascii_case(name))
                .map(|(key, value)| (key.clone(), value.clone()))
                .collect();
            let trimmed = if name == "References" {
                trim_references(wanted)
            } else {
                wanted.clone()
            };
            for (key, value) in found {
                let same = value.as_str().is_some_and(|value| {
                    value.split_whitespace().eq(wanted.split_whitespace())
                        || value.split_whitespace().eq(trimmed.split_whitespace())
                });
                if !same {
                    return Err(invalid(&format!("{name}: is not the value for this reply")));
                }
                headers.remove(&key);
            }
            headers.insert(name.to_string(), Value::String(trimmed));
        }
        if !headers
            .keys()
            .any(|key| key.eq_ignore_ascii_case("auto-submitted"))
        {
            headers.insert(
                "Auto-Submitted".into(),
                Value::String("auto-replied".into()),
            );
        }
        fields.insert("headers".into(), Value::Object(headers));
        fields.insert("to".into(), Value::String(self.from.clone()));
        prepare_builder(&SendEmailBinding::default(), Value::Object(fields))
    }
}

/// Marks a reply as one a machine made (RFC 3834), so a handler that answers
/// another handler's reply is refused by [`automatic`] instead of looping.
const AUTO_REPLIED: &str = "Auto-Submitted: auto-replied\r\n";

/// A `References` value that fits the one a builder header may hold: the
/// first id and the newest ones after it, as RFC 5322 section 3.6.4 advises.
fn trim_references(references: &str) -> String {
    const LIMIT: usize = 2000;
    if references.len() <= LIMIT {
        return references.to_string();
    }
    let ids: Vec<&str> = references.split_whitespace().collect();
    let mut kept = vec![ids[0]];
    let mut size = ids[0].len();
    let mut tail = Vec::new();
    for id in ids[1..].iter().rev() {
        if size + id.len() + 1 > LIMIT {
            break;
        }
        size += id.len() + 1;
        tail.push(*id);
    }
    kept.extend(tail.into_iter().rev());
    kept.join(" ")
}

/// `name: value` folded so no line passes the limit, with its closing CRLF.
/// The value is a list of ids, so it folds at a space.
fn fold_header(name: &str, value: &str) -> String {
    let mut out = format!("{name}:");
    let mut line = out.len();
    for part in value.split_whitespace() {
        if line + 1 + part.len() > 900 {
            out.push_str("\r\n");
            line = 0;
        }
        out.push(' ');
        out.push_str(part);
        line += 1 + part.len();
    }
    out.push_str("\r\n");
    out
}

/// The `X-` headers a handler gave `forward()`, as folded lines. Any other header is
/// dropped, as the relay of the Worker's platform drops it; one that is not
/// plain ASCII, or holds a line break, is refused.
fn forward_headers(headers: &[(String, String)]) -> Result<String, EmailError> {
    let mut out = String::new();
    let mut count = 0;
    for (name, value) in headers {
        if !name
            .get(..2)
            .is_some_and(|prefix| prefix.eq_ignore_ascii_case("x-"))
        {
            continue;
        }
        super::mime::check_header(name, value, false)?;
        count += 1;
        if count > MAX_FORWARD_HEADERS {
            return Err(EmailError::new(
                "E_HEADERS_TOO_MANY",
                format!("at most {MAX_FORWARD_HEADERS} headers may be added"),
            ));
        }
        out.push_str(&super::mime::fold_header(name, value));
    }
    // A value is folded where it has white space; one with none (a long token)
    // stays one line, and a strict relay would reject it.
    if out.split("\r\n").any(|line| line.len() > MAX_LINE_BYTES) {
        return Err(EmailError::new(
            "E_LINE_TOO_LONG",
            format!("an added header line exceeds {MAX_LINE_BYTES} bytes and has no place to fold"),
        ));
    }
    Ok(out)
}

/// What a handler did with a message.
#[derive(Debug, PartialEq, Eq)]
pub enum Outcome {
    Accepted,
    Rejected(String),
    NoHandler,
    Failed(String),
}

/// The reason of a `setReject()` as the relay may paste it into one SMTP
/// reply line: printable ASCII, bounded.
pub fn reject_reason(reason: &str) -> String {
    let cleaned: String = reason
        .chars()
        .map(|c| {
            if c.is_ascii() && !c.is_ascii_control() {
                c
            } else {
                ' '
            }
        })
        .take(MAX_REJECT_REASON_CHARS)
        .collect();
    let cleaned = cleaned.trim();
    if cleaned.is_empty() {
        "rejected".to_string()
    } else {
        cleaned.to_string()
    }
}

/// Read the outcome the dispatcher answered with.
pub fn read_outcome(bytes: &[u8]) -> Outcome {
    #[derive(serde::Deserialize)]
    struct Answer {
        outcome: String,
        #[serde(default)]
        reason: Option<String>,
        #[serde(default)]
        error: Option<String>,
    }
    let Ok(answer) = serde_json::from_slice::<Answer>(bytes) else {
        return Outcome::Failed("the email handler gave no result".into());
    };
    match answer.outcome.as_str() {
        "accepted" => Outcome::Accepted,
        "rejected" => Outcome::Rejected(reject_reason(answer.reason.as_deref().unwrap_or(""))),
        "no_handler" => Outcome::NoHandler,
        _ => Outcome::Failed(
            answer
                .error
                .unwrap_or_else(|| "the email handler failed".into())
                .chars()
                .filter(|c| !c.is_control())
                .take(500)
                .collect(),
        ),
    }
}

/// The status and JSON body of the route's answer for `outcome`.
///
/// 200 the message was accepted. 550 the handler called `setReject()`: the
/// relay should bounce with `reason`. 501 the Worker has no `email` handler.
/// 500 the handler threw: a temporary failure, so the relay retries. The body
/// does not carry what was thrown.
pub fn answer(outcome: Outcome) -> (u16, String) {
    match outcome {
        Outcome::Accepted => (200, r#"{"ok":true,"outcome":"accepted"}"#.to_string()),
        Outcome::Rejected(reason) => (
            550,
            serde_json::json!({ "ok": false, "outcome": "rejected", "reason": reason }).to_string(),
        ),
        Outcome::NoHandler => (
            501,
            serde_json::json!({
                "ok": false,
                "outcome": "no_handler",
                "error": "the Worker has no email() handler",
            })
            .to_string(),
        ),
        // The text of what was thrown stays in the log: the poster holds only
        // the route's token, and a Worker's error may carry a secret.
        Outcome::Failed(_) => (
            500,
            serde_json::json!({
                "ok": false,
                "outcome": "error",
                "error": "the email handler threw",
            })
            .to_string(),
        ),
    }
}

static IN_FLIGHT: AtomicUsize = AtomicUsize::new(0);

/// A place among the messages the node handles at once.
#[derive(Debug)]
pub struct Permit(());

impl Drop for Permit {
    fn drop(&mut self) {
        IN_FLIGHT.fetch_sub(1, Ordering::AcqRel);
    }
}

/// Take a place, or `None` when [`MAX_CONCURRENT`] are held.
pub fn admit() -> Option<Permit> {
    if IN_FLIGHT.fetch_add(1, Ordering::AcqRel) >= MAX_CONCURRENT {
        IN_FLIGHT.fetch_sub(1, Ordering::AcqRel);
        return None;
    }
    Some(Permit(()))
}

/// Ends an event the route stops waiting for, at the deadline or because the
/// relay hung up: the event is aborted, and what a handler that outlives the
/// abort through `waitUntil` still does is refused.
struct GiveUp {
    cancelled: Option<Arc<AtomicBool>>,
    _abort: crate::runtime::AbortOnDrop,
}

impl GiveUp {
    fn disarm(&mut self) {
        self.cancelled = None;
        self._abort.disarm();
    }
}

impl Drop for GiveUp {
    fn drop(&mut self) {
        if let Some(cancelled) = &self.cancelled {
            cancelled.store(true, Ordering::SeqCst);
        }
    }
}

/// The route: parse a posted message, run it through the primary Worker's
/// `email()` handler and map what it did to a status and body. A handler that
/// has not finished at the [`deadline`] is cancelled, as one whose client hung
/// up is, so it gives its place back and what it still tries is refused.
pub async fn handle(
    runtime: &crate::runtime::RuntimeManager,
    query: Option<&str>,
    body: Bytes,
    permit: Permit,
) -> (u16, String) {
    let (mut from, mut to) = (None, None);
    for (key, value) in url::form_urlencoded::parse(query.unwrap_or("").as_bytes()) {
        match key.as_ref() {
            "from" => from = Some(value.into_owned()),
            "to" => to = Some(value.into_owned()),
            _ => {}
        }
    }
    let inbound = match parse(body, from.as_deref(), to.as_deref()) {
        Ok(mut inbound) => {
            inbound.permit = Some(permit);
            inbound
        }
        Err(error) => {
            return (
                error.status,
                serde_json::json!({ "ok": false, "error": error.message }).to_string(),
            )
        }
    };
    let sends = inbound.sends.clone();
    let cancelled = inbound.cancelled.clone();
    let message_id = inbound.message_id.clone();
    let cancellation = crate::runtime::RequestCancellationLifetime::stateless();
    let mut giveup = GiveUp {
        cancelled: Some(inbound.cancelled.clone()),
        _abort: crate::runtime::AbortOnDrop::armed(cancellation.clone()),
    };
    let ran =
        crate::asyncrt::timeout(deadline(), runtime.email_worker(inbound, cancellation)).await;
    let outcome = match ran {
        Ok(Ok(bytes)) => {
            let outcome = read_outcome(&bytes);
            if let Outcome::Failed(detail) = &outcome {
                tracing::warn!(
                    event = "inbound_email_failed",
                    message_id = message_id.as_deref().unwrap_or(""),
                    %detail,
                );
            }
            outcome
        }
        Ok(Err(error)) => {
            tracing::warn!(
                event = "inbound_email_failed",
                message_id = message_id.as_deref().unwrap_or(""),
                %error,
            );
            Outcome::Failed("the message could not be handled".into())
        }
        Err(_) => {
            // No new send starts from here. One already talking to the relay
            // may still be accepted, so it is waited for before the answer is
            // chosen; the rest of the event is cancelled when `giveup` drops.
            cancelled.store(true, Ordering::SeqCst);
            sends.settle().await;
            tracing::warn!(
                event = "inbound_email_timeout",
                message_id = message_id.as_deref().unwrap_or(""),
            );
            return after_delivery(
                (
                    504,
                    r#"{"ok":false,"outcome":"error","error":"the email handler did not finish in time"}"#
                        .to_string(),
                ),
                &sends.delivered,
                message_id.as_deref(),
            );
        }
    };
    // The handler is done: what it left in `waitUntil` is not cancelled.
    giveup.disarm();
    let answered = answer(outcome);
    // A handler that failed may have left a send in `waitUntil` talking to the
    // relay; it is waited for, since a 500 makes the relay post the message
    // again. A success is not held back for it: that 200 stands.
    //
    // A 550 is final for new sends: none starts from here, and one already
    // talking to the relay is waited for, since a delivery before the reject
    // must not be bounced.
    match answered.0 {
        550 => {
            cancelled.store(true, Ordering::SeqCst);
            sends.settle().await;
        }
        500 => sends.settle().await,
        _ => {}
    }
    after_delivery(answered, &sends.delivered, message_id.as_deref())
}

/// A handler that failed, timed out or called `setReject()` after the relay accepted a message it
/// forwarded or replied to (a send still in flight when the handler failed or
/// the deadline passed counts once the relay has answered it) has still delivered it, and a retry would send it
/// again: the route answers 200 and logs the failure.
fn after_delivery(
    answered: (u16, String),
    delivered: &AtomicBool,
    message_id: Option<&str>,
) -> (u16, String) {
    if !matches!(answered.0, 500 | 504 | 550) || !delivered.load(Ordering::Acquire) {
        return answered;
    }
    tracing::warn!(
        event = if answered.0 == 550 {
            "inbound_email_rejected_after_delivery"
        } else {
            "inbound_email_failed_after_delivery"
        },
        message_id = message_id.unwrap_or(""),
        status = answered.0,
        body = %answered.1,
    );
    answer(Outcome::Accepted)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The tests that count places share one counter.
    static PLACES: std::sync::Mutex<()> = std::sync::Mutex::new(());

    const MESSAGE: &str = "Received: from mx by node\r\nFrom: Ann <ann@example.org>\r\nTo: bob@example.com\r\nSubject: hi\r\nMessage-ID: <m1@example.org>\r\nReferences: <a@x> <b@x>\r\nBcc: spy@example.net\r\n\r\nHello\r\n.dot\r\n";

    fn allow() -> ForwardAllow {
        ForwardAllow::parse("team@example.net, a@b.co, @partner.example").unwrap()
    }

    fn parsed(text: &str) -> Inbound {
        parse(
            text.as_bytes().to_vec(),
            Some("ann@example.org"),
            Some("bob@example.com"),
        )
        .unwrap()
    }

    #[test]
    fn a_message_is_parsed_and_its_envelope_comes_from_the_headers_when_not_given() {
        let inbound = parsed(MESSAGE);
        assert_eq!(inbound.from, "ann@example.org");
        assert_eq!(inbound.to, "bob@example.com");
        assert_eq!(inbound.message_id.as_deref(), Some("<m1@example.org>"));
        assert_eq!(inbound.references, ["<a@x>", "<b@x>"]);
        assert_eq!(inbound.headers[0].0, "Received");
        assert_eq!(inbound.raw, MESSAGE.as_bytes());
        let given = parse(
            MESSAGE.as_bytes().to_vec(),
            Some("bounce@example.org"),
            Some("carol@example.com"),
        )
        .unwrap();
        assert_eq!(
            (given.from.as_str(), given.to.as_str()),
            ("bounce@example.org", "carol@example.com")
        );
        let null = parse(
            MESSAGE.as_bytes().to_vec(),
            Some(""),
            Some("bob@example.com"),
        )
        .unwrap();
        assert_eq!(null.from, "");
    }

    #[test]
    fn the_envelope_is_required_and_never_read_from_a_header() {
        let forged = b"From: a@b.co\r\nTo: victim@forged.example\r\n\r\nx".to_vec();
        for (from, to) in [(None, Some("b@c.co")), (Some("a@b.co"), None), (None, None)] {
            let error = parse(forged.clone(), from, to).unwrap_err();
            assert_eq!(error.status, 400, "{from:?} {to:?}");
        }
        // The recipient is the relay's, so a reply's sender domain follows it.
        let event =
            Event::new(&mut parse(forged, Some("a@b.co"), Some("real@example.com")).unwrap());
        assert_eq!(event.to, "real@example.com");
    }

    #[test]
    fn a_message_holds_its_place_until_its_event_is_gone() {
        let _places = PLACES.lock().unwrap_or_else(|e| e.into_inner());
        let permit = admit().unwrap();
        let mut inbound = parsed(MESSAGE);
        inbound.permit = Some(permit);
        let event = Event::new(&mut inbound);
        drop(inbound);
        // The handler is still running: the event, and so the place, is held.
        let others: Vec<_> = std::iter::from_fn(admit).take(MAX_CONCURRENT).collect();
        assert_eq!(others.len(), MAX_CONCURRENT - 1);
        drop(others);
        drop(event);
        let all: Vec<_> = std::iter::from_fn(admit).take(MAX_CONCURRENT + 1).collect();
        assert_eq!(all.len(), MAX_CONCURRENT);
    }

    #[test]
    fn hostile_input_is_refused_with_a_status_and_never_panics() {
        let status = |raw: &[u8], from: Option<&str>| {
            parse(raw.to_vec(), from, Some("b@example.com"))
                .unwrap_err()
                .status
        };
        assert_eq!(status(b"", None), 400);
        assert_eq!(status(b"From: a@b.co\r\n\x00\r\n", None), 400);
        assert_eq!(status(b"no colon here\r\n\r\nbody", Some("a@b.co")), 400);
        assert_eq!(status(b" folded first\r\n\r\n", Some("a@b.co")), 400);
        assert_eq!(status(b"Bad Name: x\r\n\r\n", Some("a@b.co")), 400);
        assert_eq!(status(b"Subject: x\r\n\r\n", None), 400);
        assert_eq!(status(b"Subject: x\r\n\r\n", Some("not an address")), 400);
        assert_eq!(
            status(b"From: a@b.co\r\n\r\nx", Some("a@b.co\r\nRCPT TO:<x@y.z>")),
            400
        );
        assert_eq!(
            status(&vec![b'a'; MAX_MESSAGE_BYTES + 1], Some("a@b.co")),
            413
        );
        let long = format!("X: {}\r\n\r\n", "a".repeat(2000));
        assert_eq!(status(long.as_bytes(), Some("a@b.co")), 400);
        // A header section with no end inside its limit.
        let endless = format!("X: a\r\n{}", "Y: b\r\n".repeat(40_000));
        assert_eq!(status(endless.as_bytes(), Some("a@b.co")), 431);
        let many = format!("{}\r\nbody", "Y: b\r\n".repeat(MAX_HEADERS + 1));
        assert_eq!(status(many.as_bytes(), Some("a@b.co")), 431);
        // Bytes that are not UTF-8, in a header and next to a multi-byte
        // character at the edge of the window, are read as text with losses.
        let mut bytes = b"Subject: caf\xe9 \xff\xfe\r\nFrom: a@b.co\r\n\r\n".to_vec();
        bytes.extend("é".repeat(MAX_HEADER_SECTION_BYTES).bytes());
        let inbound = parse(bytes, Some("a@b.co"), Some("b@example.com")).unwrap();
        assert_eq!(inbound.from, "a@b.co");
    }

    #[test]
    fn an_added_header_is_folded_so_no_line_passes_the_limit() {
        let event = Event::new(&mut parsed(MESSAGE));
        let forward = |value: String| {
            event.prepare_forward(&allow(), "team@example.net", &[("X-Note".into(), value)])
        };
        // 2048 bytes of words: folded, and unfolding gives the value back.
        let words = vec!["word"; 400].join(" ");
        let sent = forward(words.clone()).unwrap();
        let text = String::from_utf8(sent.data).unwrap();
        assert!(text.split("\r\n").all(|line| line.len() <= MAX_LINE_BYTES));
        let head = text.split("\r\n\r\n").next().unwrap();
        let unfolded = head.replace("\r\n ", " ");
        assert!(unfolded.contains(&format!("X-Note: {words}\r\n")));
        // One token that cannot fold is refused.
        let error = forward("a".repeat(2048)).unwrap_err();
        assert_eq!(error.code, "E_LINE_TOO_LONG");
    }

    #[test]
    fn a_forward_keeps_the_message_and_its_message_id_and_drops_bcc() {
        let event = Event::new(&mut parsed(MESSAGE.replace("\r\n", "\n").as_str()));
        let sent = event
            .prepare_forward(
                &allow(),
                "team@example.net",
                &[
                    ("X-Processed-By".into(), "worker".into()),
                    ("Subject".into(), "dropped".into()),
                ],
            )
            .unwrap();
        let text = String::from_utf8(sent.data.clone()).unwrap();
        assert!(
            text.starts_with("X-Processed-By: worker\r\nReceived: from mx by node\r\n"),
            "{text}"
        );
        assert!(text.contains("Message-ID: <m1@example.org>\r\n"));
        assert!(!text.contains("Bcc") && !text.contains("dropped"));
        assert!(text.ends_with("\r\n\r\nHello\r\n.dot\r\n"), "{text:?}");
        // The receiving address is the envelope sender; From: stays the sender's.
        assert_eq!(sent.from, "bob@example.com");
        assert!(text.contains("From: Ann <ann@example.org>\r\n"));
        assert_eq!(sent.recipients, ["team@example.net"]);
        assert_eq!(sent.message_id, "m1@example.org");
        assert!(event
            .prepare_forward(&allow(), "not an address", &[])
            .is_err());
        let code = |name: &str, value: &str| {
            event
                .prepare_forward(&allow(), "a@b.co", &[(name.into(), value.into())])
                .unwrap_err()
                .code
        };
        assert_eq!(code("X-A", "line\r\nBcc: x@y.z"), "E_HEADER_VALUE_INVALID");
        assert_eq!(code("X-A", "é"), "E_HEADER_VALUE_INVALID");
        assert_eq!(code("X-A", "a\u{1}b"), "E_HEADER_VALUE_INVALID");
        assert_eq!(code("X-A b", "x"), "E_HEADER_NAME_INVALID");
        assert_eq!(code("X-", "x"), "E_HEADER_NAME_INVALID");
        let many: Vec<_> = (0..=MAX_FORWARD_HEADERS)
            .map(|i| (format!("X-{i}"), "v".to_string()))
            .collect();
        assert_eq!(
            event
                .prepare_forward(&allow(), "a@b.co", &many)
                .unwrap_err()
                .code,
            "E_HEADERS_TOO_MANY"
        );
        // A line past the limit in the body is refused, not folded.
        let long = format!("From: a@b.co\r\n\r\n{}\r\n", "a".repeat(MAX_LINE_BYTES + 1));
        let event =
            Event::new(&mut parse(long.into_bytes(), Some("a@b.co"), Some("b@c.co")).unwrap());
        assert_eq!(
            event
                .prepare_forward(&allow(), "a@b.co", &[])
                .unwrap_err()
                .code,
            "E_LINE_TOO_LONG"
        );
    }

    #[test]
    fn a_forward_goes_only_to_a_listed_destination() {
        let event = Event::new(&mut parsed(MESSAGE));
        let code = |allow: &ForwardAllow, to: &str| {
            event
                .prepare_forward(allow, to, &[])
                .map(|_| "sent")
                .unwrap_or_else(|error| error.code)
        };
        let allow = allow();
        for listed in [
            "team@example.net",
            "a@b.co",
            "x@partner.example",
            "x@PARTNER.example",
        ] {
            assert_eq!(code(&allow, listed), "sent", "{listed}");
        }
        for other in [
            "eve@example.org",
            "Team@example.net",
            "x@sub.partner.example",
            "x@partner.example.evil.test",
        ] {
            assert_eq!(code(&allow, other), "E_RECIPIENT_NOT_ALLOWED", "{other}");
        }
        // With nothing set, nothing is allowed.
        let none = ForwardAllow::default();
        assert_eq!(code(&none, "team@example.net"), "E_RECIPIENT_NOT_ALLOWED");
        assert_eq!(ForwardAllow::parse("  ").unwrap(), none);
        for bad in [
            "a@b.co,",
            ",a@b.co",
            "a@b.co,,c@d.ee",
            "@",
            "@bad domain",
            "not an address",
            "Name <a@b.co>",
        ] {
            assert!(ForwardAllow::parse(bad).is_err(), "{bad:?}");
        }
    }

    #[test]
    fn a_forward_keeps_the_8_bit_headers_the_message_arrived_with() {
        let bytes = b"From: ann@example.org\r\nSubject: caf\xe9 \xff\r\n\r\nbody\r\n".to_vec();
        let event = Event::new(
            &mut parse(bytes, Some("ann@example.org"), Some("bob@example.com")).unwrap(),
        );
        let sent = event.prepare_forward(&allow(), "a@b.co", &[]).unwrap();
        assert!(
            sent.data.windows(7).any(|w| w == b"caf\xe9 \xff\r"),
            "{:?}",
            sent.data
        );
        assert!(sent.smtputf8 && sent.eight_bit);
    }

    #[test]
    fn a_forward_without_a_message_id_carries_the_id_it_reports() {
        let event = Event::new(&mut parsed(
            "From: ann@example.org\r\nMessage-ID: <bad id@x>\r\nSubject: s\r\n\r\nbody\r\n",
        ));
        let sent = event.prepare_forward(&allow(), "a@b.co", &[]).unwrap();
        let text = String::from_utf8(sent.data).unwrap();
        assert!(
            text.contains(&format!("Message-ID: <{}>\r\n", sent.message_id)),
            "{text}"
        );
        assert!(sent.message_id.ends_with("@example.com"));
        assert_eq!(text.matches("Message-ID").count(), 1, "{text}");
        assert!(text.contains("Subject: s\r\n"));
    }

    #[test]
    fn a_reply_is_not_sent_to_automatic_mail_or_a_mail_system() {
        let reply = |headers: &str, from: &str| {
            let raw = format!(
                "From: {from}\r\nTo: bob@example.com\r\nMessage-ID: <m@x>\r\n{headers}\r\nx"
            );
            let event = Event::new(&mut parse(raw, Some(from), Some("bob@example.com")).unwrap());
            event
                .prepare_reply_builder(
                    serde_json::json!({ "from": "bob@example.com", "subject": "s", "text": "t" }),
                )
                .map(|_| "sent")
                .unwrap_or_else(|error| error.code)
        };
        crate::asyncrt::install_test_host_runtime();
        for refused in [
            "Auto-Submitted: auto-replied\r\n",
            "auto-submitted: auto-generated; x=y\r\n",
            "Precedence: bulk\r\n",
            "precedence: List\r\n",
            "Precedence: junk\r\n",
            "Auto-Submitted: no\r\nAuto-Submitted: auto-notified\r\n",
        ] {
            assert_eq!(
                reply(refused, "ann@example.org"),
                "E_RECIPIENT_NOT_ALLOWED",
                "{refused:?}"
            );
        }
        for from in ["MAILER-DAEMON@example.org", "postmaster@example.org"] {
            assert_eq!(reply("", from), "E_RECIPIENT_NOT_ALLOWED", "{from}");
        }
        for fine in ["Auto-Submitted: no\r\n", "Precedence: first-class\r\n", ""] {
            assert_eq!(reply(fine, "ann@example.org"), "sent", "{fine:?}");
        }
    }

    #[test]
    fn what_a_handler_does_after_the_route_gave_up_is_refused() {
        let mut inbound = parsed(MESSAGE);
        let cancelled = inbound.cancelled.clone();
        let event = Event::new(&mut inbound);
        event.check_open().unwrap();
        cancelled.store(true, Ordering::Release);
        assert_eq!(event.check_open().unwrap_err().code, "E_TIMEOUT");
    }

    #[test]
    fn the_event_shares_the_posted_bytes_instead_of_copying_them() {
        let body = Bytes::from(MESSAGE.as_bytes().to_vec());
        let mut inbound = parse(body.clone(), Some("a@b.co"), Some("b@c.co")).unwrap();
        assert_eq!(inbound.raw.as_ptr(), body.as_ptr());
        let event = Event::new(&mut inbound);
        assert_eq!(event.raw.as_ptr(), body.as_ptr());
    }

    #[test]
    fn references_that_are_not_message_ids_are_dropped() {
        let inbound = parsed(
            "From: ann@example.org\r\nTo: bob@example.com\r\nMessage-ID: <m@x>\r\nReferences: <a@x> \x01bad <b@x> junk <c<d@x>\r\n\r\nx",
        );
        assert_eq!(inbound.references, ["<a@x>", "<b@x>"]);
        let event = Event::new(&mut parsed(
            "From: ann@example.org\r\nTo: bob@example.com\r\nMessage-ID: <m@x>\r\nReferences: <a@x> \x01bad\r\n\r\nx",
        ));
        let (_, references) = event.threading().unwrap();
        assert_eq!(references, "<a@x> <m@x>");
    }

    #[test]
    fn a_message_id_or_reference_that_is_not_printable_ascii_is_not_threaded_on() {
        crate::asyncrt::install_test_host_runtime();
        let head = "From: ann@example.org\r\nTo: bob@example.com\r\n";
        let odd = parsed(&format!(
            "{head}Message-ID: <caf\u{e9}@x>\r\nReferences: <a@x> <\u{fffd}@x> <b@x>\r\n\r\nx"
        ));
        assert_eq!(odd.message_id, None);
        assert_eq!(odd.references, ["<a@x>", "<b@x>"]);
        let event = Event::new(&mut parsed(&format!(
            "{head}Message-ID: <m@x>\r\nReferences: <a@x> <\u{fffd}@x>\r\n\r\nx"
        )));
        let sent = event
            .prepare_reply_builder(
                serde_json::json!({ "from": "bob@example.com", "subject": "s", "text": "t" }),
            )
            .unwrap();
        let text = String::from_utf8(sent.data).unwrap();
        assert!(text.contains("In-Reply-To: <m@x>\r\n"), "{text}");
        assert!(text.contains("References: <a@x> <m@x>\r\n"), "{text}");
        // Without a usable Message-ID there is nothing to thread on.
        let event = Event::new(&mut parsed(&format!(
            "{head}Message-ID: <caf\u{e9}@x>\r\n\r\nx"
        )));
        let error = event
            .prepare_reply_builder(
                serde_json::json!({ "from": "bob@example.com", "subject": "s", "text": "t" }),
            )
            .unwrap_err();
        assert_eq!(error.code, "E_FIELD_MISSING");
    }

    #[test]
    fn a_builder_reply_to_a_long_thread_accepts_the_full_or_the_trimmed_chain() {
        crate::asyncrt::install_test_host_runtime();
        let ids: Vec<String> = (0..90).map(|i| format!("<{i:0>30}@example.org>")).collect();
        let event = Event::new(&mut parsed(&format!(
            "From: ann@example.org\r\nTo: bob@example.com\r\nMessage-ID: <m@x>\r\nReferences: {}\r\n\r\nx",
            ids.join("\r\n ")
        )));
        let (_, full) = event.threading().unwrap();
        let trimmed = trim_references(&full);
        assert!(full.len() > 2000 && trimmed.len() <= 2000 && trimmed != full);
        let reply = |references: &str| {
            event.prepare_reply_builder(serde_json::json!({
                "from": "bob@example.com", "subject": "s", "text": "t",
                "headers": { "References": references },
            }))
        };
        for accepted in [&full, &trimmed] {
            let text = String::from_utf8(reply(accepted).unwrap().data).unwrap();
            assert!(text.contains(&format!("References: {}", ids[0])), "{text}");
        }
        assert_eq!(reply("<other@x>").unwrap_err().code, "E_VALIDATION_ERROR");
    }

    #[test]
    fn a_rejected_message_that_was_delivered_is_accepted() {
        let delivered = AtomicBool::new(false);
        let rejected = || (550, "{}".to_string());
        assert_eq!(after_delivery(rejected(), &delivered, Some("<m@x>")).0, 550);
        delivered.store(true, Ordering::Release);
        assert_eq!(after_delivery(rejected(), &delivered, Some("<m@x>")).0, 200);
    }

    #[test]
    fn a_raw_reply_to_a_long_thread_trims_the_chain() {
        crate::asyncrt::install_test_host_runtime();
        let refs = (0..90)
            .map(|n| format!("<{n}-{}@example.org>", "x".repeat(480)))
            .collect::<Vec<_>>()
            .join("\r\n ");
        let event = Event::new(&mut parsed(&format!(
            "From: ann@example.org\r\nTo: bob@example.com\r\nMessage-ID: <m@x>\r\nReferences: {refs}\r\n\r\nx"
        )));
        let sent = event
            .prepare_reply_raw("bob@example.com", "ann@example.org", &reply_message(""))
            .unwrap();
        let text = String::from_utf8(sent.data).unwrap();
        let head = text.split("\r\n\r\n").next().unwrap();
        assert!(head.len() < 4000, "{}", head.len());
        assert!(head.contains("<0-") && head.contains("<m@x>"), "{head}");
        // A message that carries the full chain itself is still the same reply.
        event
            .prepare_reply_raw(
                "bob@example.com",
                "ann@example.org",
                &reply_message(&format!("References: {refs}\r\n <m@x>\r\n")),
            )
            .unwrap();
    }

    #[test]
    fn a_failed_or_slow_handler_after_a_delivery_is_still_accepted() {
        let delivered = AtomicBool::new(false);
        let failed = || (500, "{}".to_string());
        assert_eq!(after_delivery(failed(), &delivered, Some("<m@x>")).0, 500);
        assert_eq!(after_delivery((504, "{}".into()), &delivered, None).0, 504);
        delivered.store(true, Ordering::Release);
        assert_eq!(after_delivery(failed(), &delivered, Some("<m@x>")).0, 200);
        assert_eq!(after_delivery((504, "{}".into()), &delivered, None).0, 200);
    }

    fn reply_message(extra: &str) -> Vec<u8> {
        format!("From: bob@example.com\r\nTo: ann@example.org\r\nSubject: Re: hi\r\n{extra}\r\nThanks\r\n").into_bytes()
    }

    #[test]
    fn a_raw_reply_follows_the_rules_of_a_reply() {
        crate::asyncrt::install_test_host_runtime();
        let event = Event::new(&mut parsed(MESSAGE));
        let sent = event
            .prepare_reply_raw("bob@example.com", "ann@example.org", &reply_message(""))
            .unwrap();
        let text = String::from_utf8(sent.data.clone()).unwrap();
        assert!(text.contains("In-Reply-To: <m1@example.org>\r\n"), "{text}");
        assert!(
            text.contains("References: <a@x> <b@x> <m1@example.org>\r\n"),
            "{text}"
        );
        assert_eq!(sent.recipients, ["ann@example.org"]);
        // A value the message sets must be the computed one.
        event
            .prepare_reply_raw(
                "bob@example.com",
                "ann@example.org",
                &reply_message("In-Reply-To: <m1@example.org>\r\n"),
            )
            .unwrap();
        let wrong = event
            .prepare_reply_raw(
                "bob@example.com",
                "ann@example.org",
                &reply_message("In-Reply-To: <other@x>\r\n"),
            )
            .unwrap_err();
        assert_eq!(wrong.code, "E_VALIDATION_ERROR");
        // Only to the sender, only from the receiving domain.
        let elsewhere = event
            .prepare_reply_raw("bob@example.com", "eve@example.net", &reply_message(""))
            .unwrap_err();
        assert_eq!(elsewhere.code, "E_RECIPIENT_NOT_ALLOWED");
        let foreign = event
            .prepare_reply_raw("bob@example.net", "ann@example.org", &reply_message(""))
            .unwrap_err();
        assert_eq!(foreign.code, "E_SENDER_NOT_VERIFIED");
        // No Message-ID, no thread.
        let bare = Event::new(
            &mut parse(
                b"From: ann@example.org\r\n\r\nx".to_vec(),
                Some("ann@example.org"),
                Some("bob@example.com"),
            )
            .unwrap(),
        );
        assert_eq!(
            bare.prepare_reply_raw("bob@example.com", "ann@example.org", &reply_message(""))
                .unwrap_err()
                .code,
            "E_FIELD_MISSING"
        );
        // A long chain is a loop in the making.
        let refs = (0..=MAX_REFERENCES)
            .map(|i| format!("<{i}@x>"))
            .collect::<Vec<_>>()
            .join(" ");
        let looped = Event::new(&mut parsed(&format!("From: ann@example.org\r\nTo: bob@example.com\r\nMessage-ID: <m@x>\r\nReferences: {refs}\r\n\r\nx")));
        assert!(looped
            .prepare_reply_raw("bob@example.com", "ann@example.org", &reply_message(""))
            .is_err());
        // Many references fold, so the header stays within a line.
        let refs = (0..90)
            .map(|i| format!("<{i:0>20}@example.org>"))
            .collect::<Vec<_>>()
            .join("\r\n ");
        let long = Event::new(&mut parsed(&format!("From: ann@example.org\r\nTo: bob@example.com\r\nMessage-ID: <m@x>\r\nReferences: {refs}\r\n\r\nx")));
        long.prepare_reply_raw("bob@example.com", "ann@example.org", &reply_message(""))
            .unwrap();
    }

    #[test]
    fn a_generated_reply_is_marked_auto_replied_unless_the_worker_set_the_header() {
        crate::asyncrt::install_test_host_runtime();
        let event = Event::new(&mut parsed(MESSAGE));
        let text = |sent: Prepared| String::from_utf8(sent.data).unwrap();
        let raw = |extra: &str| {
            text(
                event
                    .prepare_reply_raw("bob@example.com", "ann@example.org", &reply_message(extra))
                    .unwrap(),
            )
        };
        assert!(raw("").contains("Auto-Submitted: auto-replied\r\n"));
        let kept = raw("Auto-Submitted: no\r\n");
        assert!(kept.contains("Auto-Submitted: no\r\n") && !kept.contains("auto-replied"));
        let builder = |headers: Value| {
            text(
                event
                    .prepare_reply_builder(serde_json::json!({
                        "from": "bob@example.com", "subject": "s", "text": "t", "headers": headers,
                    }))
                    .unwrap(),
            )
        };
        assert!(builder(serde_json::json!({})).contains("Auto-Submitted: auto-replied\r\n"));
        let kept = builder(serde_json::json!({ "auto-submitted": "no" }));
        assert!(kept.to_ascii_lowercase().contains("auto-submitted: no\r\n"));
        assert!(!kept.contains("auto-replied"));
    }

    #[test]
    fn a_builder_reply_goes_to_the_sender_with_the_thread_headers() {
        crate::asyncrt::install_test_host_runtime();
        let event = Event::new(&mut parsed(MESSAGE));
        let sent = event
            .prepare_reply_builder(serde_json::json!({
                "from": "bob@example.com", "subject": "Re: hi", "text": "ok",
            }))
            .unwrap();
        assert_eq!(sent.recipients, ["ann@example.org"]);
        let text = String::from_utf8(sent.data).unwrap();
        assert!(text.contains("To: ann@example.org\r\n"), "{text}");
        assert!(text.contains("In-Reply-To: <m1@example.org>\r\n"), "{text}");
        assert!(
            text.contains("References: <a@x> <b@x> <m1@example.org>\r\n"),
            "{text}"
        );
        let code = |fields: Value| event.prepare_reply_builder(fields).unwrap_err().code;
        let refs = (0..90)
            .map(|i| format!("<{i:0>30}@example.org>"))
            .collect::<Vec<_>>()
            .join("\r\n ");
        let long = Event::new(&mut parsed(&format!("From: ann@example.org\r\nTo: bob@example.com\r\nMessage-ID: <m@x>\r\nReferences: {refs}\r\n\r\nx")));
        let sent = long
            .prepare_reply_builder(
                serde_json::json!({ "from": "bob@example.com", "subject": "s", "text": "t" }),
            )
            .unwrap();
        let text = String::from_utf8(sent.data).unwrap();
        assert!(text.contains("<m@x>"), "the newest id is kept");
        assert!(
            text.contains(&format!("<{:0>30}@example.org>", 0)),
            "the first id is kept"
        );
        assert_eq!(
            code(
                serde_json::json!({ "from": "bob@example.com", "subject": "s", "text": "t", "to": "eve@example.net" })
            ),
            "E_VALIDATION_ERROR"
        );
        assert_eq!(
            code(serde_json::json!({ "from": "bob@example.net", "subject": "s", "text": "t" })),
            "E_SENDER_NOT_VERIFIED"
        );
        assert_eq!(
            code(
                serde_json::json!({ "from": "bob@example.com", "subject": "s", "text": "t", "headers": { "in-reply-to": "<x@y>" } })
            ),
            "E_VALIDATION_ERROR"
        );
    }

    #[test]
    fn one_reply_per_event_and_a_failed_one_counts() {
        let event = Event::new(&mut parsed(MESSAGE));
        event.check_reply_open().unwrap();
        event.take_reply().unwrap();
        assert!(event.check_reply_open().is_err());
        assert!(event.take_reply().is_err());
    }

    #[test]
    fn outcomes_map_to_statuses_and_a_reject_reason_is_one_clean_line() {
        assert_eq!(answer(Outcome::Accepted).0, 200);
        assert_eq!(answer(Outcome::NoHandler).0, 501);
        assert_eq!(answer(Outcome::Failed("boom".into())).0, 500);
        let (status, body) = answer(read_outcome(
            br#"{"outcome":"rejected","reason":"no\r\n250 ok \u00e9"}"#,
        ));
        assert_eq!(status, 550);
        let body: Value = serde_json::from_str(&body).unwrap();
        assert_eq!(body["reason"], "no  250 ok");
        assert_eq!(reject_reason(""), "rejected");
        assert_eq!(
            reject_reason(&"a".repeat(1000)).len(),
            MAX_REJECT_REASON_CHARS
        );
        assert_eq!(answer(read_outcome(b"garbage")).0, 500);
    }

    #[test]
    fn concurrent_messages_are_capped() {
        let _places = PLACES.lock().unwrap_or_else(|e| e.into_inner());
        let held: Vec<_> = (0..MAX_CONCURRENT).filter_map(|_| admit()).collect();
        assert_eq!(held.len(), MAX_CONCURRENT);
        assert!(admit().is_none());
        drop(held);
        assert!(admit().is_some());
    }
}
