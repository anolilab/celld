// Copyright 2026 Deno Land Inc. Apache-2.0 license.

//! Inbound mail: a raw RFC 5322 message posted by a relay becomes the
//! `message` of an `email()` event, and what the handler does with it
//! (`forward()`, `reply()`, `setReject()`) is checked here.
//!
//! The route answers on the internal listener, behind the operator token, so
//! the poster is the operator's own relay or webhook. A message is accepted
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

use serde_json::Value;

use super::raw::{normalise_line_ends, parse_headers, split_head, Header};
use super::{
    new_message_id, prepare_builder, prepare_raw, too_large, valid_address, EmailError, Prepared,
    SendEmailBinding, MAX_HEADER_SECTION_BYTES, MAX_LINE_BYTES, MAX_MESSAGE_BYTES,
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
/// How long the route waits for a handler before it answers 504.
pub const HANDLER_DEADLINE: std::time::Duration = std::time::Duration::from_secs(60);

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
    pub raw: Vec<u8>,
    /// Every header, in order, with its folds removed.
    pub headers: Vec<(String, String)>,
    /// The `Message-ID` header, with its angle brackets.
    pub message_id: Option<String>,
    /// The entries of every `References` header.
    pub references: Vec<String>,
    /// The place among [`MAX_CONCURRENT`] this message holds. It moves to the
    /// [`Event`], so it is kept until the handler's event has ended.
    pub permit: Option<Permit>,
}

/// Parse a posted message. `from` and `to` are the envelope as the relay gave
/// it, and both are required: the recipient decides which domain a reply may be
/// sent from, so it is never read from a header the sender wrote.
pub fn parse(raw: Vec<u8>, from: Option<&str>, to: Option<&str>) -> Result<Inbound, InboundError> {
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
        .flat_map(|header| header.value.split_whitespace().map(str::to_string))
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
    })
}

/// `<id>`: no space, control character or second bracket, so it can be put
/// into a header as written.
fn valid_message_id(id: &str) -> bool {
    id.len() <= 512
        && id.len() > 2
        && id.starts_with('<')
        && id.ends_with('>')
        && !id[1..id.len() - 1]
            .contains(|c: char| c.is_control() || c.is_whitespace() || "<>".contains(c))
}

/// What the node keeps of one inbound message for the event that handles it.
/// Everything a handler may do with the message is decided from this.
pub struct Event {
    pub from: String,
    pub to: String,
    raw: Vec<u8>,
    message_id: Option<String>,
    references: Vec<String>,
    replied: AtomicBool,
    _permit: Option<Permit>,
}

impl Event {
    pub fn new(inbound: &mut Inbound) -> Self {
        Self {
            from: inbound.from.clone(),
            to: inbound.to.clone(),
            raw: inbound.raw.clone(),
            message_id: inbound.message_id.clone(),
            references: inbound.references.clone(),
            replied: AtomicBool::new(false),
            _permit: inbound.permit.take(),
        }
    }

    /// The original message plus the `X-` headers the handler adds, for
    /// `rcpt_to`. The bytes are the received ones apart from CRLF line ends and
    /// the loss of `Bcc:`; `Received:`, `Message-ID` and any signature headers
    /// stay as they were, so a signature over them still holds.
    pub fn prepare_forward(
        &self,
        rcpt_to: &str,
        headers: &[(String, String)],
    ) -> Result<Prepared, EmailError> {
        if !valid_address(rcpt_to) {
            return Err(EmailError::new(
                "E_VALIDATION_ERROR",
                "the recipient is not a valid email address",
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
        let mut data = Vec::with_capacity(message.len() + extra.len() + 8);
        data.extend_from_slice(extra.as_bytes());
        for header in &parsed {
            if !header.name_is("bcc") && !header.name_is("resent-bcc") {
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
        let message_id = self.message_id.as_deref().map_or_else(
            || new_message_id(&self.to),
            |id| id[1..id.len() - 1].to_string(),
        );
        Ok(Prepared::new(
            self.from.clone(),
            vec![rcpt_to.to_string()],
            data,
            message_id,
        ))
    }

    /// Take the one reply this event may make. A failed reply counts, so a
    /// handler cannot try one sender after another.
    pub fn take_reply(&self) -> Result<(), EmailError> {
        if self.replied.swap(true, Ordering::AcqRel) {
            return Err(EmailError::new(
                "E_VALIDATION_ERROR",
                "this message has been replied to already",
            ));
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
        let (in_reply_to, references) = self.threading()?;
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
                if header.value.split_whitespace().collect::<Vec<_>>()
                    != wanted.split_whitespace().collect::<Vec<_>>()
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
            for (key, value) in found {
                let same = value
                    .as_str()
                    .is_some_and(|value| value.split_whitespace().eq(wanted.split_whitespace()));
                if !same {
                    return Err(invalid(&format!("{name}: is not the value for this reply")));
                }
                headers.remove(&key);
            }
            let value = if name == "References" {
                trim_references(wanted)
            } else {
                wanted.clone()
            };
            headers.insert(name.to_string(), Value::String(value));
        }
        fields.insert("headers".into(), Value::Object(headers));
        fields.insert("to".into(), Value::String(self.from.clone()));
        prepare_builder(&SendEmailBinding::default(), Value::Object(fields))
    }
}

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

/// The `X-` headers a handler gave `forward()`, as lines. Any other header is
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
        out.push_str(&format!("{name}: {value}\r\n"));
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
/// 500 the handler threw: a temporary failure, so the relay retries.
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
        Outcome::Failed(error) => (
            500,
            serde_json::json!({ "ok": false, "outcome": "error", "error": error }).to_string(),
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

/// The route: parse a posted message, run it through the primary Worker's
/// `email()` handler and map what it did to a status and body.
pub async fn handle(
    runtime: &crate::runtime::RuntimeManager,
    query: Option<&str>,
    body: Vec<u8>,
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
    let ran = crate::asyncrt::timeout(HANDLER_DEADLINE, runtime.email_worker(inbound)).await;
    let outcome = match ran {
        Ok(Ok(bytes)) => read_outcome(&bytes),
        Ok(Err(error)) => {
            tracing::warn!(event = "inbound_email_failed", %error);
            Outcome::Failed("the message could not be handled".into())
        }
        Err(_) => return (
            504,
            r#"{"ok":false,"outcome":"error","error":"the email handler did not finish in time"}"#
                .to_string(),
        ),
    };
    answer(outcome)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The tests that count places share one counter.
    static PLACES: std::sync::Mutex<()> = std::sync::Mutex::new(());

    const MESSAGE: &str = "Received: from mx by node\r\nFrom: Ann <ann@example.org>\r\nTo: bob@example.com\r\nSubject: hi\r\nMessage-ID: <m1@example.org>\r\nReferences: <a@x> <b@x>\r\nBcc: spy@example.net\r\n\r\nHello\r\n.dot\r\n";

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
    fn a_forward_keeps_the_message_and_its_message_id_and_drops_bcc() {
        let event = Event::new(&mut parsed(MESSAGE.replace("\r\n", "\n").as_str()));
        let sent = event
            .prepare_forward(
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
        assert_eq!(sent.from, "ann@example.org");
        assert_eq!(sent.recipients, ["team@example.net"]);
        assert_eq!(sent.message_id, "m1@example.org");
        assert!(event.prepare_forward("not an address", &[]).is_err());
        let code = |name: &str, value: &str| {
            event
                .prepare_forward("a@b.co", &[(name.into(), value.into())])
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
            event.prepare_forward("a@b.co", &many).unwrap_err().code,
            "E_HEADERS_TOO_MANY"
        );
        // A line past the limit in the body is refused, not folded.
        let long = format!("From: a@b.co\r\n\r\n{}\r\n", "a".repeat(MAX_LINE_BYTES + 1));
        let event =
            Event::new(&mut parse(long.into_bytes(), Some("a@b.co"), Some("b@c.co")).unwrap());
        assert_eq!(
            event.prepare_forward("a@b.co", &[]).unwrap_err().code,
            "E_LINE_TOO_LONG"
        );
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
        event.take_reply().unwrap();
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
