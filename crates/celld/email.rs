// Copyright 2026 Deno Land Inc. Apache-2.0 license.

//! The messages of a `send_email` binding: what a Worker may hand to
//! `send()`, checked and turned into the bytes of one SMTP transaction.
//!
//! The binding takes two forms, as Cloudflare's does. `new EmailMessage(from,
//! to, raw)` carries a finished RFC 5322 message with its envelope, and
//! `send({ from, to, subject, text, html, ... })` carries fields that celld
//! turns into a MIME message. Both end as a [`Prepared`] message: a sender, a
//! recipient list, and bytes with CRLF line ends that are ready for `DATA`.
//!
//! Everything a Worker supplies is bounded and checked here, on the host. The
//! JavaScript side only shapes arguments, so a Worker cannot widen what a
//! binding allows by calling around it.
//!
//! - An address is a plain ASCII `local@domain`; it has no quoted form, no
//!   display name, no space, and none of `< > "`, so it can be put into
//!   `MAIL FROM:<...>` and a header without escaping.
//! - A raw message is normalised to CRLF line ends (a bare LF or CR becomes
//!   CRLF), so the dot-stuffing of `DATA` cannot be defeated by a line end the
//!   server reads differently.
//! - A raw message's `From:` header must carry the envelope sender, so an
//!   `allowed_sender_addresses` list also holds for the header a recipient
//!   sees. Its `Message-ID` is replaced by one celld makes, its `Bcc:` header
//!   is dropped, and a `Received:` header is refused.

use anyhow::{anyhow, bail};
use base64::engine::general_purpose::STANDARD;
use base64::Engine as _;
use serde::Deserialize;
use serde_json::Value;

/// The most a message may weigh, attachments included: Cloudflare's limit.
pub const MAX_MESSAGE_BYTES: usize = 25 * 1024 * 1024;
/// The most recipients one message may have, `to`, `cc` and `bcc` together.
pub const MAX_RECIPIENTS: usize = 50;
/// The most attachments one message may have.
pub const MAX_ATTACHMENTS: usize = 32;
/// The most the header section of a raw message may weigh.
pub const MAX_HEADER_SECTION_BYTES: usize = 64 * 1024;
/// RFC 5322's limit on one line of a header.
pub const MAX_LINE_BYTES: usize = 998;
/// RFC 5321's limit on an address.
pub const MAX_ADDRESS_BYTES: usize = 254;
/// The most addresses one restriction list of a binding may hold.
pub const MAX_RESTRICTION_ADDRESSES: usize = 1000;

const MAX_CUSTOM_HEADERS: usize = 20;
const MAX_CUSTOM_HEADER_VALUE_BYTES: usize = 2048;
const MAX_CUSTOM_HEADERS_BYTES: usize = 16 * 1024;
const MAX_SUBJECT_CHARS: usize = 998;
const MAX_DISPLAY_NAME_CHARS: usize = 256;

/// Why a message was refused, with the code a Worker reads from `error.code`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EmailError {
    pub code: &'static str,
    pub message: String,
}

impl EmailError {
    pub fn new(code: &'static str, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
        }
    }

    fn invalid(message: impl Into<String>) -> Self {
        Self::new("E_VALIDATION_ERROR", message)
    }
}

impl std::fmt::Display for EmailError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: {}", self.code, self.message)
    }
}

impl std::error::Error for EmailError {}

/// One `send_email` binding of a Worker: its `env` name and the restrictions
/// of Cloudflare's configuration. An address compares exactly, as written.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct SendEmailBinding {
    pub environment: String,
    /// The one address the binding may send to.
    pub destination_address: Option<String>,
    /// The addresses the binding may send to.
    pub allowed_destination_addresses: Option<Vec<String>>,
    /// The addresses the binding may send from.
    pub allowed_sender_addresses: Option<Vec<String>>,
}

impl SendEmailBinding {
    /// The manifest entry for this binding: the one place its shape is written.
    pub fn to_manifest(&self) -> Value {
        let mut entry = serde_json::json!({
            "type": "send_email",
            "name": self.environment,
        });
        if let Some(address) = &self.destination_address {
            entry["destination_address"] = Value::from(address.as_str());
        }
        if let Some(addresses) = &self.allowed_destination_addresses {
            entry["allowed_destination_addresses"] = Value::from(addresses.clone());
        }
        if let Some(addresses) = &self.allowed_sender_addresses {
            entry["allowed_sender_addresses"] = Value::from(addresses.clone());
        }
        entry
    }

    /// Reads a manifest entry back, refusing one `to_manifest` could not have
    /// written: an unknown key, a name `env` cannot hold, an address that is
    /// not one, and both `destination_address` and
    /// `allowed_destination_addresses`, which are alternatives.
    pub fn from_manifest(entry: &Value) -> anyhow::Result<Self> {
        let object = entry.as_object().ok_or_else(|| {
            anyhow!("the manifest has a send_email binding that is not an object")
        })?;
        if let Some(key) = object.keys().find(|key| {
            !matches!(
                key.as_str(),
                "type"
                    | "name"
                    | "destination_address"
                    | "allowed_destination_addresses"
                    | "allowed_sender_addresses"
            )
        }) {
            bail!("the manifest has a send_email binding with an unknown field `{key}`");
        }
        let name = object
            .get("name")
            .and_then(Value::as_str)
            .ok_or_else(|| anyhow!("the manifest has a send_email binding without a name"))?;
        // The names a deploy accepts, so a manifest cannot install a binding
        // such as `__proto__` that `env` would not hold.
        anyhow::ensure!(
            crate::deploy::valid_binding(name),
            "the manifest has an invalid send_email binding name: {name:?}"
        );
        crate::deploy::check_env_name(name)?;
        let destination_address = match object.get("destination_address") {
            None => None,
            Some(value) => Some(address_field(name, "destination_address", value)?),
        };
        let list = |field: &str| -> anyhow::Result<Option<Vec<String>>> {
            match object.get(field) {
                None => Ok(None),
                Some(value) => address_list(name, field, value).map(Some),
            }
        };
        let allowed_destination_addresses = list("allowed_destination_addresses")?;
        let allowed_sender_addresses = list("allowed_sender_addresses")?;
        anyhow::ensure!(
            destination_address.is_none() || allowed_destination_addresses.is_none(),
            "send_email binding {name} sets both `destination_address` and \
             `allowed_destination_addresses`; use one"
        );
        Ok(Self {
            environment: name.to_string(),
            destination_address,
            allowed_destination_addresses,
            allowed_sender_addresses,
        })
    }

    fn check_sender(&self, from: &str) -> Result<(), EmailError> {
        match &self.allowed_sender_addresses {
            Some(allowed) if !allowed.iter().any(|address| address == from) => {
                Err(EmailError::new(
                    "E_SENDER_NOT_VERIFIED",
                    format!("email from {from} not allowed"),
                ))
            }
            _ => Ok(()),
        }
    }

    fn check_recipient(&self, to: &str) -> Result<(), EmailError> {
        let denied = || {
            Err(EmailError::new(
                "E_RECIPIENT_NOT_ALLOWED",
                format!("email to {to} not allowed"),
            ))
        };
        if self
            .destination_address
            .as_deref()
            .is_some_and(|address| address != to)
        {
            return denied();
        }
        match &self.allowed_destination_addresses {
            Some(allowed) if !allowed.iter().any(|address| address == to) => denied(),
            _ => Ok(()),
        }
    }
}

fn address_field(binding: &str, field: &str, value: &Value) -> anyhow::Result<String> {
    match value.as_str() {
        Some(address) if valid_address(address) => Ok(address.to_string()),
        _ => bail!("send_email binding {binding} has an invalid `{field}`: {value}"),
    }
}

fn address_list(binding: &str, field: &str, value: &Value) -> anyhow::Result<Vec<String>> {
    let items = value
        .as_array()
        .ok_or_else(|| anyhow!("send_email binding {binding}: `{field}` must be an array"))?;
    anyhow::ensure!(
        items.len() <= MAX_RESTRICTION_ADDRESSES,
        "send_email binding {binding}: `{field}` holds more than {MAX_RESTRICTION_ADDRESSES} addresses"
    );
    items
        .iter()
        .map(|item| address_field(binding, field, item))
        .collect()
}

/// Whether `address` is a plain ASCII `local@domain` that can go into
/// `MAIL FROM:<...>` and a header as written.
pub fn valid_address(address: &str) -> bool {
    if address.len() > MAX_ADDRESS_BYTES || !address.is_ascii() {
        return false;
    }
    let Some((local, domain)) = address.split_once('@') else {
        return false;
    };
    let atext = |c: char| c.is_ascii_alphanumeric() || "!#$%&'*+/=?^_`{|}~-".contains(c);
    let local_ok = !local.is_empty()
        && local.len() <= 64
        && !local.starts_with('.')
        && !local.ends_with('.')
        && !local.contains("..")
        && local.chars().all(|c| c == '.' || atext(c));
    let domain_ok = !domain.is_empty()
        && domain.len() <= 253
        && domain.split('.').all(|label| {
            !label.is_empty()
                && label.len() <= 63
                && !label.starts_with('-')
                && !label.ends_with('-')
                && label.chars().all(|c| c.is_ascii_alphanumeric() || c == '-')
        });
    local_ok && domain_ok
}

/// A message ready for one SMTP transaction.
#[derive(Debug)]
pub struct Prepared {
    /// The envelope sender.
    pub from: String,
    /// The envelope recipients: `to`, `cc` and `bcc`, in that order.
    pub recipients: Vec<String>,
    /// The message with CRLF line ends and a trailing CRLF. Not dot-stuffed.
    pub data: Vec<u8>,
    /// The `Message-ID` the message carries, without angle brackets.
    pub message_id: String,
}

fn new_message_id(from: &str) -> String {
    let mut random = [0u8; 16];
    getrandom::fill(&mut random).expect("the OS random source");
    let domain = from
        .rsplit_once('@')
        .map_or("localhost", |(_, domain)| domain);
    format!("{}@{domain}", hex(&random))
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

// ---- The raw form ----------------------------------------------------------

/// Check a message handed over as `new EmailMessage(from, to, raw)` and make it
/// ready to send.
pub fn prepare_raw(
    binding: &SendEmailBinding,
    from: &str,
    to: &str,
    raw: &[u8],
) -> Result<Prepared, EmailError> {
    if !valid_address(from) {
        return Err(EmailError::invalid(
            "the sender is not a valid email address",
        ));
    }
    if !valid_address(to) {
        return Err(EmailError::invalid(
            "the recipient is not a valid email address",
        ));
    }
    binding.check_sender(from)?;
    binding.check_recipient(to)?;
    if raw.len() > MAX_MESSAGE_BYTES {
        return Err(too_large());
    }
    if raw.contains(&0) {
        return Err(EmailError::invalid("the message holds a NUL byte"));
    }
    let message = normalise_line_ends(raw);
    if message.len() > MAX_MESSAGE_BYTES {
        return Err(too_large());
    }
    let (head, body) = split_head(&message);
    if head.len() > MAX_HEADER_SECTION_BYTES {
        return Err(EmailError::new(
            "E_HEADERS_TOO_LARGE",
            format!("the message headers exceed {MAX_HEADER_SECTION_BYTES} bytes"),
        ));
    }
    let headers = parse_headers(head)?;
    let mut from_headers = headers.iter().filter(|header| header.name_is("from"));
    let (Some(from_header), None) = (from_headers.next(), from_headers.next()) else {
        return Err(EmailError::invalid(
            "the message needs exactly one From: header",
        ));
    };
    if single_address(&from_header.value).as_deref() != Some(from) {
        return Err(EmailError::invalid("From: header does not match mail from"));
    }
    if headers.iter().any(|header| header.name_is("received")) {
        return Err(EmailError::invalid("invalid headers set"));
    }
    let message_id = new_message_id(from);
    let mut data = Vec::with_capacity(message.len() + 64);
    data.extend_from_slice(format!("Message-ID: <{message_id}>\r\n").as_bytes());
    for header in &headers {
        if !(header.name_is("message-id") || header.name_is("bcc")) {
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
    Ok(Prepared {
        from: from.to_string(),
        recipients: vec![to.to_string()],
        data,
        message_id,
    })
}

fn too_large() -> EmailError {
    EmailError::new(
        "E_CONTENT_TOO_LARGE",
        format!("the message exceeds {MAX_MESSAGE_BYTES} bytes"),
    )
}

/// `raw` with every line end, `\r\n`, `\n` or a lone `\r`, as `\r\n`.
fn normalise_line_ends(raw: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(raw.len() + raw.len() / 32);
    let mut bytes = raw.iter().copied().peekable();
    while let Some(byte) = bytes.next() {
        match byte {
            b'\r' => {
                bytes.next_if_eq(&b'\n');
                out.extend_from_slice(b"\r\n");
            }
            b'\n' => out.extend_from_slice(b"\r\n"),
            other => out.push(other),
        }
    }
    out
}

/// The header section of a normalised message, with its closing CRLF, and the
/// body after the blank line.
fn split_head(message: &[u8]) -> (&[u8], &[u8]) {
    if message.starts_with(b"\r\n") {
        return (&[], &message[2..]);
    }
    match message.windows(4).position(|window| window == b"\r\n\r\n") {
        Some(at) => (&message[..at + 2], &message[at + 4..]),
        None => (message, &[]),
    }
}

struct Header {
    name: String,
    /// The value with its folds removed.
    value: String,
    /// The bytes of this header, folds included, within the header section.
    start: usize,
    end: usize,
}

impl Header {
    fn name_is(&self, name: &str) -> bool {
        self.name.eq_ignore_ascii_case(name)
    }
}

fn parse_headers(head: &[u8]) -> Result<Vec<Header>, EmailError> {
    let mut headers: Vec<Header> = Vec::new();
    let mut offset = 0;
    while offset < head.len() {
        let end = head[offset..]
            .windows(2)
            .position(|window| window == b"\r\n")
            .map_or(head.len(), |at| offset + at + 2);
        let line = &head[offset..end.min(head.len())];
        let text = line.strip_suffix(b"\r\n").unwrap_or(line);
        if text.len() > MAX_LINE_BYTES {
            return Err(EmailError::new(
                "E_HEADERS_TOO_LARGE",
                format!("a header line exceeds {MAX_LINE_BYTES} bytes"),
            ));
        }
        if text
            .first()
            .is_some_and(|byte| matches!(byte, b' ' | b'\t'))
        {
            // A fold continues the header before it.
            let Some(last) = headers.last_mut() else {
                return Err(EmailError::invalid("the message starts with a folded line"));
            };
            last.value.push(' ');
            last.value.push_str(String::from_utf8_lossy(text).trim());
            last.end = end;
        } else {
            let colon = text
                .iter()
                .position(|byte| *byte == b':')
                .filter(|at| *at > 0)
                .ok_or_else(|| EmailError::invalid("a header line has no name"))?;
            let name = &text[..colon];
            if !name.iter().all(|byte| (33..=126).contains(byte)) {
                return Err(EmailError::invalid("a header has an invalid name"));
            }
            headers.push(Header {
                name: String::from_utf8_lossy(name).into_owned(),
                value: String::from_utf8_lossy(&text[colon + 1..])
                    .trim()
                    .to_string(),
                start: offset,
                end,
            });
        }
        offset = end;
    }
    Ok(headers)
}

/// The one address of a mailbox header value, or `None` for anything else.
///
/// Only three shapes are read: `addr`, `"display name" <addr>` and
/// `atom words <addr>`, with nothing after the `>`. A comment or a backslash
/// is refused wherever it is. Mail clients and relays disagree about where a
/// comment ends when it holds a quoted pair such as `\)`, so a header that
/// uses one could name a different sender to them than to this check. A list,
/// a group and what is not an address are refused too.
fn single_address(value: &str) -> Option<String> {
    let value = value.trim();
    if value.contains('\\') {
        return None;
    }
    let address = if let Some(quoted) = value.strip_prefix('"') {
        // The display name is one quoted string, and the address follows it.
        let (_, rest) = quoted.split_once('"')?;
        angle_address(rest.trim_start())?
    } else if let Some(open) = value.find('<') {
        // Plain words before the address: no character that has a meaning in
        // a mailbox, so no address, quote, comment, list or group.
        let words = &value[..open];
        if words.contains(|c: char| c.is_control() || "@<>,;:\"()[]".contains(c)) {
            return None;
        }
        angle_address(&value[open..])?
    } else {
        value
    };
    valid_address(address).then(|| address.to_string())
}

/// The text between `<` and `>` of `<addr>`, which must be all of `text`
/// but trailing white space.
fn angle_address(text: &str) -> Option<&str> {
    let (address, after) = text.strip_prefix('<')?.split_once('>')?;
    after.trim().is_empty().then_some(address)
}

// ---- The builder form ------------------------------------------------------

#[derive(Deserialize)]
#[serde(untagged)]
enum Mailbox {
    Plain(String),
    Named(NamedMailbox),
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct NamedMailbox {
    #[serde(default)]
    name: Option<String>,
    email: String,
}

#[derive(Deserialize)]
#[serde(untagged)]
enum Mailboxes {
    One(Mailbox),
    Many(Vec<Mailbox>),
}

impl Mailboxes {
    fn into_vec(self) -> Vec<Mailbox> {
        match self {
            Self::One(one) => vec![one],
            Self::Many(many) => many,
        }
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Builder {
    from: Mailbox,
    #[serde(default)]
    to: Option<Mailboxes>,
    #[serde(default)]
    cc: Option<Mailboxes>,
    #[serde(default)]
    bcc: Option<Mailboxes>,
    #[serde(default, rename = "replyTo")]
    reply_to: Option<Mailbox>,
    subject: String,
    #[serde(default)]
    text: Option<String>,
    #[serde(default)]
    html: Option<String>,
    #[serde(default)]
    headers: Option<std::collections::BTreeMap<String, String>>,
    #[serde(default)]
    attachments: Option<Vec<Attachment>>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Attachment {
    /// Base64, as the Worker side made it from a string or binary content.
    content: String,
    filename: String,
    #[serde(rename = "type")]
    media_type: String,
    disposition: String,
    #[serde(default, rename = "contentId")]
    content_id: Option<String>,
}

/// The headers a Worker may set through `headers`, besides any `X-` header.
const ALLOWED_HEADERS: &[&str] = &[
    "archived-at",
    "auto-submitted",
    "comments",
    "content-language",
    "expires",
    "importance",
    "in-reply-to",
    "keywords",
    "list-archive",
    "list-help",
    "list-id",
    "list-owner",
    "list-post",
    "list-subscribe",
    "list-unsubscribe",
    "list-unsubscribe-post",
    "organization",
    "otp-token",
    "precedence",
    "priority",
    "references",
    "reply-by",
    "require-recipient-valid-since",
    "sensitivity",
    "thread-index",
    "thread-topic",
];

/// The headers the message sets itself or takes from a field of the builder.
const OWN_HEADERS: &[&str] = &[
    "bcc",
    "cc",
    "content-transfer-encoding",
    "content-type",
    "date",
    "dkim-signature",
    "feedback-id",
    "from",
    "message-id",
    "mime-version",
    "received",
    "reply-to",
    "return-path",
    "subject",
    "to",
];

/// A recipient: its display name, if any, and its address.
type NamedAddress = (Option<String>, String);

fn mailbox_parts(mailbox: Mailbox) -> (Option<String>, String) {
    match mailbox {
        Mailbox::Plain(email) => (None, email),
        Mailbox::Named(named) => (named.name.filter(|name| !name.is_empty()), named.email),
    }
}

/// Check a message given as fields and build its MIME form.
pub fn prepare_builder(binding: &SendEmailBinding, builder: Value) -> Result<Prepared, EmailError> {
    let builder: Builder = serde_json::from_value(builder)
        .map_err(|error| EmailError::invalid(format!("the message is not valid: {error}")))?;
    // The parts are summed before anything is encoded, so a message that is
    // too large costs no more than the parse that found it.
    let parts = builder
        .text
        .iter()
        .chain(&builder.html)
        .map(String::len)
        .sum::<usize>()
        + builder
            .attachments
            .iter()
            .flatten()
            .map(|attachment| attachment.content.len())
            .sum::<usize>();
    if parts > MAX_MESSAGE_BYTES {
        return Err(too_large());
    }
    let (from_name, from) = mailbox_parts(builder.from);
    let too_long = |name: &Option<String>| {
        name.as_ref()
            .is_some_and(|name| name.chars().count() > MAX_DISPLAY_NAME_CHARS)
    };
    if too_long(&from_name) {
        return Err(EmailError::invalid(format!(
            "a display name exceeds {MAX_DISPLAY_NAME_CHARS} characters"
        )));
    }
    if !valid_address(&from) {
        return Err(EmailError::invalid("`from` is not a valid email address"));
    }
    binding.check_sender(&from)?;
    let mut groups: Vec<(&'static str, Vec<NamedAddress>)> = Vec::new();
    for (field, value) in [("to", builder.to), ("cc", builder.cc), ("bcc", builder.bcc)] {
        let mut group = Vec::new();
        for mailbox in value.map(Mailboxes::into_vec).unwrap_or_default() {
            let (name, email) = mailbox_parts(mailbox);
            if too_long(&name) {
                return Err(EmailError::invalid(format!(
                    "a display name exceeds {MAX_DISPLAY_NAME_CHARS} characters"
                )));
            }
            if !valid_address(&email) {
                return Err(EmailError::invalid(format!(
                    "`{field}` has an address that is not a valid email address"
                )));
            }
            group.push((name, email));
        }
        groups.push((field, group));
    }
    // A binding with one `destination_address` has a recipient already.
    if groups.iter().all(|(_, group)| group.is_empty()) {
        match &binding.destination_address {
            Some(address) => groups[0].1.push((None, address.clone())),
            None => {
                return Err(EmailError::new(
                    "E_FIELD_MISSING",
                    "at least one of `to`, `cc` or `bcc` is required",
                ))
            }
        }
    }
    let count: usize = groups.iter().map(|(_, group)| group.len()).sum();
    if count > MAX_RECIPIENTS {
        return Err(EmailError::new(
            "E_TOO_MANY_RECIPIENTS",
            format!("a message may have {MAX_RECIPIENTS} recipients, not {count}"),
        ));
    }
    let recipients: Vec<String> = groups
        .iter()
        .flat_map(|(_, group)| group.iter().map(|(_, email)| email.clone()))
        .collect();
    for recipient in &recipients {
        binding.check_recipient(recipient)?;
    }
    if builder.subject.chars().count() > MAX_SUBJECT_CHARS {
        return Err(EmailError::invalid(format!(
            "`subject` exceeds {MAX_SUBJECT_CHARS} characters"
        )));
    }
    if builder.subject.contains(['\r', '\n']) {
        return Err(EmailError::invalid("`subject` holds a line break"));
    }
    let reply_to = builder.reply_to.map(mailbox_parts);
    if reply_to.as_ref().is_some_and(|(name, _)| too_long(name)) {
        return Err(EmailError::invalid(format!(
            "a display name exceeds {MAX_DISPLAY_NAME_CHARS} characters"
        )));
    }
    if let Some((_, address)) = &reply_to {
        if !valid_address(address) {
            return Err(EmailError::invalid(
                "`replyTo` is not a valid email address",
            ));
        }
    }
    if builder.text.is_none() && builder.html.is_none() {
        return Err(EmailError::new(
            "E_FIELD_MISSING",
            "at least one of `text` or `html` is required",
        ));
    }
    let custom = custom_headers(builder.headers.unwrap_or_default())?;

    let message_id = new_message_id(&from);
    let mut head = String::new();
    head.push_str(&format!("Message-ID: <{message_id}>\r\n"));
    head.push_str(&format!(
        "Date: {}\r\n",
        chrono::DateTime::from_timestamp_millis(crate::asyncrt::wall_ms())
            .unwrap_or_default()
            .to_rfc2822()
    ));
    head.push_str("MIME-Version: 1.0\r\n");
    head.push_str(&fold_header(
        "From",
        &format_mailbox(from_name.as_deref(), &from),
    ));
    for (field, group) in &groups {
        // A bcc recipient is in the envelope and nowhere in the message.
        if *field != "bcc" && !group.is_empty() {
            let items: Vec<String> = group
                .iter()
                .map(|(name, email)| format_mailbox(name.as_deref(), email))
                .collect();
            let mut name = field.to_string();
            name[..1].make_ascii_uppercase();
            head.push_str(&fold_list(&name, &items));
        }
    }
    if let Some((name, address)) = &reply_to {
        head.push_str(&fold_header(
            "Reply-To",
            &format_mailbox(name.as_deref(), address),
        ));
    }
    head.push_str(&fold_header("Subject", &encode_text(&builder.subject)?));
    for (name, value) in &custom {
        head.push_str(&fold_header(name, &encode_text(value)?));
    }

    // A folded header breaks at spaces only, so a word with none can leave a
    // line longer than a server reads. A raw message is held to the same limit.
    if head.split("\r\n").any(|line| line.len() > MAX_LINE_BYTES) {
        return Err(EmailError::new(
            "E_HEADERS_TOO_LARGE",
            format!("a header line exceeds {MAX_LINE_BYTES} bytes"),
        ));
    }
    let attachments = builder.attachments.unwrap_or_default();
    if attachments.len() > MAX_ATTACHMENTS {
        return Err(EmailError::new(
            "E_TOO_MANY_ATTACHMENTS",
            format!("a message may have {MAX_ATTACHMENTS} attachments"),
        ));
    }
    let body = mime_body(builder.text, builder.html, attachments)?;
    let mut data = head.into_bytes();
    data.extend_from_slice(&body);
    if data.len() > MAX_MESSAGE_BYTES {
        return Err(too_large());
    }
    Ok(Prepared {
        from,
        recipients,
        data,
        message_id,
    })
}

fn custom_headers(
    headers: std::collections::BTreeMap<String, String>,
) -> Result<Vec<(String, String)>, EmailError> {
    let mut accepted = Vec::new();
    let (mut plain, mut payload) = (0usize, 0usize);
    for (name, value) in headers {
        let lower = name.to_ascii_lowercase();
        let own = OWN_HEADERS.contains(&lower.as_str()) || lower.starts_with("arc-");
        let named_ok = if let Some(rest) = lower.strip_prefix("x-") {
            !rest.is_empty()
                && name.len() <= 100
                && name
                    .chars()
                    .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_')
        } else {
            !name.is_empty()
                && name.len() <= 100
                && name.chars().all(|c| c.is_ascii_alphanumeric() || c == '-')
        };
        if !named_ok {
            return Err(EmailError::new(
                "E_HEADER_NAME_INVALID",
                format!("header name {name:?} is not valid"),
            ));
        }
        if matches!(
            lower.as_str(),
            "from" | "to" | "cc" | "bcc" | "subject" | "reply-to"
        ) {
            return Err(EmailError::new(
                "E_HEADER_USE_API_FIELD",
                format!("header {name} must be set through its own field"),
            ));
        }
        if own || !(lower.starts_with("x-") || ALLOWED_HEADERS.contains(&lower.as_str())) {
            return Err(EmailError::new(
                "E_HEADER_NOT_ALLOWED",
                format!("header {name} is not allowed"),
            ));
        }
        if value.is_empty()
            || value.len() > MAX_CUSTOM_HEADER_VALUE_BYTES
            || value.contains(['\r', '\n', '\0'])
        {
            return Err(EmailError::new(
                "E_HEADER_VALUE_INVALID",
                format!("the value of header {name} is empty, too long, or holds a line break"),
            ));
        }
        if !lower.starts_with("x-") {
            plain += 1;
        }
        payload += name.len() + value.len() + 4;
        accepted.push((name, value));
    }
    if plain > MAX_CUSTOM_HEADERS {
        return Err(EmailError::new(
            "E_HEADERS_TOO_MANY",
            format!("at most {MAX_CUSTOM_HEADERS} custom headers besides X- headers"),
        ));
    }
    if payload > MAX_CUSTOM_HEADERS_BYTES {
        return Err(EmailError::new(
            "E_HEADERS_TOO_LARGE",
            format!("custom headers exceed {MAX_CUSTOM_HEADERS_BYTES} bytes"),
        ));
    }
    Ok(accepted)
}

/// `name <address>` with the name quoted or encoded, or the bare address.
fn format_mailbox(name: Option<&str>, address: &str) -> String {
    match name {
        None => address.to_string(),
        Some(name) if name.is_ascii() => {
            let mut quoted = String::from("\"");
            for c in name.chars().filter(|c| !c.is_control()) {
                if c == '"' || c == '\\' {
                    quoted.push('\\');
                }
                quoted.push(c);
            }
            format!("{quoted}\" <{address}>")
        }
        Some(name) => {
            let name: String = name.chars().filter(|c| !c.is_control()).collect();
            format!("{} <{address}>", encoded_words(&name))
        }
    }
}

/// `text` as is when it is printable ASCII, else as RFC 2047 encoded words.
fn encode_text(text: &str) -> Result<String, EmailError> {
    if text
        .chars()
        .any(|c| c == '\0' || (c.is_control() && c != '\t'))
    {
        return Err(EmailError::invalid(
            "a header value holds a control character",
        ));
    }
    Ok(if text.is_ascii() {
        text.to_string()
    } else {
        encoded_words(text)
    })
}

/// RFC 2047 `B` words of at most 45 bytes each, joined by a space.
fn encoded_words(text: &str) -> String {
    let mut words = Vec::new();
    let mut chunk = String::new();
    for c in text.chars() {
        if chunk.len() + c.len_utf8() > 45 {
            words.push(std::mem::take(&mut chunk));
        }
        chunk.push(c);
    }
    if !chunk.is_empty() {
        words.push(chunk);
    }
    words
        .iter()
        .map(|word| format!("=?UTF-8?B?{}?=", STANDARD.encode(word)))
        .collect::<Vec<_>>()
        .join(" ")
}

/// One header line, folded at spaces so no line passes 76 bytes where a space
/// allows it.
fn fold_header(name: &str, value: &str) -> String {
    let mut out = format!("{name}:");
    let mut line = out.len();
    for word in value.split(' ') {
        if line + 1 + word.len() > 76 && line > name.len() + 1 {
            out.push_str("\r\n");
            line = 0;
        }
        out.push(' ');
        out.push_str(word);
        line += 1 + word.len();
    }
    out.push_str("\r\n");
    out
}

/// A header whose value is a comma separated list, folded after commas.
fn fold_list(name: &str, items: &[String]) -> String {
    let mut out = format!("{name}:");
    let mut line = out.len();
    for (index, item) in items.iter().enumerate() {
        let separator = if index + 1 < items.len() { "," } else { "" };
        if line + 1 + item.len() + separator.len() > 76 && line > name.len() + 1 {
            out.push_str("\r\n");
            line = 0;
        }
        out.push(' ');
        out.push_str(item);
        out.push_str(separator);
        line += 1 + item.len() + separator.len();
    }
    out.push_str("\r\n");
    out
}

fn boundary(label: &str) -> String {
    let mut random = [0u8; 12];
    getrandom::fill(&mut random).expect("the OS random source");
    format!("=_celld_{label}_{}", hex(&random))
}

/// Base64 with 76 column lines, each ended by CRLF.
fn wrapped_base64(bytes: &[u8]) -> String {
    let encoded = STANDARD.encode(bytes);
    let mut out = String::with_capacity(encoded.len() + encoded.len() / 76 * 2 + 2);
    for line in encoded.as_bytes().chunks(76) {
        out.push_str(std::str::from_utf8(line).expect("base64 is ASCII"));
        out.push_str("\r\n");
    }
    out
}

fn text_part(media_type: &str, body: &str) -> String {
    format!(
        "Content-Type: {media_type}; charset=utf-8\r\nContent-Transfer-Encoding: base64\r\n\r\n{}",
        wrapped_base64(body.as_bytes())
    )
}

fn is_token(text: &str) -> bool {
    !text.is_empty()
        && text
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || "!#$&^_.+-".contains(c))
}

fn attachment_part(attachment: &Attachment) -> Result<(String, bool), EmailError> {
    let invalid = |what: &str| EmailError::invalid(format!("an attachment has an invalid {what}"));
    let (kind, subtype) = attachment
        .media_type
        .split_once('/')
        .ok_or_else(|| invalid("type"))?;
    if !is_token(kind) || !is_token(subtype) || attachment.media_type.len() > 127 {
        return Err(invalid("type"));
    }
    let inline = match attachment.disposition.as_str() {
        "inline" => true,
        "attachment" => false,
        _ => return Err(invalid("disposition")),
    };
    if attachment.filename.is_empty()
        || attachment.filename.len() > 255
        || attachment.filename.chars().any(char::is_control)
    {
        return Err(invalid("filename"));
    }
    let content_id = match (&attachment.content_id, inline) {
        (Some(id), _) => {
            if id.is_empty()
                || id.len() > 255
                || !id
                    .chars()
                    .all(|c| c.is_ascii_alphanumeric() || "._@+-".contains(c))
            {
                return Err(invalid("contentId"));
            }
            Some(id)
        }
        (None, true) => return Err(invalid("contentId")),
        (None, false) => None,
    };
    let bytes = STANDARD
        .decode(attachment.content.as_bytes())
        .map_err(|_| invalid("content (it is not base64)"))?;
    let filename = if attachment.filename.is_ascii() {
        let mut quoted = String::new();
        for c in attachment.filename.chars() {
            if c == '"' || c == '\\' {
                quoted.push('\\');
            }
            quoted.push(c);
        }
        format!("filename=\"{quoted}\"")
    } else {
        let encoded: String = attachment
            .filename
            .bytes()
            .map(|byte| {
                if byte.is_ascii_alphanumeric() || b"._-".contains(&byte) {
                    (byte as char).to_string()
                } else {
                    format!("%{byte:02X}")
                }
            })
            .collect();
        format!("filename*=UTF-8''{encoded}")
    };
    let mut part = format!(
        "Content-Type: {}\r\nContent-Transfer-Encoding: base64\r\nContent-Disposition: {}; {filename}\r\n",
        attachment.media_type,
        if inline { "inline" } else { "attachment" },
    );
    if let Some(id) = content_id {
        part.push_str(&format!("Content-ID: <{id}>\r\n"));
    }
    part.push_str("\r\n");
    part.push_str(&wrapped_base64(&bytes));
    Ok((part, inline))
}

/// The `Content-Type` header and body of the message, from its text, its html
/// and its attachments.
fn mime_body(
    text: Option<String>,
    html: Option<String>,
    attachments: Vec<Attachment>,
) -> Result<Vec<u8>, EmailError> {
    let mut parts = Vec::new();
    for attachment in &attachments {
        parts.push(attachment_part(attachment)?);
    }
    let (inline, regular): (Vec<_>, Vec<_>) = parts.into_iter().partition(|(_, inline)| *inline);
    let multipart = |kind: &str, label: &str, parts: Vec<String>| {
        let boundary = boundary(label);
        let mut out = format!("Content-Type: multipart/{kind}; boundary=\"{boundary}\"\r\n\r\n");
        for part in parts {
            out.push_str(&format!("--{boundary}\r\n{part}"));
        }
        out.push_str(&format!("--{boundary}--\r\n"));
        out
    };
    let mut body = match (text, html) {
        (Some(text), Some(html)) => multipart(
            "alternative",
            "alt",
            vec![
                text_part("text/plain", &text),
                text_part("text/html", &html),
            ],
        ),
        (Some(text), None) => text_part("text/plain", &text),
        (None, Some(html)) => text_part("text/html", &html),
        (None, None) => unreachable!("a body was required"),
    };
    // A part of a multipart carries its headers after the boundary line, so a
    // nested multipart's own `Content-Type` line is already in place.
    if !inline.is_empty() {
        let mut parts = vec![body];
        parts.extend(inline.into_iter().map(|(part, _)| part));
        body = multipart("related", "rel", parts);
    }
    if !regular.is_empty() {
        let mut parts = vec![body];
        parts.extend(regular.into_iter().map(|(part, _)| part));
        body = multipart("mixed", "mix", parts);
    }
    Ok(body.into_bytes())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn binding() -> SendEmailBinding {
        SendEmailBinding {
            environment: "EMAIL".into(),
            ..Default::default()
        }
    }

    fn raw(
        binding: &SendEmailBinding,
        from: &str,
        to: &str,
        text: &str,
    ) -> Result<Prepared, EmailError> {
        prepare_raw(binding, from, to, text.as_bytes())
    }

    const MESSAGE: &str =
        "From: Ann <ann@example.com>\nTo: bob@example.com\nSubject: hi\n\nHello\n.dot\n";

    #[test]
    fn addresses_are_plain_ascii_and_cannot_carry_smtp_parameters() {
        for good in [
            "a@b.co",
            "first.last+tag@sub.example.com",
            "o'neil@example.org",
        ] {
            assert!(valid_address(good), "{good}");
        }
        for bad in [
            "",
            "a",
            "a@",
            "@b.co",
            "a b@c.d",
            "a@b c",
            "<a@b.co>",
            "a@b.co>",
            "a@b.co ",
            "a@b.co\r\nRCPT TO:<x@y.z>",
            "a\"b@c.d",
            "a@@b.co",
            "a..b@c.d",
            ".a@b.co",
            "a@-b.co",
            "a@b..co",
            "é@b.co",
            "a@é.co",
            "a,b@c.d",
        ] {
            assert!(!valid_address(bad), "{bad:?}");
        }
        assert!(!valid_address(&format!("{}@b.co", "a".repeat(65))));
        assert!(!valid_address(&format!("a@{}.co", "b".repeat(64))));
    }

    #[test]
    fn a_raw_message_is_normalised_and_gets_a_message_id_of_ours() {
        let message = format!("Message-ID: <theirs@x>\nBcc: hidden@example.com\n{MESSAGE}");
        let prepared = raw(&binding(), "ann@example.com", "bob@example.com", &message).unwrap();
        let text = String::from_utf8(prepared.data.clone()).unwrap();
        assert!(!text.replace("\r\n", "").contains(['\r', '\n']), "{text:?}");
        assert!(!text.contains("theirs@x"), "{text}");
        assert!(!text.contains("hidden@example.com"), "{text}");
        assert!(
            text.starts_with(&format!("Message-ID: <{}>\r\n", prepared.message_id)),
            "{text}"
        );
        assert!(prepared.message_id.ends_with("@example.com"));
        assert!(text.contains("\r\n\r\nHello\r\n.dot\r\n"), "{text:?}");
        assert_eq!(prepared.recipients, ["bob@example.com"]);
    }

    #[test]
    fn a_bare_cr_or_lf_cannot_hide_a_line_end() {
        let message = "From: ann@example.com\r\nSubject: x\r\n\rbody\r.\rQUIT\r";
        let prepared = raw(&binding(), "ann@example.com", "bob@example.com", message).unwrap();
        let text = String::from_utf8(prepared.data).unwrap();
        // Every line end is CRLF, so each `.` line start is one the sender
        // will stuff.
        assert!(!text.replace("\r\n", "").contains(['\r', '\n']), "{text:?}");
        assert!(text.contains("\r\n.\r\nQUIT\r\n"), "{text:?}");
    }

    #[test]
    fn the_from_header_must_carry_the_envelope_sender() {
        for header in [
            "From: eve@example.com",
            "From: ann@example.com, eve@example.com",
            "From: \"ann@example.com\" <eve@example.com>",
            "From: Ann@example.com",
        ] {
            let message = format!("{header}\nSubject: x\n\nbody");
            let error =
                raw(&binding(), "ann@example.com", "bob@example.com", &message).unwrap_err();
            assert!(
                error.message.contains("does not match"),
                "{header}: {error}"
            );
        }
        for header in [
            "From: ann@example.com",
            "From: \"Doe, Ann\" <ann@example.com>",
            "From: Ann Lee <ann@example.com>",
            "From: <ann@example.com>",
            "From: \"Ann (the one)\" <ann@example.com>  ",
        ] {
            let message = format!("{header}\nSubject: x\n\nbody");
            raw(&binding(), "ann@example.com", "bob@example.com", &message).unwrap();
        }
        let none = raw(
            &binding(),
            "ann@example.com",
            "bob@example.com",
            "Subject: x\n\nbody",
        );
        assert!(none.is_err());
        let two = raw(
            &binding(),
            "ann@example.com",
            "bob@example.com",
            "From: ann@example.com\nFrom: ann@example.com\n\nbody",
        );
        assert!(two.is_err());
    }

    #[test]
    fn a_from_with_a_comment_a_backslash_or_stray_text_is_refused() {
        // A mail client reads a quoted pair inside a comment, so `\)` does not
        // end it; a relay and this check must not disagree about the sender.
        for header in [
            "From: Ann (x\\) <eve@example.com> (y) <ann@example.com>",
            "From: Ann (the one) <ann@example.com>",
            "From: ann@example.com (Ann)",
            "From: Ann (a (nested) one) <ann@example.com>",
            "From: Ann\\ <ann@example.com>",
            "From: \"Ann \\\" <eve@example.com>\" <ann@example.com>",
            "From: Ann <ann@example.com> junk",
            "From: Ann <ann@example.com> <eve@example.com>",
            "From: Ann <ann@example.com> (late comment)",
            "From: a@b <ann@example.com>",
            "From: Ann < <ann@example.com>",
            "From: \"Ann\" x <ann@example.com>",
            "From: Ann \"Lee\" <ann@example.com>",
            "From: Ann: <ann@example.com>",
        ] {
            let message = format!("{header}\nSubject: x\n\nbody");
            assert!(
                raw(&binding(), "ann@example.com", "bob@example.com", &message).is_err(),
                "{header}"
            );
        }
    }

    #[test]
    fn a_received_header_and_oversized_parts_are_refused() {
        let received = format!("Received: from x\n{MESSAGE}");
        assert!(raw(&binding(), "ann@example.com", "bob@example.com", &received).is_err());
        let long_line = format!(
            "From: ann@example.com\nX-Long: {}\n\nbody",
            "a".repeat(1000)
        );
        let error = raw(&binding(), "ann@example.com", "bob@example.com", &long_line).unwrap_err();
        assert_eq!(error.code, "E_HEADERS_TOO_LARGE");
        let big = vec![b'a'; MAX_MESSAGE_BYTES + 1];
        let error =
            prepare_raw(&binding(), "ann@example.com", "bob@example.com", &big).unwrap_err();
        assert_eq!(error.code, "E_CONTENT_TOO_LARGE");
        // Normalising adds a byte per bare line end, which may push a message over.
        let mut lines = b"From: ann@example.com\n\n".to_vec();
        lines.extend(std::iter::repeat_n(b'\n', MAX_MESSAGE_BYTES - lines.len()));
        let error =
            prepare_raw(&binding(), "ann@example.com", "bob@example.com", &lines).unwrap_err();
        assert_eq!(error.code, "E_CONTENT_TOO_LARGE");
        let nul = prepare_raw(
            &binding(),
            "ann@example.com",
            "bob@example.com",
            b"From: ann@example.com\n\na\0b",
        );
        assert!(nul.is_err());
    }

    #[test]
    fn an_envelope_address_with_a_line_break_or_bracket_is_refused() {
        for bad in [
            "bob@example.com\r\nRCPT TO:<x@y.z>",
            "<bob@example.com>",
            "bob@example.com>\r\n",
        ] {
            let error = raw(&binding(), "ann@example.com", bad, MESSAGE).unwrap_err();
            assert_eq!(error.code, "E_VALIDATION_ERROR", "{bad:?}");
            let error = raw(&binding(), bad, "bob@example.com", MESSAGE).unwrap_err();
            assert_eq!(error.code, "E_VALIDATION_ERROR", "{bad:?}");
        }
    }

    #[test]
    fn restrictions_apply_to_the_envelope_exactly_as_written() {
        let only = SendEmailBinding {
            destination_address: Some("ops@example.com".into()),
            ..binding()
        };
        raw(&only, "ann@example.com", "ops@example.com", MESSAGE).unwrap();
        let error = raw(&only, "ann@example.com", "bob@example.com", MESSAGE).unwrap_err();
        assert_eq!(error.message, "email to bob@example.com not allowed");
        let listed = SendEmailBinding {
            allowed_destination_addresses: Some(vec!["bob@example.com".into()]),
            allowed_sender_addresses: Some(vec!["ann@example.com".into()]),
            ..binding()
        };
        raw(&listed, "ann@example.com", "bob@example.com", MESSAGE).unwrap();
        assert!(raw(&listed, "ann@example.com", "BOB@example.com", MESSAGE).is_err());
        let error = raw(
            &listed,
            "eve@example.com",
            "bob@example.com",
            "From: eve@example.com\n\nx",
        )
        .unwrap_err();
        assert_eq!(error.message, "email from eve@example.com not allowed");
    }

    #[test]
    fn a_manifest_binding_round_trips_and_refuses_what_a_deploy_would() {
        let full = SendEmailBinding {
            environment: "EMAIL".into(),
            destination_address: None,
            allowed_destination_addresses: Some(vec!["a@b.co".into()]),
            allowed_sender_addresses: Some(vec!["c@d.co".into()]),
        };
        assert_eq!(
            SendEmailBinding::from_manifest(&full.to_manifest()).unwrap(),
            full
        );
        let plain = SendEmailBinding {
            environment: "E".into(),
            ..Default::default()
        };
        assert_eq!(
            SendEmailBinding::from_manifest(&plain.to_manifest()).unwrap(),
            plain
        );
        let bad = |change: &dyn Fn(&mut Value)| {
            let mut entry = full.to_manifest();
            change(&mut entry);
            SendEmailBinding::from_manifest(&entry)
                .unwrap_err()
                .to_string()
        };
        assert!(bad(&|e| e["extra"] = json!(1)).contains("`extra`"));
        assert!(bad(&|e| e["name"] = json!("__proto__")).contains("__proto__"));
        assert!(bad(&|e| e["name"] = json!("bad name")).contains("invalid"));
        assert!(bad(&|e| e["destination_address"] = json!("a@b.co")).contains("use one"));
        assert!(
            bad(&|e| e["allowed_sender_addresses"] = json!(["not an address"])).contains("invalid")
        );
        assert!(bad(&|e| e["allowed_sender_addresses"] = json!("a@b.co")).contains("array"));
    }

    fn builder(extra: Value) -> Value {
        let mut base = json!({
            "from": "ann@example.com", "to": "bob@example.com", "subject": "Hi", "text": "Hello"
        });
        for (key, value) in extra.as_object().unwrap() {
            base[key] = value.clone();
        }
        base
    }

    fn build(extra: Value) -> Result<Prepared, EmailError> {
        prepare_builder(&binding(), builder(extra))
    }

    fn text_of(prepared: &Prepared) -> String {
        String::from_utf8(prepared.data.clone()).unwrap()
    }

    #[test]
    fn the_parts_of_a_builder_are_summed_before_anything_is_encoded() {
        crate::asyncrt::install_test_host_runtime();
        // Each part is under the limit and the sum is over it. The content is
        // not base64, so a message that reached the encoder would fail there.
        let half = "a".repeat(MAX_MESSAGE_BYTES / 2 + 1);
        let error = build(json!({
            "text": half.clone(),
            "attachments": [{
                "content": "!not base64!".repeat(MAX_MESSAGE_BYTES / 24 + 1),
                "filename": "a.bin", "type": "application/octet-stream",
                "disposition": "attachment"
            }]
        }))
        .unwrap_err();
        assert_eq!(error.code, "E_CONTENT_TOO_LARGE", "{error:?}");
        let error = build(json!({ "text": half.clone(), "html": half })).unwrap_err();
        assert_eq!(error.code, "E_CONTENT_TOO_LARGE", "{error:?}");
    }

    #[test]
    fn a_builder_message_has_headers_a_body_and_an_envelope() {
        crate::asyncrt::install_test_host_runtime();
        let prepared = build(json!({
            "from": { "name": "Ann \"A\" Lee", "email": "ann@example.com" },
            "to": [{ "name": "Bob", "email": "bob@example.com" }, "carol@example.com"],
            "cc": "dan@example.com", "bcc": ["eve@example.com"],
            "replyTo": "reply@example.com", "subject": "Gr\u{fc}\u{df}e",
            "headers": { "X-Case-ID": "12", "In-Reply-To": "<a@b>" }
        }))
        .unwrap();
        let text = text_of(&prepared);
        assert_eq!(
            prepared.recipients,
            [
                "bob@example.com",
                "carol@example.com",
                "dan@example.com",
                "eve@example.com"
            ]
        );
        assert!(
            text.contains("From: \"Ann \\\"A\\\" Lee\" <ann@example.com>\r\n"),
            "{text}"
        );
        assert!(
            text.contains("To: \"Bob\" <bob@example.com>, carol@example.com\r\n"),
            "{text}"
        );
        assert!(text.contains("Cc: dan@example.com\r\n"), "{text}");
        assert!(
            !text.contains("eve@example.com"),
            "bcc leaked into the message: {text}"
        );
        assert!(text.contains("Reply-To: reply@example.com\r\n"), "{text}");
        assert!(text.contains("Subject: =?UTF-8?B?"), "{text}");
        assert!(
            text.contains("X-Case-ID: 12\r\n") && text.contains("In-Reply-To: <a@b>\r\n"),
            "{text}"
        );
        assert!(
            text.contains("Content-Type: text/plain; charset=utf-8\r\n"),
            "{text}"
        );
        assert!(text.contains("\r\n\r\nSGVsbG8=\r\n"), "{text}");
        assert!(text.contains(&format!("Message-ID: <{}>", prepared.message_id)));
        assert!(!text.replace("\r\n", "").contains(['\r', '\n']));
    }

    #[test]
    fn text_and_html_and_attachments_nest_as_mime_parts() {
        crate::asyncrt::install_test_host_runtime();
        let prepared = build(json!({
            "html": "<p>x</p>",
            "attachments": [
                { "content": "aGk=", "filename": "a.txt", "type": "text/plain", "disposition": "attachment" },
                { "content": "aGk=", "filename": "p\u{e9}.png", "type": "image/png", "disposition": "inline", "contentId": "logo" }
            ]
        }))
        .unwrap();
        let text = text_of(&prepared);
        for needle in [
            "multipart/mixed",
            "multipart/related",
            "multipart/alternative",
            "text/html",
            "filename=\"a.txt\"",
            "filename*=UTF-8''p%C3%A9.png",
            "Content-ID: <logo>",
        ] {
            assert!(text.contains(needle), "{needle} in {text}");
        }
        assert_eq!(text.matches("multipart/mixed").count(), 1);
    }

    #[test]
    fn a_builder_message_is_refused_for_what_it_leaves_out_or_overreaches() {
        crate::asyncrt::install_test_host_runtime();
        let code = |extra: Value| build(extra).unwrap_err().code;
        assert_eq!(code(json!({ "to": null })), "E_FIELD_MISSING");
        assert_eq!(code(json!({ "text": null })), "E_FIELD_MISSING");
        assert_eq!(code(json!({ "to": "bad" })), "E_VALIDATION_ERROR");
        assert_eq!(
            code(json!({ "subject": "a\r\nBcc: x@y.z" })),
            "E_VALIDATION_ERROR"
        );
        assert_eq!(
            code(json!({ "subject": "a".repeat(999) })),
            "E_VALIDATION_ERROR"
        );
        assert_eq!(code(json!({ "unknown": 1 })), "E_VALIDATION_ERROR");
        // A line a server would not read is refused, as it is in a raw message:
        // a word without a space cannot be folded.
        assert_eq!(
            code(json!({ "subject": "a".repeat(990) })),
            "E_HEADERS_TOO_LARGE"
        );
        assert_eq!(
            code(json!({ "headers": { "X-A": "a".repeat(1500) } })),
            "E_HEADERS_TOO_LARGE"
        );
        assert_eq!(
            code(json!({ "from": { "name": "n".repeat(257), "email": "ann@example.com" } })),
            "E_VALIDATION_ERROR"
        );
        assert_eq!(
            code(json!({ "to": [{ "name": "n".repeat(257), "email": "bob@example.com" }] })),
            "E_VALIDATION_ERROR"
        );
        let spaced = format!("{} end", "word ".repeat(150));
        build(json!({ "subject": spaced })).unwrap();
        let many: Vec<String> = (0..51).map(|i| format!("u{i}@example.com")).collect();
        assert_eq!(code(json!({ "to": many })), "E_TOO_MANY_RECIPIENTS");
        let files: Vec<Value> = (0..33)
            .map(|_| json!({ "content": "aGk=", "filename": "a", "type": "text/plain", "disposition": "attachment" }))
            .collect();
        assert_eq!(
            code(json!({ "attachments": files })),
            "E_TOO_MANY_ATTACHMENTS"
        );
        for (attachment, why) in [
            (
                json!({ "content": "!!", "filename": "a", "type": "text/plain", "disposition": "attachment" }),
                "base64",
            ),
            (
                json!({ "content": "aGk=", "filename": "a\r\nb", "type": "text/plain", "disposition": "attachment" }),
                "filename",
            ),
            (
                json!({ "content": "aGk=", "filename": "a", "type": "text", "disposition": "attachment" }),
                "type",
            ),
            (
                json!({ "content": "aGk=", "filename": "a", "type": "x/y", "disposition": "inline" }),
                "contentId",
            ),
        ] {
            assert_eq!(
                code(json!({ "attachments": [attachment] })),
                "E_VALIDATION_ERROR",
                "{why}"
            );
        }
    }

    #[test]
    fn custom_headers_follow_the_allowlist() {
        crate::asyncrt::install_test_host_runtime();
        let code = |headers: Value| build(json!({ "headers": headers })).unwrap_err().code;
        assert_eq!(code(json!({ "Date": "x" })), "E_HEADER_NOT_ALLOWED");
        assert_eq!(code(json!({ "Received": "x" })), "E_HEADER_NOT_ALLOWED");
        assert_eq!(code(json!({ "ARC-Seal": "x" })), "E_HEADER_NOT_ALLOWED");
        assert_eq!(code(json!({ "Content-Type": "x" })), "E_HEADER_NOT_ALLOWED");
        assert_eq!(
            code(json!({ "X-Anything": "ok", "Made-Up": "x" })),
            "E_HEADER_NOT_ALLOWED"
        );
        assert_eq!(code(json!({ "From": "x@y.z" })), "E_HEADER_USE_API_FIELD");
        assert_eq!(code(json!({ "Bcc": "x@y.z" })), "E_HEADER_USE_API_FIELD");
        assert_eq!(code(json!({ "Bad Header!": "x" })), "E_HEADER_NAME_INVALID");
        assert_eq!(
            code(json!({ "X-A": "line\r\nBcc: x@y.z" })),
            "E_HEADER_VALUE_INVALID"
        );
        assert_eq!(code(json!({ "X-A": "" })), "E_HEADER_VALUE_INVALID");
        assert_eq!(
            code(json!({ "X-A": "a".repeat(2049) })),
            "E_HEADER_VALUE_INVALID"
        );
        let many: serde_json::Map<String, Value> = (0..21)
            .map(|i| (format!("Comments{i}"), json!("x")))
            .collect();
        assert_eq!(code(Value::Object(many)), "E_HEADER_NOT_ALLOWED");
        build(json!({ "headers": { "Comments": "ok", "x-lower": "ok" } })).unwrap();
    }

    #[test]
    fn a_builder_message_obeys_the_binding_restrictions_for_every_recipient() {
        crate::asyncrt::install_test_host_runtime();
        let only = SendEmailBinding {
            destination_address: Some("ops@example.com".into()),
            ..binding()
        };
        // With no recipient, the one destination is used.
        let sent = prepare_builder(&only, builder(json!({ "to": null }))).unwrap();
        assert_eq!(sent.recipients, ["ops@example.com"]);
        assert!(text_of(&sent).contains("To: ops@example.com\r\n"));
        // A cc or bcc is a recipient too.
        for field in ["to", "cc", "bcc"] {
            let error = prepare_builder(
                &only,
                builder(json!({ "to": "ops@example.com", field: "eve@example.com" })),
            )
            .unwrap_err();
            assert_eq!(error.code, "E_RECIPIENT_NOT_ALLOWED", "{field}");
        }
        let senders = SendEmailBinding {
            allowed_sender_addresses: Some(vec!["other@example.com".into()]),
            ..binding()
        };
        let error = prepare_builder(&senders, builder(json!({}))).unwrap_err();
        assert_eq!(error.code, "E_SENDER_NOT_VERIFIED");
    }
}
