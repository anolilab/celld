// Copyright 2026 Deno Land Inc. Apache-2.0 license.

//! The builder form: fields turned into a MIME message.

use super::*;

const MAX_CUSTOM_HEADERS: usize = 20;
const MAX_CUSTOM_HEADER_VALUE_BYTES: usize = 2048;
const MAX_CUSTOM_HEADERS_BYTES: usize = 16 * 1024;
const MAX_SUBJECT_CHARS: usize = 998;
const MAX_DISPLAY_NAME_CHARS: usize = 256;

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
    use crate::email::tests::binding;
    use serde_json::json;

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
