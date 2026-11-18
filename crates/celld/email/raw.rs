// Copyright 2026 Deno Land Inc. Apache-2.0 license.

//! The raw form: a finished RFC 5322 message with its envelope.

use super::*;

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
    // A line longer than RFC 5321 allows is refused, not folded: folding a
    // body would change it.
    if body
        .split(|byte| *byte == b'\n')
        .any(|line| line.strip_suffix(b"\r").unwrap_or(line).len() > MAX_LINE_BYTES)
    {
        return Err(EmailError::new(
            "E_LINE_TOO_LONG",
            format!("a line of the message body exceeds {MAX_LINE_BYTES} bytes"),
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
    // A mail client shows Sender: and the Resent-* headers next to or instead
    // of From:, so each must name the envelope sender too.
    for header in headers.iter().filter(|header| {
        ["sender", "resent-from", "resent-sender"]
            .iter()
            .any(|name| header.name_is(name))
    }) {
        if single_address(&header.value).as_deref() != Some(from) {
            return Err(EmailError::invalid(format!(
                "{}: header does not match mail from",
                header.name
            )));
        }
    }
    if headers.iter().any(|header| header.name_is("received")) {
        return Err(EmailError::invalid("invalid headers set"));
    }
    let message_id = new_message_id(from);
    let mut data = Vec::with_capacity(message.len() + 64);
    data.extend_from_slice(format!("Message-ID: <{message_id}>\r\n").as_bytes());
    for header in &headers {
        if !["message-id", "bcc", "resent-bcc"]
            .iter()
            .any(|name| header.name_is(name))
        {
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
        from.to_string(),
        vec![to.to_string()],
        data,
        message_id,
    ))
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::email::tests::binding;

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
    fn a_raw_message_is_normalised_and_gets_a_message_id_of_ours() {
        let message = format!(
            "Message-ID: <theirs@x>\nBcc: hidden@example.com\nResent-Bcc: resent@example.com\n{MESSAGE}"
        );
        let prepared = raw(&binding(), "ann@example.com", "bob@example.com", &message).unwrap();
        let text = String::from_utf8(prepared.data.clone()).unwrap();
        assert!(!text.replace("\r\n", "").contains(['\r', '\n']), "{text:?}");
        assert!(!text.contains("theirs@x"), "{text}");
        assert!(!text.contains("hidden@example.com"), "{text}");
        assert!(!text.contains("resent@example.com"), "{text}");
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
    fn sender_and_resent_headers_must_name_the_envelope_sender_too() {
        for header in [
            "Sender: eve@example.com",
            "sender: Eve <eve@example.com>",
            "Resent-From: eve@example.com",
            "Resent-Sender: eve@example.com",
            "Resent-From: ann@example.com, eve@example.com",
            "Sender:",
        ] {
            let message = format!("From: ann@example.com\n{header}\nSubject: x\n\nbody");
            let error =
                raw(&binding(), "ann@example.com", "bob@example.com", &message).unwrap_err();
            assert!(
                error.message.contains("does not match"),
                "{header}: {error}"
            );
        }
        let message = "From: ann@example.com\nSender: Ann <ann@example.com>\n\
             Resent-From: ann@example.com\nResent-To: bob@example.com\nReply-To: eve@example.com\n\nbody";
        raw(&binding(), "ann@example.com", "bob@example.com", message).unwrap();
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
    fn a_body_line_over_998_bytes_is_refused_and_8_bit_content_is_flagged() {
        let head = "From: ann@example.com\n\n";
        let ok = raw(
            &binding(),
            "ann@example.com",
            "bob@example.com",
            &format!("{head}{}\n", "x".repeat(998)),
        )
        .unwrap();
        assert!(!ok.eight_bit && !ok.smtputf8);
        for line_end in ["\n", "\r\n", ""] {
            let long = format!("{head}ok\n{}{line_end}", "x".repeat(999));
            let error = raw(&binding(), "ann@example.com", "bob@example.com", &long).unwrap_err();
            assert_eq!(error.code, "E_LINE_TOO_LONG", "{line_end:?}");
        }
        let body = raw(
            &binding(),
            "ann@example.com",
            "bob@example.com",
            &format!("{head}caf\u{e9}\n"),
        )
        .unwrap();
        assert!(body.eight_bit && !body.smtputf8);
        let header = raw(
            &binding(),
            "ann@example.com",
            "bob@example.com",
            "From: ann@example.com\nSubject: caf\u{e9}\n\nbody\n",
        )
        .unwrap();
        assert!(header.eight_bit && header.smtputf8);
    }
}
