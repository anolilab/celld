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
//!   sees. Its `Message-ID` is replaced by one celld makes, its `Bcc:` and
//!   `Resent-Bcc:` headers are dropped, and a `Received:` header is refused.

use anyhow::{anyhow, bail};
use base64::engine::general_purpose::STANDARD;
use base64::Engine as _;
use serde::Deserialize;
use serde_json::Value;

/// The `type` of a `send_email` binding in a manifest.
pub const BINDING_TYPE: &str = "send_email";

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
            "type": BINDING_TYPE,
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

/// Refuses two bindings of one name. `env` holds one, and the other's
/// restrictions would be ignored without a word.
pub fn ensure_unique(bindings: &[SendEmailBinding]) -> anyhow::Result<()> {
    let mut seen = std::collections::BTreeSet::new();
    for binding in bindings {
        anyhow::ensure!(
            seen.insert(binding.environment.as_str()),
            "send_email binding {} is declared twice; every name in `env` must be unique",
            binding.environment
        );
    }
    Ok(())
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
    /// A byte of the message is 8-bit, so the relay must take `8BITMIME`.
    pub eight_bit: bool,
    /// A header holds a non-ASCII byte, so the relay must take `SMTPUTF8`.
    pub smtputf8: bool,
}

impl Prepared {
    pub fn new(from: String, recipients: Vec<String>, data: Vec<u8>, message_id: String) -> Self {
        let head_end = data
            .windows(4)
            .position(|window| window == b"\r\n\r\n")
            .unwrap_or(data.len());
        Self {
            from,
            recipients,
            eight_bit: !data.is_ascii(),
            smtputf8: !data[..head_end].is_ascii(),
            data,
            message_id,
        }
    }
}

fn new_message_id(from: &str) -> String {
    let mut random = [0u8; 16];
    getrandom::fill(&mut random).expect("the OS random source");
    let domain = from
        .rsplit_once('@')
        .map_or("localhost", |(_, domain)| domain);
    format!("{}@{domain}", hex(&random))
}

pub(crate) fn too_large() -> EmailError {
    EmailError::new(
        "E_CONTENT_TOO_LARGE",
        format!("the message exceeds {MAX_MESSAGE_BYTES} bytes"),
    )
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

mod mime;
mod raw;

pub use mime::prepare_builder;
pub use raw::prepare_raw;

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    pub(super) fn binding() -> SendEmailBinding {
        SendEmailBinding {
            environment: "EMAIL".into(),
            ..Default::default()
        }
    }

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
}
