// Copyright 2026 Deno Land Inc. Apache-2.0 license.

//! The `send_email` of a config, checked and written as manifest bindings.

use crate::email::SendEmailBinding;
use anyhow::{anyhow, bail};
use serde_json::Value;

/// The `send_email` of a config as manifest bindings.
///
/// Cloudflare's four keys are kept: `name`, `destination_address`,
/// `allowed_destination_addresses` and `allowed_sender_addresses`. Every other
/// key stops the deploy, and so does `destination_address` together with
/// `allowed_destination_addresses`, which are alternatives. The binding is
/// built through the manifest reader, so a deploy writes only what a node
/// accepts.
pub(super) fn read_send_email(value: Option<&Value>) -> anyhow::Result<Vec<Value>> {
    let entries = match value {
        None => &[][..],
        Some(Value::Array(entries)) => entries.as_slice(),
        Some(_) => bail!("config `send_email` must be an array"),
    };
    let mut bindings = Vec::new();
    for entry in entries {
        let object = entry
            .as_object()
            .ok_or_else(|| anyhow!("a `send_email` entry must be an object"))?;
        if let Some(key) = object.keys().find(|key| {
            !matches!(
                key.as_str(),
                "name"
                    | "destination_address"
                    | "allowed_destination_addresses"
                    | "allowed_sender_addresses"
            )
        }) {
            bail!(
                "send_email declares `{key}`, which celld does not model; celld models `name`, \
                 `destination_address`, `allowed_destination_addresses` and `allowed_sender_addresses`"
            );
        }
        let name = object
            .get("name")
            .and_then(Value::as_str)
            .ok_or_else(|| anyhow!("send_email has no `name`"))?;
        if !super::valid_binding(name) {
            bail!("invalid send_email binding name: {name:?}");
        }
        let mut manifest = entry.clone();
        manifest["type"] = Value::from("send_email");
        // The one reader of the shape, shared with the node.
        let binding = SendEmailBinding::from_manifest(&manifest)?;
        bindings.push(binding.to_manifest());
    }
    Ok(bindings)
}

#[cfg(test)]
mod send_email_config_tests {
    use super::*;
    use serde_json::json;

    fn read(entry: Value) -> anyhow::Result<Vec<Value>> {
        read_send_email(Some(&json!([entry])))
    }

    fn refused(entry: Value) -> String {
        format!("{:#}", read(entry).unwrap_err())
    }

    #[test]
    fn every_documented_shape_becomes_a_manifest_binding() {
        assert!(read_send_email(None).unwrap().is_empty());
        assert_eq!(
            read(json!({ "name": "EMAIL" })).unwrap(),
            [json!({ "type": "send_email", "name": "EMAIL" })]
        );
        assert_eq!(
            read(json!({ "name": "OPS", "destination_address": "ops@example.com" })).unwrap(),
            [
                json!({ "type": "send_email", "name": "OPS", "destination_address": "ops@example.com" })
            ]
        );
        let full = read(json!({
            "name": "TEAM",
            "allowed_destination_addresses": ["a@example.com", "b@example.com"],
            "allowed_sender_addresses": ["noreply@example.com"]
        }))
        .unwrap();
        assert_eq!(full[0]["allowed_destination_addresses"][1], "b@example.com");
        assert_eq!(
            full[0]["allowed_sender_addresses"][0],
            "noreply@example.com"
        );
    }

    #[test]
    fn a_malformed_entry_stops_the_deploy() {
        assert!(refused(json!({ "name": "E", "remote": true })).contains("`remote`"));
        assert!(refused(json!({})).contains("no `name`"));
        assert!(refused(json!({ "name": "bad name" })).contains("invalid send_email binding name"));
        assert!(refused(json!({ "name": "__proto__" })).contains("__proto__"));
        assert!(
            refused(json!({ "name": "E", "destination_address": "not an address" }))
                .contains("invalid")
        );
        assert!(refused(json!({ "name": "E", "destination_address": 5 })).contains("invalid"));
        assert!(
            refused(json!({ "name": "E", "allowed_sender_addresses": "a@b.co" })).contains("array")
        );
        assert!(refused(json!({
            "name": "E", "destination_address": "a@b.co", "allowed_destination_addresses": ["a@b.co"]
        }))
        .contains("use one"));
        assert!(read_send_email(Some(&json!({}))).is_err());
        assert!(read_send_email(Some(&json!(["EMAIL"]))).is_err());
    }
}
