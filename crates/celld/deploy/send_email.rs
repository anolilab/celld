// Copyright 2026 Deno Land Inc. Apache-2.0 license.

//! The `send_email` of a config, checked and written as manifest bindings.

use crate::email::SendEmailBinding;
use serde_json::Value;

/// The `send_email` of a config as manifest bindings.
///
/// Cloudflare's four keys are kept: `name`, `destination_address`,
/// `allowed_destination_addresses` and `allowed_sender_addresses`. Every other
/// key stops the deploy, and so does `destination_address` together with
/// `allowed_destination_addresses`, which are alternatives. The binding is
/// built through the manifest reader, so a deploy writes only what a node
/// accepts.
pub(super) fn read_send_email(
    object: &serde_json::Map<String, Value>,
) -> anyhow::Result<Vec<Value>> {
    let mut bindings = Vec::new();
    for entry in super::config_entries(
        object,
        "send_email",
        &[
            "name",
            "destination_address",
            "allowed_destination_addresses",
            "allowed_sender_addresses",
        ],
    )? {
        super::entry_text(entry, "send_email", "name")?;
        let mut manifest = Value::Object(entry.clone());
        manifest["type"] = Value::from(crate::email::BINDING_TYPE);
        // The one reader of the shape, shared with the node.
        bindings.push(SendEmailBinding::from_manifest(&manifest)?);
    }
    crate::email::ensure_unique(&bindings)?;
    Ok(bindings.iter().map(SendEmailBinding::to_manifest).collect())
}

#[cfg(test)]
mod send_email_config_tests {
    use super::*;
    use serde_json::json;

    fn config(value: Value) -> serde_json::Map<String, Value> {
        let mut object = serde_json::Map::new();
        object.insert("send_email".into(), value);
        object
    }

    fn read(entry: Value) -> anyhow::Result<Vec<Value>> {
        read_send_email(&config(json!([entry])))
    }

    fn refused_all(entries: Value) -> String {
        format!("{:#}", read_send_email(&config(entries)).unwrap_err())
    }

    fn refused(entry: Value) -> String {
        format!("{:#}", read(entry).unwrap_err())
    }

    #[test]
    fn every_documented_shape_becomes_a_manifest_binding() {
        assert!(read_send_email(&serde_json::Map::new()).unwrap().is_empty());
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
        assert!(refused_all(
            json!([{ "name": "E" }, { "name": "E", "destination_address": "a@b.co" }])
        )
        .contains("declared twice"));
        assert!(read_send_email(&config(json!({}))).is_err());
        assert!(read_send_email(&config(json!(["EMAIL"]))).is_err());
    }

    #[test]
    fn an_asset_only_project_cannot_declare_one() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::create_dir(dir.path().join("public")).unwrap();
        let path = dir.path().join("wrangler.jsonc");
        std::fs::write(
            &path,
            r#"{ "name": "app", "assets": { "directory": "public" },
                "send_email": [{ "name": "EMAIL" }] }"#,
        )
        .unwrap();
        let error = crate::deploy::read_project(
            &path,
            dir.path(),
            &crate::deploy::VarOverrides::None,
            false,
        )
        .err()
        .expect("refused");
        assert!(
            error.to_string().contains("cannot declare Worker bindings"),
            "{error:#}"
        );
    }
}
