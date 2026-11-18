// Copyright 2026 Deno Land Inc. Apache-2.0 license.

//! The `ratelimits` of a config, checked and written as manifest bindings.

use crate::engine_api::RateLimitBinding;
use anyhow::{anyhow, bail};
use serde_json::Value;

/// The `ratelimits` of a config as manifest bindings.
///
/// Wrangler's own checks, applied here so a malformed entry stops the deploy
/// and never reaches a node: `simple.limit` a positive integer, `simple.period`
/// 10 or 60, and every key celld does not model refused.
pub(super) fn read_ratelimits(value: Option<&Value>) -> anyhow::Result<Vec<Value>> {
    let ratelimits = match value {
        None => &[][..],
        Some(Value::Array(ratelimits)) => ratelimits.as_slice(),
        Some(_) => bail!("config `ratelimits` must be an array"),
    };
    let mut bindings = Vec::new();
    for ratelimit in ratelimits {
        let object = ratelimit
            .as_object()
            .ok_or_else(|| anyhow!("a `ratelimits` entry must be an object"))?;
        if let Some(key) = object
            .keys()
            .find(|key| !matches!(key.as_str(), "name" | "namespace_id" | "simple"))
        {
            bail!(
                "ratelimit declares `{key}`, which celld does not model; \
                 celld models `name`, `namespace_id`, and `simple`"
            );
        }
        let name = object
            .get("name")
            .and_then(Value::as_str)
            .ok_or_else(|| anyhow!("ratelimit has no `name`"))?;
        if !super::valid_binding(name) {
            bail!("invalid ratelimit binding name: {name:?}");
        }
        // Wrangler writes the id as a string of digits; celld takes any stable
        // string that can name a cell, as it does for a KV namespace.
        let namespace_id = object
            .get("namespace_id")
            .and_then(Value::as_str)
            .ok_or_else(|| anyhow!("ratelimit {name} needs a string `namespace_id`"))?;
        if !celld_logic::cell::valid_cell_scope(namespace_id) {
            bail!(
                "ratelimit {name} has a `namespace_id` that cannot name a cell: \
                 {namespace_id:?}; use ASCII letters, digits, and `_ - . : $`"
            );
        }
        let simple = object
            .get("simple")
            .and_then(Value::as_object)
            .ok_or_else(|| {
                anyhow!("ratelimit {name} needs a `simple` object with `limit` and `period`")
            })?;
        if let Some(key) = simple
            .keys()
            .find(|key| !matches!(key.as_str(), "limit" | "period"))
        {
            bail!("ratelimit {name} `simple` declares `{key}`; it takes `limit` and `period`");
        }
        let limit = simple
            .get("limit")
            .and_then(Value::as_u64)
            .filter(|limit| celld_logic::ratelimit::valid_limit(*limit))
            .and_then(|limit| u32::try_from(limit).ok())
            .ok_or_else(|| anyhow!("ratelimit {name} `simple.limit` must be a positive integer"))?;
        let period = simple
            .get("period")
            .and_then(Value::as_u64)
            .filter(|period| celld_logic::ratelimit::valid_period(*period))
            .and_then(|period| u32::try_from(period).ok())
            .ok_or_else(|| anyhow!("ratelimit {name} `simple.period` must be 10 or 60"))?;
        bindings.push(
            RateLimitBinding {
                environment: name.to_string(),
                namespace: namespace_id.to_string(),
                limit,
                period,
            }
            .to_manifest(),
        );
    }
    Ok(bindings)
}

#[cfg(test)]
mod ratelimit_config_tests {
    use super::*;
    use serde_json::json;

    fn read(entry: Value) -> anyhow::Result<Vec<Value>> {
        read_ratelimits(Some(&json!([entry])))
    }

    fn refused(entry: Value) -> String {
        format!("{:#}", read(entry).unwrap_err())
    }

    #[test]
    fn a_valid_entry_becomes_a_manifest_binding() {
        let bindings = read(json!({
            "name": "LIMITER", "namespace_id": "1001", "simple": { "limit": 5, "period": 60 }
        }))
        .unwrap();
        assert_eq!(
            bindings,
            [json!({
                "type": "ratelimit", "name": "LIMITER", "namespace_id": "1001",
                "simple": { "limit": 5, "period": 60 }
            })]
        );
        assert!(read_ratelimits(None).unwrap().is_empty());
    }

    #[test]
    fn limit_must_be_a_positive_integer() {
        for limit in [json!(0), json!(-1), json!(1.5), json!("5"), json!(null)] {
            let entry = json!({
                "name": "L", "namespace_id": "1", "simple": { "limit": limit, "period": 10 }
            });
            assert!(refused(entry).contains("`simple.limit`"), "{limit}");
        }
    }

    #[test]
    fn period_must_be_ten_or_sixty() {
        for period in [json!(0), json!(30), json!(61), json!("10"), json!(null)] {
            let entry = json!({
                "name": "L", "namespace_id": "1", "simple": { "limit": 1, "period": period }
            });
            assert!(refused(entry).contains("`simple.period`"), "{period}");
        }
        assert!(read(json!({
            "name": "L", "namespace_id": "1", "simple": { "limit": 1, "period": 10 }
        }))
        .is_ok());
    }

    #[test]
    fn a_malformed_entry_is_refused_with_its_reason() {
        let simple = json!({ "limit": 1, "period": 10 });
        assert!(refused(json!({ "namespace_id": "1", "simple": simple })).contains("no `name`"));
        assert!(
            refused(json!({ "name": "bad name", "namespace_id": "1", "simple": simple }))
                .contains("invalid ratelimit binding name")
        );
        assert!(refused(json!({ "name": "L", "simple": simple })).contains("`namespace_id`"));
        assert!(
            refused(json!({ "name": "L", "namespace_id": 1001, "simple": simple }))
                .contains("`namespace_id`")
        );
        assert!(
            refused(json!({ "name": "L", "namespace_id": "a/b", "simple": simple }))
                .contains("cannot name a cell")
        );
        assert!(refused(json!({ "name": "L", "namespace_id": "1" })).contains("`simple`"));
        assert!(
            refused(json!({ "name": "L", "namespace_id": "1", "simple": simple, "burst": 3 }))
                .contains("`burst`")
        );
        assert!(refused(json!({
            "name": "L", "namespace_id": "1", "simple": { "limit": 1, "period": 10, "burst": 3 }
        }))
        .contains("`burst`"));
        assert!(read_ratelimits(Some(&json!({})))
            .unwrap_err()
            .to_string()
            .contains("must be an array"));
    }
}
