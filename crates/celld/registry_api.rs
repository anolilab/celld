// Copyright 2026 Deno Land Inc. Apache-2.0 license.

//! The birth registry's debug routes on the internal listener, served only
//! with `CELLD_REGISTRY_DEBUG_API=1`, and the key helpers a bench needs.

use crate::registry::Registry;
use crate::registry_objects as objects;
use celld_logic::lost_record::Answer;
use celld_logic::registry::{self as reg, Level};
use std::collections::{BTreeMap, HashSet};
use std::sync::atomic::Ordering;
use std::sync::Arc;

pub fn enabled() -> bool {
    static ENABLED: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *ENABLED
        .get_or_init(|| crate::env_vars::flag("CELLD_REGISTRY_DEBUG_API", false).unwrap_or(false))
}

/// The registry key of a cell scope, for a bench.
pub fn key_of(scope: &str) -> [u8; 16] {
    objects::key_of(scope)
}

pub fn hex_key(key: &[u8; 16]) -> String {
    objects::hex_key(key)
}

fn record_json(record: &reg::Record) -> serde_json::Value {
    serde_json::json!({
        "epoch": record.epoch, "max_txid": record.max_txid,
        "base": record.has_pack(), "pack": objects::hex_key(&{
            let mut bytes = [0_u8; 16];
            bytes[..12].copy_from_slice(&record.pack);
            bytes
        })[..24].to_owned(),
    })
}

/// Serve one debug request. `None` for a path that is not a registry route.
pub async fn serve(
    registry: &Arc<Registry>,
    method: &str,
    path: &str,
    query: &BTreeMap<String, String>,
    body: &serde_json::Value,
) -> Option<(u16, String)> {
    let reply = |status: u16, value: serde_json::Value| Some((status, value.to_string()));
    match (method, path) {
        ("GET", "/__registry/lookup") => {
            let key = match (query.get("key"), query.get("scope")) {
                (Some(key), _) => objects::parse_key(key),
                (None, Some(scope)) => Some(objects::key_of(scope)),
                _ => None,
            };
            let Some(key) = key else {
                return reply(400, serde_json::json!({ "error": "scope or key" }));
            };
            let result = registry.lookup(&key).await;
            let (answer, record) = match result.answer {
                Answer::New => ("new", None),
                Answer::Exists(record) => ("exists", Some(record_json(&record))),
                Answer::Unknown(_) => ("unknown", None),
            };
            reply(
                200,
                serde_json::json!({
                    "answer": answer, "record": record, "gets": result.gets,
                    "bytes": result.bytes, "micros": result.micros,
                }),
            )
        }
        ("GET", "/__registry/state") => {
            let mut state = registry.state();
            if query.contains_key("samples") {
                state["samples"] =
                    registry.samples_json(query.get("samples").is_some_and(|v| v == "take"));
            }
            reply(200, state)
        }
        ("GET", "/__registry/partition") => {
            let Some(index) = query.get("p").and_then(|p| p.parse::<usize>().ok()) else {
                return reply(400, serde_json::json!({ "error": "p" }));
            };
            let io = crate::registry::Io::default();
            let Some(snapshot) = registry.refresh(&io).await else {
                return reply(503, serde_json::json!({ "error": "no root" }));
            };
            if index >= snapshot.map.partitions.len() {
                return reply(404, serde_json::json!({ "error": "no such partition" }));
            }
            let partition = snapshot.map.partitions[index];
            let in_part = |record: &&reg::Record| snapshot.map.partition_for(&record.key) == index;
            let mut delta_keys: HashSet<reg::Key> = HashSet::new();
            for delta in &snapshot.deltas {
                delta_keys.extend(
                    delta
                        .records
                        .iter()
                        .filter(in_part)
                        .map(|record| record.key),
                );
            }
            let mut report = crate::registry_compactor::partition_report(&snapshot.map, index);
            let declared =
                partition.l1.records() + partition.l2.records() + delta_keys.len() as u64;
            report["delta_keys"] = delta_keys.len().into();
            report["declared_records"] = declared.into();
            if query.contains_key("full") {
                let mut distinct = delta_keys;
                for (no, level) in [(1_u8, partition.l1), (2, partition.l2)] {
                    let Level::Run(run) = level else { continue };
                    let key = objects::run_key(&registry.prefix, partition.lo, no, &run);
                    let tag = objects::tag_of(partition.lo, no, &run);
                    let Ok(mut reader) = crate::registry_compactor::RunReader::open(
                        &registry.store,
                        &key,
                        &run,
                        tag,
                    )
                    .await
                    else {
                        return reply(500, serde_json::json!({ "error": "open a run" }));
                    };
                    while let Ok(Some(records)) = reader.next_chunk_public().await {
                        distinct.extend(records.iter().map(|record| record.key));
                    }
                }
                report["distinct_keys"] = distinct.len().into();
            }
            reply(200, report)
        }
        ("POST", "/__registry/force") => {
            if !registry.role.holder.load(Ordering::Relaxed) {
                return reply(409, serde_json::json!({ "error": "not the holder" }));
            }
            let job = body
                .get("job")
                .and_then(|job| job.as_str())
                .unwrap_or("pass");
            let partition = body.get("partition").and_then(|p| p.as_u64()).unwrap_or(0) as usize;
            if job != "pass" {
                registry
                    .forced
                    .lock()
                    .unwrap()
                    .push_back((job.to_owned(), partition));
            }
            registry.pass_now.notify_one();
            reply(200, serde_json::json!({ "queued": job }))
        }
        ("POST", "/__registry/cover") => {
            // The backstop for one cell, as a first per-cell write would run it:
            // look the cell up, queue it, request a pass and wait for coverage.
            let Some(scope) = body.get("scope").and_then(|scope| scope.as_str()) else {
                return reply(400, serde_json::json!({ "error": "scope" }));
            };
            let started = crate::asyncrt::mono_ms();
            let ok = registry.ensure_covered(scope).await.is_ok();
            reply(
                200,
                serde_json::json!({ "ok": ok, "ms": crate::asyncrt::mono_ms().saturating_sub(started) }),
            )
        }
        ("POST", "/__registry/hold") => {
            let on = body.get("on").and_then(|on| on.as_bool()).unwrap_or(false);
            let holds = &registry.holds;
            match body.get("what").and_then(|what| what.as_str()) {
                Some("commit") => holds.commit.store(on, Ordering::Relaxed),
                Some("schedule") => holds.schedule.store(on, Ordering::Relaxed),
                Some("root_write") => holds.root_write_ms.store(
                    if on {
                        body.get("ms").and_then(|ms| ms.as_u64()).unwrap_or(0)
                    } else {
                        0
                    },
                    Ordering::Relaxed,
                ),
                _ => return reply(400, serde_json::json!({ "error": "what" })),
            }
            reply(200, serde_json::json!({ "ok": true }))
        }
        _ => None,
    }
}
