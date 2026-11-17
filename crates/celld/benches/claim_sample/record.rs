//! The lines of the sample log: one per poll of one node.
//!
//! A poll that reached the node carries its `storage_counters` and, from
//! `/state/step0`, the ring entries that no earlier line of the same process
//! carries. A poll that did not reach it says so. The summary needs nothing but
//! these lines, so a sampler that is stopped and started again loses nothing.

use std::collections::HashMap;

use serde_json::{json, Value};

pub const RING_NAMES: [&str; 5] = [
    "mints",
    "first_claims",
    "second_claims",
    "releases",
    "resumes",
];

/// A process, told apart from every other by its session and its start.
pub type ProcessKey = (String, u64);

/// The next sequence number of each ring that has been logged, per process.
#[derive(Default)]
pub struct Cursors {
    logged: HashMap<ProcessKey, [u64; RING_NAMES.len()]>,
}

/// The line of a poll that did not reach the node.
pub fn down(t_ms: u64, node: &str, error: &str) -> Value {
    json!({ "t_ms": t_ms, "node": node, "ok": false, "error": error })
}

pub fn identity(value: &Value) -> Option<ProcessKey> {
    Some((
        value["session"].as_str()?.to_owned(),
        value["process_start_ms"].as_u64()?,
    ))
}

impl Cursors {
    /// The line of a poll that read `state` (the node's `/state`) and, when it
    /// could, `step0` (its `/state/step0`). A node that reports no process
    /// identity, as an older one does, gets its counters logged and no rings.
    /// The two reads are of one process or the rings are not logged: a restart
    /// between them would pair one process's identity with another's entries.
    pub fn line(&mut self, t_ms: u64, node: &str, state: &Value, step0: Option<&Value>) -> Value {
        let counters = &state["storage_counters"];
        let mut line = json!({ "t_ms": t_ms, "node": node, "ok": true, "counters": counters });
        let Some(key) = identity(counters) else {
            line["rings"] = Value::Null;
            return line;
        };
        let Some(step0) = step0 else {
            line["rings"] = Value::Null;
            line["rings_error"] = "no answer from /state/step0".into();
            return line;
        };
        if identity(step0).as_ref() != Some(&key) {
            line["rings"] = Value::Null;
            line["rings_error"] = "the process changed between the two reads".into();
            return line;
        }
        let logged = self.logged.entry(key).or_insert([0; RING_NAMES.len()]);
        let mut rings = serde_json::Map::new();
        for (slot, name) in RING_NAMES.iter().enumerate() {
            let ring = &step0["rings"][name];
            let next_seq = ring["next_seq"].as_u64().unwrap_or(0);
            let new: Vec<&Value> = ring["entries"]
                .as_array()
                .into_iter()
                .flatten()
                .filter(|entry| entry["seq"].as_u64().is_some_and(|seq| seq >= logged[slot]))
                .collect();
            logged[slot] = logged[slot].max(next_seq);
            rings.insert(
                (*name).to_owned(),
                json!({ "next_seq": next_seq, "entries": new }),
            );
        }
        line["rings"] = Value::Object(rings);
        line
    }
}
