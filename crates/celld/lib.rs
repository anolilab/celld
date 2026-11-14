// Copyright 2026 Deno Land Inc. Apache-2.0 license.

#![warn(clippy::disallowed_methods, clippy::disallowed_types)]

//! Effect adapters for the clean-sheet core.
//!
//! The executable owns one serial actor which is the only caller of
//! `celld_logic::on_event`. Adapter futures never borrow core state; they send
//! versioned completion events back through its mailbox.

mod tokio_select;
#[doc(hidden)]
pub use tokio_select::__asyncrt_select_support;

pub mod actor;
pub mod assets;
pub mod asyncrt;
pub mod bucket;
pub mod cell_cli;
pub mod cell_directory;
pub mod cell_dispatch;
mod cell_host;
pub mod cell_runtime;
pub mod clean_reload;
pub mod cli_options;
pub mod cli_output;
pub mod container;
pub mod control_plane;
pub mod d1_cli;
pub mod d1_export;
pub mod dead_node_gc;
pub mod deploy;
pub mod dev;
pub mod docker;
pub mod drain_token;
pub mod egress_filter;
pub mod engine_api;
pub mod env_vars;
pub(crate) mod facet_streams;
pub mod fleet;
pub mod generation;
pub mod host_channels;
pub mod host_services;
pub mod http_streams;
pub mod js;
pub mod kv_blob;
pub mod kv_cli;
pub(crate) mod local_store;
pub mod ltx_repl;
pub mod ltx_replication;
pub mod machine;
pub mod memory;
pub mod node_log;
pub(crate) mod operator_cell;
pub mod operator_token;
#[doc(hidden)]
pub mod otlp;
pub mod ownership_store;
pub mod peer_auth;
pub mod peer_probe;
pub mod pool;
pub mod protocol;
pub(crate) mod queue_batching;
pub mod queue_cli;
pub mod queue_policy;
pub mod r2_cli;
pub(crate) mod r2_store;
pub mod replication;
pub mod runtime;
pub mod staged;
pub mod startup;
pub mod storage;
pub mod telemetry;
pub mod wake;
pub mod wake_entry;
pub mod wake_format;
pub mod ws_client;
pub mod ws_registry;

/// Completion token for a resident-isolate reservation made by the decision
/// core. Dropping the queued/running job reports that the cell is idle again;
/// the token contains no selection or lifecycle policy of its own.
pub struct CellActivityGuard {
    finish: Option<Box<dyn FnOnce() + Send>>,
}

impl CellActivityGuard {
    pub fn new(finish: impl FnOnce() + Send + 'static) -> Self {
        Self {
            finish: Some(Box::new(finish)),
        }
    }
}

impl Drop for CellActivityGuard {
    fn drop(&mut self) {
        if let Some(finish) = self.finish.take() {
            finish();
        }
    }
}

/// A stateless entrypoint selected for one fetch request.
#[derive(Clone, Debug, PartialEq)]
pub struct WorkerFetchEntrypoint {
    pub name: String,
    /// The caller's `ctx.props` as V8 structured-clone bytes. An empty vector
    /// represents an omitted or `undefined` props value.
    pub props: Vec<u8>,
}

/// Resource limits selected by a Worker Loader stub for one invocation.
#[doc(hidden)]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, serde::Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct WorkerInvocationLimits {
    pub cpu_ms: Option<u32>,
    pub sub_requests: Option<u32>,
}

/// One operation on a Worker entrypoint. A property read has no arguments,
/// while a call owns its complete receiver path and structured-clone payload.
pub enum WorkerRpcOperation {
    Get { path: Vec<String> },
    Call { path: Vec<String>, args: Vec<u8> },
}

pub enum WorkerJob {
    Fetch {
        queued_at: std::time::Instant,
        /// An entrypoint dispatcher target, or the direct default export when
        /// absent.
        entrypoint: Option<WorkerFetchEntrypoint>,
        invocation_limits: Option<WorkerInvocationLimits>,
        url: String,
        method: String,
        body: js::RequestBody,
        headers: Vec<(String, String)>,
        request_id: Option<js::RequestId>,
        /// Receives one completed invocation report for Dynamic Worker tails.
        /// Ordinary fetches leave this empty.
        tail_report: Option<tokio::sync::oneshot::Sender<String>>,
        reply: tokio::sync::oneshot::Sender<anyhow::Result<js::HttpResponse>>,
    },
    Rpc {
        entrypoint: String,
        operation: WorkerRpcOperation,
        /// The caller's `ctx.props` as V8 structured-clone bytes, the same
        /// encoding as `args`. It is empty when the caller sent none, which no
        /// encoded value can be. A Worker Loader entrypoint or a transferred
        /// Service Binding can set it.
        props: Vec<u8>,
        invocation_limits: Option<WorkerInvocationLimits>,
        reply: tokio::sync::oneshot::Sender<anyhow::Result<Vec<u8>>>,
    },
    Queue {
        queued_at: std::time::Instant,
        batch: js::QueueBatch,
        reply: tokio::sync::oneshot::Sender<anyhow::Result<js::QueueDispatchResult>>,
    },
}

/// Temporary host seam required by the verbatim JS adapter. The runtime
/// adapter will construct the real shared Worker queue; lifecycle policy does
/// not move into this type.
/// Compatibility switches copied with the JS adapter. These are runtime
/// semantics, not lifecycle decisions.
pub fn worker_compat(metadata: &serde_json::Value) -> js::Compat {
    let flags = metadata
        .get("compatibility_flags")
        .and_then(serde_json::Value::as_array);
    let has_flag = |name: &str| {
        flags.is_some_and(|flags| {
            flags
                .iter()
                .any(|flag| flag.as_str().is_some_and(|flag| flag == name))
        })
    };
    let date = metadata
        .get("compatibility_date")
        .and_then(serde_json::Value::as_str);
    let switch = |enable: &str, disable: &str, since: &str| {
        if has_flag(enable) {
            return true;
        }
        if has_flag(disable) {
            return false;
        }
        date.is_some_and(|date| date >= since)
    };
    js::Compat {
        delete_all_deletes_alarm: switch(
            "delete_all_deletes_alarm",
            "delete_all_preserves_alarm",
            "2026-02-24",
        ),
        js_rpc: has_flag("js_rpc"),
        fetcher_get_put_delete: !switch(
            "fetcher_no_get_put_delete",
            "fetcher_has_get_put_delete",
            "2024-03-26",
        ),
        sqlite_vec: has_flag("sqlite_vec"),
        websocket_standard_binary_type: has_flag("websocket_standard_binary_type"),
        queue_json_messages: switch("queue_json_messages", "queue_v8_messages", "2024-03-18"),
    }
}

#[cfg(test)]
pub(crate) mod test_stub;
