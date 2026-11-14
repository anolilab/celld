//! What a script may address, decided by the host.
//!
//! A cell's scope, a namespace key and a Durable Object id are all strings the
//! script can write down: the id key is a hash of the namespace key, so any
//! script can compute a valid id for any namespace. Nothing the script holds
//! therefore proves it may reach a cell. Two host-side rules replace that
//! proof:
//!
//! - an op that acts on "the cell" takes that cell from the running event, and
//!   refuses a scope argument that names another cell ([`own_cell_arg`]);
//! - an op that routes to another cell refuses a target the script never
//!   declared ([`Declared::allows`]).
use super::*;
use std::collections::HashSet;

/// The cells a script declared: its Durable Object classes and the exact
/// cells behind its D1, KV and Queue bindings.
#[derive(Default)]
pub(super) struct Declared {
    script: String,
    classes: HashSet<String>,
    /// Scopes of the shared reserved classes a binding names. Their namespace
    /// key is the same for the whole fleet, so a class check would admit every
    /// database, namespace and queue in it.
    shared: HashSet<String>,
}

impl Declared {
    pub(super) fn of(config: &WorkerConfig) -> Self {
        let mut shared = HashSet::new();
        for (_, identity) in &config.d1_bindings {
            shared.insert(crate::engine_api::d1_cell_scope(identity));
        }
        for (_, namespace) in &config.kv_bindings {
            for shard in 0..celld_logic::kv::SHARDS {
                shared.insert(crate::engine_api::kv_cell_scope(namespace, shard));
            }
        }
        for binding in &config.queue_bindings {
            shared.insert(crate::engine_api::queue_cell_scope(&binding.queue));
        }
        Self {
            script: config.script_name.clone(),
            classes: config.do_classes.iter().cloned().collect(),
            shared,
        }
    }

    /// Whether the script may route to `target`: a cell of a class it
    /// declared whose id the class's own namespace derived, or one of the
    /// exact cells its bindings name.
    pub(super) fn allows(&self, target: &str) -> bool {
        let Some((class, instance)) = target.split_once(':') else {
            return false;
        };
        if crate::deploy::is_shared_reserved_class(class) {
            return self.shared.contains(target);
        }
        if !self.classes.contains(class) {
            return false;
        }
        let Some(id) = decode_durable_object_id(instance) else {
            return false;
        };
        use hmac::Mac;
        let key = durable_object_id_key(&crate::engine_api::namespace_key(&self.script, class));
        durable_object_id_hmac(&key, &id[..16])
            .verify_truncated_left(&id[16..])
            .is_ok()
    }
}

/// Whether the running event belongs to the runtime's own code: a cell of a
/// shared reserved class (D1, KV, Queue), which runs only the engine's classes
/// in isolates that hold no tenant code. A workflow or cron cell is not one:
/// it runs the application's code, so it is held to what the script declared.
fn runtime_event(context: &IoContext) -> bool {
    context.egress.lock().unwrap().last().is_some_and(|frame| {
        crate::deploy::reserved_class_of(&frame.storage)
            .is_some_and(crate::deploy::is_shared_reserved_class)
    })
}

/// Refuse a scope that the script did not declare. Throws and returns `false`.
pub(super) fn may_address(scope: &mut v8::PinScope, target: &str) -> bool {
    let context = event_context(scope);
    if runtime_event(&context) || actor_runtime_state(scope).declared.allows(target) {
        return true;
    }
    loader_throw(
        scope,
        "this script did not declare the Durable Object it addressed",
    );
    false
}

/// Which argument of an op names the cell it acts on, for the ops that act on
/// the running event's own cell.
pub(super) const fn cell_scope_arg(op: &str) -> Option<usize> {
    match op.as_bytes() {
        b"__facet_fetch" | b"__facet_rpc" | b"__facet_abort" => Some(2),
        b"__storage_get"
        | b"__storage_get_many"
        | b"__storage_put"
        | b"__storage_put_many"
        | b"__storage_put_serialized"
        | b"__storage_queue_put"
        | b"__storage_queue_put_many"
        | b"__storage_queue_put_serialized"
        | b"__storage_delete"
        | b"__storage_delete_many"
        | b"__storage_delete_all"
        | b"__storage_list"
        | b"__storage_sync_list_start"
        | b"__storage_transaction_control"
        | b"__sql_ingest"
        | b"__sql_cursor_start"
        | b"__sql_database_size"
        | b"__d1_run"
        | b"__d1_export"
        | b"__alarm_set"
        | b"__alarm_get"
        | b"__alarm_delete"
        | b"__queue_alarm_set_wait"
        | b"__actor_abort"
        | b"__facet_delete"
        | b"__container_running"
        | b"__container_address"
        | b"__container_start"
        | b"__container_monitor"
        | b"__container_destroy"
        | b"__container_signal"
        | b"__container_inactivity"
        | b"__container_exec" => Some(0),
        _ => None,
    }
}

/// Whether the running event may act as `cell`: it is that cell's own event,
/// or it is calling a capability that cell minted.
///
/// The delegation is the event's, not the call's: while the call is in flight,
/// other code in the same event can act as `cell` too. It holds a capability to
/// that cell already and the call runs that cell's code on its behalf, so the
/// window widens what it can do only for the length of the call. Binding it to
/// the call alone needs the JavaScript context id on every storage op.
fn acts_as(context: &IoContext, cell: &str) -> bool {
    let own = context
        .egress
        .lock()
        .unwrap()
        .last()
        .is_some_and(|frame| frame.storage == cell);
    own || context.delegated_cells.lock().unwrap().contains_key(cell)
}

/// `__cell_delegate(cell, on)`: the harness opens and closes the window in
/// which the running event calls a stub that `cell` minted. The harness reads
/// `cell` from its own entry for the stub, which no script can write, and the
/// op is a parameter of internal scripts only.
pub(super) fn op_cell_delegate(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    _rv: v8::ReturnValue<v8::Value>,
) {
    let cell = args.get(0).to_rust_string_lossy(scope);
    let on = args.get(1).boolean_value(scope);
    let context = event_context(scope);
    let mut delegated = context.delegated_cells.lock().unwrap();
    if on {
        *delegated.entry(cell).or_default() += 1;
    } else if let Some(count) = delegated.get_mut(&cell) {
        *count -= 1;
        if *count == 0 {
            delegated.remove(&cell);
        }
    }
}

/// Whether the running event is the cell that opened a cursor. A cursor that
/// is not open has no owner and nothing to read, so it passes and the op
/// reports it gone itself.
pub(super) fn own_cursor(scope: &mut v8::PinScope, opened_by: Option<String>) -> bool {
    let Some(opened_by) = opened_by else {
        return true;
    };
    acts_as(&event_context(scope), &opened_by)
}

/// Refuse a scope argument that is not the cell whose event is running.
/// Throws and returns `false`.
pub(super) fn own_cell_arg(
    scope: &mut v8::PinScope,
    args: &v8::FunctionCallbackArguments,
    index: usize,
) -> bool {
    let named = args.get(index as i32).to_rust_string_lossy(scope);
    if acts_as(&event_context(scope), &named) {
        return true;
    }
    loader_throw(
        scope,
        "this operation names a cell other than the running one",
    );
    false
}

/// Ops that take no cell scope, or take an id or a name that another check
/// covers. Every op in the table is either in [`cell_scope_arg`] or here, so
/// adding an op makes this test ask which one it is.
#[cfg(test)]
const NOT_CELL_SCOPED: &[&str] = &[
    "__heap_limit_excessively_exceeded",
    "__heap_over_admission_share",
    "__ws_send",
    "__ws_send_binary",
    "__ws_close",
    "__ws_alloc",
    "__cell_delegate",
    "__ws_prepare_worker_handoff",
    "__ws_accept",
    "__ws_accept_regular",
    "__ws_list",
    "__ws_attachment_set",
    "__ws_auto_response_set",
    "__ws_auto_response_get",
    "__ws_auto_response_ts",
    "__ws_connect",
    "__ws_bind_target",
    "__ws_next",
    "__ws_upgrade",
    "__hr_create",
    "__hr_write",
    "__hr_end",
    "__hr_cmd",
    "__hr_event",
    "__hr_take",
    "__hr_free",
    "__tcp_connect",
    "__tcp_read",
    "__tcp_write",
    "__tcp_shutdown",
    "__tcp_close",
    "__tcp_starttls",
    "__container_exec_read",
    "__container_exec_write",
    "__container_exec_close",
    "__container_exec_wait",
    "__container_exec_kill",
    "__container_exec_drop",
    "__sql_cursor_next",
    "__sql_cursor_close",
    "__log",
    "__storage_flush_pending_puts",
    "__storage_sync",
    "__storage_cancel_pending_puts",
    "__cron_plan",
    "__kv_blob",
    "__process_exit",
    "__storage_sync_list_next",
    "$$urlParse",
    "$$urlPatternParse",
    "$$urlPatternMatchInput",
    "$$atob",
    "$$btoa",
    "$$textDecoderLabel",
    "$$textDecoderNew",
    "$$textDecoderDecode",
    "$$textDecoderDecodeOnce",
    "$$textDecoderFree",
    "__loader_load",
    "__loader_fetch",
    "__loader_rpc",
    "__loader_drop",
    "__do_call",
    "__svc_call",
    "__svc_call_cancellable",
    "__svc_rpc",
    "__queue_dispatch",
    "__queue_policy",
    "__do_call_cancellable",
    "__do_call_cancel",
    "__rpc_signal_new",
    "__rpc_signal_abort",
    "__rpc_signal_release",
    "__rpc_signal_poll",
    "__rpc_signal_wait",
    "__rpc_signal_unsubscribe",
    "__do_id",
    "__rpc_call",
    "__sc_encode",
    "__sc_decode",
    "__structured_clone",
    "__op_fetch",
    "__r2_head",
    "__r2_get",
    "__r2_put",
    "__r2_put_begin",
    "__r2_put_chunk",
    "__r2_put_end",
    "__r2_delete",
    "__r2_list",
    "__r2_mp_begin",
    "__r2_mp_resume",
    "__r2_mp_part",
    "__r2_mp_complete",
    "__r2_mp_abort",
    "__asset_fetch",
    "__http_stream_read",
    "__http_stream_cancel",
    "__http_stream_tee",
    "__response_stream_create",
    "__response_stream_write",
    "__response_stream_closed",
    "__response_stream_close",
    "__op_timer",
    "__isolate_condemn",
    "__timer_alloc",
    "__io_context_id",
    "__with_input_gate_context",
    "__gate_acquire",
    "__gate_wait",
    "__gate_release",
    "__timer_cancel",
    "__crypto_operation",
    "$$randomValues",
    "$$digest",
    "$$hmacSign",
    "$$hmacVerify",
    "$$aesEncrypt",
    "$$aesDecrypt",
    "$$pbkdf2",
    "$$hkdf",
    "$$scrypt",
    "$$timingSafeEqual",
    "__event_begin",
    "__event_end",
    "__pending_event_begin",
    "__pending_event_end",
    "__vfs_mkdir",
    "__vfs_read_file",
    "__vfs_stat",
    "__wait_until",
    "__wait_until_active",
    "__event_depth",
    "__als_get",
    "__als_set",
    "__util_type_flags",
    "__util_constructor_name",
    "__util_proxy_details",
    "__util_promise_details",
    "__util_preview_entries",
    "__builtin_module",
    "__zlib",
    "__zlib_stream_new",
    "__zlib_stream_push",
    "__zlib_stream_end",
    "__zlib_stream_drop",
];

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_op_is_classified_once() {
        for name in OP_NAMES {
            let scoped = cell_scope_arg(name).is_some();
            let listed = NOT_CELL_SCOPED.contains(name);
            assert!(
                scoped != listed,
                "{name} must be in exactly one of cell_scope_arg and NOT_CELL_SCOPED"
            );
        }
        for name in NOT_CELL_SCOPED {
            assert!(OP_NAMES.contains(name), "{name} is not an op");
        }
    }
}
