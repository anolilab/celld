// Copyright 2026 Deno Land Inc. Apache-2.0 license.

//! The host op behind a `send_email` binding.
//!
//! The binding's JavaScript only shapes what a Worker passed. Everything that
//! decides whether a message is sent happens here: the binding's restrictions
//! come from the deployment the isolate was built from, never from the
//! Worker's arguments, and the message is checked and built by
//! [`crate::email`]. The connection to the operator's SMTP relay is made by
//! the host in [`crate::smtp`], after the write the handler made so far is
//! durable, as for any effect that leaves the node.

use super::*;
use crate::email::{too_large, EmailError, Prepared, SendEmailBinding};

/// The longest call, in UTF-16 units of its JSON: a message is at most 25 MiB,
/// and the rest of the call (addresses, headers, quoting) is small. A message
/// that needs more once JSON has escaped it is refused, so a call costs the
/// host no more than about this before it is parsed.
const MAX_CALL_UNITS: usize = crate::email::MAX_MESSAGE_BYTES + 256 * 1024;

#[derive(serde::Deserialize)]
struct Call {
    binding: String,
    /// `raw` or `builder`.
    kind: String,
    /// The envelope of a raw message.
    #[serde(default)]
    from: Option<String>,
    #[serde(default)]
    to: Option<String>,
    /// The fields of a builder message.
    #[serde(default)]
    message: Option<serde_json::Value>,
}

fn prepare(
    binding: &SendEmailBinding,
    call: Call,
    raw: Option<Vec<u8>>,
) -> Result<Prepared, EmailError> {
    match (call.kind.as_str(), call.from, call.to, call.message) {
        ("raw", Some(from), Some(to), None) => {
            let raw = raw
                .ok_or_else(|| EmailError::new("E_VALIDATION_ERROR", "the message has no body"))?;
            crate::email::prepare_raw(binding, &from, &to, &raw)
        }
        ("builder", None, None, Some(message)) => crate::email::prepare_builder(binding, message),
        _ => Err(EmailError::new(
            "E_VALIDATION_ERROR",
            "the call is malformed",
        )),
    }
}

fn answer(result: Result<String, EmailError>) -> String {
    match result {
        Ok(message_id) => serde_json::json!({ "ok": true, "messageId": message_id }),
        Err(error) => {
            serde_json::json!({ "ok": false, "code": error.code, "message": error.message })
        }
    }
    .to_string()
}

/// The answer of a call the op refused: a Worker reads its `code` like that of
/// any other failure of `send()`.
fn refuse(scope: &mut v8::PinScope, mut rv: v8::ReturnValue<v8::Value>, error: EmailError) {
    let async_id = asyncrt::enqueue(async move { Ok::<String, String>(answer(Err(error))) });
    rv.set(promise_for(scope, async_id));
}

pub(super) fn op_email_send(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    mut rv: v8::ReturnValue<v8::Value>,
) {
    // A send costs a subrequest, as a fetch or a Workers AI call does.
    let context = event_context(scope);
    if let Err(error) = context.charge_subrequest() {
        return loader_throw(scope, &error);
    }
    // Bound what the host copies before it copies it: the JavaScript side
    // limits a message, but this op is the boundary.
    let too_long = |length: usize| length > MAX_CALL_UNITS;
    if v8::Local::<v8::String>::try_from(args.get(0)).map_or(true, |text| too_long(text.length()))
        || v8::Local::<v8::ArrayBufferView>::try_from(args.get(1))
            .is_ok_and(|view| view.byte_length() > crate::email::MAX_MESSAGE_BYTES)
    {
        return refuse(scope, rv, too_large());
    }
    // The slot comes before anything is copied or built, so an isolate that
    // sends faster than the relay takes messages holds no more than a few.
    let Some(slot) = actor_runtime_state(scope).declared.take_send_slot() else {
        return refuse(
            scope,
            rv,
            EmailError::new(
                "E_RATE_LIMIT_EXCEEDED",
                format!(
                    "at most {} messages may be in flight at once",
                    crate::smtp::MAX_PER_ISOLATE
                ),
            ),
        );
    };
    let raw_call = args.get(0).to_rust_string_lossy(scope);
    let call: Call = match serde_json::from_str(&raw_call) {
        Ok(call) => call,
        Err(error) => return loader_throw(scope, &format!("send(): {error}")),
    };
    let raw = view_bytes(args.get(1));
    let name = call.binding.clone();
    let declared = &actor_runtime_state(scope).declared;
    let script = declared.script().to_string();
    let Some(binding) = declared.send_email(&name) else {
        return loader_throw(scope, "send(): this Worker has no such send_email binding");
    };
    let prepared = prepare(&binding, call, raw);
    let gate = egress_gate_request(&context, celld_logic::Channel::Fetch);
    let async_id = asyncrt::enqueue(async move {
        // Held until the send ends, and given back if it is dropped.
        let _slot = slot;
        let prepared = match prepared {
            Ok(prepared) => prepared,
            Err(error) => return Ok::<String, String>(answer(Err(error))),
        };
        await_egress_gate(gate).await?;
        let sent = match crate::smtp::relay() {
            Ok(relay) => crate::smtp::deliver(relay, &script, &prepared).await,
            Err(error) => Err(error),
        };
        if let Err(error) = &sent {
            // The stage and the reply code are all that is kept: the relay's
            // own text is never logged or passed on.
            tracing::warn!(event = "send_email_failed", code = error.code, detail = %error.message);
        }
        Ok(answer(sent.map(|()| prepared.message_id)))
    });
    rv.set(promise_for(scope, async_id));
}
