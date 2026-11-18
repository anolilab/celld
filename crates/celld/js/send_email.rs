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
use crate::email::{EmailError, Prepared, SendEmailBinding};

/// The longest call, in UTF-16 units of its JSON. A message is at most 25 MiB,
/// and JSON escaping can write a control character as six.
const MAX_CALL_UNITS: usize = 6 * crate::email::MAX_MESSAGE_BYTES;

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

fn answer(result: Result<String, (&'static str, String)>) -> String {
    match result {
        Ok(message_id) => serde_json::json!({ "ok": true, "messageId": message_id }),
        Err((code, message)) => {
            serde_json::json!({ "ok": false, "code": code, "message": message })
        }
    }
    .to_string()
}

pub(super) fn op_email_send(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    mut rv: v8::ReturnValue<v8::Value>,
) {
    // Bound what the host copies before it copies it: the JavaScript side
    // limits a message, but this op is the boundary.
    let too_long = |length: usize| length > MAX_CALL_UNITS;
    if v8::Local::<v8::String>::try_from(args.get(0)).map_or(true, |text| too_long(text.length())) {
        return loader_throw(scope, "send(): the message is too large");
    }
    if v8::Local::<v8::ArrayBufferView>::try_from(args.get(1))
        .is_ok_and(|view| view.byte_length() > crate::email::MAX_MESSAGE_BYTES)
    {
        return loader_throw(scope, "send(): the message is too large");
    }
    let raw_call = args.get(0).to_rust_string_lossy(scope);
    let call: Call = match serde_json::from_str(&raw_call) {
        Ok(call) => call,
        Err(error) => return loader_throw(scope, &format!("send(): {error}")),
    };
    let raw = view_bytes(args.get(1));
    let name = call.binding.clone();
    let Some(binding) = actor_runtime_state(scope).declared.send_email(&name) else {
        return loader_throw(scope, "send(): this Worker has no such send_email binding");
    };
    let prepared = prepare(&binding, call, raw);
    let gate = egress_gate_request(&event_context(scope), celld_logic::Channel::Fetch);
    let async_id = asyncrt::enqueue(async move {
        let prepared = match prepared {
            Ok(prepared) => prepared,
            Err(error) => return Ok::<String, String>(answer(Err((error.code, error.message)))),
        };
        await_egress_gate(gate).await?;
        let sent = match crate::smtp::relay() {
            Ok(relay) => {
                crate::smtp::deliver(relay, &prepared.from, &prepared.recipients, &prepared.data)
                    .await
            }
            Err(error) => Err(error),
        };
        if let Err(error) = &sent {
            // The stage and the reply code are all that is kept: the relay's
            // own text is never logged or passed on.
            tracing::warn!(event = "send_email_failed", code = error.code, detail = %error.message);
        }
        Ok(answer(
            sent.map(|()| prepared.message_id)
                .map_err(|error| (error.code, error.message)),
        ))
    });
    rv.set(promise_for(scope, async_id));
}
