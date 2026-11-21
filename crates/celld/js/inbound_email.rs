// Copyright 2026 Deno Land Inc. Apache-2.0 license.

//! The `email()` event: one inbound message handed to the Worker's handler,
//! and the host ops behind `message.forward()` and `message.reply()`.
//!
//! The message itself stays on the event's context ([`crate::email::inbound::Event`]).
//! The ops name an action and carry only what the handler chose (a recipient,
//! the `X-` headers, a reply); the envelope, the original bytes and the
//! thread are read here, so a handler cannot forward or reply as anything but
//! the message it was given. A send goes through
//! [`send_email::send_prepared`](super::send_email), with the same slot,
//! subrequest and size limits as a `send_email` binding.

use super::send_email::{begin_send, refuse, send_prepared};
use super::*;
use crate::email::inbound::{Event, Inbound};
use crate::email::EmailError;

/// The longest JSON a `forward()` call may carry: a recipient and up to
/// [`crate::email::inbound::MAX_FORWARD_HEADERS`] short headers, with room for
/// headers the host drops.
const MAX_FORWARD_CALL_BYTES: usize = 64 * 1024;

/// Start an `email()` event: build its context, hand the message to the
/// harness and return what is now in flight.
pub(super) fn begin_email(
    tc: &mut v8::PinScope,
    mut message: Inbound,
    reply: tokio::sync::oneshot::Sender<Result<Vec<u8>>>,
) -> Begun {
    let context = IoContext::with_resource_limits(actor_runtime_state(tc).resource_limits);
    // Held on the host for `forward()`, apart from the copy the isolate gets.
    let _ = context
        .inbound_email
        .set(Box::new(Event::new(&mut message)));
    bootstrap::begin_job_event(tc, context, Answer::Rpc(reply), None, false, |tc| {
        let dispatch = internal_function(tc, "__dispatchEntrypointEmail")?;
        let Inbound {
            from,
            to,
            raw,
            headers,
            ..
        } = message;
        let meta = serde_json::json!({
            "from": from,
            "to": to,
            "rawSize": raw.len(),
            "headers": headers,
        })
        .to_string();
        let meta = v8::String::new(tc, &meta).ok_or_else(|| anyhow!("email metadata too large"))?;
        let raw = bytes_value(tc, raw);
        let entrypoint = v8::String::new(tc, "default").unwrap();
        let recv = v8::undefined(tc).into();
        dispatch
            .call(tc, recv, &[entrypoint.into(), meta.into(), raw])
            .ok_or_else(|| anyhow!("email dispatch threw"))
    })
}

#[derive(serde::Deserialize)]
struct ForwardCall {
    to: String,
    #[serde(default)]
    headers: Vec<(String, String)>,
}

#[derive(serde::Deserialize)]
struct ReplyCall {
    /// `raw` or `builder`.
    kind: String,
    #[serde(default)]
    from: Option<String>,
    #[serde(default)]
    to: Option<String>,
    #[serde(default)]
    message: Option<serde_json::Value>,
}

fn not_an_email_event(scope: &mut v8::PinScope, what: &str) {
    loader_throw(
        scope,
        &format!("{what}(): only an email() handler's message can do this"),
    );
}

/// `message.forward(rcptTo, headers)`: the received message, unchanged but for
/// the `X-` headers, to one recipient through the operator's relay.
pub(super) fn op_email_forward(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    mut rv: v8::ReturnValue<v8::Value>,
) {
    if event_context(scope).inbound_email.get().is_none() {
        return not_an_email_event(scope, "forward");
    }
    let too_long = v8::Local::<v8::String>::try_from(args.get(0)).map_or(true, |text| {
        text.utf8_length(scope) > MAX_FORWARD_CALL_BYTES
    });
    if too_long {
        return refuse(
            scope,
            rv,
            EmailError::new("E_VALIDATION_ERROR", "the call is malformed"),
        );
    }
    let (context, slot) = match begin_send(scope, &args, 1) {
        Ok(begun) => begun,
        Err(error) => return refuse(scope, rv, error),
    };
    let text = args.get(0).to_rust_string_lossy(scope);
    let call: ForwardCall = match serde_json::from_str(&text) {
        Ok(call) => call,
        Err(error) => return loader_throw(scope, &format!("forward(): {error}")),
    };
    let script = actor_runtime_state(scope).declared.script().to_string();
    let prepared = context
        .inbound_email
        .get()
        .expect("checked above")
        .prepare_forward(&call.to, &call.headers);
    let gate = egress_gate_request(&context, celld_logic::Channel::Fetch);
    let async_id = asyncrt::enqueue(send_prepared(slot, prepared, gate, script));
    rv.set(promise_for(scope, async_id));
}

/// `message.reply(reply)`: a message to the sender of the received one,
/// threaded to it, through the operator's relay. One per event.
pub(super) fn op_email_reply(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    mut rv: v8::ReturnValue<v8::Value>,
) {
    let context = event_context(scope);
    let Some(event) = context.inbound_email.get() else {
        return not_an_email_event(scope, "reply");
    };
    // A refused reply is the event's one reply, as a failed one is.
    if let Err(error) = event.take_reply() {
        // A raw stream's reservation is given back with the event.
        return refuse(scope, rv, error);
    }
    let (context, slot) = match begin_send(scope, &args, 2) {
        Ok(begun) => begun,
        Err(error) => return refuse(scope, rv, error),
    };
    let text = args.get(0).to_rust_string_lossy(scope);
    let call: ReplyCall = match serde_json::from_str(&text) {
        Ok(call) => call,
        Err(error) => return loader_throw(scope, &format!("reply(): {error}")),
    };
    let raw = view_bytes(args.get(1));
    let event = context.inbound_email.get().expect("checked above");
    let prepared = match (call.kind.as_str(), call.from, call.to, call.message, raw) {
        ("raw", Some(from), Some(to), None, Some(raw)) => event.prepare_reply_raw(&from, &to, &raw),
        ("builder", None, None, Some(message), None) => event.prepare_reply_builder(message),
        _ => Err(EmailError::new(
            "E_VALIDATION_ERROR",
            "the call is malformed",
        )),
    };
    let script = actor_runtime_state(scope).declared.script().to_string();
    let gate = egress_gate_request(&context, celld_logic::Channel::Fetch);
    let async_id = asyncrt::enqueue(send_prepared(slot, prepared, gate, script));
    rv.set(promise_for(scope, async_id));
}
