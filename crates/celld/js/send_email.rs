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

/// The longest call, in UTF-8 bytes of its JSON: a message is at most 25 MiB,
/// and the rest of the call (addresses, headers, quoting) is small. A message
/// that needs more once JSON has escaped it is refused, so a call costs the
/// host no more than about this before it is parsed.
const MAX_CALL_BYTES: usize = crate::email::MAX_MESSAGE_BYTES + 256 * 1024;

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

pub(super) fn answer(result: Result<String, EmailError>) -> String {
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
pub(super) fn refuse(
    scope: &mut v8::PinScope,
    mut rv: v8::ReturnValue<v8::Value>,
    error: EmailError,
) {
    let async_id = asyncrt::enqueue(async move { Ok::<String, String>(answer(Err(error))) });
    rv.set(promise_for(scope, async_id));
}

pub(super) fn rate_limited() -> EmailError {
    EmailError::new(
        "E_RATE_LIMIT_EXCEEDED",
        format!(
            "at most {} messages may be in flight at once",
            crate::smtp::MAX_PER_ISOLATE
        ),
    )
}

/// A send past the invocation's subrequest budget: a Worker reads its `code`
/// like that of any other refusal of `send()`.
pub(super) fn subrequest_limit(message: String) -> EmailError {
    EmailError::new("E_RATE_LIMIT_EXCEEDED", message)
}

/// Take the isolate's slot and charge the subrequest for a send whose raw
/// stream the binding is about to read, before it reads any of it. Answers
/// `{ ok: true, token }`, to name the slot in [`op_email_send`] and
/// [`op_email_release`], or the refusal as `send()` shall reject with it.
pub(super) fn op_email_reserve(
    scope: &mut v8::PinScope,
    _args: v8::FunctionCallbackArguments,
    mut rv: v8::ReturnValue<v8::Value>,
) {
    let context = event_context(scope);
    let text = match context.charge_subrequest() {
        Err(error) => answer(Err(subrequest_limit(error))),
        Ok(()) => match actor_runtime_state(scope).declared.take_send_slot() {
            Some(slot) => {
                // Kept with the event: if it ends while the stream stalls, the
                // slot goes with it.
                format!(r#"{{"ok":true,"token":{}}}"#, context.reserve_send(slot))
            }
            None => answer(Err(rate_limited())),
        },
    };
    let text = v8::String::new(scope, &text).unwrap();
    rv.set(text.into());
}

/// The token of a reservation, as the binding passes it back.
fn reserved_token(
    scope: &mut v8::PinScope,
    args: &v8::FunctionCallbackArguments,
    index: i32,
) -> Option<u64> {
    let value = args.get(index);
    if !value.is_number() {
        return None;
    }
    value
        .integer_value(scope)
        .and_then(|token| u64::try_from(token).ok())
}

/// Give back a slot [`op_email_reserve`] took, when the send did not start.
pub(super) fn op_email_release(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    _rv: v8::ReturnValue<v8::Value>,
) {
    release_reserved(scope, &args, 0);
}

/// Give back the slot of the reservation named by argument `index`, if any.
pub(super) fn release_reserved(
    scope: &mut v8::PinScope,
    args: &v8::FunctionCallbackArguments,
    index: i32,
) {
    if let Some(token) = reserved_token(scope, args, index) {
        drop(event_context(scope).take_reserved_send(token));
    }
}

/// Whether the call's JSON weighs more than a call may once copied into the
/// host. The size is that of the copy (UTF-8, up to three times the UTF-16
/// length), measured before the copy is made.
fn call_too_long(scope: &mut v8::PinScope, text: v8::Local<v8::String>) -> bool {
    text.utf8_length(scope) > MAX_CALL_BYTES
}

/// Send one checked message through the relay and answer as `send()` does.
///
/// The one place a message leaves the node for a Worker: the send waits for the
/// write the handler made so far to be durable, holds its slot until the relay
/// has answered, and logs only the stage and the reply code of a failure.
/// `send()`, `forward()` and `reply()` all end here, so their limits cannot
/// drift apart.
pub(super) async fn send_prepared(
    slot: crate::smtp::InFlight,
    prepared: Result<Prepared, EmailError>,
    gate: EgressGate,
    script: String,
    tracked: Option<crate::email::inbound::SendGuard>,
) -> Result<String, String> {
    // Held until the send ends, and given back if it is dropped.
    let _slot = slot;
    let prepared = match prepared {
        Ok(prepared) => prepared,
        Err(error) => return Ok(answer(Err(error))),
    };
    // The refusal reaches `send()` with a code, like any other failure.
    if let Err(message) = await_egress_gate(gate).await {
        return Ok(answer(Err(EmailError::new(
            "E_INTERNAL_SERVER_ERROR",
            message,
        ))));
    }
    let sent = match crate::smtp::relay() {
        Ok(relay) => crate::smtp::deliver(relay, &script, &prepared).await,
        Err(error) => Err(error),
    };
    if let Err(error) = &sent {
        // The stage and the reply code are all that is kept: the relay's
        // own text is never logged or passed on.
        tracing::warn!(event = "send_email_failed", code = error.code, detail = %error.message);
    }
    if let (Ok(()), Some(tracked)) = (&sent, &tracked) {
        tracked.delivered();
    }
    Ok(answer(sent.map(|()| prepared.message_id)))
}

/// The start of a send that leaves the node, whichever call it is: its
/// reserved slot or a new one, the subrequest it costs, and the bound on what
/// the host copies, before any of it is copied. `token_at` is the argument that
/// holds the token of a stream's reservation, if there is one; argument 0 is
/// the call's JSON and argument 1 its bytes.
pub(super) fn begin_send(
    scope: &mut v8::PinScope,
    args: &v8::FunctionCallbackArguments,
    token_at: i32,
) -> Result<(Arc<IoContext>, crate::smtp::InFlight), EmailError> {
    // A send whose stream was read took its slot and paid its subrequest
    // first; it holds that slot from here, and any early refusal gives it back.
    let token = reserved_token(scope, args, token_at);
    let reserved = token.is_some();
    let context = event_context(scope);
    let held = token.and_then(|token| context.take_reserved_send(token));
    if reserved && held.is_none() {
        return Err(EmailError::new(
            "E_VALIDATION_ERROR",
            "the call is malformed",
        ));
    }
    // A send costs a subrequest, as a fetch or a Workers AI call does.
    if held.is_none() {
        context.charge_subrequest().map_err(subrequest_limit)?;
    }
    // Bound what the host copies before it copies it: the JavaScript side
    // limits a message, but this op is the boundary.
    if v8::Local::<v8::String>::try_from(args.get(0))
        .map_or(true, |text| call_too_long(scope, text))
        || v8::Local::<v8::ArrayBufferView>::try_from(args.get(1))
            .is_ok_and(|view| view.byte_length() > crate::email::MAX_MESSAGE_BYTES)
    {
        return Err(too_large());
    }
    // The slot comes before anything is copied or built, so an isolate that
    // sends faster than the relay takes messages holds no more than a few.
    let slot = held
        .or_else(|| actor_runtime_state(scope).declared.take_send_slot())
        .ok_or_else(rate_limited)?;
    Ok((context, slot))
}

pub(super) fn op_email_send(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    mut rv: v8::ReturnValue<v8::Value>,
) {
    let (context, slot) = match begin_send(scope, &args, 2) {
        Ok(begun) => begun,
        Err(error) => return refuse(scope, rv, error),
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
    let async_id = asyncrt::enqueue(send_prepared(slot, prepared, gate, script, None));
    rv.set(promise_for(scope, async_id));
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_call_is_bounded_by_the_bytes_the_host_would_copy() {
        crate::js::Engine::init();
        let mut isolate = v8::Isolate::new(v8::CreateParams::default());
        v8::scope!(let handle, &mut isolate);
        let context = v8::Context::new(handle, Default::default());
        let scope = &mut v8::ContextScope::new(handle, context);
        // Three bytes each: 9M units are under the cap as a count and 27M
        // bytes are over it as the copy the host would make.
        let wide = v8::String::new(scope, &"\u{20ac}".repeat(9_000_000)).unwrap();
        assert!(wide.length() <= MAX_CALL_BYTES);
        assert!(call_too_long(scope, wide));
        let narrow = v8::String::new(scope, &"a".repeat(1000)).unwrap();
        assert!(!call_too_long(scope, narrow));
    }

    #[test]
    fn a_spent_subrequest_budget_reaches_send_as_a_coded_rejection() {
        let answer: serde_json::Value = serde_json::from_str(&answer(Err(subrequest_limit(
            "Worker exceeded subrequest limit of 3".into(),
        ))))
        .unwrap();
        assert_eq!(answer["ok"], false);
        assert_eq!(answer["code"], "E_RATE_LIMIT_EXCEEDED");
        assert_eq!(answer["message"], "Worker exceeded subrequest limit of 3");
    }

    #[test]
    fn a_reserved_slot_is_taken_only_by_its_own_token() {
        use crate::smtp::InFlight;
        use std::sync::atomic::{AtomicUsize, Ordering};
        let counter = Arc::new(AtomicUsize::new(0));
        let (first, second) = (IoContext::new(), IoContext::new());
        let first_token = first.reserve_send(InFlight::take(&counter).unwrap());
        let second_token = second.reserve_send(InFlight::take(&counter).unwrap());
        // The first event ends: its slot goes with it.
        drop(first);
        assert_eq!(counter.load(Ordering::Relaxed), 1);
        // Its late release or send, resolved to the second event, finds nothing.
        assert!(second.take_reserved_send(first_token).is_none());
        assert_eq!(counter.load(Ordering::Relaxed), 1);
        // The second send still holds its slot, and once only.
        let held = second.take_reserved_send(second_token);
        assert!(held.is_some());
        assert!(second.take_reserved_send(second_token).is_none());
        drop(held);
        assert_eq!(counter.load(Ordering::Relaxed), 0);
    }
}
