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

use super::send_email::{begin_send, refuse, release_reserved, send_prepared};
use super::*;
use crate::email::inbound::{Event, Inbound};
use crate::email::{EmailError, Prepared};

/// The longest JSON a `forward()` call may carry: a recipient and up to
/// [`crate::email::inbound::MAX_FORWARD_HEADERS`] short headers, with room for
/// headers the host drops.
const MAX_FORWARD_CALL_BYTES: usize = 64 * 1024;

/// Start an `email()` event: build its context, hand the message to the
/// harness and return what is now in flight.
pub(super) fn begin_email(
    tc: &mut v8::PinScope,
    mut message: Inbound,
    request_id: Option<RequestId>,
    reply: tokio::sync::oneshot::Sender<Result<Vec<u8>>>,
) -> Begun {
    let context = IoContext::with_resource_limits(actor_runtime_state(tc).resource_limits);
    // Held on the host for `forward()`, apart from the copy the isolate gets.
    let _ = context
        .inbound_email
        .set(Box::new(Event::new(&mut message)));
    bootstrap::begin_job_event(
        tc,
        context,
        request_id,
        Answer::Rpc(reply),
        None,
        false,
        |tc| {
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
            let meta =
                v8::String::new(tc, &meta).ok_or_else(|| anyhow!("email metadata too large"))?;
            // V8 owns what it is given and a Worker may write to it, so it gets a
            // copy of its own; the event keeps the shared original.
            let raw = bytes_value(tc, raw.to_vec());
            let entrypoint = v8::String::new(tc, "default").unwrap();
            let recv = v8::undefined(tc).into();
            dispatch
                .call(tc, recv, &[entrypoint.into(), meta.into(), raw])
                .ok_or_else(|| anyhow!("email dispatch threw"))
        },
    )
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
    let context = event_context(scope);
    let Some(event) = context.inbound_email.get() else {
        return not_an_email_event(scope, "forward");
    };
    if let Err(error) = event.check_open() {
        return refuse(scope, rv, error);
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
        .prepare_forward(
            crate::email::inbound::forward_allow(),
            &call.to,
            &call.headers,
        );
    let gate = egress_gate_request(&context, celld_logic::Channel::Fetch);
    let delivered = context.inbound_email.get().map(|event| event.delivered());
    let async_id = asyncrt::enqueue(send_prepared(slot, prepared, gate, script, delivered));
    rv.set(promise_for(scope, async_id));
}

/// Check the call, then take the event's one reply and prepare the message.
/// A malformed call leaves the reply to be made.
fn prepare_reply(
    event: &Event,
    call: ReplyCall,
    raw: Option<Vec<u8>>,
) -> Result<Prepared, EmailError> {
    match (call.kind.as_str(), call.from, call.to, call.message, raw) {
        ("raw", Some(from), Some(to), None, Some(raw)) => {
            event.take_reply()?;
            event.prepare_reply_raw(&from, &to, &raw)
        }
        ("builder", None, None, Some(message), None) => {
            event.take_reply()?;
            event.prepare_reply_builder(message)
        }
        _ => Err(EmailError::new(
            "E_VALIDATION_ERROR",
            "the call is malformed",
        )),
    }
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
    if let Err(error) = event.check_open().and_then(|()| event.check_reply_open()) {
        // A raw message's slot was reserved before this call: give it back.
        release_reserved(scope, &args, 2);
        return refuse(scope, rv, error);
    }
    let (context, slot) = match begin_send(scope, &args, 2) {
        Ok(begun) => begun,
        Err(error) => return refuse(scope, rv, error),
    };
    // The call is read and checked before the reply is taken, and the reply is
    // taken once the send is admitted: one turned away for lack of a slot or a
    // subrequest, or malformed, can be made again; a refused or failed one
    // cannot. An early return gives the slot back.
    let text = args.get(0).to_rust_string_lossy(scope);
    let call: ReplyCall = match serde_json::from_str(&text) {
        Ok(call) => call,
        Err(error) => return loader_throw(scope, &format!("reply(): {error}")),
    };
    let raw = view_bytes(args.get(1));
    let event = context.inbound_email.get().expect("checked above");
    let prepared = prepare_reply(event, call, raw);
    let script = actor_runtime_state(scope).declared.script().to_string();
    let gate = egress_gate_request(&context, celld_logic::Channel::Fetch);
    let delivered = Some(event.delivered());
    let async_id = asyncrt::enqueue(send_prepared(slot, prepared, gate, script, delivered));
    rv.set(promise_for(scope, async_id));
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::email::inbound::parse;

    fn event() -> Event {
        let raw =
            b"From: ann@example.org\r\nTo: bob@example.com\r\nMessage-ID: <m@x>\r\n\r\nhi".to_vec();
        Event::new(&mut parse(raw, Some("ann@example.org"), Some("bob@example.com")).unwrap())
    }

    fn call(kind: &str, message: Option<serde_json::Value>) -> ReplyCall {
        ReplyCall {
            kind: kind.into(),
            from: None,
            to: None,
            message,
        }
    }

    #[test]
    fn a_malformed_reply_call_does_not_use_up_the_reply() {
        let event = event();
        // A raw call with no addresses and a builder call with no message.
        for malformed in [
            call("raw", None),
            call("builder", None),
            call("other", None),
        ] {
            let error = prepare_reply(&event, malformed, None).unwrap_err();
            assert_eq!(error.code, "E_VALIDATION_ERROR");
            event.check_reply_open().unwrap();
        }
        // A well-formed one takes it, whatever it then makes of the message.
        let _ = prepare_reply(&event, call("builder", Some(serde_json::json!({}))), None);
        assert!(event.check_reply_open().is_err());
    }
}
