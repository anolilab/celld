//! Tail Workers: delivery of a traced Worker's trace items to the `tail()`
//! handler of each Worker in its `tail_consumers`.
//!
//! A traced invocation hands its finished trace item to [`TailSink::submit`],
//! which is one non-blocking `try_send` per consumer. Everything else happens
//! on a task of its own: it takes the items that have gathered, sends them as
//! one `tail(events)` call, and waits for that call's event to end, its
//! `waitUntil` work included, before the next batch, so a consumer never has
//! two events running for one producer. The
//! producer never waits for, and never sees the result of, that call.
//!
//! Each consumer has a bounded buffer, and the node has one byte budget for
//! all of them. A consumer that is slow, hung or failing lets its buffer fill,
//! and the items that do not fit are dropped and counted.

use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, LazyLock};
use std::time::Duration;

/// Trace items one consumer can have waiting. A full buffer drops the newest.
pub(crate) const BUFFER_ITEMS: usize = 256;
/// The most trace items one `tail()` call carries.
pub(crate) const BATCH_ITEMS: usize = 100;
/// The bytes of trace items the whole node holds for delivery: the items in
/// buffers, and the encoded batch of each running call. An item past it is
/// dropped, and so is a batch the node has no room to encode.
pub(crate) const NODE_BUFFER_BYTES: usize = 64 << 20;
/// The share of that budget one producer can hold in buffers, so one Worker
/// with a stuck consumer cannot leave the others nothing.
pub(crate) const PRODUCER_BUFFER_BYTES: usize = NODE_BUFFER_BYTES / 4;
/// How long one `tail()` call may run before the runtime ends it.
const DELIVERY_TIMEOUT: Duration = Duration::from_secs(30);
/// How long past that the lane waits for the runtime to report the end, if it
/// never does.
const DELIVERY_GRACE: Duration = Duration::from_secs(30);

/// The most a `tail()` call may run: [`DELIVERY_TIMEOUT`], or the handler
/// budget when that is shorter. The runtime ends the call's event at this
/// limit, rejecting its pending operations like any handler that runs out of
/// budget.
pub(crate) fn delivery_budget() -> Duration {
    bounded(crate::js::handler_budget())
}

fn bounded(handler_budget: Duration) -> Duration {
    DELIVERY_TIMEOUT.min(handler_budget)
}

/// Sends one batch, a JSON array of trace items, to the `tail()` handler of a
/// consumer script. The future ends when the call does.
pub(crate) type Deliver = Arc<
    dyn Fn(String, String) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send>>
        + Send
        + Sync,
>;

/// A byte limit shared by every buffer that draws on it.
pub(crate) struct Budget {
    used: AtomicUsize,
    limit: usize,
}

static NODE_BUDGET: LazyLock<Arc<Budget>> =
    LazyLock::new(|| Arc::new(Budget::new(NODE_BUFFER_BYTES)));

impl Budget {
    fn new(limit: usize) -> Self {
        Self {
            used: AtomicUsize::new(0),
            limit,
        }
    }

    /// Claim `bytes`, or `None` when that would pass the limit.
    fn reserve(self: &Arc<Self>, bytes: usize) -> Option<Reservation> {
        let mut used = self.used.load(Ordering::Relaxed);
        loop {
            let after = used
                .checked_add(bytes)
                .filter(|after| *after <= self.limit)?;
            match self
                .used
                .compare_exchange_weak(used, after, Ordering::Relaxed, Ordering::Relaxed)
            {
                Ok(_) => {
                    return Some(Reservation {
                        budget: self.clone(),
                        bytes,
                    })
                }
                Err(current) => used = current,
            }
        }
    }
}

/// Bytes held in a [`Budget`] until the item they pay for is gone.
struct Reservation {
    budget: Arc<Budget>,
    bytes: usize,
}

impl Drop for Reservation {
    fn drop(&mut self) {
        self.budget.used.fetch_sub(self.bytes, Ordering::Relaxed);
    }
}

/// One trace item and its claim on the budgets. Every consumer's buffer holds
/// the same `Arc<Item>`, so the claim is made once and released when the last
/// of them is done with the item.
struct Item {
    json: String,
    _producer: Reservation,
    _node: Reservation,
}

type Queued = Arc<Item>;

struct Lane {
    tx: tokio::sync::mpsc::Sender<Queued>,
    /// Items dropped since the delivery task last reported.
    dropped: Arc<AtomicU64>,
    consumer: String,
}

/// The buffers of one traced Worker, one per consumer. The sink lives as long
/// as the producer's generation: dropping it retires the buffers, and the
/// items they still hold are dropped, because the consumer of that generation
/// is no longer there to take them.
pub struct TailSink {
    script: String,
    lanes: Vec<Lane>,
    retired: Arc<AtomicBool>,
    /// What this producer holds, within `node`.
    producer: Arc<Budget>,
    node: Arc<Budget>,
}

impl Drop for TailSink {
    fn drop(&mut self) {
        // Before the senders drop, so a lane that wakes on the closed channel
        // sees it.
        self.retired.store(true, Ordering::SeqCst);
    }
}

impl TailSink {
    /// `None` for a Worker with no consumers, so an untraced Worker pays
    /// nothing. The delivery tasks run on the process runtime.
    pub(crate) fn new(script: &str, consumers: &[String], deliver: Deliver) -> Option<Arc<Self>> {
        Self::with_budget(
            script,
            consumers,
            deliver,
            NODE_BUDGET.clone(),
            PRODUCER_BUFFER_BYTES,
        )
    }

    fn with_budget(
        script: &str,
        consumers: &[String],
        deliver: Deliver,
        node: Arc<Budget>,
        producer_bytes: usize,
    ) -> Option<Arc<Self>> {
        if consumers.is_empty() {
            return None;
        }
        let retired = Arc::new(AtomicBool::new(false));
        let lanes = consumers
            .iter()
            .map(|consumer| {
                let (tx, rx) = tokio::sync::mpsc::channel(BUFFER_ITEMS);
                let dropped = Arc::new(AtomicU64::new(0));
                crate::asyncrt::spawn(deliver_lane(
                    script.to_string(),
                    consumer.clone(),
                    rx,
                    dropped.clone(),
                    retired.clone(),
                    node.clone(),
                    deliver.clone(),
                ))
                .detach();
                Lane {
                    tx,
                    dropped,
                    consumer: consumer.clone(),
                }
            })
            .collect();
        Some(Arc::new(Self {
            script: script.to_string(),
            lanes,
            retired,
            producer: Arc::new(Budget::new(producer_bytes)),
            node,
        }))
    }

    /// Queue one trace item, a JSON object, for every consumer. Never blocks.
    pub(crate) fn submit(&self, item: String) {
        let queued: Option<Queued> = self
            .producer
            .reserve(item.len())
            .zip(self.node.reserve(item.len()))
            .map(|(producer, node)| {
                Arc::new(Item {
                    json: item,
                    _producer: producer,
                    _node: node,
                })
            });
        for lane in &self.lanes {
            let sent = queued
                .as_ref()
                .and_then(|queued| lane.tx.try_send(queued.clone()).ok());
            if sent.is_none() && lane.dropped.fetch_add(1, Ordering::Relaxed) == 0 {
                // The first drop since the last report; the task reports the
                // total when it next gets to run.
                tracing::warn!(
                    script = %self.script,
                    consumer = %lane.consumer,
                    "tail consumer is not keeping up; dropping trace items"
                );
            }
        }
    }
}

/// What a trace item shows in place of a value it redacts.
const REDACTED: &str = "REDACTED";

/// Whether a trace item hides the value of this request header. `name` is
/// lower case. The rule is the one `TraceItem.event.request.headers` applies
/// on Cloudflare: `cookie`, `set-cookie`, and any name containing `auth`,
/// `jwt`, `key`, `secret` or `token`.
fn header_is_redacted(name: &str) -> bool {
    name == "cookie"
        || name == "set-cookie"
        || ["auth", "jwt", "key", "secret", "token"]
            .iter()
            .any(|part| name.contains(part))
}

/// The request headers of a trace item with the values of
/// [`header_is_redacted`] names replaced, and whether any was.
pub(crate) fn redact_headers(
    headers: &serde_json::Map<String, serde_json::Value>,
) -> (serde_json::Map<String, serde_json::Value>, bool) {
    let mut changed = false;
    let redacted = headers
        .iter()
        .map(|(name, value)| {
            if header_is_redacted(name) {
                changed = true;
                (
                    name.clone(),
                    serde_json::Value::String(REDACTED.to_string()),
                )
            } else {
                (name.clone(), value.clone())
            }
        })
        .collect();
    (redacted, changed)
}

/// The counts of one run of URL characters, to tell an identifier from a word.
#[derive(Default)]
struct Run {
    /// The decoded characters of the run.
    bytes: Vec<u8>,
    hex: usize,
    digits: usize,
    upper: usize,
    lower: usize,
    non_hex: bool,
}

impl Run {
    fn add(&mut self, byte: u8) {
        self.bytes.push(byte);
        let separator = matches!(byte, b'+' | b'-' | b'_');
        if byte.is_ascii_hexdigit() {
            self.hex += 1;
        } else if !separator {
            self.non_hex = true;
        }
        self.upper += usize::from(byte.is_ascii_uppercase());
        self.lower += usize::from(byte.is_ascii_lowercase());
        self.digits += usize::from(byte.is_ascii_digit());
    }

    /// A hex identifier of 32 digits or more, a base64-like identifier of 21
    /// characters or more with two each of digits, upper and lower case
    /// letters, or a payment card number (13 to 19 digits that pass the Luhn
    /// check).
    fn is_secret(&self) -> bool {
        (self.hex >= 32 && !self.non_hex)
            || (self.bytes.len() >= 21 && self.digits >= 2 && self.upper >= 2 && self.lower >= 2)
            || is_card_number(&self.bytes)
    }
}

fn is_card_number(run: &[u8]) -> bool {
    let (mut digits, mut sum, mut double) = (0u32, 0u32, false);
    for &byte in run.iter().rev() {
        if matches!(byte, b'+' | b'-' | b'_') {
            continue;
        }
        if !byte.is_ascii_digit() {
            return false;
        }
        let mut digit = u32::from(byte - b'0');
        if double {
            digit *= 2;
            if digit > 9 {
                digit -= 9;
            }
        }
        sum += digit;
        digits += 1;
        double = !double;
    }
    (13..=19).contains(&digits) && sum % 10 == 0
}

/// The URL of a trace item with each run of letters, digits and `+ - _` that
/// looks like an identifier or a card number replaced. A run ends at any other
/// character, so a path segment or a query value is one run. A percent-encoded
/// character counts as the character it stands for, so encoding an identifier
/// does not hide it, and a replaced run is replaced whole, encoded span
/// included. This is the rule `TraceItem.event.request.url` applies on
/// Cloudflare.
pub(crate) fn redact_url(url: &str) -> String {
    let mut out = Vec::with_capacity(url.len());
    let mut start = 0;
    let mut run = Run::default();
    let end_run = |out: &mut Vec<u8>, run: &mut Run, span: &[u8]| {
        if run.is_secret() {
            out.extend_from_slice(REDACTED.as_bytes());
        } else {
            out.extend_from_slice(span);
        }
        *run = Run::default();
    };
    let bytes = url.as_bytes();
    let mut index = 0;
    while index < bytes.len() {
        let byte = bytes[index];
        let escaped = (byte == b'%')
            .then(|| bytes.get(index + 1..index + 3))
            .flatten()
            .filter(|pair| pair.iter().all(u8::is_ascii_hexdigit))
            .and_then(|pair| std::str::from_utf8(pair).ok())
            .and_then(|pair| u8::from_str_radix(pair, 16).ok());
        let (decoded, width) = match escaped {
            Some(decoded) => (decoded, 3),
            None => (byte, 1),
        };
        if decoded.is_ascii_alphanumeric() || matches!(decoded, b'+' | b'-' | b'_') {
            run.add(decoded);
        } else {
            end_run(&mut out, &mut run, &bytes[start..index]);
            out.extend_from_slice(&bytes[index..index + width]);
            start = index + width;
        }
        index += width;
    }
    end_run(&mut out, &mut run, &bytes[start..]);
    // Only whole ASCII runs were replaced, so the bytes are still UTF-8.
    String::from_utf8(out).unwrap_or_else(|_| REDACTED.to_string())
}

/// The bytes of the batch the consumer receives, the array of items.
fn encoded_len(items: &[Queued]) -> usize {
    items.iter().map(|queued| queued.json.len()).sum::<usize>() + items.len().max(1) + 1
}

/// The batch the consumer receives: the array of items.
fn encode_batch(items: &[Queued]) -> String {
    let mut body = String::with_capacity(encoded_len(items));
    body.push('[');
    for (index, queued) in items.iter().enumerate() {
        if index != 0 {
            body.push(',');
        }
        body.push_str(&queued.json);
    }
    body.push(']');
    body
}

/// What a producer that retired lost in all: the batch the lane had taken, the
/// items still in its buffer, and those the buffer turned away earlier.
fn retired_loss(
    batch: usize,
    rx: &mut tokio::sync::mpsc::Receiver<Queued>,
    dropped: &AtomicU64,
) -> u64 {
    let mut lost = batch as u64 + dropped.swap(0, Ordering::Relaxed);
    while rx.try_recv().is_ok() {
        lost += 1;
    }
    lost
}

async fn deliver_lane(
    script: String,
    consumer: String,
    mut rx: tokio::sync::mpsc::Receiver<Queued>,
    dropped: Arc<AtomicU64>,
    retired: Arc<AtomicBool>,
    node: Arc<Budget>,
    deliver: Deliver,
) {
    let mut batch = Vec::with_capacity(BATCH_ITEMS);
    loop {
        batch.clear();
        if rx.recv_many(&mut batch, BATCH_ITEMS).await == 0 {
            return;
        }
        if retired.load(Ordering::SeqCst) {
            let lost = retired_loss(batch.len(), &mut rx, &dropped);
            tracing::warn!(
                %script, %consumer, dropped = lost,
                "tail producer retired; its buffered trace items were dropped"
            );
            return;
        }
        let lost = dropped.swap(0, Ordering::Relaxed);
        if lost != 0 {
            tracing::warn!(
                %script, %consumer, dropped = lost,
                "tail consumer buffer was full; trace items were dropped"
            );
        }
        // The encoded batch is a second copy of the items, so it is paid for
        // from the node budget before it exists. Once it does, the items
        // release theirs.
        let Some(_encoded) = node.reserve(encoded_len(&batch)) else {
            tracing::warn!(
                %script, %consumer, dropped = batch.len(),
                "tail buffer budget is spent; a batch of trace items was dropped"
            );
            continue;
        };
        let body = encode_batch(&batch);
        let items = batch.len();
        batch.clear();
        // The call ends when the handler returns or throws, or when the
        // runtime ends its event at `delivery_budget`, `waitUntil` work
        // included. The next batch waits for that, so events never overlap;
        // the grace only covers a runtime that never reports.
        let call = deliver(consumer.clone(), body);
        match crate::asyncrt::timeout(delivery_budget() + DELIVERY_GRACE, call).await {
            Ok(Ok(())) => {}
            Ok(Err(error)) => tracing::warn!(
                %script, %consumer, items, %error,
                "tail consumer failed; its trace items are lost"
            ),
            Err(_) => tracing::warn!(
                %script, %consumer, items,
                "tail consumer did not report; its trace items are lost"
            ),
        }
    }
}

/// The delivery a node uses: a `tail()` call on the consumer's default
/// entrypoint, in the generation of the Worker that produced the items.
pub(crate) fn rpc_deliver(generation: crate::generation::GenerationId) -> Deliver {
    Arc::new(move |script, items| {
        Box::pin(async move {
            let sender = crate::js::TAIL_DISPATCH_TX
                .get()
                .ok_or_else(|| "no tail dispatch channel".to_string())?;
            let (reply, receive) = tokio::sync::oneshot::channel();
            sender
                .send(crate::js::TailDispatchReq {
                    generation,
                    script,
                    items,
                    reply,
                })
                .map_err(|_| "tail dispatch channel closed".to_string())?;
            match receive.await {
                // A handler that threw answers `[0]`, not a failed call.
                Ok(Ok(reply)) if reply.first() == Some(&0) => Err("tail() threw".to_string()),
                Ok(Ok(_)) => Ok(()),
                Ok(Err(error)) => Err(format!("{error:#}")),
                Err(_) => Err("the consumer dropped the call".to_string()),
            }
        })
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicUsize;
    use std::sync::Mutex;
    use std::time::Duration;

    /// What a fake consumer saw: each call's consumer and JSON, and how many
    /// calls have started, which a call blocked on the gate has not finished.
    #[derive(Clone, Default)]
    struct Seen {
        calls: Arc<Mutex<Vec<(String, String)>>>,
        entered: Arc<AtomicUsize>,
    }

    impl Seen {
        fn calls(&self) -> Vec<(String, String)> {
            self.calls.lock().unwrap().clone()
        }

        /// Wait until `done` holds, or fail after two seconds.
        async fn until(&self, what: &str, done: impl Fn(&Seen) -> bool) {
            for _ in 0..400 {
                if done(self) {
                    return;
                }
                crate::asyncrt::sleep(Duration::from_millis(5)).await;
            }
            panic!("timed out waiting for {what}");
        }
    }

    fn recording(seen: &Seen, gate: Option<Arc<tokio::sync::Semaphore>>, fail: bool) -> Deliver {
        let seen = seen.clone();
        Arc::new(move |consumer, items| {
            let seen = seen.clone();
            let gate = gate.clone();
            Box::pin(async move {
                seen.entered.fetch_add(1, Ordering::SeqCst);
                if let Some(gate) = gate {
                    gate.acquire().await.unwrap().forget();
                }
                seen.calls.lock().unwrap().push((consumer, items));
                if fail {
                    Err("consumer threw".to_string())
                } else {
                    Ok(())
                }
            })
        })
    }

    #[test]
    fn a_url_hides_hex_and_base64_identifiers_and_card_numbers() {
        // Plain URLs stay as they are.
        assert_eq!(redact_url(""), "");
        assert_eq!(
            redact_url("https://domain/path?a=1&b=2"),
            "https://domain/path?a=1&b=2"
        );
        // Hex identifiers of 32 digits or more, with or without separators.
        assert_eq!(
            redact_url("https://domain/0123456789abcdef0123456789abcdef/x"),
            "https://domain/REDACTED/x"
        );
        assert_eq!(
            redact_url("https://domain/0123456789abcdef-0123456789abcdef/x"),
            "https://domain/REDACTED/x"
        );
        assert_eq!(
            redact_url("https://domain/0123456789abcdef0123456789abcde/x"),
            "https://domain/0123456789abcdef0123456789abcde/x"
        );
        assert_eq!(
            redact_url("https://domain/0123456789abcdef0123456789abcdefg/x"),
            "https://domain/0123456789abcdef0123456789abcdefg/x"
        );
        // Base64-like identifiers.
        assert_eq!(
            redact_url("https://domain/01234567890123456azAZ/x"),
            "https://domain/REDACTED/x"
        );
        for kept in [
            "0123456789012345azAZ",
            "012345678901234567zAZ",
            "012345678901234567azZ",
            "IThinkIShallNeverSee0",
        ] {
            let url = format!("https://domain/{kept}/x");
            assert_eq!(redact_url(&url), url);
        }
        // Card numbers that pass the Luhn check, with or without separators.
        for number in ["4222222222222", "378282246310005", "4111-1111-1111-1111"] {
            assert_eq!(
                redact_url(&format!("https://domain/path?number={number}")),
                "https://domain/path?number=REDACTED"
            );
        }
        for kept in ["4111111111111112", "79927398713"] {
            let url = format!("https://domain/path?number={kept}");
            assert_eq!(redact_url(&url), url);
        }
        // Text around a run, and non-ASCII text, is kept.
        assert_eq!(
            redact_url("https://domain/caf\u{e9}?token=0123456789abcdef0123456789abcdef&x=1"),
            "https://domain/caf\u{e9}?token=REDACTED&x=1"
        );
    }

    #[test]
    fn a_percent_encoded_identifier_is_hidden_too() {
        // `0123456789abcdef` twice, every character encoded.
        let hex: String = "0123456789abcdef0123456789abcdef"
            .bytes()
            .map(|byte| format!("%{byte:02X}"))
            .collect();
        assert_eq!(
            redact_url(&format!("https://domain/{hex}/x?t={hex}")),
            "https://domain/REDACTED/x?t=REDACTED"
        );
        // Half encoded, with an encoded character that is not part of a run.
        assert_eq!(
            redact_url("https://domain/01234567%38%39abcdef0123456789abcdef"),
            "https://domain/REDACTED"
        );
        let base64: String = "aB3dE6gH9jK2mN5pQ8sT1"
            .bytes()
            .map(|byte| format!("%{byte:02x}"))
            .collect();
        assert_eq!(
            redact_url(&format!("https://domain/p?k={base64}&y=2")),
            "https://domain/p?k=REDACTED&y=2"
        );
        // Short encoded values and an encoded separator are kept as written.
        let kept = "https://domain/a%2Fb%20c?q=%41%42&r=%zz%+1";
        assert_eq!(redact_url(kept), kept);
    }

    #[test]
    fn credential_headers_are_redacted_and_others_are_not() {
        let headers = serde_json::json!({
            "authorization": "Bearer abc",
            "proxy-authorization": "Basic abc",
            "cookie": "a=b",
            "set-cookie": "a=b",
            "x-api-key": "k",
            "x-jwt-assertion": "j",
            "x-client-secret": "s",
            "x-csrf-token": "t",
            "content-type": "text/plain",
            "user-agent": "curl",
        });
        let (redacted, changed) = redact_headers(headers.as_object().unwrap());
        assert!(changed);
        for (name, value) in &redacted {
            let expected = if name == "content-type" || name == "user-agent" {
                headers[name].clone()
            } else {
                serde_json::json!("REDACTED")
            };
            assert_eq!(value, &expected, "{name}");
        }
        let plain = serde_json::json!({ "accept": "*/*" });
        let (same, changed) = redact_headers(plain.as_object().unwrap());
        assert!(!changed);
        assert_eq!(serde_json::Value::Object(same), plain);
    }

    #[test]
    fn no_consumer_means_no_sink() {
        crate::asyncrt::install_test_host_runtime();
        let deliver: Deliver = Arc::new(|_, _| Box::pin(async { Ok(()) }));
        assert!(TailSink::new("producer", &[], deliver).is_none());
    }

    #[tokio::test]
    async fn items_reach_every_consumer_as_one_array() {
        crate::asyncrt::install_test_host_runtime();
        let seen = Seen::default();
        let sink = TailSink::new(
            "producer",
            &["a".to_string(), "b".to_string()],
            recording(&seen, None, false),
        )
        .unwrap();
        sink.submit(r#"{"n":1}"#.to_string());
        seen.until("both consumers", |seen| seen.calls().len() == 2)
            .await;
        let mut calls = seen.calls();
        calls.sort();
        assert_eq!(
            calls,
            vec![
                ("a".to_string(), r#"[{"n":1}]"#.to_string()),
                ("b".to_string(), r#"[{"n":1}]"#.to_string()),
            ]
        );
    }

    #[tokio::test]
    async fn items_that_gather_during_a_call_go_out_together() {
        crate::asyncrt::install_test_host_runtime();
        let seen = Seen::default();
        let gate = Arc::new(tokio::sync::Semaphore::new(0));
        let sink = TailSink::new(
            "producer",
            &["a".to_string()],
            recording(&seen, Some(gate.clone()), false),
        )
        .unwrap();
        sink.submit(r#"{"n":1}"#.to_string());
        seen.until("the first call to start", |seen| {
            seen.entered.load(Ordering::SeqCst) == 1
        })
        .await;
        sink.submit(r#"{"n":2}"#.to_string());
        sink.submit(r#"{"n":3}"#.to_string());
        gate.add_permits(2);
        seen.until("both calls", |seen| seen.calls().len() == 2)
            .await;
        assert_eq!(
            seen.calls()
                .iter()
                .map(|(_, body)| body.as_str())
                .collect::<Vec<_>>(),
            vec![r#"[{"n":1}]"#, r#"[{"n":2},{"n":3}]"#]
        );
    }

    #[tokio::test]
    async fn a_stuck_consumer_never_blocks_the_producer_and_the_buffer_is_bounded() {
        crate::asyncrt::install_test_host_runtime();
        let seen = Seen::default();
        // Closed: the first call hangs, as a consumer that never returns.
        let gate = Arc::new(tokio::sync::Semaphore::new(0));
        let sink = TailSink::new(
            "producer",
            &["slow".to_string()],
            recording(&seen, Some(gate.clone()), false),
        )
        .unwrap();
        sink.submit("{}".to_string());
        seen.until("the call to start", |seen| {
            seen.entered.load(Ordering::SeqCst) == 1
        })
        .await;
        let started = crate::asyncrt::mono_ms();
        for _ in 0..(BUFFER_ITEMS + 50) {
            sink.submit("{}".to_string());
        }
        assert!(crate::asyncrt::mono_ms() - started < 1000, "submit blocked");
        assert_eq!(sink.lanes[0].dropped.load(Ordering::Relaxed), 50);
        // Freed, the consumer receives what the buffer held, in batches, and
        // nothing more.
        gate.add_permits(1000);
        let delivered = |seen: &Seen| -> usize {
            seen.calls()
                .iter()
                .map(|(_, body)| body.matches("{}").count())
                .sum()
        };
        seen.until("the buffer to drain", |seen| {
            delivered(seen) == 1 + BUFFER_ITEMS
        })
        .await;
        assert!(seen
            .calls()
            .iter()
            .all(|(_, body)| body.matches("{}").count() <= BATCH_ITEMS));
    }

    #[tokio::test]
    async fn a_slow_consumer_never_has_two_calls_running() {
        crate::asyncrt::install_test_host_runtime();
        let seen = Seen::default();
        let gate = Arc::new(tokio::sync::Semaphore::new(0));
        let sink = TailSink::new(
            "producer",
            &["slow".to_string()],
            recording(&seen, Some(gate.clone()), false),
        )
        .unwrap();
        sink.submit(r#"{"n":1}"#.to_string());
        seen.until("the first call to start", |seen| {
            seen.entered.load(Ordering::SeqCst) == 1
        })
        .await;
        sink.submit(r#"{"n":2}"#.to_string());
        // Long enough for any delivery timeout the lane could apply in a test.
        crate::asyncrt::sleep(Duration::from_millis(400)).await;
        assert_eq!(
            seen.entered.load(Ordering::SeqCst),
            1,
            "a second call started"
        );
        // The call ends; what gathered meanwhile goes out as the next call.
        gate.add_permits(2);
        seen.until("both calls", |seen| seen.calls().len() == 2)
            .await;
        assert_eq!(seen.calls()[1].1, r#"[{"n":2}]"#);
    }

    #[tokio::test]
    async fn items_still_buffered_when_the_producer_retires_are_dropped() {
        crate::asyncrt::install_test_host_runtime();
        let seen = Seen::default();
        let gate = Arc::new(tokio::sync::Semaphore::new(0));
        let sink = TailSink::new(
            "producer",
            &["a".to_string()],
            recording(&seen, Some(gate.clone()), false),
        )
        .unwrap();
        sink.submit(r#"{"n":1}"#.to_string());
        seen.until("the first call to start", |seen| {
            seen.entered.load(Ordering::SeqCst) == 1
        })
        .await;
        sink.submit(r#"{"n":2}"#.to_string());
        sink.submit(r#"{"n":3}"#.to_string());
        // The producer's generation retires: its sink is dropped.
        drop(sink);
        gate.add_permits(10);
        crate::asyncrt::sleep(Duration::from_millis(200)).await;
        assert_eq!(
            seen.calls().len(),
            1,
            "items of a retired producer reached a consumer"
        );
    }

    #[tokio::test]
    async fn buffered_items_of_all_producers_share_one_byte_budget() {
        crate::asyncrt::install_test_host_runtime();
        let budget = Arc::new(Budget::new(100));
        let seen = Seen::default();
        let gate = Arc::new(tokio::sync::Semaphore::new(0));
        let sink = |script: &str| {
            TailSink::with_budget(
                script,
                &["slow".to_string()],
                recording(&seen, Some(gate.clone()), false),
                budget.clone(),
                usize::MAX,
            )
            .unwrap()
        };
        let (first, second) = (sink("first"), sink("second"));
        // Each producer has a call running on 30 bytes, held as the batch
        // `[...]` of 32.
        first.submit("a".repeat(30));
        seen.until("the first call to start", |seen| {
            seen.entered.load(Ordering::SeqCst) == 1
        })
        .await;
        second.submit("b".repeat(30));
        seen.until("the second call to start", |seen| {
            seen.entered.load(Ordering::SeqCst) == 2
        })
        .await;
        assert_eq!(budget.used.load(Ordering::Relaxed), 64);
        // 40 more bytes pass the node's 100, whichever producer sends them.
        first.submit("c".repeat(40));
        assert_eq!(first.lanes[0].dropped.load(Ordering::Relaxed), 1);
        // 36 fit.
        second.submit("d".repeat(36));
        assert_eq!(second.lanes[0].dropped.load(Ordering::Relaxed), 0);
        assert_eq!(budget.used.load(Ordering::Relaxed), 100);
        // What is delivered is released.
        gate.add_permits(10);
        seen.until("the budget to be released", |_| {
            budget.used.load(Ordering::Relaxed) == 0
        })
        .await;
    }

    #[tokio::test]
    async fn one_producer_at_its_cap_leaves_room_for_another() {
        crate::asyncrt::install_test_host_runtime();
        let node = Arc::new(Budget::new(1000));
        let seen = Seen::default();
        let gate = Arc::new(tokio::sync::Semaphore::new(0));
        let sink = |script: &str| {
            TailSink::with_budget(
                script,
                &["slow".to_string()],
                recording(&seen, Some(gate.clone()), false),
                node.clone(),
                100,
            )
            .unwrap()
        };
        let (greedy, other) = (sink("greedy"), sink("other"));
        greedy.submit("a".repeat(60));
        seen.until("the call to start", |seen| {
            seen.entered.load(Ordering::SeqCst) == 1
        })
        .await;
        greedy.submit("a".repeat(40));
        // What it buffers is capped at 100, whatever the node has left.
        greedy.submit("a".repeat(61));
        assert_eq!(greedy.lanes[0].dropped.load(Ordering::Relaxed), 1);
        // Another producer still buffers.
        other.submit("b".repeat(60));
        assert_eq!(other.lanes[0].dropped.load(Ordering::Relaxed), 0);
        // The running call's batch of 62, and the 40 and 60 waiting.
        assert_eq!(node.used.load(Ordering::Relaxed), 162);
        gate.add_permits(10);
        seen.until("the budget to be released", |_| {
            node.used.load(Ordering::Relaxed) == 0
        })
        .await;
    }

    #[tokio::test]
    async fn a_producer_with_several_consumers_buffers_its_whole_cap_of_items() {
        crate::asyncrt::install_test_host_runtime();
        let node = Arc::new(Budget::new(10_000));
        let seen = Seen::default();
        let gate = Arc::new(tokio::sync::Semaphore::new(0));
        let consumers = ["a", "b", "c"].map(String::from);
        let sink = TailSink::with_budget(
            "producer",
            &consumers,
            recording(&seen, Some(gate.clone()), false),
            node.clone(),
            100,
        )
        .unwrap();
        sink.submit("x".to_string());
        seen.until("every consumer's call to start", |seen| {
            seen.entered.load(Ordering::SeqCst) == 3
        })
        .await;
        // Ten distinct items of ten bytes fill the cap of 100 once, however
        // many consumers share them.
        for _ in 0..10 {
            sink.submit("y".repeat(10));
        }
        for lane in &sink.lanes {
            assert_eq!(lane.dropped.load(Ordering::Relaxed), 0);
        }
        assert_eq!(sink.producer.used.load(Ordering::Relaxed), 100);
        sink.submit("z".to_string());
        for lane in &sink.lanes {
            assert_eq!(lane.dropped.load(Ordering::Relaxed), 1);
        }
        // The claim goes when the last consumer has taken the item.
        gate.add_permits(100);
        seen.until("the budget to be released", |_| {
            node.used.load(Ordering::Relaxed) == 0
        })
        .await;
        assert_eq!(sink.producer.used.load(Ordering::Relaxed), 0);
    }

    #[tokio::test]
    async fn a_running_batch_counts_against_the_node_with_the_items_waiting() {
        crate::asyncrt::install_test_host_runtime();
        let node = Arc::new(Budget::new(100));
        let seen = Seen::default();
        let gate = Arc::new(tokio::sync::Semaphore::new(0));
        let sink = TailSink::with_budget(
            "producer",
            &["slow".to_string()],
            recording(&seen, Some(gate.clone()), false),
            node.clone(),
            usize::MAX,
        )
        .unwrap();
        sink.submit("a".repeat(40));
        seen.until("the call to start", |seen| {
            seen.entered.load(Ordering::SeqCst) == 1
        })
        .await;
        // The batch the call runs on is held once, as its encoded 42 bytes.
        assert_eq!(node.used.load(Ordering::Relaxed), 42);
        sink.submit("b".repeat(60));
        assert_eq!(sink.lanes[0].dropped.load(Ordering::Relaxed), 1);
        sink.submit("b".repeat(58));
        assert_eq!(node.used.load(Ordering::Relaxed), 100);
        gate.add_permits(10);
        seen.until("the budget to be released", |_| {
            node.used.load(Ordering::Relaxed) == 0
        })
        .await;
    }

    #[tokio::test]
    async fn a_batch_the_node_cannot_encode_is_dropped_and_released() {
        crate::asyncrt::install_test_host_runtime();
        let node = Arc::new(Budget::new(100));
        let seen = Seen::default();
        let sink = TailSink::with_budget(
            "producer",
            &["a".to_string()],
            recording(&seen, None, false),
            node.clone(),
            usize::MAX,
        )
        .unwrap();
        // The item fits, but it and its encoded copy do not.
        sink.submit("a".repeat(60));
        seen.until("the budget to be released", |_| {
            node.used.load(Ordering::Relaxed) == 0
        })
        .await;
        assert!(seen.calls().is_empty());
    }

    #[test]
    fn a_retired_producer_reports_everything_it_lost_once() {
        let (tx, mut rx) = tokio::sync::mpsc::channel(8);
        let budget = Arc::new(Budget::new(100));
        for _ in 0..3 {
            tx.try_send(Arc::new(Item {
                json: "{}".to_string(),
                _producer: budget.reserve(1).unwrap(),
                _node: budget.reserve(1).unwrap(),
            }))
            .unwrap();
        }
        let dropped = AtomicU64::new(5);
        // Two taken as a batch, three still buffered, five turned away.
        assert_eq!(retired_loss(2, &mut rx, &dropped), 10);
        assert_eq!(dropped.load(Ordering::Relaxed), 0);
        assert_eq!(budget.used.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn a_call_is_bounded_by_thirty_seconds_or_a_shorter_handler_budget() {
        assert_eq!(bounded(Duration::from_secs(300)), Duration::from_secs(30));
        assert_eq!(bounded(Duration::from_secs(2)), Duration::from_secs(2));
    }

    #[tokio::test]
    async fn a_failing_consumer_does_not_stop_later_deliveries() {
        crate::asyncrt::install_test_host_runtime();
        let seen = Seen::default();
        let sink = TailSink::new(
            "producer",
            &["bad".to_string()],
            recording(&seen, None, true),
        )
        .unwrap();
        sink.submit(r#"{"n":1}"#.to_string());
        seen.until("the first call", |seen| seen.calls().len() == 1)
            .await;
        sink.submit(r#"{"n":2}"#.to_string());
        seen.until("the second call", |seen| seen.calls().len() == 2)
            .await;
    }
}
