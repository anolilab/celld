//! Tail Workers: delivery of a traced Worker's trace items to the `tail()`
//! handler of each Worker in its `tail_consumers`.
//!
//! A traced invocation hands its finished trace item to [`TailSink::submit`],
//! which is one non-blocking `try_send` per consumer. Everything else happens
//! on a task of its own: it takes the items that have gathered, sends them as
//! one `tail(events)` call, and waits for the answer before the next batch.
//! The producer never waits for, and never sees the result of, that call.
//!
//! Each consumer has a bounded buffer. A consumer that is slow, hung or
//! failing lets it fill, and the items that do not fit are dropped and counted.

use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

/// Trace items one consumer can have waiting. A full buffer drops the newest.
pub(crate) const BUFFER_ITEMS: usize = 256;
/// The most trace items one `tail()` call carries.
pub(crate) const BATCH_ITEMS: usize = 100;
/// How long one `tail()` call may run before delivery gives up on it.
const DELIVERY_TIMEOUT: Duration = Duration::from_secs(30);

/// Sends one batch, a JSON array of trace items, to the `tail()` handler of a
/// consumer script.
pub(crate) type Deliver = Arc<
    dyn Fn(String, String) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send>>
        + Send
        + Sync,
>;

struct Lane {
    tx: tokio::sync::mpsc::Sender<Arc<str>>,
    /// Items dropped since the delivery task last reported.
    dropped: Arc<AtomicU64>,
    consumer: String,
}

/// The buffers of one traced Worker, one per consumer. Dropping the sink ends
/// the delivery tasks once they have sent what is buffered.
pub struct TailSink {
    script: String,
    lanes: Vec<Lane>,
}

impl TailSink {
    /// `None` for a Worker with no consumers, so an untraced Worker pays
    /// nothing. The delivery tasks run on the process runtime.
    pub(crate) fn new(script: &str, consumers: &[String], deliver: Deliver) -> Option<Arc<Self>> {
        if consumers.is_empty() {
            return None;
        }
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
        }))
    }

    /// Queue one trace item, a JSON object, for every consumer. Never blocks.
    pub(crate) fn submit(&self, item: String) {
        let item: Arc<str> = item.into();
        for lane in &self.lanes {
            if lane.tx.try_send(item.clone()).is_err()
                && lane.dropped.fetch_add(1, Ordering::Relaxed) == 0
            {
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

/// The batch the consumer receives: the array of items.
fn encode_batch(items: &[Arc<str>]) -> String {
    let mut body =
        String::with_capacity(items.iter().map(|item| item.len() + 1).sum::<usize>() + 2);
    body.push('[');
    for (index, item) in items.iter().enumerate() {
        if index != 0 {
            body.push(',');
        }
        body.push_str(item);
    }
    body.push(']');
    body
}

async fn deliver_lane(
    script: String,
    consumer: String,
    mut rx: tokio::sync::mpsc::Receiver<Arc<str>>,
    dropped: Arc<AtomicU64>,
    deliver: Deliver,
) {
    let mut batch = Vec::with_capacity(BATCH_ITEMS);
    loop {
        batch.clear();
        if rx.recv_many(&mut batch, BATCH_ITEMS).await == 0 {
            return;
        }
        let lost = dropped.swap(0, Ordering::Relaxed);
        if lost != 0 {
            tracing::warn!(
                %script, %consumer, dropped = lost,
                "tail consumer buffer was full; trace items were dropped"
            );
        }
        let call = deliver(consumer.clone(), encode_batch(&batch));
        match crate::asyncrt::timeout(DELIVERY_TIMEOUT, call).await {
            Ok(Ok(())) => {}
            Ok(Err(error)) => tracing::warn!(
                %script, %consumer, items = batch.len(), %error,
                "tail consumer failed; its trace items are lost"
            ),
            Err(_) => tracing::warn!(
                %script, %consumer, items = batch.len(),
                "tail consumer timed out; its trace items are lost"
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
