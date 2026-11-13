//! A scripted shell for lifecycle tests of the core.
//!
//! The shell plays the executor for the effects that need no decision: it
//! answers owner reads from the records it has seen, grants every claim, and
//! restores, starts and publishes on request. Every other effect stays in the
//! inbox for the test to answer in the order it wants. `on_event` validates
//! the state after every event in a test build, so a broken invariant panics
//! inside `send`.

use crate::*;

pub(crate) struct Shell {
    pub state: State,
    /// Effects the shell has not answered.
    pub inbox: Vec<Effect>,
    owners: BTreeMap<CellId, Epoch>,
    /// Restores of these cells fail, as when the replica cannot be read.
    pub failing_restores: BTreeSet<CellId>,
    next_request: RequestId,
    /// The monotonic and wall clock, in milliseconds, advanced per event.
    pub now: u64,
}

impl Shell {
    pub(crate) fn new(config: Config) -> Self {
        let config = Config {
            // The lease protocol is not under test, and a node that needs one
            // refuses every request until a lease write completes.
            require_node_lease: false,
            ..config
        };
        Self {
            state: State::new("node", config),
            inbox: Vec::new(),
            owners: BTreeMap::new(),
            failing_restores: BTreeSet::new(),
            next_request: 1,
            now: 1_000_000,
        }
    }

    /// A shell whose node holds a live lease, and so has begun confirming
    /// the cells on its disk: the `ReadLocalCells` effect waits in the inbox.
    pub(crate) fn leased(config: Config) -> Self {
        let mut shell = Self {
            state: State::new("node", config),
            inbox: Vec::new(),
            owners: BTreeMap::new(),
            failing_restores: BTreeSet::new(),
            next_request: 1,
            now: 1_000_000,
        };
        let now = shell.now;
        shell.send(Event::StartNodeLease {
            now_ms: now,
            now_mono_ms: now,
            spec: NodeLeaseSpec {
                addr: "node:8081".into(),
                peer_protocol: 1,
                generation: "g".into(),
                resume_generation: None,
                ttl_ms: 30_000,
            },
        });
        let op = shell.expect("a self lease read", |effect| match effect {
            Effect::ReadSelfNodeLease { op } => Some(*op),
            _ => None,
        });
        shell.send(Event::SelfNodeLeaseRead {
            op,
            now_ms: now,
            now_mono_ms: now,
            result: Ok(None),
        });
        let op = shell.expect("a lease write", |effect| match effect {
            Effect::CasNodeLease { op, .. } => Some(*op),
            _ => None,
        });
        shell.send(Event::NodeLeaseCasCompleted {
            op,
            now_mono_ms: now,
            result: Ok(LeaseCasOutcome::Applied { etag: "e".into() }),
            stamped_log_state: None,
        });
        assert!(shell.state.node_authoritative());
        shell
    }

    /// Report the cells found on disk after the lease, as live local epochs.
    pub(crate) fn local_cells(&mut self, cells: &[(&str, Epoch)]) {
        self.expect("a local inventory read", |effect| {
            matches!(effect, Effect::ReadLocalCells).then_some(())
        });
        self.send(Event::LocalCellsRead {
            result: Ok(cells
                .iter()
                .map(|(id, epoch)| LocalCell {
                    id: (*id).into(),
                    epoch: *epoch,
                    live: true,
                })
                .collect()),
        });
    }

    pub(crate) fn send(&mut self, event: Event) -> Vec<Effect> {
        let effects = on_event(&mut self.state, event);
        self.inbox.extend(effects.iter().cloned());
        effects
    }

    /// Advance the clock and return it.
    pub(crate) fn tick(&mut self, ms: u64) -> u64 {
        self.now += ms;
        self.now
    }

    /// Route a new request to `cell` and answer the activation it causes.
    pub(crate) fn request(&mut self, cell: &str) -> RequestId {
        let request = self.next_request;
        self.next_request += 1;
        let now_mono_ms = self.tick(1);
        self.send(Event::RequestAt {
            request,
            cell: cell.into(),
            now_mono_ms,
        });
        self.settle();
        request
    }

    /// End a request's activity, as the shell does when the response is done.
    pub(crate) fn finish(&mut self, request: RequestId) {
        self.send(Event::ActivityFinished { request });
        self.settle();
    }

    /// Answer activation effects until none is left.
    pub(crate) fn settle(&mut self) {
        loop {
            let Some(index) = self.inbox.iter().position(|effect| {
                matches!(
                    effect,
                    Effect::ReadOwner { .. }
                        | Effect::CasOwner { .. }
                        | Effect::Restore { .. }
                        | Effect::StartRuntime { .. }
                        | Effect::Publish { .. }
                )
            }) else {
                return;
            };
            let reply = match self.inbox.remove(index) {
                Effect::ReadOwner { op, cell } => Event::OwnerRead {
                    op,
                    now_ms: self.now,
                    result: Ok(self.owners.get(&cell).map(|epoch| OwnerRecord {
                        node: Some(self.state.node().to_string()),
                        epoch: *epoch,
                        etag: format!("etag-{epoch}"),
                    })),
                },
                Effect::CasOwner {
                    op, cell, epoch, ..
                } => {
                    self.owners.insert(cell, epoch);
                    Event::OwnerCasCompleted {
                        op,
                        result: Ok(CasOutcome::Applied),
                    }
                }
                Effect::Restore { op, cell, .. } => Event::RestoreCompleted {
                    op,
                    result: if self.failing_restores.contains(&cell) {
                        Err(Failure::Definite)
                    } else {
                        Ok(RestoreOutcome {
                            restored: false,
                            alarm: None,
                        })
                    },
                },
                Effect::StartRuntime { op, .. } => Event::RuntimeStarted {
                    op,
                    isolate: None,
                    generation: 0,
                    result: Ok(()),
                },
                Effect::Publish { op, .. } => Event::Published { op, result: Ok(()) },
                _ => unreachable!("only answerable effects are selected"),
            };
            self.send(reply);
        }
    }

    /// Remove and return the first unanswered effect `pick` accepts.
    pub(crate) fn take<T>(&mut self, pick: impl Fn(&Effect) -> Option<T>) -> Option<T> {
        let index = self
            .inbox
            .iter()
            .position(|effect| pick(effect).is_some())?;
        let effect = self.inbox.remove(index);
        pick(&effect)
    }

    /// Like [`Shell::take`], but the effect must be there.
    pub(crate) fn expect<T>(&mut self, what: &str, pick: impl Fn(&Effect) -> Option<T>) -> T {
        match self.take(pick) {
            Some(found) => found,
            None => panic!("expected {what}; unanswered effects: {:?}", self.inbox),
        }
    }

    /// Carry an eviction that has begun on `cell` through its proof and stop.
    pub(crate) fn finish_eviction(&mut self, cell: &str) {
        let op = self.expect("an eviction proof", |effect| match effect {
            Effect::EnsureDurable { op, cell: c, .. } if c == cell => Some(*op),
            _ => None,
        });
        self.send(Event::DurabilityChecked { op, result: Ok(()) });
        let op = self.expect("an eviction stop", |effect| match effect {
            Effect::StopRuntime { op, cell: c, .. } if c == cell => Some(*op),
            _ => None,
        });
        self.send(Event::RuntimeStopped { op });
        self.settle();
    }
}
