//! An alarm handler may run longer than one operation, and a firing alarm
//! must never be started again while its first run may still be executing
//! (#42).

use crate::*;

const DEADLINE_MS: u64 = 15_000;
const ALARM_DEADLINE_MS: u64 = 315_000;
const CELL: &str = "cell";
const START_MS: u64 = 1_000_000;
const FIRE_MS: u64 = START_MS + 5_000;

struct Sim {
    state: State,
    timers: Vec<(Timer, u64)>,
}

impl Sim {
    fn resident_with_alarm() -> Self {
        let config = Config {
            ownership_on_evict: OwnershipOnEvict::Sticky,
            require_node_lease: false,
            operation_deadline_ms: Some(DEADLINE_MS),
            alarm_deadline_ms: Some(ALARM_DEADLINE_MS),
            alarm_resident_ms: 60_000,
            ..crate::test_support::config()
        };
        let mut sim = Self {
            state: State::new("node", config),
            timers: Vec::new(),
        };
        sim.feed(Event::RequestAt {
            request: 1,
            cell: CELL.into(),
            now_mono_ms: START_MS,
        });
        sim.feed(Event::AlarmObserved {
            cell: CELL.into(),
            alarm: wake::AlarmSnapshot::without_wake(Some(FIRE_MS as i64)),
            covered: true,
            now_ms: START_MS,
            now_mono_ms: START_MS,
        });
        sim
    }

    /// Run one event and answer the activation effects it causes, so the cell
    /// ends up resident. Everything else is returned for the test to inspect.
    fn feed(&mut self, event: Event) -> Vec<Effect> {
        let mut pending = on_event(&mut self.state, event);
        let mut out = Vec::new();
        while !pending.is_empty() {
            let mut next = Vec::new();
            for effect in pending {
                let reply = match &effect {
                    Effect::ReadOwner { op, .. } => Some(Event::OwnerRead {
                        op: *op,
                        now_ms: START_MS,
                        result: Ok(None),
                    }),
                    Effect::CasOwner { op, .. } => Some(Event::OwnerCasCompleted {
                        op: *op,
                        result: Ok(CasOutcome::Applied),
                    }),
                    Effect::Restore { op, .. } => Some(Event::RestoreCompleted {
                        op: *op,
                        result: Ok(RestoreOutcome {
                            restored: false,
                            alarm: None,
                        }),
                    }),
                    Effect::StartRuntime { op, .. } => Some(Event::RuntimeStarted {
                        op: *op,
                        isolate: None,
                        generation: 0,
                        result: Ok(()),
                    }),
                    Effect::Publish { op, .. } => Some(Event::Published {
                        op: *op,
                        result: Ok(()),
                    }),
                    _ => None,
                };
                if let Effect::ScheduleTimer { timer, at_mono_ms } = &effect {
                    self.timers.push((timer.clone(), *at_mono_ms));
                }
                match reply {
                    Some(reply) => next.extend(on_event(&mut self.state, reply)),
                    None => out.push(effect),
                }
            }
            pending = next;
        }
        out
    }

    fn fire(&mut self, timer: Timer, at_ms: u64) -> Vec<Effect> {
        self.feed(Event::TimerFired {
            timer,
            now_ms: at_ms,
            now_mono_ms: at_ms,
        })
    }

    /// When the core asked for `op`'s newest deadline to fire.
    fn deadline(&self, op: OpId) -> u64 {
        self.timers
            .iter()
            .rev()
            .find(|(timer, _)| *timer == Timer::OperationDeadline { op })
            .map(|(_, at)| *at)
            .expect("a deadline was scheduled")
    }

    /// The newest alarm timer the core has asked for.
    fn alarm_timer(&self) -> (Timer, u64) {
        self.timers
            .iter()
            .rev()
            .find(|(timer, _)| matches!(timer, Timer::CellAlarm { .. }))
            .cloned()
            .expect("an alarm timer was scheduled")
    }

    fn finish(&mut self, op: OpId, epoch: Epoch, at_ms: u64, next: Option<i64>) -> Vec<Effect> {
        self.feed(Event::AlarmFinished {
            op,
            cell: CELL.into(),
            epoch,
            now_ms: at_ms,
            now_mono_ms: at_ms,
            result: Ok((wake::AlarmSnapshot::without_wake(next), true, None)),
        })
    }
}

fn fires(effects: &[Effect]) -> Vec<(OpId, Epoch)> {
    effects
        .iter()
        .filter_map(|effect| match effect {
            Effect::FireAlarm { op, epoch, .. } => Some((*op, *epoch)),
            _ => None,
        })
        .collect()
}

/// Fire the alarm timer the core most recently asked for, if it is due.
fn fire_due_alarm(sim: &mut Sim, at_ms: u64) -> Vec<Effect> {
    let (timer, due) = sim.alarm_timer();
    assert!(due <= at_ms, "alarm timer is not yet due: {due} > {at_ms}");
    sim.fire(timer, at_ms)
}

#[test]
fn a_running_alarm_outlives_the_operation_deadline() {
    let mut sim = Sim::resident_with_alarm();

    let first = fire_due_alarm(&mut sim, FIRE_MS);
    let [(op, _)] = fires(&first)[..] else {
        panic!("expected exactly one firing, got {first:?}");
    };
    assert_eq!(sim.deadline(op), FIRE_MS + ALARM_DEADLINE_MS);

    // Its abort, when it comes, is a lost-report backstop of one operation.
    let expiry = FIRE_MS + ALARM_DEADLINE_MS;
    sim.fire(Timer::OperationDeadline { op }, expiry);
    assert_eq!(sim.deadline(op), expiry + DEADLINE_MS);
}

#[test]
fn expired_alarm_is_not_started_again_until_it_finishes() {
    let mut sim = Sim::resident_with_alarm();

    let first = fire_due_alarm(&mut sim, FIRE_MS);
    let [(op, epoch)] = fires(&first)[..] else {
        panic!("expected exactly one firing, got {first:?}");
    };

    let expiry = FIRE_MS + ALARM_DEADLINE_MS;
    let expired = sim.fire(Timer::OperationDeadline { op }, expiry);
    assert!(fires(&expired).is_empty(), "expiry restarted: {expired:?}");
    assert!(
        expired.contains(&Effect::AbortAlarm {
            op,
            cell: CELL.into()
        }),
        "expiry did not abort the handler: {expired:?}"
    );

    // The re-arm delay that used to start the second run.
    let (timer, _) = sim.alarm_timer();
    let retried = sim.fire(timer, expiry + 600);
    assert!(
        fires(&retried).is_empty(),
        "second run started while the first is still executing: {retried:?}"
    );

    // The aborted handler reports its retry record; only now may it re-arm.
    let retry_at = (expiry + 2_000) as i64;
    sim.finish(op, epoch, expiry + 1_000, Some(retry_at));
    let (timer, due) = sim.alarm_timer();
    assert_eq!(due, expiry + 2_000);
    let again = sim.fire(timer, expiry + 2_000);
    let [(second, _)] = fires(&again)[..] else {
        panic!("expected a new firing after the finish, got {again:?}");
    };
    assert_ne!(second, op);
}

#[test]
fn alarm_that_never_reports_back_re_arms_after_the_second_deadline() {
    let mut sim = Sim::resident_with_alarm();

    let first = fire_due_alarm(&mut sim, FIRE_MS);
    let [(op, _)] = fires(&first)[..] else {
        panic!("expected exactly one firing, got {first:?}");
    };

    let first_expiry = FIRE_MS + ALARM_DEADLINE_MS;
    sim.fire(Timer::OperationDeadline { op }, first_expiry);
    let (timer, _) = sim.alarm_timer();
    let early = sim.fire(timer, first_expiry + 600);
    assert!(fires(&early).is_empty(), "restarted early: {early:?}");

    // The abort was swallowed, so no finish ever arrives: the bound is one
    // more deadline, after which the alarm is retried as it always was.
    let second_expiry = first_expiry + DEADLINE_MS;
    let abandoned = sim.fire(Timer::OperationDeadline { op }, second_expiry);
    assert!(
        !abandoned
            .iter()
            .any(|effect| matches!(effect, Effect::AbortAlarm { .. })),
        "second expiry aborted again instead of giving up: {abandoned:?}"
    );
    let (timer, due) = sim.alarm_timer();
    assert_eq!(due, second_expiry + 500);
    let retried = sim.fire(timer, second_expiry + 500);
    let [(second, _)] = fires(&retried)[..] else {
        panic!("expected a retry after the second deadline, got {retried:?}");
    };
    assert_ne!(second, op);
}
