//! A breadth-first explorer of every interleaving of a small model.
//!
//! A [`Model`] owns a world, the actions enabled in a world and the invariants
//! that must hold in it. [`explore`] visits each distinct world once, in order
//! of its distance from the start, so the first world that breaks an invariant
//! is reached by a shortest trace, and that trace is rebuilt from the parent
//! links the search kept.
//!
//! Worlds are identified by a fingerprint of their `Debug` text, which lets a
//! model embed the real decision core, whose state is not `Hash`. The two
//! halves of the fingerprint come from separately seeded hashers, so a
//! collision would need both to collide at once. The search is exhaustive up
//! to the bounds in [`Limits`]; a report says which bound it hit, and a run
//! that hit one proves nothing about the worlds beyond it.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::fmt::Debug;
use std::hash::{Hash, Hasher};
use std::time::{Duration, Instant};

/// An invariant that failed in a world.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Violation {
    /// Stable name of the invariant, used to say which one a mutant breaks.
    pub invariant: &'static str,
    pub detail: String,
}

impl Violation {
    pub(crate) fn new(invariant: &'static str, detail: impl Into<String>) -> Self {
        Self {
            invariant,
            detail: detail.into(),
        }
    }
}

/// What one action did: the world after it, and the labels of the rare events
/// it passed through. Labels are how a run proves that the adversary was not
/// vacuous: a report lists each label with the number of times it was taken.
pub(crate) struct Step<W> {
    pub world: W,
    pub labels: Vec<&'static str>,
}

pub(crate) trait Model {
    type World: Clone + Debug;
    type Action: Clone + Debug;

    fn init(&self) -> Self::World;
    fn actions(&self, world: &Self::World) -> Vec<Self::Action>;
    fn apply(&self, world: &Self::World, action: &Self::Action) -> Step<Self::World>;
    /// Invariants over a world, with the action that led to it.
    fn check(&self, world: &Self::World) -> Vec<Violation>;
}

/// Bounds of a run. A run that reaches one stops and says so.
#[derive(Clone, Copy, Debug)]
pub(crate) struct Limits {
    pub max_states: usize,
    pub max_depth: usize,
    /// Stop at the first violation (a mutant run) or keep going and collect
    /// the shortest trace of each invariant (a pass run).
    pub stop_at_first: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Bound {
    States,
    Depth,
}

#[derive(Clone, Debug)]
pub(crate) struct Counterexample {
    pub violation: Violation,
    pub trace: Vec<String>,
}

#[derive(Debug)]
pub(crate) struct Report {
    pub states: usize,
    pub transitions: usize,
    /// The deepest level that was expanded.
    pub depth: usize,
    /// The bound that stopped the run, if any. `None` means every reachable
    /// world within the model's own bounds was visited.
    pub bound_hit: Option<Bound>,
    /// The shortest trace of each invariant that failed.
    pub violations: BTreeMap<&'static str, Counterexample>,
    pub labels: BTreeMap<&'static str, usize>,
    /// The shortest trace that first reached each label.
    pub label_traces: BTreeMap<&'static str, Vec<String>>,
    pub elapsed: Duration,
}

impl Report {
    pub(crate) fn passed(&self) -> bool {
        self.violations.is_empty()
    }

    /// No invariant failed except the named, known ones.
    pub(crate) fn passed_except(&self, known: &[&str]) -> bool {
        self.violations.keys().all(|name| known.contains(name))
    }

    pub(crate) fn complete(&self) -> bool {
        self.bound_hit.is_none()
    }

    /// One line a log can carry for a run.
    pub(crate) fn summary(&self, name: &str) -> String {
        format!(
            "{name}: {} states, {} transitions, depth {}, bound hit: {}, {} violation(s), {:.2?}",
            self.states,
            self.transitions,
            self.depth,
            match self.bound_hit {
                None => "none (exhaustive)".to_string(),
                Some(bound) => format!("{bound:?}"),
            },
            self.violations.len(),
            self.elapsed,
        )
    }

    pub(crate) fn print_violations(&self) {
        for (name, found) in &self.violations {
            eprintln!(
                "  violation {name}: {} (trace of {} steps)",
                found.violation.detail,
                found.trace.len()
            );
            for (index, step) in found.trace.iter().enumerate() {
                eprintln!("    {:>2}. {step}", index + 1);
            }
        }
    }

    pub(crate) fn print_labels(&self) {
        for (label, count) in &self.labels {
            eprintln!("  reached {label}: {count}");
        }
    }

    pub(crate) fn print_label_trace(&self, label: &str) {
        if let Some(trace) = self.label_traces.get(label) {
            eprintln!("  first trace to {label}:");
            for (index, step) in trace.iter().enumerate() {
                eprintln!("    {:>2}. {step}", index + 1);
            }
        }
    }
}

thread_local! {
    static TEXT: std::cell::RefCell<String> = const { std::cell::RefCell::new(String::new()) };
}

/// Two independently seeded hashes of the text a value prints. The text goes
/// through one buffer that every call reuses.
pub(crate) fn fingerprint(value: &impl Debug) -> u128 {
    use std::fmt::Write;
    TEXT.with(|text| {
        let mut text = text.borrow_mut();
        text.clear();
        write!(text, "{value:?}").expect("formatting does not fail");
        let mut low = std::collections::hash_map::DefaultHasher::new();
        0xA5u8.hash(&mut low);
        text.hash(&mut low);
        let mut high = std::collections::hash_map::DefaultHasher::new();
        0x5Au8.hash(&mut high);
        text.len().hash(&mut high);
        text.hash(&mut high);
        (u128::from(high.finish()) << 64) | u128::from(low.finish())
    })
}

pub(crate) fn explore<M: Model>(model: &M, limits: Limits) -> Report {
    let started = Instant::now();
    let init = model.init();
    let init_fp = fingerprint(&init);
    // Each visited world keeps only its parent and the action that led to it.
    let mut parents: HashMap<u128, (u128, String)> = HashMap::new();
    let mut seen: BTreeSet<u128> = BTreeSet::new();
    seen.insert(init_fp);
    let mut report = Report {
        states: 1,
        transitions: 0,
        depth: 0,
        bound_hit: None,
        violations: BTreeMap::new(),
        labels: BTreeMap::new(),
        label_traces: BTreeMap::new(),
        elapsed: Duration::ZERO,
    };
    let record = |report: &mut Report,
                  parents: &HashMap<u128, (u128, String)>,
                  fp: u128,
                  violations: Vec<Violation>| {
        for violation in violations {
            report
                .violations
                .entry(violation.invariant)
                .or_insert_with(|| {
                    let mut trace = Vec::new();
                    let mut at = fp;
                    while let Some((parent, action)) = parents.get(&at) {
                        trace.push(action.clone());
                        at = *parent;
                    }
                    trace.reverse();
                    Counterexample { violation, trace }
                });
        }
    };
    let found = model.check(&init);
    record(&mut report, &parents, init_fp, found);
    let mut frontier = vec![(init, init_fp)];
    let mut depth = 0;
    'levels: while !frontier.is_empty() {
        if report.violations.len() > 0 && limits.stop_at_first {
            break;
        }
        if depth >= limits.max_depth {
            report.bound_hit = Some(Bound::Depth);
            break;
        }
        let mut next = Vec::new();
        for (world, fp) in &frontier {
            for action in model.actions(world) {
                report.transitions += 1;
                let step = model.apply(world, &action);
                for label in step.labels {
                    *report.labels.entry(label).or_default() += 1;
                    if !report.label_traces.contains_key(label) {
                        let mut trace = vec![format!("{action:?}")];
                        let mut at = *fp;
                        while let Some((parent, earlier)) = parents.get(&at) {
                            trace.push(earlier.clone());
                            at = *parent;
                        }
                        trace.reverse();
                        report.label_traces.insert(label, trace);
                    }
                }
                let next_fp = fingerprint(&step.world);
                if !seen.insert(next_fp) {
                    continue;
                }
                parents.insert(next_fp, (*fp, format!("{action:?}")));
                report.states += 1;
                let violations = model.check(&step.world);
                let violating = !violations.is_empty();
                record(&mut report, &parents, next_fp, violations);
                if violating && limits.stop_at_first {
                    break 'levels;
                }
                // A world that broke an invariant is not expanded: what
                // follows it is noise about the same failure.
                if !violating {
                    next.push((step.world, next_fp));
                }
                if report.states >= limits.max_states {
                    report.bound_hit = Some(Bound::States);
                    break 'levels;
                }
            }
        }
        depth += 1;
        report.depth = depth;
        frontier = next;
    }
    report.elapsed = started.elapsed();
    report
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Two counters that each count to three: 16 worlds, and the explorer
    /// finds the world where they sum to six by the shortest path.
    struct Counters;

    impl Model for Counters {
        type World = (u8, u8);
        type Action = u8;

        fn init(&self) -> (u8, u8) {
            (0, 0)
        }

        fn actions(&self, world: &(u8, u8)) -> Vec<u8> {
            let mut actions = Vec::new();
            if world.0 < 3 {
                actions.push(0);
            }
            if world.1 < 3 {
                actions.push(1);
            }
            actions
        }

        fn apply(&self, world: &(u8, u8), action: &u8) -> Step<(u8, u8)> {
            let mut world = *world;
            if *action == 0 {
                world.0 += 1;
            } else {
                world.1 += 1;
            }
            Step {
                world,
                labels: vec!["step"],
            }
        }

        fn check(&self, world: &(u8, u8)) -> Vec<Violation> {
            if world.0 + world.1 == 6 {
                vec![Violation::new("sum", "both counters are full")]
            } else {
                Vec::new()
            }
        }
    }

    #[test]
    fn visits_each_world_once_and_finds_the_shortest_trace() {
        let report = explore(
            &Counters,
            Limits {
                max_states: 1_000,
                max_depth: 100,
                stop_at_first: false,
            },
        );
        assert_eq!(report.states, 16);
        assert!(report.complete());
        let found = &report.violations["sum"];
        assert_eq!(found.trace.len(), 6);
    }

    #[test]
    fn reports_the_bound_it_hit() {
        let report = explore(
            &Counters,
            Limits {
                max_states: 1_000,
                max_depth: 2,
                stop_at_first: false,
            },
        );
        assert_eq!(report.bound_hit, Some(Bound::Depth));
        assert!(report.passed());
    }
}
