// Copyright 2026 Deno Land Inc. Apache-2.0 license.

//! What a node does when a cell's ownership record is absent.
//!
//! A missing record is almost always a new cell. It is also what a lost record
//! looks like, and claiming epoch 1 over acknowledged state would serve an
//! empty database beside it. The birth registry answers "has this cell ever
//! existed" from immutable bucket objects. This module holds the two pure
//! steps on the way: [`answer`] turns what one lookup read into an
//! [`Answer`], and [`decide`] turns the answer into the action the guard takes.
//! The owner of the I/O gathers the [`Facts`] and performs the action.
//!
//! Only `Exists` and `New` are conclusions. Every failure to read something
//! the answer depends on is [`Answer::Unknown`], with the first thing that
//! was in doubt, and it is never rounded to `New`.

use crate::registry::{merge, Record};

/// What one level of a partition said to a lookup.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Probe {
    /// The map says the level holds nothing.
    Empty,
    /// The bucket was read and passed its checks, and holds the key.
    Found(Record),
    /// The bucket was read and passed its checks, and does not hold the key.
    Absent,
    /// A 404, a failed check, a failed read, another format.
    Unreadable,
}

impl Probe {
    /// The level was read and is known not to hold the key.
    fn clean(self) -> bool {
        matches!(self, Self::Empty | Self::Absent)
    }
}

/// How the root of one lookup was obtained.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RootRead {
    /// Read after the `own.json` read returned absent.
    Fresh,
    /// Read earlier than that. Everything it names is still true, but it can
    /// miss a cell registered since.
    Stale,
    /// Missing, unreadable, of another format or failing its check. A missing
    /// root is never an empty registry.
    Unreadable,
}

/// What a lookup knows, as facts the reader gathered.
#[derive(Debug, Clone, Copy)]
pub struct Facts {
    pub root: RootRead,
    /// The gate is open: the root's seed belongs to its current gate number.
    pub gate_open: bool,
    /// Every delta the root names is loaded and readable.
    pub deltas_loaded: bool,
    /// The map the root names is loaded and readable.
    pub map_loaded: bool,
    /// The root names more live deltas than the hard cap.
    pub over_cap: bool,
    /// The lookup passed its deadline.
    pub late: bool,
    /// The record of the key in the root's deltas, if one holds it.
    pub in_deltas: Option<Record>,
    pub l1: Probe,
    pub l2: Probe,
}

/// The first thing that kept a lookup from a conclusion.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Doubt {
    /// The root could not be read.
    RootUnreadable,
    /// The root names more live deltas than the hard cap, so it is unreadable.
    OverCap,
    /// The root was read before the record read returned absent.
    RootNotFresh,
    /// A node that does not follow the writer rule may have written objects
    /// that the registry does not cover.
    GateClosed,
    /// A delta the root names is missing or unreadable.
    DeltaUnloaded,
    /// The map the root names is missing or unreadable.
    MapUnloaded,
    /// The lookup passed its deadline.
    Late,
    /// A run's bucket at this level (1 or 2) is missing, damaged, of another
    /// run or of another index.
    LevelUnreadable(u8),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Answer {
    /// The key is in a delta or run that the root names.
    Exists(Record),
    /// Every part of the registry was read and none holds the key.
    New,
    Unknown(Doubt),
}

/// What the registry says about a key.
///
/// The root is judged first. A root that cannot be read, or that names more
/// deltas than the cap, makes the answer `Unknown` whatever a cached hint of
/// the map read, so a hint can never turn a failed root read into a
/// conclusion.
///
/// A key found in a part the root names is `Exists` whatever else failed or
/// ran late: registered keys are never removed, so a hit is true. `New` needs
/// every part read and clean, from a root read after the record read returned
/// absent, with the gate open and inside the deadline. Anything else is
/// `Unknown`.
///
/// `New` rests on the writer rule: a node writes a cell's first per-cell
/// object, or credits a pack row for it, only after it read a root that covers
/// the cell, and every node that does not follow that rule closes the gate
/// first. So a cell with stored state outside the node log is always in a part
/// the root names, and a registration still waiting to be folded belongs to a
/// cell that has no such state yet.
pub fn answer(facts: &Facts) -> Answer {
    if facts.root == RootRead::Unreadable {
        return Answer::Unknown(Doubt::RootUnreadable);
    }
    if facts.over_cap {
        return Answer::Unknown(Doubt::OverCap);
    }
    let mut found = facts.in_deltas;
    for probe in [facts.l1, facts.l2] {
        if let Probe::Found(record) = probe {
            found = Some(found.map_or(record, |kept| merge(kept, record)));
        }
    }
    if let Some(record) = found {
        return Answer::Exists(record);
    }
    if facts.root != RootRead::Fresh {
        return Answer::Unknown(Doubt::RootNotFresh);
    }
    if !facts.gate_open {
        return Answer::Unknown(Doubt::GateClosed);
    }
    if !facts.deltas_loaded {
        return Answer::Unknown(Doubt::DeltaUnloaded);
    }
    if !facts.map_loaded {
        return Answer::Unknown(Doubt::MapUnloaded);
    }
    if facts.late {
        return Answer::Unknown(Doubt::Late);
    }
    for (level, probe) in [(1, facts.l1), (2, facts.l2)] {
        if !probe.clean() {
            return Answer::Unknown(Doubt::LevelUnreadable(level));
        }
    }
    Answer::New
}

/// Whether shared eviction packs exist, which decides what an `Unknown`
/// answer means. The caller supplies it: an unreadable root cannot say.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Packs {
    /// No cell's state lives in a shared pack. The listing of a cell's own
    /// objects still sees every cell that has state outside the node log.
    Absent,
    /// A cell can be packed, and the listing cannot see it.
    Present,
}

/// How long a claim retries an `Unknown` answer before it refuses, once packs
/// exist.
pub const UNKNOWN_RETRY_MS: u64 = 2_000;

/// What the guard does with a cell that has no ownership record.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Decision {
    /// The record is present: claim as today. The registry is not asked.
    Claim,
    /// The record is absent and the registry was not asked yet: ask it.
    Ask,
    /// The registry has seen the cell, so a lost record is suspected: read the
    /// record again and pass the result to [`after_reread`].
    Reread,
    /// The cell is new: claim epoch 1. The create-only write of the record
    /// decides, and the loser reads again.
    ClaimNew,
    /// The registry cannot tell and no pack hides a cell from the listing: the
    /// listing decides, as before the registry.
    List,
    /// The registry cannot tell and the listing cannot see packed cells: ask
    /// again.
    Retry,
    /// Refuse the claim. With `hold`, hold the cell closed for the recheck
    /// interval and log that the record was lost; without it, let the request
    /// retry at once.
    Refuse { hold: bool },
}

/// The guard table. `answer` is `None` while the registry has not been asked,
/// and `waited_ms` is how long an unknown answer has been retried.
///
/// | Record | Registry | Packs absent | Packs present |
/// | --- | --- | --- | --- |
/// | present | not asked | claim | claim |
/// | absent | `Exists` | re-read, then refuse and hold | same |
/// | absent | `New` | claim epoch 1 | same |
/// | absent | `Unknown` | the listing decides | retry for 2 s, then refuse |
pub fn decide(
    record_present: bool,
    answer: Option<Answer>,
    packs: Packs,
    waited_ms: u64,
) -> Decision {
    if record_present {
        return Decision::Claim;
    }
    match (answer, packs) {
        (None, _) => Decision::Ask,
        (Some(Answer::Exists(_)), _) => Decision::Reread,
        (Some(Answer::New), _) => Decision::ClaimNew,
        (Some(Answer::Unknown(_)), Packs::Absent) => Decision::List,
        (Some(Answer::Unknown(_)), Packs::Present) if waited_ms < UNKNOWN_RETRY_MS => {
            Decision::Retry
        }
        (Some(Answer::Unknown(_)), Packs::Present) => Decision::Refuse { hold: false },
    }
}

/// The step after a [`Decision::Reread`]: the record appeared, so claim as
/// today; it is still absent beside a registry entry, so refuse and hold.
pub fn after_reread(record_present: bool) -> Decision {
    if record_present {
        Decision::Claim
    } else {
        Decision::Refuse { hold: true }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn record(epoch: u32) -> Record {
        Record::birth([4; 16], epoch)
    }

    fn base() -> Record {
        Record::base([4; 16], [1; 12], 1, 3)
    }

    fn clean() -> Facts {
        Facts {
            root: RootRead::Fresh,
            gate_open: true,
            deltas_loaded: true,
            map_loaded: true,
            over_cap: false,
            late: false,
            in_deltas: None,
            l1: Probe::Empty,
            l2: Probe::Absent,
        }
    }

    #[test]
    fn new_needs_every_part_read_and_clean() {
        assert_eq!(answer(&clean()), Answer::New);
        for (l1, l2) in [
            (Probe::Empty, Probe::Empty),
            (Probe::Absent, Probe::Absent),
            (Probe::Absent, Probe::Empty),
        ] {
            assert_eq!(answer(&Facts { l1, l2, ..clean() }), Answer::New);
        }
    }

    #[test]
    fn every_source_of_doubt_is_unknown_and_named() {
        let cases = [
            (
                Facts {
                    root: RootRead::Unreadable,
                    ..clean()
                },
                Doubt::RootUnreadable,
            ),
            (
                Facts {
                    root: RootRead::Stale,
                    ..clean()
                },
                Doubt::RootNotFresh,
            ),
            (
                Facts {
                    gate_open: false,
                    ..clean()
                },
                Doubt::GateClosed,
            ),
            (
                Facts {
                    deltas_loaded: false,
                    ..clean()
                },
                Doubt::DeltaUnloaded,
            ),
            (
                Facts {
                    map_loaded: false,
                    ..clean()
                },
                Doubt::MapUnloaded,
            ),
            (
                Facts {
                    over_cap: true,
                    ..clean()
                },
                Doubt::OverCap,
            ),
            (
                Facts {
                    late: true,
                    ..clean()
                },
                Doubt::Late,
            ),
            (
                Facts {
                    l1: Probe::Unreadable,
                    ..clean()
                },
                Doubt::LevelUnreadable(1),
            ),
            (
                Facts {
                    l2: Probe::Unreadable,
                    ..clean()
                },
                Doubt::LevelUnreadable(2),
            ),
        ];
        for (facts, doubt) in cases {
            assert_eq!(answer(&facts), Answer::Unknown(doubt));
        }
    }

    #[test]
    fn a_key_found_in_a_part_the_root_names_is_exists_despite_other_failures() {
        let found = record(2);
        let spoiled = [
            Facts {
                root: RootRead::Stale,
                ..clean()
            },
            Facts {
                gate_open: false,
                ..clean()
            },
            Facts {
                deltas_loaded: false,
                ..clean()
            },
            Facts {
                map_loaded: false,
                ..clean()
            },
            Facts {
                late: true,
                ..clean()
            },
        ];
        for spoil in spoiled {
            let in_l2 = Facts {
                l1: Probe::Unreadable,
                l2: Probe::Found(found),
                ..spoil
            };
            assert_eq!(answer(&in_l2), Answer::Exists(found));
            let in_delta = Facts {
                in_deltas: Some(found),
                l1: Probe::Unreadable,
                l2: Probe::Unreadable,
                ..spoil
            };
            assert_eq!(answer(&in_delta), Answer::Exists(found));
        }
        // Two parts that hold the key give the record the merge rule keeps.
        let both = Facts {
            l1: Probe::Found(record(9)),
            l2: Probe::Found(base()),
            in_deltas: Some(record(5)),
            ..clean()
        };
        assert_eq!(answer(&both), Answer::Exists(base()));
    }

    #[test]
    fn a_root_that_cannot_be_trusted_beats_anything_a_hint_read() {
        for facts in [
            Facts {
                root: RootRead::Unreadable,
                l1: Probe::Found(record(1)),
                ..clean()
            },
            Facts {
                root: RootRead::Unreadable,
                in_deltas: Some(record(1)),
                ..clean()
            },
            Facts {
                over_cap: true,
                l2: Probe::Found(record(1)),
                ..clean()
            },
        ] {
            assert!(matches!(answer(&facts), Answer::Unknown(_)), "{facts:?}");
        }
    }

    /// Every combination of the facts: `New` exactly when everything is read
    /// and clean, `Exists` exactly when a part holds the key under a usable
    /// root, and `Unknown` otherwise. A hit is never lost and `New` is never
    /// reached on a doubt.
    #[test]
    fn the_answer_is_new_exactly_when_nothing_is_in_doubt() {
        let probes = [
            Probe::Empty,
            Probe::Found(record(1)),
            Probe::Absent,
            Probe::Unreadable,
        ];
        let mut combinations = 0;
        for root in [RootRead::Fresh, RootRead::Stale, RootRead::Unreadable] {
            for bits in 0..32_u32 {
                for in_deltas in [None, Some(record(3))] {
                    for l1 in probes {
                        for l2 in probes {
                            let facts = Facts {
                                root,
                                gate_open: bits & 1 != 0,
                                deltas_loaded: bits & 2 != 0,
                                map_loaded: bits & 4 != 0,
                                over_cap: bits & 8 != 0,
                                late: bits & 16 != 0,
                                in_deltas,
                                l1,
                                l2,
                            };
                            combinations += 1;
                            let hit = in_deltas.is_some()
                                || matches!(l1, Probe::Found(_))
                                || matches!(l2, Probe::Found(_));
                            let usable = root != RootRead::Unreadable && !facts.over_cap;
                            let everything_clean = root == RootRead::Fresh
                                && facts.gate_open
                                && facts.deltas_loaded
                                && facts.map_loaded
                                && !facts.late
                                && l1.clean()
                                && l2.clean();
                            match answer(&facts) {
                                Answer::Exists(_) => assert!(hit && usable, "{facts:?}"),
                                Answer::New => {
                                    assert!(!hit && usable && everything_clean, "{facts:?}");
                                }
                                Answer::Unknown(_) => {
                                    assert!(!usable || (!hit && !everything_clean), "{facts:?}");
                                }
                            }
                        }
                    }
                }
            }
        }
        assert_eq!(combinations, 3 * 32 * 2 * 16);
    }

    #[test]
    fn the_guard_table_in_both_columns() {
        let unknown = Answer::Unknown(Doubt::Late);
        for packs in [Packs::Absent, Packs::Present] {
            // A present record claims as today and never asks the registry.
            for answer in [
                None,
                Some(Answer::New),
                Some(Answer::Exists(record(1))),
                Some(unknown),
            ] {
                assert_eq!(decide(true, answer, packs, 0), Decision::Claim);
            }
            // An absent record asks first.
            assert_eq!(decide(false, None, packs, 0), Decision::Ask);
            // `Exists` re-reads, `New` claims epoch 1, in both columns.
            assert_eq!(
                decide(false, Some(Answer::Exists(record(1))), packs, 0),
                Decision::Reread
            );
            assert_eq!(
                decide(false, Some(Answer::New), packs, 0),
                Decision::ClaimNew
            );
        }
        // `Unknown` before packs: the listing decides, however long it took.
        for waited in [0, 1_999, 2_000, u64::MAX] {
            assert_eq!(
                decide(false, Some(unknown), Packs::Absent, waited),
                Decision::List
            );
        }
        // `Unknown` after packs: retry for 2 s, then refuse without a hold.
        for (waited, expected) in [
            (0, Decision::Retry),
            (1_999, Decision::Retry),
            (UNKNOWN_RETRY_MS, Decision::Refuse { hold: false }),
            (u64::MAX, Decision::Refuse { hold: false }),
        ] {
            assert_eq!(
                decide(false, Some(unknown), Packs::Present, waited),
                expected,
                "waited {waited} ms"
            );
        }
    }

    #[test]
    fn every_unknown_source_takes_the_unknown_row_of_the_table() {
        for doubt in [
            Doubt::RootUnreadable,
            Doubt::OverCap,
            Doubt::RootNotFresh,
            Doubt::GateClosed,
            Doubt::DeltaUnloaded,
            Doubt::MapUnloaded,
            Doubt::Late,
            Doubt::LevelUnreadable(1),
            Doubt::LevelUnreadable(2),
        ] {
            let unknown = Some(Answer::Unknown(doubt));
            assert_eq!(decide(false, unknown, Packs::Absent, 0), Decision::List);
            assert_eq!(decide(false, unknown, Packs::Present, 0), Decision::Retry);
            assert_eq!(
                decide(false, unknown, Packs::Present, UNKNOWN_RETRY_MS),
                Decision::Refuse { hold: false }
            );
        }
    }

    #[test]
    fn a_reread_that_still_finds_no_record_refuses_and_holds() {
        assert_eq!(after_reread(true), Decision::Claim);
        assert_eq!(after_reread(false), Decision::Refuse { hold: true });
    }
}
