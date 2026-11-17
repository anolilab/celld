//! The summary of a sample log: how many agents a fleet starts, how many of
//! them an id minted by a live process, how many of those come back after the
//! process that bore them is gone, and what that is worth under the variants
//! of the ownership cost model.
//!
//! Every figure comes from the log alone. Counters are summed per process, so
//! a restart, which resets them, loses nothing but what happened after the
//! process's last poll.

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};

use serde_json::{json, Value};

use super::record::{identity, RING_NAMES};

const DAY_MS: f64 = 86_400_000.0;
/// A month, for the figures that are a month's.
const MONTH_DAYS: f64 = 30.0;
/// Returns of the agents born in this many days are reported apart: an agent
/// born late in the window has had little time to return.
const EARLY_DAYS: f64 = 2.0;
/// A window shorter than this cannot pass the gate.
const MIN_WINDOW_DAYS: f64 = 7.0;

/// The cost model. **Modelled, not measured**: these figures price a
/// request (a class A request $0.005 and a class B request $0.0004
/// per 1,000, a regional bucket), multiplied out for 438M agents a month on
/// 100 nodes. Change them here and nowhere else.
pub struct Model {
    /// The load the dollar figures below were worked out for.
    pub agents_a_month: f64,
    pub nodes: f64,
    /// $ a month saved on every minted agent when it never returns, by variant:
    /// one create-only write and four reads that the grant skips, and the
    /// audit listing (A-set and A-reg); the four reads alone (reads only).
    pub a_set_base: f64,
    pub a_reg_base: f64,
    pub reads_only_base: f64,
    /// $ a month lost per unit of the return share without a drain (r_n) and
    /// with one (r_d), by variant.
    pub a_set_return: f64,
    pub a_reg_return_n: f64,
    pub a_reg_return_d: f64,
    pub reads_only_return: f64,
    /// $ a month for the checkpoints of the session marks, one changed chunk
    /// per node per 30 seconds, at `nodes` nodes (A-set and reads only).
    pub checkpoints: f64,
    /// $ a month lost on every minted agent that a preserved restart carries
    /// over in its first epoch (A-set and A-reg): one create-only write.
    pub preserved_resume: f64,
    /// The net saving a month at which a variant is worth building.
    pub go_threshold: f64,
}

pub const MODEL: Model = Model {
    agents_a_month: 438_000_000.0,
    nodes: 100.0,
    a_set_base: 2_900.0,
    a_reg_base: 2_900.0,
    reads_only_base: 701.0,
    a_set_return: 2_365.0,
    a_reg_return_n: 5_600.0,
    a_reg_return_d: 3_240.0,
    reads_only_return: 175.0,
    checkpoints: 44.0,
    preserved_resume: 2_190.0,
    go_threshold: 1_000.0,
};

/// The measured shares and volume that the model is applied to.
#[derive(Clone, Copy)]
pub struct Shares {
    pub m: f64,
    pub r_n: f64,
    pub r_d: f64,
    pub r_p: f64,
    /// First claims a month.
    pub volume: f64,
    pub nodes: f64,
}

/// The net saving a month of each variant against the design without grants.
/// Positive is a saving.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Savings {
    pub a_set: f64,
    pub a_reg: f64,
    pub reads_only: f64,
    /// What A-set and A-reg lose to preserved restarts. Not in the figures
    /// above, which assume none.
    pub preserved_resume_loss: f64,
}

impl Model {
    /// The terms with `m` scale with the volume against the modelled load, and
    /// the checkpoints with the number of nodes.
    pub fn savings(&self, shares: Shares) -> Savings {
        let scale = shares.m * shares.volume / self.agents_a_month;
        let checkpoints = self.checkpoints * shares.nodes / self.nodes;
        Savings {
            a_set: scale * (self.a_set_base - self.a_set_return * shares.r_n) - checkpoints,
            a_reg: scale
                * (self.a_reg_base
                    - self.a_reg_return_n * shares.r_n
                    - self.a_reg_return_d * shares.r_d),
            reads_only: scale * (self.reads_only_base - self.reads_only_return * shares.r_n)
                - checkpoints,
            preserved_resume_loss: scale * shares.r_p * self.preserved_resume,
        }
    }
}

/// The counters that are summed over the processes, with the path of each in
/// `storage_counters`.
const COUNTERS: [&str; 17] = [
    "ids_minted",
    "claims",
    "first_claims",
    "minted_untracked",
    "first_claims_minted_local",
    "first_claims_minted_local_no_ensemble",
    "second_claims_prior_released",
    "second_claims_prior_self",
    "second_claims_prior_other",
    "releases_drain",
    "releases_handoff",
    "preserved_resumes",
    "evictions.snapshot",
    "evictions.paged",
    "evictions.over_budget",
    "evictions.publish_failed",
    "evictions.empty",
];

fn counter(counters: &Value, path: &str) -> u64 {
    path.split('.')
        .fold(counters, |value, key| &value[key])
        .as_u64()
        .unwrap_or(0)
}

/// One process of one node, as the polls saw it.
struct Process {
    node: String,
    start_ms: u64,
    first_poll_ms: u64,
    last_poll_ms: u64,
    first: Value,
    last: Value,
    /// The last poll showed the process draining.
    drained: bool,
    /// It started after the first poll of the log, so its counters began
    /// inside the window.
    in_window: bool,
    ended: bool,
}

impl Process {
    /// Whether the process was running at `t_ms`. One that has not ended is
    /// taken to run on.
    fn alive_at(&self, t_ms: u64) -> bool {
        t_ms >= self.start_ms && (!self.ended || t_ms <= self.last_poll_ms)
    }

    /// What the process counted in the window. A process that began inside it
    /// counted everything it reports. One that was already running counted
    /// what it did not report at the first poll too, which is not the window's.
    fn counted(&self, path: &str) -> u64 {
        let last = counter(&self.last, path);
        if self.in_window {
            last
        } else {
            last.saturating_sub(counter(&self.first, path))
        }
    }
}

/// The shares of the agents whose id a live process minted.
#[derive(Default, Clone, Copy)]
struct Returns {
    /// The agents counted in m.
    agents: u64,
    /// Their first second claim came from another process, and the bearing one
    /// did not release the cell in a drain.
    without_drain: u64,
    /// The same, and it did.
    after_drain: u64,
    /// The bearing process let the cell go while it lived: a balancing move.
    left_while_lived: u64,
    /// The bearing process claimed the cell a second time.
    in_session_wakes: u64,
    /// A preserved restart carried the cell over in its first epoch.
    resumed: u64,
}

fn share(part: u64, whole: u64) -> Option<f64> {
    (whole > 0).then(|| part as f64 / whole as f64)
}

/// Nearest-rank percentile of an ascending slice.
fn percentile(sorted: &[u64], p: f64) -> Option<u64> {
    let rank = ((p / 100.0) * sorted.len() as f64).ceil() as usize;
    (!sorted.is_empty()).then(|| sorted[rank.clamp(1, sorted.len()) - 1])
}

pub struct Summary {
    pub json: Value,
    pub markdown: String,
}

/// Parse the lines of a sample log.
pub fn parse_log(text: &str) -> Result<Vec<Value>, String> {
    text.lines()
        .enumerate()
        .filter(|(_, line)| !line.trim().is_empty())
        .map(|(number, line)| {
            serde_json::from_str(line).map_err(|error| format!("line {}: {error}", number + 1))
        })
        .collect()
}

/// Everything the rings say about an agent, by its key.
#[derive(Default)]
struct Events {
    mints: HashMap<String, usize>,
    /// Key, bearing process, time, and whether the node had a fleet ensemble.
    first_claims: Vec<(String, usize, u64, bool)>,
    /// The earliest second claim of a key: time and process.
    second_claims: HashMap<String, (u64, usize)>,
    releases: HashMap<(usize, String, u64), String>,
    resumes: HashMap<String, Vec<(u64, u64)>>,
}

/// The nodes' polls and the processes they found.
struct Polled<'a> {
    /// Per node: polls made and polls answered.
    polls: BTreeMap<&'a str, (u64, u64)>,
    /// Nodes that answered without reporting a process identity.
    without_counters: BTreeSet<&'a str>,
    processes: Vec<Process>,
    index: HashMap<(String, u64), usize>,
}

fn poll_processes<'a>(records: &[&'a Value], window_start: u64) -> Polled<'a> {
    let mut polls: BTreeMap<&str, (u64, u64)> = BTreeMap::new();
    let mut without_counters: BTreeSet<&str> = BTreeSet::new();
    let mut processes: Vec<Process> = Vec::new();
    let mut index: HashMap<(String, u64), usize> = HashMap::new();
    for record in records {
        let node = record["node"].as_str().unwrap_or("?");
        let ok = record["ok"].as_bool() == Some(true);
        let entry = polls.entry(node).or_default();
        entry.0 += 1;
        if !ok {
            continue;
        }
        entry.1 += 1;
        let t_ms = record["t_ms"].as_u64().unwrap_or(0);
        let counters = &record["counters"];
        let Some(key) = identity(counters) else {
            without_counters.insert(node);
            continue;
        };
        let start_ms = key.1;
        let slot = *index.entry(key).or_insert_with(|| {
            processes.push(Process {
                node: node.to_owned(),
                start_ms,
                first_poll_ms: t_ms,
                last_poll_ms: t_ms,
                first: counters.clone(),
                last: counters.clone(),
                drained: false,
                in_window: start_ms >= window_start,
                ended: false,
            });
            processes.len() - 1
        });
        let process = &mut processes[slot];
        process.last_poll_ms = t_ms;
        process.last = counters.clone();
        process.drained = counters["drain_started_ms"].is_number();
    }
    // A process has ended when its node was later seen running another, or
    // when its last poll found it draining.
    let mut latest: HashMap<&str, usize> = HashMap::new();
    for (slot, process) in processes.iter().enumerate() {
        let newest = latest.entry(process.node.as_str()).or_insert(slot);
        if processes[*newest].first_poll_ms < process.first_poll_ms {
            *newest = slot;
        }
    }
    let newest: HashSet<usize> = latest.values().copied().collect();
    for (slot, process) in processes.iter_mut().enumerate() {
        process.ended = !newest.contains(&slot) || process.drained;
    }

    Polled {
        polls,
        without_counters,
        processes,
        index,
    }
}

/// The ring entries, once per process and sequence number, and how many
/// entries went missing between one poll and the next.
fn read_rings(
    records: &[&Value],
    processes: &[Process],
    index: &HashMap<(String, u64), usize>,
) -> (Events, u64) {
    let mut events = Events::default();
    let mut seen: HashSet<(usize, usize, u64)> = HashSet::new();
    let mut expected: HashMap<(usize, usize), u64> = HashMap::new();
    let mut gaps: u64 = 0;
    for record in records {
        let Some(&slot) = identity(&record["counters"]).and_then(|key| index.get(&key)) else {
            continue;
        };
        let rings = &record["rings"];
        if !rings.is_object() {
            continue;
        }
        for (ring_slot, name) in RING_NAMES.iter().enumerate() {
            let ring = &rings[name];
            let entries = ring["entries"].as_array().map_or(&[][..], Vec::as_slice);
            let first_seq = entries.first().and_then(|entry| entry["seq"].as_u64());
            // A process already running at the first poll has an unknown
            // history, so its first line sets the baseline; one that began
            // in the window must start from the first entry.
            let baseline = if processes[slot].in_window {
                0
            } else {
                first_seq.unwrap_or(ring["next_seq"].as_u64().unwrap_or(0))
            };
            let next = *expected.entry((slot, ring_slot)).or_insert(baseline);
            if let Some(first) = first_seq {
                gaps += first.saturating_sub(next);
            }
            expected.insert(
                (slot, ring_slot),
                next.max(ring["next_seq"].as_u64().unwrap_or(next)),
            );
            for entry in entries {
                let (Some(seq), Some(key)) = (entry["seq"].as_u64(), entry["key"].as_str()) else {
                    continue;
                };
                if !seen.insert((slot, ring_slot, seq)) {
                    continue;
                }
                let t_ms = entry["t_ms"].as_u64().unwrap_or(0);
                let key = key.to_owned();
                match *name {
                    "mints" => {
                        events.mints.entry(key).or_insert(slot);
                    }
                    "first_claims" => {
                        let ensemble = entry["ensemble"].as_bool().unwrap_or(false);
                        events.first_claims.push((key, slot, t_ms, ensemble));
                    }
                    "second_claims" => {
                        let earliest = events.second_claims.entry(key).or_insert((t_ms, slot));
                        if t_ms < earliest.0 {
                            *earliest = (t_ms, slot);
                        }
                    }
                    "releases" => {
                        let epoch = entry["epoch"].as_u64().unwrap_or(0);
                        let reason = entry["reason"].as_str().unwrap_or("").to_owned();
                        events.releases.entry((slot, key, epoch)).or_insert(reason);
                    }
                    "resumes" => {
                        let epoch = entry["epoch"].as_u64().unwrap_or(0);
                        events.resumes.entry(key).or_default().push((t_ms, epoch));
                    }
                    other => unreachable!("no ring {other}"),
                }
            }
        }
    }

    (events, gaps)
}

/// What became of each sampled agent whose id a live process minted: all of
/// them, those born in the first two days, and the delays of the returns.
fn follow_agents(
    events: &Events,
    processes: &[Process],
    window_start: u64,
) -> (Returns, Returns, Vec<u64>) {
    let early_end = window_start as f64 + EARLY_DAYS * DAY_MS;
    let (mut all, mut early) = (Returns::default(), Returns::default());
    let mut delays: Vec<u64> = Vec::new();
    for (key, bearer, claimed_ms, ensemble) in &events.first_claims {
        let minted_alive = events
            .mints
            .get(key)
            .is_some_and(|minter| processes[*minter].alive_at(*claimed_ms));
        // A node without a fleet ensemble cannot grant, so its claims are not m.
        if !minted_alive || !ensemble {
            continue;
        }
        let second = events.second_claims.get(key);
        let resumed = events.resumes.get(key).is_some_and(|resumes| {
            resumes
                .iter()
                .any(|(t, epoch)| *epoch == 1 && second.is_none_or(|(later, _)| t <= later))
        });
        let release = events.releases.get(&(*bearer, key.clone(), 1));
        let tally = |returns: &mut Returns| {
            returns.agents += 1;
            returns.resumed += u64::from(resumed);
            match second {
                None => {}
                Some((_, claimer)) if claimer == bearer => returns.in_session_wakes += 1,
                Some(_) => match release.map(String::as_str) {
                    Some("drain") => returns.after_drain += 1,
                    Some(_) => returns.left_while_lived += 1,
                    None => returns.without_drain += 1,
                },
            }
        };
        tally(&mut all);
        if (*claimed_ms as f64) < early_end {
            tally(&mut early);
        }
        // The delay of an agent that came back after its session ended.
        if let Some((second_ms, claimer)) = second {
            if claimer != bearer && release.is_none_or(|reason| reason == "drain") {
                delays.push(second_ms.saturating_sub(*claimed_ms));
            }
        }
    }
    delays.sort_unstable();
    (all, early, delays)
}

pub fn summarize(records: &[Value]) -> Summary {
    let mut records: Vec<&Value> = records.iter().collect();
    records.sort_by_key(|record| record["t_ms"].as_u64().unwrap_or(0));
    let window_start = records
        .first()
        .and_then(|r| r["t_ms"].as_u64())
        .unwrap_or(0);
    let window_end = records.last().and_then(|r| r["t_ms"].as_u64()).unwrap_or(0);
    let window_days = window_end.saturating_sub(window_start) as f64 / DAY_MS;

    let Polled {
        polls,
        without_counters,
        processes,
        index,
    } = poll_processes(&records, window_start);

    // The counters, summed per process.
    let totals: BTreeMap<&str, u64> = COUNTERS
        .iter()
        .map(|name| {
            (
                *name,
                processes.iter().map(|process| process.counted(name)).sum(),
            )
        })
        .collect();
    let total = |name: &str| totals[name];

    let (events, gaps) = read_rings(&records, &processes, &index);

    let (all, early, delays) = follow_agents(&events, &processes, window_start);
    let sampled_first_claims = events.first_claims.len() as u64;

    // The figures.
    let first_claims = total("first_claims");
    let volume = if window_days > 0.0 {
        first_claims as f64 * MONTH_DAYS / window_days
    } else {
        0.0
    };
    let per_month =
        |count: u64| (window_days > 0.0).then(|| count as f64 * MONTH_DAYS / window_days);
    let nodes_answered = polls.values().filter(|(_, ok)| *ok > 0).count();
    let m_upper = share(total("ids_minted"), first_claims);
    let m_local = share(total("first_claims_minted_local"), first_claims);
    let m = share(all.agents, sampled_first_claims);
    let r_n = share(all.without_drain, all.agents);
    let r_d = share(all.after_drain, all.agents);
    let r_p = share(all.resumed, all.agents);
    let untracked_share = share(total("minted_untracked"), total("ids_minted"));
    let evictions_total: u64 = COUNTERS
        .iter()
        .filter(|name| name.starts_with("evictions."))
        .map(|name| total(name))
        .sum();
    let samples: BTreeSet<u64> = processes
        .iter()
        .filter_map(|process| process.last["step0_sample"].as_u64())
        .collect();

    // Session lengths of the processes that ended in the window.
    let lengths: Vec<u64> = {
        let mut lengths: Vec<u64> = processes
            .iter()
            .filter(|process| process.ended)
            .map(|process| process.last_poll_ms.saturating_sub(process.start_ms))
            .collect();
        lengths.sort_unstable();
        lengths
    };
    let node_weeks = nodes_answered as f64 * window_days / 7.0;
    let started_in_window = processes.iter().filter(|process| process.in_window).count();
    let drained = processes.iter().filter(|process| process.drained).count();

    // The gate.
    let mut gate: Vec<(String, bool)> = vec![
        (
            format!("the window is at least {MIN_WINDOW_DAYS} days ({window_days:.2})"),
            window_days >= MIN_WINDOW_DAYS,
        ),
        (
            format!(
                "every node answered and reports the counters ({} never answered, {} without counters)",
                polls.len() - nodes_answered,
                without_counters.len()
            ),
            nodes_answered == polls.len() && without_counters.is_empty(),
        ),
        (
            format!("no ring shows a gap ({gaps} entries missed)"),
            gaps == 0,
        ),
        (
            format!("the rings are on (sample {samples:?})"),
            !samples.is_empty() && !samples.contains(&0),
        ),
    ];
    let untracked_high = untracked_share.is_some_and(|share| share > 0.01);
    gate.push((
        format!(
            "minted_untracked is at most 1% of ids_minted ({})",
            percent(untracked_share)
        ),
        !untracked_high,
    ));

    // The dollars: at the measured volume, at the modelled one, and with every
    // minted agent returning without a drain.
    let nodes = nodes_answered as f64;
    let measured = m.map(|m| Shares {
        m,
        r_n: r_n.unwrap_or(0.0),
        r_d: r_d.unwrap_or(0.0),
        r_p: r_p.unwrap_or(0.0),
        volume,
        nodes,
    });
    let at_measured = measured.map(|shares| MODEL.savings(shares));
    let at_modelled = measured.map(|shares| {
        MODEL.savings(Shares {
            volume: MODEL.agents_a_month,
            nodes: MODEL.nodes,
            ..shares
        })
    });
    let pessimistic = measured.map(|shares| {
        MODEL.savings(Shares {
            r_n: 1.0,
            r_d: 0.0,
            ..shares
        })
    });
    // The most the variants could save if every id that a process minted were
    // used while it lived and none came back: an upper bound on all of them.
    let best_case = m_upper.map(|m| {
        MODEL.savings(Shares {
            m,
            r_n: 0.0,
            r_d: 0.0,
            r_p: 0.0,
            volume,
            nodes,
        })
    });

    let savings_json = |savings: Option<Savings>| {
        savings.map(|s| {
            json!({
                "a_set": s.a_set,
                "a_reg": s.a_reg,
                "reads_only": s.reads_only,
                "preserved_resume_loss": s.preserved_resume_loss,
            })
        })
    };
    let json = json!({
        "window": {
            "start_ms": window_start,
            "end_ms": window_end,
            "days": window_days,
            "nodes_polled": polls.len(),
            "nodes_answered": nodes_answered,
            "nodes_without_counters": without_counters,
            "polls": polls.iter().map(|(node, (all, ok))| (
                (*node).to_owned(), json!({ "polls": all, "answered": ok }),
            )).collect::<BTreeMap<_, _>>(),
        },
        "gate": gate.iter().map(|(what, ok)| json!({ "check": what, "ok": ok })).collect::<Vec<_>>(),
        "counters": totals,
        "volume": {
            "first_claims_in_window": first_claims,
            "first_claims_a_month": volume,
        },
        "m": {
            "upper": m_upper,
            "local": m_local,
            "estimate": m,
            "sampled_first_claims": sampled_first_claims,
            "sampled_in_m": all.agents,
            "no_ensemble_share_of_local": share(
                total("first_claims_minted_local_no_ensemble"),
                total("first_claims_minted_local"),
            ),
            "minted_untracked_share": untracked_share,
        },
        "returns": {
            "r_n": r_n,
            "r_d": r_d,
            "r_p": r_p,
            "left_while_lived": share(all.left_while_lived, all.agents),
            "in_session_wakes": share(all.in_session_wakes, all.agents),
            "agents": all.agents,
            "born_in_first_two_days": {
                "agents": early.agents,
                "r_n": share(early.without_drain, early.agents),
                "r_d": share(early.after_drain, early.agents),
            },
            "delay_ms": {
                "returns": delays.len(),
                "p50": percentile(&delays, 50.0),
                "p90": percentile(&delays, 90.0),
                "p99": percentile(&delays, 99.0),
                "max": delays.last(),
            },
        },
        "sessions": {
            "processes": processes.len(),
            "started_in_window": started_in_window,
            "per_node_week": share_f(started_in_window as f64, node_weeks),
            "drained": drained,
            "drains_per_node_week": share_f(drained as f64, node_weeks),
            "ended": lengths.len(),
            "length_ms_p50": percentile(&lengths, 50.0),
            "length_ms_p90": percentile(&lengths, 90.0),
        },
        "drain_releases_a_month": per_month(total("releases_drain")),
        "empty_evictions_share": share(total("evictions.empty"), evictions_total),
        "net_dollars_a_month": {
            "at_measured_volume": savings_json(at_measured),
            "at_modelled_volume": savings_json(at_modelled),
            "every_minted_agent_returns_without_a_drain": savings_json(pessimistic),
            "m_upper_and_no_returns": savings_json(best_case),
            "go_threshold": MODEL.go_threshold,
        },
    });

    let markdown = render(&Render {
        window_days,
        window_start,
        window_end,
        nodes_answered,
        nodes_polled: polls.len(),
        gate: &gate,
        untracked_high,
        first_claims,
        volume,
        totals: &totals,
        sampled_first_claims,
        m_upper,
        m_local,
        m,
        all,
        early,
        delays: &delays,
        lengths: &lengths,
        started_in_window,
        drained,
        node_weeks,
        drain_releases: per_month(total("releases_drain")),
        empty_share: share(total("evictions.empty"), evictions_total),
        measured,
        at_measured,
        at_modelled,
        pessimistic,
        best_case,
    });
    Summary { json, markdown }
}

fn share_f(part: f64, whole: f64) -> Option<f64> {
    (whole > 0.0).then(|| part / whole)
}

fn percent(share: Option<f64>) -> String {
    share.map_or_else(
        || "n/a".to_owned(),
        |share| format!("{:.2}%", share * 100.0),
    )
}

fn dollars(value: Option<f64>) -> String {
    value.map_or_else(
        || "n/a".to_owned(),
        |value| {
            let rounded = value.round();
            // A figure that rounds to nothing has no sign.
            let sign = if rounded < 0.0 { "-" } else { "" };
            format!("{sign}${}", group(rounded.abs() as u64))
        },
    )
}

fn group(number: u64) -> String {
    let digits = number.to_string();
    let mut grouped = String::new();
    for (place, digit) in digits.chars().enumerate() {
        if place > 0 && (digits.len() - place).is_multiple_of(3) {
            grouped.push(',');
        }
        grouped.push(digit);
    }
    grouped
}

fn time(ms: u64) -> String {
    chrono::DateTime::from_timestamp_millis(ms as i64).map_or_else(
        || ms.to_string(),
        |at| at.format("%Y-%m-%d %H:%M UTC").to_string(),
    )
}

fn hours(ms: Option<u64>) -> String {
    ms.map_or_else(
        || "n/a".to_owned(),
        |ms| format!("{:.1} h", ms as f64 / 3.6e6),
    )
}

struct Render<'a> {
    window_days: f64,
    window_start: u64,
    window_end: u64,
    nodes_answered: usize,
    nodes_polled: usize,
    gate: &'a [(String, bool)],
    untracked_high: bool,
    first_claims: u64,
    volume: f64,
    totals: &'a BTreeMap<&'static str, u64>,
    sampled_first_claims: u64,
    m_upper: Option<f64>,
    m_local: Option<f64>,
    m: Option<f64>,
    all: Returns,
    early: Returns,
    delays: &'a [u64],
    lengths: &'a [u64],
    started_in_window: usize,
    drained: usize,
    node_weeks: f64,
    drain_releases: Option<f64>,
    empty_share: Option<f64>,
    measured: Option<Shares>,
    at_measured: Option<Savings>,
    at_modelled: Option<Savings>,
    pessimistic: Option<Savings>,
    best_case: Option<Savings>,
}

fn render(r: &Render<'_>) -> String {
    let mut out = String::new();
    let mut line = |text: String| {
        out.push_str(&text);
        out.push('\n');
    };
    line("# Ownership claim provenance: sample summary".into());
    line(String::new());
    line(format!(
        "Window: {} to {}, {:.2} days. {} of {} nodes answered.",
        time(r.window_start),
        time(r.window_end),
        r.window_days,
        r.nodes_answered,
        r.nodes_polled
    ));
    line(String::new());
    line("## Gate".into());
    for (what, ok) in r.gate {
        line(format!("- [{}] {what}", if *ok { "x" } else { " " }));
    }
    if r.untracked_high {
        line("- `minted_untracked` is high: m_local is too low, so m, which comes from the rings, is used alone.".into());
    }
    line(String::new());
    line("## Volume".into());
    line(format!(
        "First claims: {} in the window, {:.0} a month (30 days) from the nodes that answered.",
        r.first_claims, r.volume
    ));
    line(String::new());
    line("## m, the share of first claims a grant can reach".into());
    line("| Figure | Value | Bias |".into());
    line("| --- | --- | --- |".into());
    line(format!(
        "| m_upper = ids_minted / first_claims | {} | **an upper bound**: counts every id drawn, also one that is never used |",
        percent(r.m_upper)
    ));
    line(format!(
        "| m_local = first_claims_minted_local / first_claims | {} | a lower bound: misses an id first used through another node while its process lives |",
        percent(r.m_local)
    ));
    line(format!(
        "| m, from the rings ({} of {} sampled first claims) | {} | the estimate the dollars use; a mint or claim lost with a process in the last poll makes it slightly low |",
        r.all.agents, r.sampled_first_claims, percent(r.m)
    ));
    line(format!(
        "| claims of m_local without a fleet ensemble | {} | these cannot be granted, so they are left out of m |",
        percent(share(
            r.totals["first_claims_minted_local_no_ensemble"],
            r.totals["first_claims_minted_local"]
        ))
    ));
    line(String::new());
    line("## Returns, of the agents counted in m".into());
    line(
        "These are lower bounds: an agent born late in the window had less time to return.".into(),
    );
    line("| Figure | All | Born in the first 2 days |".into());
    line("| --- | --- | --- |".into());
    for (name, all, early) in [
        (
            "r_n: returned after its process ended without a drain",
            r.all.without_drain,
            r.early.without_drain,
        ),
        (
            "r_d: returned after a drain released it",
            r.all.after_drain,
            r.early.after_drain,
        ),
        (
            "left while the session lived (balancing move)",
            r.all.left_while_lived,
            r.early.left_while_lived,
        ),
        (
            "in-session wakes (claimed again by the bearing process)",
            r.all.in_session_wakes,
            r.early.in_session_wakes,
        ),
        (
            "r_p: carried over by a preserved restart in its first epoch",
            r.all.resumed,
            r.early.resumed,
        ),
    ] {
        line(format!(
            "| {name} | {} ({all}) | {} ({early}) |",
            percent(share(all, r.all.agents)),
            percent(share(early, r.early.agents)),
        ));
    }
    line(String::new());
    line(format!(
        "Delay from first claim to return (n = {}): p50 {}, p90 {}, p99 {}, max {}.",
        r.delays.len(),
        hours(percentile(r.delays, 50.0)),
        hours(percentile(r.delays, 90.0)),
        hours(percentile(r.delays, 99.0)),
        hours(r.delays.last().copied()),
    ));
    line(String::new());
    line("## Sessions and drains".into());
    line(format!(
        "{} processes started in the window ({:.2} per node-week), {} were seen draining ({:.2} per node-week). Ended sessions lasted p50 {}, p90 {}.",
        r.started_in_window,
        share_f(r.started_in_window as f64, r.node_weeks).unwrap_or(0.0),
        r.drained,
        share_f(r.drained as f64, r.node_weeks).unwrap_or(0.0),
        hours(percentile(r.lengths, 50.0)),
        hours(percentile(r.lengths, 90.0)),
    ));
    line(format!(
        "Drain releases a month, the saving no cost row counts: {}. Empty evictions: {} of all evictions.",
        r.drain_releases.map_or_else(|| "n/a".to_owned(), |n| format!("{n:.0}")),
        percent(r.empty_share),
    ));
    line(String::new());
    line("## Net dollars a month".into());
    line(format!(
        "**Modelled**: the constants are the cost model's, for {:.0}M agents a month on {:.0} nodes, and the measured m, r_n, r_d and volume are put into them. A positive figure is a saving against the design without grants. A variant is worth building at {} a month at the measured volume.",
        MODEL.agents_a_month / 1e6,
        MODEL.nodes,
        dollars(Some(MODEL.go_threshold))
    ));
    line(String::new());
    line("| Variant | At the measured volume | At 438M agents | Every minted agent returns without a drain |".into());
    line("| --- | --- | --- | --- |".into());
    for (name, pick) in [
        ("A-set", (|s: Savings| s.a_set) as fn(Savings) -> f64),
        ("A-reg", |s| s.a_reg),
        ("Reads only", |s| s.reads_only),
    ] {
        line(format!(
            "| {name} | {} | {} | {} |",
            dollars(r.at_measured.map(pick)),
            dollars(r.at_modelled.map(pick)),
            dollars(r.pessimistic.map(pick)),
        ));
    }
    if let (Some(shares), Some(measured)) = (r.measured, r.at_measured) {
        line(String::new());
        line(format!(
            "Put in: m = {:.4}, r_n = {:.4}, r_d = {:.4}, {:.0} first claims a month, {:.0} nodes. A preserved restart (r_p = {:.4}) would take {} a month from A-set and A-reg; it is not in the figures.",
            shares.m, shares.r_n, shares.r_d, shares.volume, shares.nodes, shares.r_p,
            dollars(Some(measured.preserved_resume_loss)),
        ));
    }
    line(String::new());
    line(format!(
        "Both biases push toward \"go\": m_upper is from above and r from below. If m_upper alone, with no agent returning, gives A-set {} against the {} a month that a variant needs, none of them can reach it.",
        dollars(r.best_case.map(|s| s.a_set)),
        dollars(Some(MODEL.go_threshold)),
    ));
    out
}
