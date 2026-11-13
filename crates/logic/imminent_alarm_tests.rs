//! A cell whose alarm is about to fire is worth keeping resident only while
//! the node has room to spare.

use crate::scripted::Shell;
use crate::*;

const WINDOW_MS: u64 = 3_600_000;

fn config() -> Config {
    Config {
        max_resident: 1,
        alarm_resident_ms: WINDOW_MS,
        ..crate::test_support::config()
    }
}

/// A node holding "a", idle, with an alarm due well inside the window.
fn holding_a_cell_with_a_near_alarm() -> Shell {
    let mut shell = Shell::new(config());
    let request = shell.request("a");
    let (now_ms, now_mono_ms) = (shell.now, shell.now);
    shell.send(Event::AlarmObserved {
        cell: "a".into(),
        alarm: wake::AlarmSnapshot::without_wake(Some((now_ms + 60_000) as i64)),
        covered: true,
        now_ms,
        now_mono_ms,
    });
    shell.finish(request);
    shell
}

fn eviction_started(shell: &Shell, cell: &str) -> bool {
    shell
        .inbox
        .iter()
        .any(|effect| matches!(effect, Effect::EnsureDurable { cell: c, .. } if c == cell))
}

#[test]
fn a_full_node_sheds_a_cell_with_a_near_alarm_for_a_waiter() {
    let mut shell = holding_a_cell_with_a_near_alarm();

    shell.request("b");

    assert!(
        eviction_started(&shell, "a"),
        "the waiter was parked behind a cell the node had no room to keep: {:?}",
        shell.inbox
    );
}

#[test]
fn an_idle_node_keeps_a_cell_with_a_near_alarm() {
    let config = Config {
        max_resident: 4,
        idle_evict_ms: Some(1_000),
        ..config()
    };
    let mut shell = Shell::new(config);
    let request = shell.request("a");
    let (now_ms, now_mono_ms) = (shell.now, shell.now);
    shell.send(Event::AlarmObserved {
        cell: "a".into(),
        alarm: wake::AlarmSnapshot::without_wake(Some((now_ms + 60_000) as i64)),
        covered: true,
        now_ms,
        now_mono_ms,
    });
    shell.finish(request);
    let now = shell.tick(10_000);

    shell.send(Event::LoadSampled {
        load: pressure::Load {
            resident_cells: 1,
            rss_bytes: 0,
            in_use_bytes: 0,
            cgroup_working_set_bytes: None,
            cgroup_current_bytes: None,
            container_reserved_bytes: 0,
        },
        now_mono_ms: now,
    });

    assert!(!eviction_started(&shell, "a"));
}
