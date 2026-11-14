// Copyright 2026 Deno Land Inc. Apache-2.0 license.

//! The files an eviction retains, and the wake and the next epoch that take them.

use super::*;

#[test]
fn a_resumed_cell_continues_its_lineage_and_a_restore_reads_every_row() {
    run(async {
        let rig = rig();
        first_lifetime(&rig).await;
        rig.ltx
            .evict(CELL, 1, Retained::Resumable, None)
            .await
            .expect("first eviction");
        assert!(evicted(&rig, 1).is_file() && !live(&rig, 1).exists());
        assert!(meta(&rig, 1).is_dir());

        let resumed = rig
            .ltx
            .activate(options(1, false, true))
            .await
            .expect("resume");
        // The rename ends the retained name, so the pre-write image is never
        // offered to the next epoch.
        assert!(!evicted(&rig, 1).exists());
        assert!(resumed.restored);
        write(&resumed.path, "INSERT INTO t VALUES (2);");
        rig.ltx
            .evict(CELL, 1, Retained::Resumable, None)
            .await
            .expect("second eviction");

        let restored = rig
            .ltx
            .restore_snapshot(CELL)
            .await
            .expect("restore")
            .expect("a restorable cell");
        assert_eq!(restored.epoch, 1);
        assert_eq!(rows(restored.path()), [1, 2]);
    });
}

#[test]
fn a_crash_after_a_wake_restores_the_new_rows_and_not_the_retained_base() {
    run(async {
        let rig = rig();
        first_lifetime(&rig).await;
        rig.ltx
            .evict(CELL, 1, Retained::Resumable, None)
            .await
            .expect("first eviction");
        let resumed = rig
            .ltx
            .activate(options(1, false, true))
            .await
            .expect("resume");
        write(&resumed.path, "INSERT INTO t VALUES (2);");
        synced(&rig).await;

        // The process is gone without a second eviction. The wake renamed the
        // retained base, so the next epoch finds none to reuse and restores the
        // bucket's chain, which holds the acknowledged row.
        assert!(!evicted(&rig, 1).exists());
        let next = rig
            .ltx
            .activate(options(2, false, false))
            .await
            .expect("claim the next epoch");
        assert!(next.restored);
        assert_eq!(rows(&next.path), [1, 2]);
    });
}

#[test]
fn the_next_epoch_takes_a_retained_base_and_drops_its_lineage() {
    run(async {
        let rig = rig();
        first_lifetime(&rig).await;
        rig.ltx
            .evict(CELL, 1, Retained::Resumable, None)
            .await
            .expect("eviction");

        let next = rig
            .ltx
            .activate(options(2, false, false))
            .await
            .expect("claim the next epoch");

        assert_eq!(rows(&next.path), [1]);
        assert!(!evicted(&rig, 1).exists());
        assert!(!meta(&rig, 1).exists());
    });
}

#[test]
fn a_base_kept_without_its_metadata_cannot_be_resumed() {
    run(async {
        let rig = rig();
        first_lifetime(&rig).await;
        rig.ltx
            .evict(CELL, 1, Retained::Base, None)
            .await
            .expect("eviction");
        assert!(!meta(&rig, 1).exists());

        let error = rig
            .ltx
            .activate(options(1, false, true))
            .await
            .map(|_| ())
            .expect_err("a resume without metadata");

        assert!(error
            .downcast_ref::<crate::replication::RetainedUnavailable>()
            .is_some());
        assert!(error.to_string().contains("no LTX metadata retained"));
        assert!(evicted(&rig, 1).is_file());
    });
}

#[test]
fn a_live_database_whose_metadata_is_gone_cannot_be_resumed() {
    run(async {
        let rig = rig();
        first_lifetime(&rig).await;
        rig.ltx
            .evict(CELL, 1, Retained::Resumable, None)
            .await
            .expect("eviction");
        // What a failed rename leaves: the live name, without its lineage.
        std::fs::rename(evicted(&rig, 1), live(&rig, 1)).expect("rename");
        std::fs::remove_dir_all(meta(&rig, 1)).expect("remove the metadata");

        let error = rig
            .ltx
            .activate(options(1, false, true))
            .await
            .map(|_| ())
            .expect_err("a resume without metadata");

        assert!(error.to_string().contains("no LTX metadata retained"));
    });
}

#[test]
fn the_cache_ceiling_removes_a_retained_database_with_the_files_beside_it() {
    run(async {
        let rig = rig();
        first_lifetime(&rig).await;
        rig.ltx
            .evict(CELL, 1, Retained::Resumable, None)
            .await
            .expect("eviction");

        rig.ltx.prune_local_cache(u64::MAX).expect("prune");
        assert!(evicted(&rig, 1).is_file() && meta(&rig, 1).is_dir());

        rig.ltx.prune_local_cache(0).expect("prune");
        assert!(!evicted(&rig, 1).exists());
        assert!(!meta(&rig, 1).exists());

        let error = rig
            .ltx
            .activate(options(1, false, true))
            .await
            .map(|_| ())
            .expect_err("a resume without its database");
        assert!(error
            .downcast_ref::<crate::replication::RetainedUnavailable>()
            .is_some());
        assert!(error.to_string().contains("no database retained"));
    });
}

#[test]
fn a_lost_retained_base_fails_the_resume_for_the_core_to_claim_again() {
    run(async {
        let rig = rig();
        first_lifetime(&rig).await;
        rig.ltx
            .evict(CELL, 1, Retained::Resumable, None)
            .await
            .expect("eviction");
        std::fs::remove_file(evicted(&rig, 1)).expect("remove the retained base");

        let error = rig
            .ltx
            .activate(options(1, false, true))
            .await
            .map(|_| ())
            .expect_err("a resume without a database");

        assert!(error
            .downcast_ref::<crate::replication::RetainedUnavailable>()
            .is_some());
        assert!(error.to_string().contains("no database retained"));
    });
}

#[test]
fn a_recovery_after_a_wake_gathers_the_rows_above_the_snapshot() {
    run(async {
        let rig = rig();
        first_lifetime(&rig).await;
        rig.ltx
            .evict(CELL, 1, Retained::Resumable, None)
            .await
            .expect("first eviction");
        // The mark a recovery reads is the snapshot's position, from this
        // process's memory and, for a process that never served the cell,
        // from the bucket's listing.
        assert_eq!(rig.ltx.covered_txid(CELL, 1, 1_000).await, 1);
        assert_eq!(rig_over(&rig).ltx.covered_txid(CELL, 1, 1_000).await, 1);

        let resumed = rig
            .ltx
            .activate(options(1, false, true))
            .await
            .expect("resume");
        write(&resumed.path, "INSERT INTO t VALUES (2);");
        let second = position(&rig, 1);
        assert_eq!(second, 2);

        // The wake's rows are above the mark, so a recovery gathers them from
        // the bundles instead of skipping them as covered.
        assert_eq!(rig.ltx.covered_txid(CELL, 1, 1_000).await, 1);
    });
}

#[test]
fn the_graceful_seal_waits_for_the_snapshot_of_a_resumed_cell() {
    run(async {
        let rig = rig();
        first_lifetime(&rig).await;
        rig.ltx
            .evict(CELL, 1, Retained::Resumable, None)
            .await
            .expect("first eviction");
        assert!(rig.ltx.all_tails_tiered());

        let resumed = rig
            .ltx
            .activate(options(1, false, true))
            .await
            .expect("resume");
        write(&resumed.path, "INSERT INTO t VALUES (2);");
        let second = position(&rig, 1);
        // A fleet credit for the new row: acknowledged, and in no per-cell
        // object. A sealed record would orphan it.
        let handle = rig
            .ltx
            .cells
            .lock()
            .unwrap()
            .get(&(CELL.to_string(), 1))
            .cloned()
            .expect("resident handle");
        handle.shipped_txid.fetch_max(second, Ordering::SeqCst);
        assert!(!rig.ltx.all_tails_tiered());

        rig.ltx
            .evict(CELL, 1, Retained::Resumable, None)
            .await
            .expect("second eviction");
        assert!(rig.ltx.all_tails_tiered());
    });
}

/// A filesystem that refuses to remove a directory while `refusing` is set.
struct RefusesRemoval {
    inner: celld_ltx::DirectFileSystem,
    refusing: Arc<AtomicBool>,
    /// Creating a LTX metadata directory fails while set, which fails
    /// opening a database after its files are in place.
    refusing_meta: Arc<AtomicBool>,
}

impl celld_ltx::FileSystem for RefusesRemoval {
    fn read(&self, path: &Path) -> std::io::Result<Vec<u8>> {
        self.inner.read(path)
    }
    fn read_dir(&self, path: &Path) -> std::io::Result<Vec<celld_ltx::HostDirEntry>> {
        self.inner.read_dir(path)
    }
    fn metadata(&self, path: &Path) -> std::io::Result<celld_ltx::HostMetadata> {
        self.inner.metadata(path)
    }
    fn create(&self, path: &Path) -> std::io::Result<celld_ltx::HostFile> {
        self.inner.create(path)
    }
    fn open(&self, path: &Path) -> std::io::Result<celld_ltx::HostFile> {
        self.inner.open(path)
    }
    fn write(&self, path: &Path, bytes: &[u8]) -> std::io::Result<()> {
        self.inner.write(path, bytes)
    }
    fn sync_all(&self, path: &Path) -> std::io::Result<()> {
        self.inner.sync_all(path)
    }
    fn rename(&self, from: &Path, to: &Path) -> std::io::Result<()> {
        self.inner.rename(from, to)
    }
    fn remove_file(&self, path: &Path) -> std::io::Result<()> {
        self.inner.remove_file(path)
    }
    fn remove_dir_all(&self, path: &Path) -> std::io::Result<()> {
        if self.refusing.load(Ordering::SeqCst) {
            return Err(std::io::Error::other("refused"));
        }
        self.inner.remove_dir_all(path)
    }
    fn create_dir_all(&self, path: &Path) -> std::io::Result<()> {
        let meta = path
            .file_name()
            .is_some_and(|name| name.to_string_lossy().ends_with("-litestream"));
        if meta && self.refusing_meta.load(Ordering::SeqCst) {
            return Err(std::io::Error::other("refused"));
        }
        self.inner.create_dir_all(path)
    }
}

#[test]
fn a_base_whose_lineage_cannot_be_removed_is_not_taken_and_strands_nothing() {
    run(async {
        let refusing = Arc::new(AtomicBool::new(false));
        let host = ltx_host_over(Arc::new(RefusesRemoval {
            inner: celld_ltx::DirectFileSystem,
            refusing: refusing.clone(),
            refusing_meta: Arc::default(),
        }));
        let rig = rig_on(None, host);
        first_lifetime(&rig).await;
        rig.ltx
            .evict(CELL, 1, Retained::Resumable, None)
            .await
            .expect("eviction");

        refusing.store(true, Ordering::SeqCst);
        rig.ltx
            .activate(options(2, false, false))
            .await
            .map(|_| ())
            .expect_err("a lineage that cannot be removed");

        // The base stays where the inventory and the cache find it, with the
        // files beside it, for the retry.
        assert!(evicted(&rig, 1).is_file() && meta(&rig, 1).is_dir());
        assert!(!live(&rig, 2).exists());

        refusing.store(false, Ordering::SeqCst);
        let next = rig
            .ltx
            .activate(options(2, false, false))
            .await
            .expect("claim the next epoch");
        assert_eq!(rows(&next.path), [1]);
        assert!(!evicted(&rig, 1).exists() && !meta(&rig, 1).exists());
    });
}

/// A connection that outlives the eviction's close, so SQLite does not fold
/// the write-ahead log into the database file and the log keeps the row.
fn commit_behind_the_close(path: &Path) -> rusqlite::Connection {
    let held = rusqlite::Connection::open(path).expect("open the cell database");
    held.execute_batch("INSERT INTO t VALUES (2);")
        .expect("write");
    held
}

#[test]
fn a_resumable_base_with_pages_in_its_log_is_not_reused_by_the_next_epoch() {
    run(async {
        let rig = rig();
        let path = first_lifetime(&rig).await;
        let _held = commit_behind_the_close(&path);
        rig.ltx
            .evict(CELL, 1, Retained::Resumable, None)
            .await
            .expect("eviction");
        assert!(evicted(&rig, 1).is_file());
        assert!(rig.ltx.wal_holds_pages(&live(&rig, 1)));

        // The base alone is older than the snapshot the eviction published.
        let next = rig
            .ltx
            .activate(options(2, false, false))
            .await
            .expect("claim the next epoch");

        assert!(next.restored);
        assert_eq!(rows(&next.path), [1, 2]);
    });
}

#[test]
fn a_base_whose_log_still_holds_pages_is_not_kept() {
    run(async {
        let rig = rig();
        let path = first_lifetime(&rig).await;
        let _held = commit_behind_the_close(&path);

        rig.ltx
            .evict(CELL, 1, Retained::Base, None)
            .await
            .expect("eviction");
        assert!(!evicted(&rig, 1).exists());

        let next = rig
            .ltx
            .activate(options(2, false, false))
            .await
            .expect("claim the next epoch");
        assert_eq!(rows(&next.path), [1, 2]);
    });
}

#[test]
fn a_clean_reload_lists_nothing_on_a_node_that_keeps_no_dormant_databases() {
    run(async {
        let rig = rig_configured(Some(EVERY_ROW), production_ltx_host(), false);
        first_lifetime(&rig).await;
        rig.ltx.close_for_reload(CELL, 1).await.expect("reload");
        let before = rig.store.offset_lists.load(Ordering::SeqCst);

        rig.ltx
            .activate(options(1, false, true))
            .await
            .expect("reopen");

        assert_eq!(rig.store.offset_lists.load(Ordering::SeqCst), before);
        assert_eq!(resident(&rig, 1).compaction.as_ref().unwrap().base_txid, 1);
    });
}

#[test]
fn a_clean_reload_needs_no_bucket_on_a_node_that_keeps_no_dormant_databases() {
    run(async {
        let rig = rig_configured(Some(EVERY_ROW), production_ltx_host(), false);
        first_lifetime(&rig).await;
        rig.ltx.close_for_reload(CELL, 1).await.expect("reload");

        rig.store.down.store(true, Ordering::SeqCst);
        rig.ltx
            .activate(options(1, false, true))
            .await
            .expect("a reload reads nothing remote");
    });
}

#[test]
fn a_failed_resume_returns_the_database_to_the_cache() {
    run(async {
        let refusing_meta = Arc::new(AtomicBool::new(false));
        let host = ltx_host_over(Arc::new(RefusesRemoval {
            inner: celld_ltx::DirectFileSystem,
            refusing: Arc::default(),
            refusing_meta: refusing_meta.clone(),
        }));
        let rig = rig_on(None, host);
        first_lifetime(&rig).await;
        rig.ltx
            .evict(CELL, 1, Retained::Resumable, None)
            .await
            .expect("eviction");
        // The cache has read the disk once, so it knows only what it is told.
        rig.ltx.prune_local_cache(u64::MAX).expect("prune");

        refusing_meta.store(true, Ordering::SeqCst);
        rig.ltx
            .activate(options(1, false, true))
            .await
            .map(|_| ())
            .expect_err("a database that cannot be opened");
        refusing_meta.store(false, Ordering::SeqCst);

        // Neither a live file nor an unaccounted base is left at the epoch.
        assert!(evicted(&rig, 1).is_file() && !live(&rig, 1).exists());
        rig.ltx.prune_local_cache(0).expect("prune");
        assert!(!evicted(&rig, 1).exists() && !meta(&rig, 1).exists());
    });
}
