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

/// What a filesystem refuses while its flag is set.
#[derive(Default)]
struct Refusals {
    /// Removing a directory.
    removal: AtomicBool,
    /// Creating a LTX metadata directory, which fails opening a database
    /// after its files are in place.
    meta: AtomicBool,
    /// Renaming the record of an eviction's snapshot into place, after it
    /// was written.
    cut: AtomicBool,
    /// The retained database vanishes before a rename of it, as when the
    /// cache removes it between a wake's check and its rename.
    vanish: AtomicBool,
    /// Every path synced, in order. Not a refusal: it shows what was made
    /// durable.
    synced: std::sync::Mutex<Vec<std::path::PathBuf>>,
}

struct RefusingFilesystem {
    inner: celld_ltx::DirectFileSystem,
    refusals: Arc<Refusals>,
}

/// A rig over a filesystem that refuses what `Refusals` says.
fn rig_refusing() -> (Rig, Arc<Refusals>) {
    let refusals = Arc::<Refusals>::default();
    let host = ltx_host_over(Arc::new(RefusingFilesystem {
        inner: celld_ltx::DirectFileSystem,
        refusals: refusals.clone(),
    }));
    (rig_on(None, host), refusals)
}

impl celld_ltx::FileSystem for RefusingFilesystem {
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
        self.refusals
            .synced
            .lock()
            .unwrap()
            .push(path.to_path_buf());
        self.inner.sync_all(path)
    }
    fn rename(&self, from: &Path, to: &Path) -> std::io::Result<()> {
        if to.extension().is_some_and(|ext| ext == "cut")
            && self.refusals.cut.load(Ordering::SeqCst)
        {
            return Err(std::io::Error::other("refused"));
        }
        if from.extension().is_some_and(|ext| ext == "evicted")
            && self.refusals.vanish.load(Ordering::SeqCst)
        {
            self.inner.remove_file(from)?;
        }
        self.inner.rename(from, to)
    }
    fn remove_file(&self, path: &Path) -> std::io::Result<()> {
        self.inner.remove_file(path)
    }
    fn remove_dir_all(&self, path: &Path) -> std::io::Result<()> {
        if self.refusals.removal.load(Ordering::SeqCst) {
            return Err(std::io::Error::other("refused"));
        }
        self.inner.remove_dir_all(path)
    }
    fn create_dir_all(&self, path: &Path) -> std::io::Result<()> {
        let meta = path
            .file_name()
            .is_some_and(|name| name.to_string_lossy().ends_with("-litestream"));
        if meta && self.refusals.meta.load(Ordering::SeqCst) {
            return Err(std::io::Error::other("refused"));
        }
        self.inner.create_dir_all(path)
    }
}

#[test]
fn a_base_whose_lineage_cannot_be_removed_is_not_taken_and_strands_nothing() {
    run(async {
        let (rig, refusals) = rig_refusing();
        first_lifetime(&rig).await;
        rig.ltx
            .evict(CELL, 1, Retained::Resumable, None)
            .await
            .expect("eviction");

        refusals.removal.store(true, Ordering::SeqCst);
        rig.ltx
            .activate(options(2, false, false))
            .await
            .map(|_| ())
            .expect_err("a lineage that cannot be removed");

        // The base stays where the inventory and the cache find it, with the
        // files beside it, for the retry.
        assert!(evicted(&rig, 1).is_file() && meta(&rig, 1).is_dir());
        assert!(!live(&rig, 2).exists());

        refusals.removal.store(false, Ordering::SeqCst);
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
fn a_failed_resume_returns_the_database_to_the_cache() {
    run(async {
        let (rig, refusals) = rig_refusing();
        first_lifetime(&rig).await;
        rig.ltx
            .evict(CELL, 1, Retained::Resumable, None)
            .await
            .expect("eviction");
        // The cache has read the disk once, so it knows only what it is told.
        rig.ltx.prune_local_cache(u64::MAX).expect("prune");

        refusals.meta.store(true, Ordering::SeqCst);
        rig.ltx
            .activate(options(1, false, true))
            .await
            .map(|_| ())
            .expect_err("a database that cannot be opened");
        refusals.meta.store(false, Ordering::SeqCst);

        // Neither a live file nor an unaccounted base is left at the epoch.
        assert!(evicted(&rig, 1).is_file() && !live(&rig, 1).exists());
        rig.ltx.prune_local_cache(0).expect("prune");
        assert!(!evicted(&rig, 1).exists() && !meta(&rig, 1).exists());
    });
}

#[test]
fn a_clean_reload_of_a_resumed_cell_needs_no_bucket() {
    run(async {
        let rig = rig_with(Some(EVERY_ROW));
        first_lifetime(&rig).await;
        rig.ltx
            .evict(CELL, 1, Retained::Resumable, None)
            .await
            .expect("eviction");
        let resumed = rig
            .ltx
            .activate(options(1, false, true))
            .await
            .expect("resume");
        write(&resumed.path, "INSERT INTO t VALUES (2);");
        rig.ltx.close_for_reload(CELL, 1).await.expect("reload");

        rig.store.down.store(true, Ordering::SeqCst);
        let before = rig.store.requests.load(Ordering::SeqCst);
        rig.ltx
            .activate(options(1, false, true))
            .await
            .expect("a reload reads nothing remote");

        assert_eq!(rig.store.requests.load(Ordering::SeqCst), before);
        assert_eq!(resident(&rig, 1).compaction.as_ref().unwrap().base_txid, 2);
    });
}

#[test]
fn a_wake_with_nothing_kept_falls_back_without_a_request() {
    run(async {
        let rig = rig_with(Some(EVERY_ROW));
        first_lifetime(&rig).await;
        // What a root that opened a facet keeps: a base and no lineage.
        rig.ltx
            .evict(CELL, 1, Retained::Base, None)
            .await
            .expect("eviction");

        rig.store.down.store(true, Ordering::SeqCst);
        let before = rig.store.requests.load(Ordering::SeqCst);
        let error = rig
            .ltx
            .activate(options(1, false, true))
            .await
            .map(|_| ())
            .expect_err("nothing to reopen");

        assert!(error
            .downcast_ref::<crate::replication::RetainedUnavailable>()
            .is_some());
        assert_eq!(rig.store.requests.load(Ordering::SeqCst), before);
    });
}

#[test]
fn a_wake_whose_database_the_cache_removed_falls_back_without_a_request() {
    run(async {
        let rig = rig_with(Some(EVERY_ROW));
        first_lifetime(&rig).await;
        rig.ltx
            .evict(CELL, 1, Retained::Resumable, None)
            .await
            .expect("eviction");
        rig.ltx.prune_local_cache(0).expect("prune");

        rig.store.down.store(true, Ordering::SeqCst);
        let before = rig.store.requests.load(Ordering::SeqCst);
        let error = rig
            .ltx
            .activate(options(1, false, true))
            .await
            .map(|_| ())
            .expect_err("the cache removed the database");

        assert!(error
            .downcast_ref::<crate::replication::RetainedUnavailable>()
            .is_some());
        assert_eq!(rig.store.requests.load(Ordering::SeqCst), before);
    });
}

#[test]
fn an_eviction_that_cannot_record_its_snapshot_keeps_only_a_base() {
    run(async {
        let (rig, refusals) = rig_refusing();
        first_lifetime(&rig).await;

        refusals.cut.store(true, Ordering::SeqCst);
        rig.ltx
            .evict(CELL, 1, Retained::Resumable, None)
            .await
            .expect("eviction");

        // The next epoch can still take the base; a wake at this epoch cannot.
        assert!(evicted(&rig, 1).is_file() && !meta(&rig, 1).exists());
        let error = rig
            .ltx
            .activate(options(1, false, true))
            .await
            .map(|_| ())
            .expect_err("no lineage to reopen");
        assert!(error
            .downcast_ref::<crate::replication::RetainedUnavailable>()
            .is_some());
    });
}

#[test]
fn a_stray_record_file_goes_with_the_lineage_the_next_epoch_clears() {
    run(async {
        let rig = rig();
        first_lifetime(&rig).await;
        rig.ltx
            .evict(CELL, 1, Retained::Resumable, None)
            .await
            .expect("eviction");
        let stray = live(&rig, 1).with_extension("cut.tmp");
        std::fs::write(&stray, "7").expect("leave a record file behind");

        rig.ltx
            .activate(options(2, false, false))
            .await
            .expect("claim the next epoch");

        assert!(!stray.exists());
    });
}

#[test]
fn a_cache_prune_between_a_wakes_check_and_its_rename_is_a_missing_database() {
    run(async {
        let (rig, refusals) = rig_refusing();
        first_lifetime(&rig).await;
        rig.ltx
            .evict(CELL, 1, Retained::Resumable, None)
            .await
            .expect("eviction");

        refusals.vanish.store(true, Ordering::SeqCst);
        let error = rig
            .ltx
            .activate(options(1, false, true))
            .await
            .map(|_| ())
            .expect_err("the database vanished");

        assert!(error
            .downcast_ref::<crate::replication::RetainedUnavailable>()
            .is_some());
    });
}

#[test]
fn a_prune_that_cannot_clear_the_lineage_clears_it_on_the_next_prune() {
    run(async {
        let (rig, refusals) = rig_refusing();
        first_lifetime(&rig).await;
        rig.ltx
            .evict(CELL, 1, Retained::Resumable, None)
            .await
            .expect("eviction");

        refusals.removal.store(true, Ordering::SeqCst);
        rig.ltx
            .prune_local_cache(0)
            .expect_err("the lineage cannot be removed");
        // The database is gone, and nothing but the cache knows the files
        // beside it.
        assert!(!evicted(&rig, 1).exists() && meta(&rig, 1).is_dir());

        refusals.removal.store(false, Ordering::SeqCst);
        rig.ltx.prune_local_cache(u64::MAX).expect("prune");

        assert!(!meta(&rig, 1).exists());
    });
}

#[test]
fn a_record_that_cannot_be_renamed_into_place_leaves_no_file_behind() {
    run(async {
        let (rig, refusals) = rig_refusing();
        first_lifetime(&rig).await;

        refusals.cut.store(true, Ordering::SeqCst);
        rig.ltx
            .record_cut(CELL, 1, 1)
            .expect_err("the record cannot be renamed");

        assert!(!live(&rig, 1).with_extension("cut.tmp").exists());
        assert!(!live(&rig, 1).with_extension("cut").exists());
    });
}

#[test]
fn a_record_is_durable_in_its_directory_once_it_is_renamed_into_place() {
    run(async {
        let (rig, refusals) = rig_refusing();
        first_lifetime(&rig).await;
        refusals.synced.lock().unwrap().clear();

        rig.ltx.record_cut(CELL, 1, 1).expect("record");

        let directory = live(&rig, 1).parent().unwrap().to_path_buf();
        assert_eq!(
            refusals.synced.lock().unwrap().last(),
            Some(&directory),
            "the directory holding the renamed record is synced last"
        );
    });
}

#[test]
fn a_base_whose_lineage_cannot_be_removed_is_not_kept() {
    run(async {
        let (rig, refusals) = rig_refusing();
        first_lifetime(&rig).await;

        refusals.removal.store(true, Ordering::SeqCst);
        rig.ltx
            .evict(CELL, 1, Retained::Base, None)
            .await
            .expect("eviction");
        refusals.removal.store(false, Ordering::SeqCst);

        // The metadata survived, so a kept base would pass for resumable.
        assert!(meta(&rig, 1).is_dir());
        assert!(!evicted(&rig, 1).exists());
        let error = rig
            .ltx
            .activate(options(1, false, true))
            .await
            .map(|_| ())
            .expect_err("no resume at the old epoch");
        assert!(error
            .downcast_ref::<crate::replication::RetainedUnavailable>()
            .is_some());
    });
}
