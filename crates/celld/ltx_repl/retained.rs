// Copyright 2026 Deno Land Inc. Apache-2.0 license.

//! The files an eviction leaves on this node for a later activation.
//!
//! A closed database is kept as `.evicted`, which the next epoch takes as its
//! base, or with the LTX metadata of its lineage beside it, which a wake at
//! the same epoch reopens. This module owns that layout: what a stop keeps,
//! how a resume takes it back, and how every path that discards it clears the
//! files beside the database.

use super::*;

/// What an eviction leaves of the closed database on this node.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Retained {
    /// No local file: the next activation restores from the bucket.
    Nothing,
    /// The database as `.evicted`, a base for the next epoch to open.
    Base,
    /// A base plus the LTX metadata and WAL files of the epoch it closed at,
    /// so a wake at that same epoch reopens it without claiming a new one.
    Resumable,
}

/// The name a retained database goes by.
pub(crate) fn evicted_path(db: &Path) -> PathBuf {
    db.with_extension("evicted")
}

/// The live name a retained database returns to.
pub(crate) fn live_path(evicted: &Path) -> PathBuf {
    evicted.with_extension("sqlite")
}

/// Where an eviction records the end of the snapshot it published, for the
/// handle that reopens the database to compact from.
fn cut_path(db: &Path) -> PathBuf {
    db.with_extension("cut")
}

/// The file an eviction writes the record to before it takes its name.
fn cut_temporary_path(db: &Path) -> PathBuf {
    db.with_extension("cut.tmp")
}

/// Remove what a database keeps beside it: the LTX metadata that carries its
/// writer lineage, the record of its snapshot and SQLite's sidecar files.
/// Every removal is tried, and the first failure other than a path that was
/// already gone is answered.
pub(crate) fn remove_lineage(
    filesystem: &dyn celld_ltx::FileSystem,
    db: &Path,
) -> std::io::Result<()> {
    let mut first_error = None;
    let mut note = |result: std::io::Result<()>| {
        if let Err(error) = result {
            if error.kind() != std::io::ErrorKind::NotFound && first_error.is_none() {
                first_error = Some(error);
            }
        }
    };
    note(filesystem.remove_dir_all(&Db::meta_path_for_path(db)));
    note(filesystem.remove_file(&cut_path(db)));
    note(filesystem.remove_file(&cut_temporary_path(db)));
    for sidecar in Db::sidecar_paths_for_path(db) {
        note(filesystem.remove_file(&sidecar));
    }
    first_error.map_or(Ok(()), Err)
}

/// Where a reopened handle's compaction continues from.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum ResumeCut {
    /// The end of the snapshot the epoch was last evicted at, or 0 for an
    /// epoch that has none: compaction continues from the next transaction.
    At(u64),
    /// The record of the snapshot cannot be read. A cut is never guessed, so
    /// the handle does not compact.
    Unknown,
}

/// What a resume takes back from the files an eviction left.
pub(super) struct ResumePlan<'a> {
    guard: Option<ReturnToCache<'a>>,
    pub(super) cut: ResumeCut,
}

impl ResumePlan<'_> {
    /// The activation reached its handle, which owns the database now.
    pub(super) fn keep(&mut self) {
        if let Some(guard) = &mut self.guard {
            guard.keep();
        }
    }
}

/// A retained database that a resume renamed to its live name. Unless the
/// activation reaches its handle, it goes back to `.evicted` and into the
/// preserved cache: the core then claims the next epoch, and nothing else
/// counts or removes files at the epoch this node leaves.
pub(super) struct ReturnToCache<'a> {
    repl: &'a LtxRepl,
    cell: String,
    epoch: u64,
    live: PathBuf,
    armed: bool,
}

impl ReturnToCache<'_> {
    /// The activation reached its handle, which owns the database now.
    pub(super) fn keep(&mut self) {
        self.armed = false;
    }
}

impl Drop for ReturnToCache<'_> {
    fn drop(&mut self) {
        if self.armed {
            self.repl
                .preserve_as_evicted(&self.cell, self.epoch, &self.live);
        }
    }
}

impl LtxRepl {
    /// Prepare what a same-epoch wake continues: put its database at the live
    /// path and read where the handle's compaction continues from.
    ///
    /// Everything here is local. The files are checked before anything else,
    /// so a wake with nothing kept fails with [`RetainedUnavailable`] having
    /// asked the bucket for nothing.
    ///
    /// A clean reload left the database at its live path. A dormant cell left
    /// it as `.evicted`, and the rename is what ends that name: no epoch
    /// directory holds both, so a crash after the first write cannot offer the
    /// pre-write image to the next epoch as its base, which would drop every
    /// row since.
    ///
    /// The LTX metadata must be there too. Opening a database without it
    /// starts a writer lineage at TXID 1 inside an epoch that already holds
    /// files (#158), so a missing directory fails the wake and the core claims
    /// the next epoch instead.
    ///
    /// A database that was renamed goes back to `.evicted`, and into the
    /// cache, when the activation does not reach its handle: the plan holds it
    /// until `keep`.
    pub(super) fn prepare_resume(
        &self,
        cell: &str,
        epoch: u64,
        dst: &Path,
    ) -> anyhow::Result<ResumePlan<'_>> {
        let is_file = |path: &Path| {
            self.ltx_host
                .metadata(path)
                .is_ok_and(|metadata| metadata.is_file)
        };
        // A live file with its metadata gone is what a failed rename leaves,
        // so the metadata is checked whichever name holds the database.
        let meta = Db::meta_path_for_path(dst);
        let retained = evicted_path(dst);
        let unavailable = |missing| {
            anyhow::Error::new(crate::replication::RetainedUnavailable {
                cell: cell.to_string(),
                epoch,
                missing,
            })
        };
        let from_eviction = !is_file(dst);
        if from_eviction && !is_file(&retained) {
            return Err(unavailable("database"));
        }
        if !self
            .ltx_host
            .metadata(&meta)
            .is_ok_and(|metadata| metadata.is_dir)
        {
            return Err(unavailable("LTX metadata"));
        }
        let cut = self.read_cut(dst);
        if !from_eviction {
            return Ok(ResumePlan { guard: None, cut });
        }
        // The cache can remove the database between the check and the rename.
        self.ltx_host.rename(&retained, dst).map_err(|error| {
            if error.kind() == std::io::ErrorKind::NotFound {
                unavailable("database")
            } else {
                error.into()
            }
        })?;
        self.preserved
            .lock()
            .expect("preserved cache poisoned")
            .forget(&retained);
        Ok(ResumePlan {
            guard: Some(ReturnToCache {
                repl: self,
                cell: cell.to_string(),
                epoch,
                live: dst.to_path_buf(),
                armed: true,
            }),
            cut,
        })
    }

    /// The end of the snapshot the epoch was last evicted at. A database that
    /// no eviction published a snapshot for has no record, and compacts from
    /// the first transaction.
    fn read_cut(&self, db: &Path) -> ResumeCut {
        match self.ltx_host.filesystem().read(&cut_path(db)) {
            Ok(bytes) => String::from_utf8(bytes)
                .ok()
                .and_then(|text| text.trim().parse().ok())
                .map_or(ResumeCut::Unknown, ResumeCut::At),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => ResumeCut::At(0),
            Err(_) => ResumeCut::Unknown,
        }
    }

    /// Record the end of the snapshot an eviction published, beside the
    /// database it closes.
    pub(super) fn record_cut(&self, cell: &str, epoch: u64, end: u64) -> std::io::Result<()> {
        let db = self.db_path(cell, epoch);
        let temporary = cut_temporary_path(&db);
        let filesystem = self.ltx_host.filesystem();
        let recorded = filesystem
            .write(&temporary, end.to_string().as_bytes())
            .and_then(|()| filesystem.sync_all(&temporary))
            .and_then(|()| filesystem.rename(&temporary, &cut_path(&db)));
        if recorded.is_err() {
            let _ = filesystem.remove_file(&temporary);
        }
        recorded
    }

    /// Whether the write-ahead log of the database at `db` holds pages. SQLite
    /// folds the log into the file and removes it when its last connection
    /// closes, so a log that survives the close was not folded. A log that
    /// cannot be read is taken to hold pages.
    pub(super) fn wal_holds_pages(&self, db: &Path) -> bool {
        match self.ltx_host.metadata(&Db::wal_path_for_path(db)) {
            Ok(metadata) => metadata.len > 0,
            Err(error) => error.kind() != std::io::ErrorKind::NotFound,
        }
    }

    /// Remove what a retained epoch kept beside its database, before the next
    /// epoch takes the database as its base. Its LTX metadata describes a
    /// lineage that epoch no longer writes.
    ///
    /// This runs ahead of the rename: once the database has left its name, no
    /// inventory or cache entry reaches the files beside it, so a failure or a
    /// crash after the rename would strand them. A failure here leaves the
    /// base where it was, still a valid base that the activation can retry.
    pub(super) fn forget_lineage(&self, cell: &str, epoch: u64) -> anyhow::Result<()> {
        let db = self.db_path(cell, epoch);
        Ok(remove_lineage(self.ltx_host.filesystem().as_ref(), &db)?)
    }

    /// Rename the database at `db` to `.evicted` and register it with the
    /// preserved cache. Answers whether the rename happened; a failure is
    /// logged and leaves the database where it was.
    pub(super) fn preserve_as_evicted(&self, cell: &str, epoch: u64, db: &Path) -> bool {
        let preserved = evicted_path(db);
        if let Err(error) = self.ltx_host.rename(db, &preserved) {
            warn!(cell, epoch, %error, "preserve local snapshot failed");
            return false;
        }
        if let Err(error) = self
            .preserved
            .lock()
            .expect("preserved cache poisoned")
            .insert(preserved)
        {
            warn!(cell, epoch, %error, "index preserved local snapshot failed");
        }
        true
    }

    /// Settle the files of a closed stream: keep what `retained` asks for, and
    /// remove the rest. `close_result` is the close of the managed database,
    /// which decides whether anything may be kept at all.
    pub(super) fn finish_remove_local(
        &self,
        cell: &str,
        epoch: u64,
        retained: Retained,
        close_result: anyhow::Result<()>,
    ) {
        let retained = match close_result {
            Ok(()) => retained,
            Err(error) => {
                warn!(
                    cell,
                    epoch,
                    %error,
                    "close managed replica failed; discarding local snapshot"
                );
                // A failed close cannot qualify the live database as a
                // reusable baseline. An orderly eviction already published
                // its final snapshot, and every other caller requested no
                // local reuse, so force any next epoch through remote restore.
                Retained::Nothing
            }
        };
        let db = self.db_path(cell, epoch);
        // A close that could not fold the write-ahead log leaves pages there
        // that the database file lacks. A base is the file alone, so it would
        // be older than the snapshot that was published; only a resumable
        // database keeps the log with it.
        let retained = if retained == Retained::Base && self.wal_holds_pages(&db) {
            warn!(
                cell,
                epoch, "the write-ahead log still holds pages; discarding local snapshot"
            );
            Retained::Nothing
        } else {
            retained
        };
        let renamed = retained != Retained::Nothing && self.preserve_as_evicted(cell, epoch, &db);
        // A resumable database keeps what the next open of this epoch reads
        // beside it: the LTX metadata that carries its writer lineage, and the
        // WAL files that belong to the name it returns to. Every other base is
        // an image for the next epoch, which starts its own lineage and
        // would otherwise inherit a stale one (#158).
        if !(renamed && retained == Retained::Resumable) {
            let _ = remove_lineage(self.ltx_host.filesystem().as_ref(), &db);
        }
        if retained == Retained::Nothing {
            let _ = self.ltx_host.remove_file(&db);
        }
    }
}
