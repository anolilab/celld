// Copyright 2026 Deno Land Inc. Apache-2.0 license.

//! The replication streams of Durable Object facets, shared by both engines.
//!
//! A facet is a database and a replication stream of its own, as a sibling
//! file is in workerd's own server. Its stream nests under the root's
//! coordinates (`engine_api::facet_cell`), is activated on first use with the
//! restore its root's activation used, and stops with the root. A facet has
//! no ownership record and no fence of its own; the root's cover it, since a
//! facet runs only inside its root.

use anyhow::anyhow;
use anyhow::Context as _;
use std::collections::BTreeSet;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::Mutex;

use crate::ltx_replication::Replication;

/// The backoff of a facet stop that failed after its root stopped.
const FACET_STOP_RETRY_FIRST: std::time::Duration = std::time::Duration::from_millis(50);
const FACET_STOP_RETRY_MAX: std::time::Duration = std::time::Duration::from_secs(5);

/// A resident root object's facets.
struct FacetRoot {
    epoch: u64,
    spec: Option<celld_logic::RestoreSpec>,
    streams: BTreeSet<String>,
    /// The root began to stop. Its facets neither open nor delete until the
    /// stop fails before any facet stopped (`resume`): a facet stopped at
    /// this epoch would otherwise restart its lineage inside an epoch its
    /// eviction sealed, and an open that finished after the stop's snapshot
    /// would outlive the root.
    stopping: bool,
    /// Serializes the root's first opens: two activations of one stream
    /// would unlink the file the first one's connection writes.
    opening: Arc<tokio::sync::Mutex<()>>,
    /// Serializes the root's deletes and clones, which replace a facet's
    /// files. Two of them on one facet would each delete what the other
    /// copied, and a stop waits for the one in flight before it snapshots.
    replacing: Arc<tokio::sync::Mutex<()>>,
}

/// The facet streams of every resident root on this node.
#[derive(Clone, Default)]
pub(crate) struct FacetStreams(Arc<Mutex<HashMap<String, FacetRoot>>>);

impl FacetStreams {
    /// A root's activation: its facets activate later with `spec`.
    pub(crate) fn register(&self, root: &str, spec: &celld_logic::RestoreSpec) {
        self.0.lock().expect("facet streams poisoned").insert(
            root.to_string(),
            FacetRoot {
                epoch: spec.epoch,
                spec: Some(spec.clone()),
                streams: BTreeSet::new(),
                stopping: false,
                opening: Arc::default(),
                replacing: Arc::default(),
            },
        );
    }

    /// Activate a facet's stream, once per root activation, and answer its
    /// database file.
    /// `db_path` is the engine's own placement of a cell's database.
    pub(crate) async fn open(
        &self,
        replication: Option<&Replication>,
        db_path: impl Fn(&str, u64) -> PathBuf,
        root: &str,
        epoch: u64,
        names: &[String],
    ) -> anyhow::Result<PathBuf> {
        self.open_inner(replication, db_path, root, epoch, names, false)
            .await
    }

    /// `replacing` is a clone that holds the root's replace lock: it opens
    /// its own facets although the root began to stop, since the stop waits
    /// for the clone (`settled`) and lists the streams after it.
    async fn open_inner(
        &self,
        replication: Option<&Replication>,
        db_path: impl Fn(&str, u64) -> PathBuf,
        root: &str,
        epoch: u64,
        names: &[String],
        replacing: bool,
    ) -> anyhow::Result<PathBuf> {
        let facet = crate::engine_api::facet_cell(root, names);
        let path = db_path(&facet, epoch);
        let resident = |roots: &HashMap<String, FacetRoot>| {
            roots
                .get(root)
                .filter(|entry| entry.epoch == epoch && (replacing || !entry.stopping))
                .map(|entry| {
                    (
                        entry.streams.contains(&facet),
                        entry.spec.clone(),
                        entry.opening.clone(),
                    )
                })
                .ok_or_else(|| anyhow!("{root} epoch {epoch} is not resident"))
        };
        let opening = {
            let roots = self.0.lock().expect("facet streams poisoned");
            let (open, _, opening) = resident(&roots)?;
            if open {
                return Ok(path);
            }
            opening
        };
        let _opening = opening.lock().await;
        let spec = {
            let roots = self.0.lock().expect("facet streams poisoned");
            let (open, spec, _) = resident(&roots)?;
            if open {
                return Ok(path);
            }
            spec
        };
        match (replication, spec) {
            (Some(replication), Some(mut spec)) => {
                // A clean reload resumes the files it closed. A facet that
                // was not open then has none, so it restores as any facet
                // of this epoch does.
                if spec.resume.is_some() && crate::asyncrt::fs().metadata(&path).is_err() {
                    spec.resume = None;
                }
                let (restored_path, _, _) = replication.restore(&facet, &spec, false).await?;
                anyhow::ensure!(
                    restored_path == path,
                    "replication restored {} instead of {}",
                    restored_path.display(),
                    path.display()
                );
            }
            (Some(_), None) => anyhow::bail!("{root} was activated without a restore"),
            (None, _) => {
                let parent = path.parent().context("facet database has no parent")?;
                crate::asyncrt::fs()
                    .create_dir_all(parent)
                    .with_context(|| format!("create facet directory {}", parent.display()))?;
            }
        }
        let mut roots = self.0.lock().expect("facet streams poisoned");
        match roots
            .get_mut(root)
            .filter(|entry| entry.epoch == epoch && (replacing || !entry.stopping))
        {
            Some(entry) => {
                entry.streams.insert(facet);
            }
            // The root stopped while the stream activated.
            None => {
                drop(roots);
                if let Some(replication) = replication {
                    replication.ltx().discard(&facet, epoch);
                }
                anyhow::bail!("{root} epoch {epoch} stopped while its facet opened");
            }
        }
        Ok(path)
    }

    /// Delete a facet's stream and every stream below it, resident or not,
    /// locally and in the bucket. `local` is the facet's local directory.
    /// Only the root's resident owner deletes: a delete that runs after the
    /// root moved would remove what the new owner writes.
    pub(crate) async fn delete(
        &self,
        replication: Option<&Replication>,
        local: impl Fn(&str) -> PathBuf,
        root: &str,
        epoch: u64,
        names: &[String],
    ) -> anyhow::Result<()> {
        let replacing = self.replacing(root)?;
        let _replacing = replacing.lock().await;
        self.delete_locked(replication, local, root, epoch, names, false)
            .await
    }

    /// Wait for the delete or clone in flight on a root, which a root that
    /// began to stop (`stopping`) answers last: its copy writes a facet's
    /// database, which the stop snapshots and evicts.
    pub(crate) async fn settled(&self, root: &str) {
        if let Ok(replacing) = self.replacing(root) {
            drop(replacing.lock().await);
        }
    }

    /// The root's replace lock, which every delete and clone holds.
    fn replacing(&self, root: &str) -> anyhow::Result<Arc<tokio::sync::Mutex<()>>> {
        self.0
            .lock()
            .expect("facet streams poisoned")
            .get(root)
            .map(|entry| entry.replacing.clone())
            .ok_or_else(|| anyhow!("{root} is not resident"))
    }

    async fn delete_locked(
        &self,
        replication: Option<&Replication>,
        local: impl Fn(&str) -> PathBuf,
        root: &str,
        epoch: u64,
        names: &[String],
        replacing: bool,
    ) -> anyhow::Result<()> {
        let facet = crate::engine_api::facet_cell(root, names);
        let below = format!("{facet}/");
        let doomed: Vec<String> = {
            let mut roots = self.0.lock().expect("facet streams poisoned");
            let entry = roots
                .get_mut(root)
                .filter(|entry| entry.epoch == epoch && (replacing || !entry.stopping))
                .ok_or_else(|| anyhow!("{root} epoch {epoch} is not resident"))?;
            let doomed: Vec<String> = entry
                .streams
                .iter()
                .filter(|stream| **stream == facet || stream.starts_with(&below))
                .cloned()
                .collect();
            for stream in &doomed {
                entry.streams.remove(stream);
            }
            doomed
        };
        match replication {
            Some(replication) => {
                for stream in &doomed {
                    replication.ltx().delete_local(stream, epoch);
                }
                replication.ltx().delete_streams(&facet).await
            }
            None => {
                let local = local(&facet);
                match crate::asyncrt::fs().remove_dir_all(&local) {
                    Err(error) if error.kind() != std::io::ErrorKind::NotFound => {
                        Err(error).with_context(|| format!("remove facet {}", local.display()))
                    }
                    _ => Ok(()),
                }
            }
        }
    }

    /// Replace the facet at `target` with a copy of the one at `source`.
    ///
    /// The target's streams are deleted, the target activates empty, and
    /// SQLite's backup API copies the source's committed pages into it in one
    /// write transaction of the target's own database. The replicator ships
    /// that transaction as any other write, so the copy is a write of the
    /// target's stream that `prove` proves before the clone ends. The copy
    /// holds whatever the source had committed, durable or not, so the source
    /// is proved too, and a crash cannot leave the copy with a write that the
    /// source lost. The clone holds the replace lock through both proofs, so
    /// a stop of the root waits for the whole clone. The source keeps
    /// running: it is read through a second connection, and a transaction it
    /// has not committed is not copied. Facets below either facet are not
    /// copied; the target's own are deleted with it.
    ///
    /// Every failure before the target is deleted is a `CloneRefused`: a
    /// source above `CLONE_MAX_BYTES`, a root that is stopping, a source that
    /// cannot be opened or read. The target is untouched then.
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn clone_facet<P, PF>(
        &self,
        replication: Option<&Replication>,
        db_path: impl Fn(&str, u64) -> PathBuf,
        local: impl Fn(&str) -> PathBuf,
        prove: P,
        root: &str,
        epoch: u64,
        source: &[String],
        target: &[String],
    ) -> anyhow::Result<()>
    where
        P: Fn(String, u64) -> PF,
        PF: std::future::Future<Output = anyhow::Result<()>>,
    {
        let replacing = self.replacing(root).map_err(refuse)?;
        let _replacing = replacing.lock().await;
        // A stop that began before the lock was taken may be past waiting for
        // it. One that begins later waits for this clone, so the clone may
        // open its own facets from here on.
        self.ensure_running(root, epoch).map_err(refuse)?;
        let from = self
            .open_inner(replication, &db_path, root, epoch, source, true)
            .await
            .map_err(refuse)?;
        // A source that was never opened has no file, and its copy is empty.
        let present = match crate::asyncrt::fs().metadata(&from) {
            Ok(_) => true,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => false,
            Err(error) => {
                return Err(refuse(
                    anyhow::Error::new(error).context(format!("inspect {}", from.display())),
                ));
            }
        };
        if present {
            crate::asyncrt::blocking({
                let from = from.clone();
                move || ensure_clonable(&from)
            })
            .await
            .map_err(|error| refuse(anyhow!("facet size check panicked: {error}")))?
            .map_err(refuse)?;
        }
        self.delete_locked(replication, local, root, epoch, target, true)
            .await?;
        let to = self
            .open_inner(replication, &db_path, root, epoch, target, true)
            .await?;
        if present {
            copy_database(from, to).await?;
        }
        for stream in [
            crate::engine_api::facet_cell(root, source),
            crate::engine_api::facet_cell(root, target),
        ] {
            prove(stream, epoch).await?;
        }
        Ok(())
    }

    /// The root's entry is at `epoch` and has not begun to stop.
    fn ensure_running(&self, root: &str, epoch: u64) -> anyhow::Result<()> {
        self.0
            .lock()
            .expect("facet streams poisoned")
            .get(root)
            .filter(|entry| entry.epoch == epoch && !entry.stopping)
            .map(|_| ())
            .ok_or_else(|| anyhow!("{root} epoch {epoch} is not resident"))
    }

    /// The root begins to stop: see `FacetRoot::stopping`. Answers the
    /// streams the stop must stop, after which no open can add one.
    pub(crate) fn stopping(&self, root: &str, epoch: u64) -> Vec<String> {
        let mut roots = self.0.lock().expect("facet streams poisoned");
        match roots.get_mut(root).filter(|entry| entry.epoch == epoch) {
            Some(entry) => {
                entry.stopping = true;
                entry.streams.iter().cloned().collect()
            }
            None => Vec::new(),
        }
    }

    /// Stop a root and its facets. The root's stream stops first, and only
    /// while it is resident: a retry after the root stopped goes on to the
    /// facets that remain. A root stop that fails or is abandoned leaves
    /// every facet resident and resumes them, so a root that restarts in
    /// place keeps its facets. The facets stop after the root, each
    /// forgotten once it stopped. Nothing moves the root's ownership before
    /// this returns, so every facet still stops before a new owner starts.
    ///
    /// Once the root stopped, a facet stop that fails is retried here until
    /// it succeeds instead of returning: the facet's handoff snapshot is what
    /// carries its acknowledged tail into the bucket, and a stop that failed
    /// would let the caller restart a root whose stream is gone. The actor's
    /// release loop has no overall timeout for the same reason.
    pub(crate) async fn stop_root<R, F, FF>(
        &self,
        root: &str,
        epoch: u64,
        root_resident: bool,
        stop_root: impl FnOnce(Vec<String>) -> R,
        mut stop_facet: F,
    ) -> anyhow::Result<()>
    where
        R: std::future::Future<Output = anyhow::Result<()>>,
        F: FnMut(String) -> FF,
        FF: std::future::Future<Output = anyhow::Result<()>>,
    {
        self.stopping(root, epoch);
        // None starts after `stopping`, since each opens its facets.
        self.settled(root).await;
        if root_resident {
            // The streams after the clone settled: the root's retention
            // depends on every facet that opened, a clone's target included.
            let open = self.stopping(root, epoch);
            if let Err(error) = stop_root(open).await {
                self.resume(root, epoch);
                return Err(error);
            }
        }
        for facet in self.stopping(root, epoch) {
            let mut delay = FACET_STOP_RETRY_FIRST;
            while let Err(error) = stop_facet(facet.clone()).await {
                tracing::warn!(
                    root,
                    epoch,
                    facet,
                    %error,
                    "a facet stop failed after its root stopped; retrying"
                );
                crate::asyncrt::sleep(delay).await;
                delay = (delay * 2).min(FACET_STOP_RETRY_MAX);
            }
            self.stopped(root, epoch, &facet);
        }
        self.forget(root, epoch);
        Ok(())
    }

    /// The root's stop failed before any facet stopped, and the root
    /// restarts in place: its facets open and delete again.
    pub(crate) fn resume(&self, root: &str, epoch: u64) {
        if let Some(entry) = self
            .0
            .lock()
            .expect("facet streams poisoned")
            .get_mut(root)
            .filter(|entry| entry.epoch == epoch)
        {
            entry.stopping = false;
        }
    }

    /// The facet streams of a resident root.
    pub(crate) fn resident(&self, root: &str, epoch: u64) -> Vec<String> {
        self.0
            .lock()
            .expect("facet streams poisoned")
            .get(root)
            .filter(|entry| entry.epoch == epoch)
            .map(|entry| entry.streams.iter().cloned().collect())
            .unwrap_or_default()
    }

    /// A facet stream that stopped. The root's entry stays until the root
    /// itself stops, so a stop that fails and restarts the root in place
    /// can still open its facets.
    pub(crate) fn stopped(&self, root: &str, epoch: u64, stream: &str) {
        if let Some(entry) = self
            .0
            .lock()
            .expect("facet streams poisoned")
            .get_mut(root)
            .filter(|entry| entry.epoch == epoch)
        {
            entry.streams.remove(stream);
        }
    }

    /// The root stopped: its entry goes.
    pub(crate) fn forget(&self, root: &str, epoch: u64) {
        let mut roots = self.0.lock().expect("facet streams poisoned");
        if roots.get(root).is_some_and(|entry| entry.epoch == epoch) {
            roots.remove(root);
        }
    }
}

/// The largest source database a clone copies. The copy is one write
/// transaction, so the replicator ships it as one segment, and the caller
/// waits for the proof that the segment reached the bucket, which has a
/// deadline of 10 s by default (`CELLD_LTX_DURABILITY_TIMEOUT_SECS`). A debug
/// build proved 28 MiB in 9 s, so 16 MiB keeps a margin of two for a slow
/// store. A segment is also capped at 64 MiB after base64 encoding
/// (`node_log::STREAM_FRAME_CAP`), which this stays far below.
pub(crate) const CLONE_MAX_BYTES: u64 = 16 * 1024 * 1024;

/// How many times a busy backup step is retried, 5 ms apart, before the
/// clone fails: about ten seconds.
const CLONE_BUSY_ATTEMPTS: u32 = 2_000;

/// A source database that is too large to clone, as opposed to a clone that
/// failed: the target is untouched.
#[derive(Debug)]
pub(crate) struct CloneRefused(pub String);

impl std::fmt::Display for CloneRefused {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for CloneRefused {}

/// An error raised before a clone touched its target is a refusal, whatever
/// its cause: the target's data is intact and its name stays usable.
pub(crate) fn refuse(error: anyhow::Error) -> anyhow::Error {
    match error.downcast_ref::<CloneRefused>() {
        Some(_) => error,
        None => CloneRefused(format!("{error:#}")).into(),
    }
}

pub(crate) fn ensure_clonable(path: &std::path::Path) -> anyhow::Result<()> {
    let source =
        rusqlite::Connection::open_with_flags(path, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY)
            .with_context(|| format!("open facet database {}", path.display()))?;
    ensure_clonable_size(&source)
}

fn ensure_clonable_size(source: &rusqlite::Connection) -> anyhow::Result<()> {
    let pages: u64 = source.query_row("PRAGMA page_count", [], |row| row.get(0))?;
    let page_size: u64 = source.query_row("PRAGMA page_size", [], |row| row.get(0))?;
    let bytes = pages * page_size;
    if bytes > CLONE_MAX_BYTES {
        return Err(CloneRefused(format!(
            "the facet database is {bytes} bytes, and a clone copies at most {CLONE_MAX_BYTES}"
        ))
        .into());
    }
    Ok(())
}

/// Copy the committed pages of the database at `from` over the one at `to`,
/// in one write transaction of `to`. A source with a writer of its own is
/// read as of one moment: the copy holds a read transaction on the source
/// for the time it takes to read at most `CLONE_MAX_BYTES`, and that
/// snapshot is the one whose size is checked against the cap. A source writer
/// does not wait for it (it may wait up to its busy timeout for a checkpoint),
/// and the replicator's checkpoint waits for it only for that time.
///
/// Both connections are plain: a facet's database is restored with
/// `allow_paged = false` (see `FacetStreams::open`), so no fault-in VFS is
/// needed to read the source.
pub(crate) async fn copy_database(from: PathBuf, to: PathBuf) -> anyhow::Result<()> {
    copy_database_within(from, to, CLONE_BUSY_ATTEMPTS).await
}

async fn copy_database_within(from: PathBuf, to: PathBuf, attempts: u32) -> anyhow::Result<()> {
    crate::asyncrt::blocking(move || copy_snapshot(&from, &to, attempts, || {}))
        .await
        .map_err(|error| anyhow!("facet copy panicked: {error}"))?
}

/// `after_size` runs once the snapshot's size is known, before the backup.
fn copy_snapshot(
    from: &std::path::Path,
    to: &std::path::Path,
    attempts: u32,
    after_size: impl FnOnce(),
) -> anyhow::Result<()> {
    let source =
        rusqlite::Connection::open_with_flags(from, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY)
            .with_context(|| format!("open facet database {}", from.display()))?;
    // Read the size inside a read transaction that the backup then uses, so
    // the size that is checked is the size that is copied: a source that
    // grew since `ensure_clonable` is a failed clone, not an oversized
    // target. The target is gone by now, so it is not a refused one.
    source.execute_batch("BEGIN")?;
    ensure_clonable_size(&source).map_err(|error| anyhow!("{error:#}"))?;
    after_size();
    let mut destination = rusqlite::Connection::open(to)
        .with_context(|| format!("open facet database {}", to.display()))?;
    // A busy step returns at once and is retried below, so the attempts
    // bound the wait. The default five second busy timeout would make
    // each attempt wait that long.
    source.busy_timeout(std::time::Duration::ZERO)?;
    destination.busy_timeout(std::time::Duration::ZERO)?;
    let backup = rusqlite::backup::Backup::new(&source, &mut destination)?;
    for _ in 0..attempts {
        match backup.step(-1)? {
            rusqlite::backup::StepResult::Done => return Ok(()),
            // A lock held for a moment by a writer.
            _ => std::thread::sleep(std::time::Duration::from_millis(5)),
        }
    }
    anyhow::bail!("the facet copy stayed busy")
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures_util::FutureExt as _;
    use rusqlite::Connection;

    fn database(path: &std::path::Path, rows: u32) -> Connection {
        let connection = Connection::open(path).unwrap();
        connection
            .pragma_update(None, "journal_mode", "WAL")
            .unwrap();
        connection
            .execute_batch("CREATE TABLE t (v INTEGER);")
            .unwrap();
        for row in 0..rows {
            connection
                .execute("INSERT INTO t VALUES (?)", [row])
                .unwrap();
        }
        connection
    }

    fn rows(path: &std::path::Path) -> i64 {
        Connection::open(path)
            .unwrap()
            .query_row("SELECT count(*) FROM t", [], |row| row.get(0))
            .unwrap()
    }

    #[tokio::test]
    async fn a_copy_holds_what_the_source_committed_and_nothing_else() {
        crate::asyncrt::install_test_host_runtime();
        let directory = tempfile::tempdir().unwrap();
        let (from, to) = (directory.path().join("from"), directory.path().join("to"));
        let source = database(&from, 3);
        // A write transaction that has not committed is not copied, and does
        // not stop the copy.
        source
            .execute_batch("BEGIN; INSERT INTO t VALUES (99);")
            .unwrap();
        database(&to, 7);
        copy_database(from.clone(), to.clone()).await.unwrap();
        assert_eq!(rows(&to), 3);
        // The source commits after the copy: the copy does not see it.
        source.execute_batch("COMMIT;").unwrap();
        assert_eq!(rows(&from), 4);
        assert_eq!(rows(&to), 3);
    }

    #[tokio::test]
    async fn a_source_above_the_budget_is_refused_and_the_target_is_kept() {
        crate::asyncrt::install_test_host_runtime();
        let directory = tempfile::tempdir().unwrap();
        let (from, to) = (directory.path().join("from"), directory.path().join("to"));
        let source = database(&from, 0);
        source.execute_batch("CREATE TABLE big (b BLOB);").unwrap();
        for _ in 0..=CLONE_MAX_BYTES / (1024 * 1024) {
            source
                .execute("INSERT INTO big VALUES (zeroblob(1048576))", [])
                .unwrap();
        }
        database(&to, 7);
        let error = ensure_clonable(&from).unwrap_err();
        assert!(error.downcast_ref::<CloneRefused>().is_some(), "{error:#}");
        // Past that check the target is gone, so a source that grew since is
        // an ordinary failure of the copy, not a refusal.
        let error = copy_database(from, to.clone()).await.unwrap_err();
        assert!(error.downcast_ref::<CloneRefused>().is_none(), "{error:#}");
        assert_eq!(rows(&to), 7);
    }

    /// The cap holds for the pages that are copied: a source that grows past
    /// it between the size check and the backup is not copied.
    #[test]
    fn a_source_that_grows_during_the_copy_is_not_copied_past_the_budget() {
        let directory = tempfile::tempdir().unwrap();
        let (from, to) = (directory.path().join("from"), directory.path().join("to"));
        let source = database(&from, 0);
        source.execute_batch("CREATE TABLE big (b BLOB);").unwrap();
        source
            .execute("INSERT INTO big VALUES (zeroblob(1048576))", [])
            .unwrap();
        // The size check sees 1 MiB, and then the source grows to 20 MiB.
        let grow = || {
            for _ in 0..19 {
                source
                    .execute("INSERT INTO big VALUES (zeroblob(1048576))", [])
                    .unwrap();
            }
        };
        let result = copy_snapshot(&from, &to, 10, grow);
        let copied = Connection::open(&to).and_then(|db| {
            db.query_row("SELECT count(*) FROM big", [], |row| row.get::<_, i64>(0))
        });
        assert!(
            result.is_ok() && copied == Ok(1),
            "the copy is the snapshot that was checked: {result:?} {copied:?}"
        );
    }

    #[tokio::test]
    async fn a_copy_into_a_database_that_stays_locked_fails() {
        crate::asyncrt::install_test_host_runtime();
        let directory = tempfile::tempdir().unwrap();
        let (from, to) = (directory.path().join("from"), directory.path().join("to"));
        database(&from, 3);
        let target = database(&to, 7);
        // A writer of the target holds its write lock for the whole copy.
        target
            .execute_batch("BEGIN IMMEDIATE; INSERT INTO t VALUES (1);")
            .unwrap();
        let error = copy_database_within(from, to.clone(), 20)
            .await
            .unwrap_err();
        assert!(format!("{error:#}").contains("busy"), "{error:#}");
        target.execute_batch("COMMIT;").unwrap();
        assert_eq!(rows(&to), 8);
    }

    /// A restored root that has no replicator: facets are files.
    fn streams() -> (FacetStreams, tempfile::TempDir) {
        let directory = tempfile::tempdir().unwrap();
        let streams = FacetStreams::default();
        streams.register(
            "root",
            &celld_logic::RestoreSpec {
                epoch: 1,
                fresh: true,
                took_over: false,
                resume: None,
                prior: None,
            },
        );
        (streams, directory)
    }

    #[tokio::test]
    async fn a_refused_clone_leaves_the_target_as_it_was() {
        crate::asyncrt::install_test_host_runtime();
        let (streams, directory) = streams();
        let path = |cell: &str, _: u64| directory.path().join(cell.replace('/', "_"));
        let local = |cell: &str| directory.path().join(cell.replace('/', "_"));
        let (source, target) = (vec!["a".to_string()], vec!["b".to_string()]);
        let from = path(&crate::engine_api::facet_cell("root", &source), 1);
        let to = path(&crate::engine_api::facet_cell("root", &target), 1);
        let big = database(&from, 0);
        big.execute_batch("CREATE TABLE big (b BLOB);").unwrap();
        for _ in 0..=CLONE_MAX_BYTES / (1024 * 1024) {
            big.execute("INSERT INTO big VALUES (zeroblob(1048576))", [])
                .unwrap();
        }
        database(&to, 7);
        let error = streams
            .clone_facet(None, path, local, no_proof, "root", 1, &source, &target)
            .await
            .unwrap_err();
        assert!(error.downcast_ref::<CloneRefused>().is_some(), "{error:#}");
        assert_eq!(rows(&to), 7);
    }

    /// A source that was never opened has no file: its copy is empty, and
    /// the clone does not fail for it.
    #[tokio::test]
    async fn a_clone_of_a_source_that_was_never_opened_empties_the_target() {
        crate::asyncrt::install_test_host_runtime();
        let (streams, directory) = streams();
        let path = |cell: &str, _: u64| directory.path().join(cell.replace('/', "_")).join("db");
        let local = |cell: &str| directory.path().join(cell.replace('/', "_"));
        let (source, target) = (vec!["a".to_string()], vec!["b".to_string()]);
        let to = path(&crate::engine_api::facet_cell("root", &target), 1);
        crate::asyncrt::fs()
            .create_dir_all(to.parent().unwrap())
            .unwrap();
        database(&to, 7);
        streams
            .clone_facet(None, path, local, no_proof, "root", 1, &source, &target)
            .await
            .unwrap();
        assert!(crate::asyncrt::fs().metadata(&to).is_err());
    }

    async fn no_proof(_: String, _: u64) -> anyhow::Result<()> {
        Ok(())
    }

    /// Every failure before the target is deleted is a refusal, so the target
    /// keeps its data and its name stays usable.
    #[allow(clippy::disallowed_methods)] // a plain file stands in for a corrupt database
    #[tokio::test]
    async fn a_clone_that_fails_before_it_touches_the_target_is_refused() {
        crate::asyncrt::install_test_host_runtime();
        let (streams, directory) = streams();
        let path = |cell: &str, _: u64| directory.path().join(cell.replace('/', "_"));
        let local = |cell: &str| directory.path().join(cell.replace('/', "_"));
        let (source, target) = (vec!["a".to_string()], vec!["b".to_string()]);
        let from = path(&crate::engine_api::facet_cell("root", &source), 1);
        let to = path(&crate::engine_api::facet_cell("root", &target), 1);
        database(&to, 7);

        // The source is not a database.
        std::fs::write(&from, vec![0x5a; 4096]).unwrap();
        let error = streams
            .clone_facet(None, path, local, no_proof, "root", 1, &source, &target)
            .await
            .unwrap_err();
        assert!(error.downcast_ref::<CloneRefused>().is_some(), "{error:#}");
        assert_eq!(rows(&to), 7);

        // The root began to stop before the clone took its lock.
        streams.stopping("root", 1);
        let error = streams
            .clone_facet(None, path, local, no_proof, "root", 1, &source, &target)
            .await
            .unwrap_err();
        assert!(error.downcast_ref::<CloneRefused>().is_some(), "{error:#}");
        assert_eq!(rows(&to), 7);

        // The root is not resident.
        let error = streams
            .clone_facet(None, path, local, no_proof, "other", 1, &source, &target)
            .await
            .unwrap_err();
        assert!(error.downcast_ref::<CloneRefused>().is_some(), "{error:#}");
    }

    /// A stop that begins while a clone holds the replace lock must not leave
    /// the clone's target deleted and unopened.
    #[tokio::test]
    async fn a_stop_that_begins_during_a_clone_does_not_empty_the_target() {
        crate::asyncrt::install_test_host_runtime();
        let (streams, directory) = streams();
        let (source, target) = (vec!["a".to_string()], vec!["b".to_string()]);
        let place = |cell: &str, _: u64| directory.path().join(cell.replace('/', "_")).join("db");
        let local = |cell: &str| directory.path().join(cell.replace('/', "_"));
        let from = place(&crate::engine_api::facet_cell("root", &source), 1);
        let to = place(&crate::engine_api::facet_cell("root", &target), 1);
        crate::asyncrt::fs()
            .create_dir_all(from.parent().unwrap())
            .unwrap();
        crate::asyncrt::fs()
            .create_dir_all(to.parent().unwrap())
            .unwrap();
        database(&from, 3);
        database(&to, 7);
        // The stop begins when the clone opens its source, after it took the
        // lock and before it deletes its target.
        let path = |cell: &str, epoch: u64| {
            streams.stopping("root", 1);
            place(cell, epoch)
        };
        streams
            .clone_facet(None, path, local, no_proof, "root", 1, &source, &target)
            .await
            .unwrap();
        assert_eq!(rows(&to), 3);
        // The stop lists the target's stream, which opened after it began.
        let listed = streams.stopping("root", 1);
        assert!(
            listed.contains(&crate::engine_api::facet_cell("root", &target)),
            "{listed:?}"
        );
    }

    /// A stop that begins after the copy waits for the two proofs that
    /// complete the clone.
    #[tokio::test]
    async fn a_stop_waits_for_the_proofs_of_a_clone() {
        crate::asyncrt::install_test_host_runtime();
        let (streams, directory) = streams();
        let path = |cell: &str, _: u64| directory.path().join(cell.replace('/', "_")).join("db");
        let local = |cell: &str| directory.path().join(cell.replace('/', "_"));
        let (source, target) = (vec!["a".to_string()], vec!["b".to_string()]);
        let from = path(&crate::engine_api::facet_cell("root", &source), 1);
        crate::asyncrt::fs()
            .create_dir_all(from.parent().unwrap())
            .unwrap();
        database(&from, 3);
        let proofs = Arc::new(tokio::sync::Semaphore::new(0));
        let prove = |_: String, _: u64| {
            let proofs = proofs.clone();
            async move {
                proofs.acquire().await?.forget();
                Ok(())
            }
        };
        let mut clone =
            Box::pin(streams.clone_facet(None, path, local, prove, "root", 1, &source, &target));
        // The copy is made, and the first proof is pending.
        assert!(clone.as_mut().now_or_never().is_none());
        let mut stop = Box::pin(streams.stop_root(
            "root",
            1,
            false,
            |_| async { Ok(()) },
            |_| async { Ok(()) },
        ));
        assert!(stop.as_mut().now_or_never().is_none());
        proofs.add_permits(2);
        clone.await.unwrap();
        stop.await.unwrap();
    }

    /// A stop that begins during a clone hands the root's stop the clone's
    /// target, which the root's retention decision needs.
    #[tokio::test]
    async fn a_stop_during_a_clone_sees_the_target_facet() {
        crate::asyncrt::install_test_host_runtime();
        let (streams, directory) = streams();
        let path = |cell: &str, _: u64| directory.path().join(cell.replace('/', "_")).join("db");
        let local = |cell: &str| directory.path().join(cell.replace('/', "_"));
        let (source, target) = (vec!["a".to_string()], vec!["b".to_string()]);
        let from = path(&crate::engine_api::facet_cell("root", &source), 1);
        crate::asyncrt::fs()
            .create_dir_all(from.parent().unwrap())
            .unwrap();
        database(&from, 3);
        let proofs = Arc::new(tokio::sync::Semaphore::new(0));
        let prove = |_: String, _: u64| {
            let proofs = proofs.clone();
            async move {
                proofs.acquire().await?.forget();
                Ok(())
            }
        };
        let mut clone =
            Box::pin(streams.clone_facet(None, path, local, prove, "root", 1, &source, &target));
        assert!(clone.as_mut().now_or_never().is_none());
        let seen = std::sync::Mutex::new(Vec::new());
        let mut stop = Box::pin(streams.stop_root(
            "root",
            1,
            true,
            |open| async {
                *seen.lock().unwrap() = open;
                Ok(())
            },
            |_| async { Ok(()) },
        ));
        assert!(stop.as_mut().now_or_never().is_none());
        proofs.add_permits(2);
        clone.await.unwrap();
        stop.await.unwrap();
        let target = crate::engine_api::facet_cell("root", &target);
        assert!(seen.lock().unwrap().contains(&target));
    }

    #[tokio::test]
    async fn a_delete_a_clone_and_a_stop_wait_for_a_replace_in_flight() {
        crate::asyncrt::install_test_host_runtime();
        let (streams, directory) = streams();
        let path = |cell: &str, _: u64| directory.path().join(cell.replace('/', "_"));
        let local = |cell: &str| directory.path().join(cell.replace('/', "_"));
        let names = vec!["a".to_string()];
        let replacing = streams.replacing("root").unwrap();
        let held = replacing.lock().await;
        assert!(streams
            .delete(None, local, "root", 1, &names)
            .now_or_never()
            .is_none());
        assert!(streams
            .clone_facet(
                None,
                path,
                local,
                no_proof,
                "root",
                1,
                &names,
                &["b".to_string()]
            )
            .now_or_never()
            .is_none());
        let mut stop = Box::pin(streams.stop_root(
            "root",
            1,
            false,
            |_| async { Ok(()) },
            |_| async { Ok(()) },
        ));
        assert!(stop.as_mut().now_or_never().is_none());
        drop(held);
        stop.await.unwrap();
    }
}
