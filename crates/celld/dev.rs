// Copyright 2026 Deno Land Inc. Apache-2.0 license.

//! The local application stack behind `celld dev`.
//!
//! Development still uses the standalone deployment, ownership, and LTX
//! paths. This module supplies only the infrastructure those paths require:
//! one persisted local object store and one supervised celld node.

use crate::bucket::Bucket;
use crate::{deploy, fleet};
use anyhow::{bail, Context as _};
use glob::{MatchOptions, Pattern};
use notify::{RecursiveMode, Watcher as _};
use nu_ansi_term::{Color, Style};
use sha2::{Digest as _, Sha256};
use std::collections::BTreeMap;
use std::ffi::OsString;
use std::io::IsTerminal as _;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::Duration;
use tokio::io::{AsyncBufReadExt as _, BufReader};
use tokio::process::{Child, Command};
use tokio::sync::{mpsc, oneshot};

pub const DEFAULT_PORT: u16 = 9876;
const BUCKET: &str = "celld-dev";

/// The narrow bridge used by the supervised binary process. The public fleet
/// parser never receives this path, so no regular subcommand can select it.
#[doc(hidden)]
pub fn open_local_bucket(database: &Path) -> anyhow::Result<Bucket> {
    Bucket::open_dev(database)
}

/// Where the local node of `celld dev` keeps the operator token it generates,
/// for a node that was given `state_dir` as its `CELLD_DEV_STATE_DIR` value
/// (`None` keeps it in the project). The project is resolved the way `run`
/// resolves it, so a relative path, `..` or a symbolic link finds the same
/// directory.
#[doc(hidden)]
#[allow(clippy::disallowed_methods)] // Project resolution uses the operator's host filesystem.
pub fn operator_token_path_with(
    project: &Path,
    state_dir: Option<OsString>,
) -> anyhow::Result<PathBuf> {
    let project = std::fs::canonicalize(project)
        .with_context(|| format!("resolve project directory {}", project.display()))?;
    Ok(DevState::resolve(&project, None, state_dir)?
        .path()
        .join(crate::operator_token::FILE_NAME))
}

/// The dev store and the operator token file of the node that `celld dev`
/// runs for `project` (a canonical directory), for a command that reaches that
/// node: the same state directory `run` resolves from `state_dir` and
/// `CELLD_DEV_STATE_DIR`.
#[allow(clippy::disallowed_methods)] // Local development state is an operator-owned host path.
pub(crate) fn local_node_files(
    project: &Path,
    state_dir: Option<PathBuf>,
) -> anyhow::Result<(PathBuf, PathBuf)> {
    let state = DevState::resolve(project, state_dir, std::env::var_os("CELLD_DEV_STATE_DIR"))?;
    // The same check `celld dev` makes: a state directory reached through a
    // link, or owned by another user, is refused before anything in it is read.
    state.ensure_not_redirected("use")?;
    let database = state.path().join("objects.sqlite3");
    anyhow::ensure!(
        database.exists(),
        "no local state at {}; start `celld dev` for this project first",
        state.path().display()
    );
    Ok((
        database,
        state.path().join(crate::operator_token::FILE_NAME),
    ))
}

#[derive(Debug)]
struct Options {
    project: Option<PathBuf>,
    state_dir: Option<PathBuf>,
    clean: bool,
    stack: StackOptions,
}

/// The local state directory of one project.
///
/// `--clean` deletes this directory and everything below it. The path is
/// private and always ends in `.celld/dev`: `resolve` builds it from the
/// project, or from a state directory plus the hash of the project, and
/// `discard` refuses any path without that suffix, so a command-line value
/// can only choose where the directory lives and never what is deleted.
#[derive(Debug)]
struct DevState {
    path: PathBuf,
    /// `<state directory>/<project hash>`, when a state directory is in use.
    /// The directory is shared, so another user may have write access to it
    /// and `discard` must not follow a link planted at this level.
    shared_root: Option<PathBuf>,
}

impl DevState {
    /// The location of the local state below its parent, and also the suffix
    /// that `discard` requires before it deletes.
    const DIRECTORY: &'static str = ".celld/dev";

    /// Choose the local state of `project`: the `--state-dir` flag wins, then
    /// the `CELLD_DEV_STATE_DIR` value, then the project itself. An empty
    /// value counts as absent. A state directory is shared by projects, so
    /// each one gets its own `<hash>/.celld/dev` below it, and `--clean` in
    /// one project leaves the others alone.
    fn resolve(
        project: &Path,
        flag: Option<PathBuf>,
        env: Option<OsString>,
    ) -> anyhow::Result<Self> {
        let directory = flag
            .filter(|directory| !directory.as_os_str().is_empty())
            .or_else(|| {
                env.map(PathBuf::from)
                    .filter(|directory| !directory.as_os_str().is_empty())
            });
        let shared_root = match directory {
            Some(directory) => Some(
                std::path::absolute(&directory)
                    .with_context(|| format!("resolve state directory {}", directory.display()))?
                    .join(project_hash(project)),
            ),
            None => None,
        };
        let parent = shared_root.as_deref().unwrap_or(project);
        Ok(Self {
            path: parent.join(Self::DIRECTORY),
            shared_root,
        })
    }

    /// Create `<state directory>/<project hash>` private to the user, so
    /// nobody else can swap a link into it. Without a state directory, only
    /// check the project-local state.
    fn prepare_shared_root(&self) -> anyhow::Result<()> {
        match &self.shared_root {
            Some(root) => {
                self.tighten();
                // `create_private` follows links and only restricts a path it
                // creates, so a `DIR/<hash>` that another user planted must be
                // refused before it is used.
                self.ensure_not_redirected("use")?;
                crate::config_dir::create_private(root)?;
                // In a directory others can write to, a link or directory can
                // be planted between the first check and the create, which
                // accepts an existing path. Checking again once `DIR/<hash>`
                // exists closes that window: from then on it is ours, private,
                // and nobody else can create the levels below it.
                self.ensure_not_redirected("use")
            }
            None => {
                self.tighten();
                self.ensure_not_redirected("use")
            }
        }
    }

    /// The directories that make up this state, outermost first.
    fn levels(&self) -> Vec<PathBuf> {
        match &self.shared_root {
            Some(root) => vec![
                root.clone(),
                root.join(".celld"),
                root.join(Self::DIRECTORY),
            ],
            None => self
                .path
                .parent()
                .map(Path::to_path_buf)
                .into_iter()
                .chain([self.path.clone()])
                .collect(),
        }
    }

    /// Set mode 0700 on every level that the user owns and others can write
    /// to, so `celld dev` repairs a directory it created or someone loosened
    /// instead of refusing it. A filesystem that keeps no modes ignores this,
    /// and `ensure_not_redirected` then refuses with that hint. Only `celld
    /// dev` tightens: a command that sends the token never does.
    #[cfg(unix)]
    #[allow(clippy::disallowed_methods)] // Local development state is an operator-owned host path.
    fn tighten(&self) {
        use std::os::unix::fs::{MetadataExt as _, PermissionsExt as _};
        // A shared state directory is only checked, never tightened: another
        // user who can write next to it could swap a level for a link between
        // the check and the chmod. celld creates its levels private, so a
        // loose one there is refused. A project-local level is tightened only
        // when it is a real directory, never through a link.
        if self.shared_root.is_some() {
            return;
        }
        for level in self.levels() {
            // A level that is a link may point at a directory shared with
            // others; it is checked, never chmod-ed, and refused if loose.
            if std::fs::symlink_metadata(&level).is_ok_and(|m| m.file_type().is_symlink()) {
                continue;
            }
            let Ok(resolved) = std::fs::metadata(&level) else {
                continue;
            };
            // SAFETY: `geteuid` takes no arguments and cannot fail.
            if resolved.uid() == unsafe { libc::geteuid() }
                && resolved.permissions().mode() & 0o022 != 0
            {
                let _ = std::fs::set_permissions(&level, std::fs::Permissions::from_mode(0o700));
            }
        }
    }

    #[cfg(not(unix))]
    fn tighten(&self) {}

    /// Refuse a state directory that is reached through a link, that another
    /// user owns, or that group or other users can write to: `DIR/<hash>`,
    /// `.celld` and `.celld/dev`, or the last two for a project-local state.
    /// Missing levels are fine. Whoever can write there can replace the store
    /// and the token file, which is what a command that sends the token to the
    /// node of the store has to rule out. `celld dev` creates every level
    /// private to the user, so this refuses only a directory someone loosened.
    #[allow(clippy::disallowed_methods)] // Local development state is an operator-owned host path.
    fn ensure_not_redirected(&self, action: &str) -> anyhow::Result<()> {
        for level in self.levels() {
            let metadata = match std::fs::symlink_metadata(&level) {
                Ok(metadata) => metadata,
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
                Err(error) => {
                    return Err(error).with_context(|| format!("inspect {}", level.display()));
                }
            };
            if self.shared_root.is_some() {
                anyhow::ensure!(
                    !metadata.file_type().is_symlink(),
                    "refusing to {action} {}: {} is a symbolic link",
                    self.path.display(),
                    level.display()
                );
            }
            #[cfg(unix)]
            {
                use std::os::unix::fs::{MetadataExt as _, PermissionsExt as _};
                // The owner of what the level points to, so a project that
                // links `.celld` to a directory it owns is fine and one that
                // links it to another user's directory is refused.
                let resolved = std::fs::metadata(&level)
                    .with_context(|| format!("inspect {}", level.display()))?;
                // SAFETY: `geteuid` takes no arguments and cannot fail.
                anyhow::ensure!(
                    resolved.uid() == unsafe { libc::geteuid() },
                    "refusing to {action} {}: {} is owned by another user",
                    self.path.display(),
                    level.display()
                );
                // The mode of what the level points to, so a project that
                // links `.celld` elsewhere is judged by that directory.
                let mode = resolved.permissions().mode();
                anyhow::ensure!(
                    mode & 0o022 == 0,
                    "refusing to {action} {}: {} can be written to by other users; \
                     run `chmod go-w {}`. If the filesystem does not keep permissions \
                     (a Windows drive under WSL, vfat, CIFS), use --state-dir on one that does",
                    self.path.display(),
                    level.display(),
                    level.display()
                );
            }
        }
        Ok(())
    }

    fn path(&self) -> &Path {
        &self.path
    }

    /// Delete the local state so the next run starts from nothing. The answer
    /// reports whether a directory was present, because the console line for
    /// an empty project must not claim that data was removed.
    ///
    /// An absent directory is the wanted result of this command, so it is not
    /// an error.
    #[allow(clippy::disallowed_methods)] // Local development state is an operator-owned host path.
    fn discard(&self) -> anyhow::Result<bool> {
        // The constructor already guarantees this. The check stays because it
        // is the tripwire for a future constructor that takes a path: a
        // recursive delete must never widen by accident.
        anyhow::ensure!(
            self.path.ends_with(Self::DIRECTORY),
            "refusing to discard {}, which is not a celld dev state directory",
            self.path.display()
        );
        self.tighten();
        self.ensure_not_redirected("discard")?;
        match std::fs::remove_dir_all(&self.path) {
            Ok(()) => Ok(true),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(false),
            Err(error) => Err(error)
                .with_context(|| format!("discard local state directory {}", self.path.display())),
        }
    }
}

fn project_hash(project: &Path) -> String {
    crate::control_plane::hex(&Sha256::digest(project.as_os_str().as_encoded_bytes()))
}

#[derive(Debug)]
struct StackOptions {
    listener: SocketAddr,
    logs: bool,
    strict_compat_flags: bool,
    watch: bool,
    watch_ignores: Vec<Pattern>,
}

struct Store {
    database: PathBuf,
}

#[derive(Clone, Copy)]
struct Console {
    color: bool,
}

impl Console {
    fn new() -> Self {
        let force = std::env::var_os("FORCE_COLOR").is_some_and(|value| value != "0");
        let color =
            std::env::var_os("NO_COLOR").is_none() && (force || std::io::stdout().is_terminal());
        Self { color }
    }

    fn paint(&self, style: Style, value: &str) -> String {
        if self.color {
            style.paint(value).to_string()
        } else {
            value.to_string()
        }
    }

    fn header(&self) {
        crate::note!("{}", self.paint(Color::Cyan.bold(), "celld dev"));
    }

    fn detail(&self, label: &str, value: &str) {
        crate::note!(
            "  {}  {value}",
            self.paint(Style::new().dimmed(), &format!("{label:<7}"))
        );
    }

    fn progress(&self, message: &str) {
        crate::note!("  {} {message}", self.paint(Color::Yellow.normal(), "●"));
    }

    fn ready(&self, origin: &str) {
        crate::note!(
            "  {}  {}",
            self.paint(Color::Green.bold(), "ready"),
            self.paint(Color::Cyan.bold(), origin)
        );
    }

    fn log(&self, line: &str) {
        let style = if line.contains(" ERROR ") {
            Color::Red.normal()
        } else if line.contains(" WARN ") {
            Color::Yellow.normal()
        } else {
            Style::new().dimmed()
        };
        crate::note!("{}", self.paint(style, line));
    }

    fn failure(&self, message: &str) {
        crate::note!("  {} {message}", self.paint(Color::Red.bold(), "error"));
    }
}

struct ProjectWatcher {
    project: PathBuf,
    ignored: Vec<Pattern>,
    _watcher: notify::RecommendedWatcher,
    changes: mpsc::UnboundedReceiver<notify::Result<notify::Event>>,
}

impl ProjectWatcher {
    fn new(project: &Path, ignored: Vec<Pattern>) -> anyhow::Result<Self> {
        let (sender, changes) = mpsc::unbounded_channel();
        let mut watcher = notify::recommended_watcher(move |event| {
            let _ = sender.send(event);
        })
        .context("create the project watcher")?;
        watcher
            .watch(project, RecursiveMode::Recursive)
            .with_context(|| format!("watch the project directory {}", project.display()))?;
        Ok(Self {
            project: project.to_path_buf(),
            ignored,
            _watcher: watcher,
            changes,
        })
    }

    #[allow(clippy::disallowed_methods)] // Host-side debounce time is outside the Actor domain.
    async fn changed(&mut self) -> anyhow::Result<()> {
        self.next_relevant_change().await?;
        loop {
            // An editor can replace one save through several writes and a
            // rename. Wait for one quiet interval so esbuild never reads the
            // temporary middle of that transaction.
            match tokio::time::timeout(Duration::from_millis(150), self.next_relevant_change())
                .await
            {
                Ok(result) => result?,
                Err(_) => return Ok(()),
            }
        }
    }

    async fn next_relevant_change(&mut self) -> anyhow::Result<()> {
        loop {
            match self
                .changes
                .recv()
                .await
                .context("the project watcher stopped")?
            {
                Ok(event) if relevant_project_event(&self.project, &event, &self.ignored) => {
                    return Ok(())
                }
                Ok(_) => {}
                Err(error) => return Err(error).context("watch the project directory"),
            }
        }
    }
}

fn relevant_project_event(project: &Path, event: &notify::Event, ignored: &[Pattern]) -> bool {
    // A read is not a change. The inotify backend reports `Access(Open(Any))`
    // for every file that the bundler and the deploy step read, so a rebuild
    // produces the events that request the next rebuild and the supervisor
    // never settles. The path filters cannot close this loop, because the
    // files that the rebuild reads are the project sources it must watch. A
    // write still arrives as a `Modify` event, and a `Create` or a `Remove`
    // event keeps its own kind, so no real change becomes invisible.
    if matches!(event.kind, notify::EventKind::Access(_)) {
        return false;
    }
    event
        .paths
        .iter()
        .any(|path| !ignored_project_path(project, path, ignored))
}

fn ignored_project_path(project: &Path, path: &Path, ignored: &[Pattern]) -> bool {
    const IGNORED_DIRECTORIES: &[&str] = &[
        ".cache",
        ".celld",
        ".git",
        ".hg",
        ".next",
        ".nuxt",
        ".parcel-cache",
        ".pnpm-store",
        ".svn",
        ".turbo",
        ".wrangler",
        ".yarn",
        "bower_components",
        "coverage",
        "node_modules",
        "target",
    ];
    let relative = path.strip_prefix(project).unwrap_or(path);
    relative
        .components()
        .filter_map(|component| match component {
            std::path::Component::Normal(name) => Some(name),
            _ => None,
        })
        .any(|name| IGNORED_DIRECTORIES.iter().any(|ignored| name == *ignored))
        || relative.ancestors().any(|candidate| {
            !candidate.as_os_str().is_empty()
                && ignored.iter().any(|pattern| {
                    let options = MatchOptions {
                        case_sensitive: true,
                        require_literal_separator: true,
                        require_literal_leading_dot: true,
                    };
                    pattern.matches_path_with(candidate, options)
                        // A directory creation event arrives before its child
                        // path. Probe one child so `generated/**` also ignores
                        // the event that creates `generated` itself.
                        || pattern.matches_path_with(
                            &candidate.join("__celld_watch_descendant__"),
                            options,
                        )
                })
        })
}

struct ShutdownSignals {
    #[cfg(unix)]
    interrupt: tokio::signal::unix::Signal,
    #[cfg(unix)]
    terminate: tokio::signal::unix::Signal,
}

impl ShutdownSignals {
    fn install() -> anyhow::Result<Self> {
        #[cfg(unix)]
        {
            Ok(Self {
                interrupt: tokio::signal::unix::signal(
                    tokio::signal::unix::SignalKind::interrupt(),
                )?,
                terminate: tokio::signal::unix::signal(
                    tokio::signal::unix::SignalKind::terminate(),
                )?,
            })
        }
        #[cfg(not(unix))]
        {
            Ok(Self {})
        }
    }

    async fn received(&mut self) {
        #[cfg(unix)]
        crate::asyncrt::select! {
            _ = self.interrupt.recv() => {}
            _ = self.terminate.recv() => {}
        }
        #[cfg(not(unix))]
        let _ = tokio::signal::ctrl_c().await;
    }
}

pub fn print_help() -> anyhow::Result<()> {
    crate::cli_output::Output::new(crate::cli_output::Format::Text).help(
        &format!("celld dev — run an application with persistent local storage\n\n\
USAGE:\n  celld dev [PROJECT] [--host IP] [--port PORT] [--state-dir DIR] [--logs] [--strict-compat-flags] [--no-watch] [--clean]\n\n\
PROJECT is a directory or a Wrangler config. It defaults to the current\n\
directory. celld stores all local state in PROJECT/.celld/dev, and it keeps\n\
that state across a restart. --state-dir DIR, or CELLD_DEV_STATE_DIR, keeps it\n\
in DIR/<project hash>/.celld/dev instead. A configuration change does not migrate the\n\
state, so a cell can keep a value that the new configuration rejects. Use\n\
--clean to start from an empty local state.\n\n\
A .dev.vars file beside the config supplies Worker variables in dotenv form,\n\
as for wrangler dev. Without one, .env and then .env.local supply them, and\n\
an entry of these files that cannot be a binding is skipped with a warning.\n\
The entries override the vars of the config.\n\n\
OPTIONS:\n  --host IP              Worker listener host (default: 127.0.0.1)\n  --port PORT            Worker listener port, or 0 for a free one (default: {DEFAULT_PORT})\n  --state-dir DIR        Keep the local state in DIR/<project hash>/.celld/dev\n  --clean                Delete the local state before the server starts\n  --logs                 Show the node warning and information logs\n  --strict-compat-flags\n                         Fail on a compatibility flag celld does not know or\n                         does not handle yet, instead of warning\n  --no-watch             Do not rebuild when a project file changes\n  --watch-ignore PATTERN Ignore a project-relative glob; repeat as needed\n  -h, --help             Show this help"
        ),
    )
}

fn options_from_arguments(arguments: Vec<String>) -> anyhow::Result<Option<Options>> {
    let mut project = None;
    let mut state_dir = None;
    let mut host = IpAddr::V4(Ipv4Addr::LOCALHOST);
    let mut port = DEFAULT_PORT;
    let mut clean = false;
    let mut logs = false;
    let mut strict_compat_flags = false;
    let mut watch = true;
    let mut watch_ignores = Vec::new();
    let mut arguments = arguments.into_iter();
    while let Some(argument) = arguments.next() {
        match argument.as_str() {
            "--help" | "-h" => return Ok(None),
            "--clean" => clean = true,
            "--logs" => logs = true,
            "--strict-compat-flags" => strict_compat_flags = true,
            "--no-watch" => watch = false,
            "--watch-ignore" => {
                let value = arguments
                    .next()
                    .context("--watch-ignore requires a value")?;
                watch_ignores.push(
                    Pattern::new(&value)
                        .with_context(|| format!("invalid --watch-ignore pattern {value:?}"))?,
                );
            }
            "--state-dir" => {
                let value = arguments.next().context("--state-dir requires a value")?;
                state_dir = Some(PathBuf::from(value));
            }
            "--host" => {
                let value = arguments.next().context("--host requires a value")?;
                host = value
                    .parse::<IpAddr>()
                    .with_context(|| format!("invalid --host value {value:?}"))?;
            }
            "--port" => {
                let value = arguments.next().context("--port requires a value")?;
                port = value
                    .parse::<u16>()
                    .with_context(|| format!("invalid --port value {value:?}"))?;
            }
            other if other.starts_with('-') => bail!("unknown argument for `celld dev`: {other}"),
            value if project.is_none() => project = Some(PathBuf::from(value)),
            value => bail!("celld dev accepts one PROJECT, but also received {value:?}"),
        }
    }
    if !watch && !watch_ignores.is_empty() {
        bail!("--watch-ignore cannot be used with --no-watch");
    }
    Ok(Some(Options {
        project,
        state_dir,
        clean,
        stack: StackOptions {
            listener: SocketAddr::new(host, port),
            logs,
            strict_compat_flags,
            watch,
            watch_ignores,
        },
    }))
}

#[allow(clippy::disallowed_methods)] // Project resolution uses the operator's host filesystem.
pub async fn run(arguments: Vec<String>) -> anyhow::Result<()> {
    let Some(options) = options_from_arguments(arguments)? else {
        print_help()?;
        return Ok(());
    };
    let config = deploy::resolve_config(options.project)?;
    let config = std::fs::canonicalize(&config)
        .with_context(|| format!("resolve Wrangler config {}", config.display()))?;
    let project = config
        .parent()
        .context("the Wrangler config has no project directory")?;
    let state = DevState::resolve(
        project,
        options.state_dir,
        std::env::var_os("CELLD_DEV_STATE_DIR"),
    )?;
    let console = Console::new();
    console.header();
    console.detail("Project", &config.display().to_string());
    console.detail("State", &state.path().display().to_string());
    state.prepare_shared_root()?;
    // The delete runs before the directory is recreated, and the console names
    // the outcome. A developer who reaches for this flag is already unsure
    // which state the run uses, so a silent delete would answer nothing.
    if options.clean {
        let discarded = state.discard()?;
        console.detail(
            "Clean",
            if discarded {
                "discarded the existing local state"
            } else {
                "no local state to discard"
            },
        );
    }
    prepare_state(state.path())?;
    if !options.stack.logs {
        console.detail("Logs", "hidden (use --logs)");
    }
    if !options.stack.watch {
        console.detail("Watch", "disabled");
    }
    if crate::egress_filter::dev_default_mode().is_some() {
        console.detail(
            "Egress",
            "open in dev, so Workers can reach local services (set CELLD_EGRESS=public to refuse them)",
        );
    }

    let project_hash = project_hash(project);
    let store = open_store(state.path(), &console).await?;
    run_stack(
        &config,
        state.path(),
        options.stack,
        &project_hash,
        &store,
        &console,
    )
    .await
}

#[allow(clippy::disallowed_methods)] // Local development state is an operator-owned host path.
fn prepare_state(state: &Path) -> anyhow::Result<()> {
    for directory in [state.to_path_buf(), state.join("runtime")] {
        crate::config_dir::create_private(&directory)
            .with_context(|| format!("create local state directory {}", directory.display()))?;
    }
    Ok(())
}

#[allow(clippy::disallowed_methods)] // Local development state is an operator-owned host path.
async fn open_store(state: &Path, console: &Console) -> anyhow::Result<Store> {
    console.progress("opening the local storage");
    let database = std::fs::canonicalize(state)
        .context("resolve the local state directory")?
        .join("objects.sqlite3");
    let bucket = open_local_bucket(&database)?;
    fleet::validate_bucket(&bucket).await?;
    bucket.probe_cas().await?;
    Ok(Store { database })
}

/// The Worker variables for `celld dev`, from the files that `wrangler dev`
/// reads beside the config (Wrangler getVarsForDev()). A `.dev.vars` file
/// wins outright. Without one, `.env` and then `.env.local` apply, and a later
/// file overrides an earlier one. Each entry overrides the `vars` of the
/// config. Only `celld dev` reads the files, so a local credential cannot
/// reach a fleet.
fn read_dev_vars(config: &Path) -> anyhow::Result<deploy::VarOverrides> {
    // A present `.dev.vars` replaces the dotenv files even when it is empty,
    // as in Wrangler, so an entry that the project keeps in `.env` for
    // another tool does not reach the Worker beside it.
    if let Some(contents) = read_optional(&config.with_file_name(".dev.vars"))? {
        return Ok(deploy::VarOverrides::DevVars(parse_dotenv(&contents)));
    }
    let mut vars = BTreeMap::new();
    for name in [".env", ".env.local"] {
        if let Some(contents) = read_optional(&config.with_file_name(name))? {
            vars.extend(parse_dotenv(&contents));
        }
    }
    Ok(deploy::VarOverrides::DotEnv(vars))
}

/// The contents of `path`, or `None` when it does not exist. One read, not an
/// existence probe and then a read, so a file that vanishes between the two
/// cannot fail the build.
#[allow(clippy::disallowed_methods)] // The project is on the operator's host filesystem.
fn read_optional(path: &Path) -> anyhow::Result<Option<String>> {
    match std::fs::read_to_string(path) {
        Ok(contents) => Ok(Some(contents)),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(error).with_context(|| format!("read {}", path.display())),
    }
}

fn parse_dotenv(contents: &str) -> BTreeMap<String, String> {
    let mut vars = BTreeMap::new();
    let mut lines = contents.lines();
    while let Some(line) = lines.next() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        // dotenv accepts the `export NAME=value` form of a shell-sourced
        // file. Without the strip, the name `export NAME` fails binding
        // validation, and a `.env` that loaded before stops the build.
        let line = line.strip_prefix("export ").map_or(line, str::trim_start);
        let Some((name, value)) = line.split_once('=') else {
            continue;
        };
        let (name, value) = (name.trim(), value.trim());
        if name.is_empty() {
            continue;
        }
        let value = match quoted_lines(value, &mut lines) {
            Some(value) => value,
            // One matched pair of quotes comes off; a lone quote is part of
            // the value, as in dotenv, so a secret that ends in one survives.
            None => ['"', '\'']
                .into_iter()
                .find_map(|quote| value.strip_prefix(quote)?.strip_suffix(quote))
                .unwrap_or(value)
                .to_string(),
        };
        vars.insert(name.to_string(), value);
    }
    vars
}

/// A quoted value that continues on the following lines, as dotenv reads a
/// PEM key, joined by newlines without its quotes. Line by line, each base64
/// line of the key became a variable of its own or a name that fails binding
/// validation. `None` leaves `lines` untouched: the value ends on its own
/// line, or no later line closes the quote, so the opening quote is a lone
/// character of a one-line value.
fn quoted_lines(value: &str, lines: &mut std::str::Lines<'_>) -> Option<String> {
    let quote = value
        .chars()
        .next()
        .filter(|quote| matches!(quote, '"' | '\''))?;
    let first = &value[1..];
    if first.ends_with(quote) {
        return None;
    }
    let mut rest = lines.clone();
    let mut joined = first.to_string();
    while let Some(line) = rest.next() {
        joined.push('\n');
        match line.trim_end().strip_suffix(quote) {
            Some(last) => {
                joined.push_str(last);
                *lines = rest;
                return Some(joined);
            }
            None => joined.push_str(line),
        }
    }
    None
}

async fn deploy_project(
    config: &Path,
    store: &Store,
    logs: bool,
    strict_compat_flags: bool,
) -> anyhow::Result<()> {
    let bucket = open_local_bucket(&store.database)?;
    let built = deploy::build(&deploy::Options {
        config: Some(config.to_path_buf()),
        bucket: None,
        endpoint: None,
        region: None,
        dry_run: false,
        strict: false,
        strict_compat_flags,
        no_activate: false,
        json: false,
        vars: read_dev_vars(config)?,
        local_images: true,
        sourcemap: None,
        tag: None,
    })?;
    if logs {
        built.report();
    }
    crate::wake_format::ensure_ready(&bucket).await?;
    deploy::write(&bucket, &built, true).await.map(drop)
}

async fn run_stack(
    config: &Path,
    state: &Path,
    options: StackOptions,
    project_hash: &str,
    store: &Store,
    console: &Console,
) -> anyhow::Result<()> {
    // Install both handlers before the child can become ready. A caller can
    // send SIGTERM as soon as the readiness request answers, and installing a
    // handler afterwards leaves the new node orphaned under the default Unix
    // signal action.
    let mut signals = ShutdownSignals::install()?;
    let project = config
        .parent()
        .context("the Wrangler config has no project directory")?;
    let mut watcher = options
        .watch
        .then(|| ProjectWatcher::new(project, options.watch_ignores))
        .transpose()?;

    console.progress("building the application");
    deploy_project(config, store, options.logs, options.strict_compat_flags).await?;
    let mut running = start_node(
        state,
        options.listener,
        options.logs,
        project_hash,
        store,
        console,
    )
    .await?;
    // A reload restarts the node on the address the first start resolved, so a
    // port chosen for `--port 0` stays the one the operator was told.
    let listener = running.listener;

    loop {
        match wait_for_node_event(&mut running.child, &mut signals, watcher.as_mut()).await? {
            NodeEvent::Exited(status) => {
                let _ = running.output.await;
                if status.success() {
                    return Ok(());
                }
                bail!("the local celld node exited with {status}");
            }
            NodeEvent::Shutdown => {
                stop_running_node(running).await;
                return Ok(());
            }
            NodeEvent::Reload => {
                console.progress("change detected; rebuilding the application");
                if let Err(error) =
                    deploy_project(config, store, options.logs, options.strict_compat_flags).await
                {
                    console.failure(&format!("reload failed: {error:#}"));
                    continue;
                }
                console.progress("restarting the application");
                stop_running_node(running).await;
                running =
                    start_node(state, listener, options.logs, project_hash, store, console).await?;
            }
        }
    }
}

struct RunningNode {
    child: Child,
    /// The address the node listens on, with the port it was given when the
    /// request was for port 0.
    listener: SocketAddr,
    internal: String,
    /// The node's operator token, read once the node is up.
    token: Option<crate::operator_token::OperatorToken>,
    output: tokio::task::JoinHandle<()>,
}

enum NodeEvent {
    Exited(std::process::ExitStatus),
    Reload,
    Shutdown,
}

#[allow(clippy::disallowed_methods)] // Child supervision uses real host process and time APIs.
async fn start_node(
    state: &Path,
    listener: SocketAddr,
    logs: bool,
    project_hash: &str,
    store: &Store,
    console: &Console,
) -> anyhow::Result<RunningNode> {
    let executable = std::env::current_exe().context("locate the celld executable")?;
    let listen = listener.to_string();
    let token_file = state.join(crate::operator_token::FILE_NAME);
    let mut command = Command::new(executable);
    // The supervisor owns the child's complete storage and listener topology.
    // Inherited fleet settings can otherwise override that private topology or
    // make its guard reject a valid `celld dev` invocation.
    for variable in [
        "AWS_DEFAULT_REGION",
        "AWS_REGION",
        "CELLD_ADDR",
        "CELLD_ADVERTISE",
        "CELLD_BUCKET",
        "CELLD_CLOUD",
        "CELLD_INTERNAL_ADDR",
        // The node of `celld dev` keeps its own token in the state directory,
        // where `celld d1 --local` reads it; a token the shell holds for a
        // fleet is not this node's and would leave that file missing or stale.
        crate::operator_token::ENV,
        "CELLD_TEST_BUCKET",
        "CELLD_TRUST_FORWARDED_HEADERS",
        "CELLD_UNSAFE_PUBLIC_ADVERTISE",
        "S3_ENDPOINT",
    ] {
        command.env_remove(variable);
    }
    if let Some(mode) = crate::egress_filter::dev_default_mode() {
        command.env(crate::egress_filter::MODE_ENV, mode);
    }
    command
        .args([
            "--no-control-plane",
            "--bucket",
            BUCKET,
            "--listen",
            &listen,
            "--internal-listen",
            "127.0.0.1:0",
        ])
        .env("CELLD_INTERNAL_DEV_STORE", &store.database)
        .env("CELLD_WATCH", state.join("runtime"))
        .env(crate::operator_token::FILE_ENV, &token_file)
        .env("CELLD_NODE", format!("dev-{}", &project_hash[..12]))
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit());
    // `run_stack` normally consumes `RunningNode` through the graceful stop
    // path. An error or cancellation can instead drop it, so the child must
    // not outlive the value that proves the supervisor still owns it.
    command.kill_on_drop(true);
    if !logs {
        // The supervisor still forwards ERROR records and the child's stderr.
        // INFO and WARN records stay hidden until the operator asks for them.
        command.env("RUST_LOG", "error");
    }
    #[cfg(unix)]
    {
        use std::os::unix::process::CommandExt as _;
        // A terminal sends Ctrl-C to its foreground process group. Keep the
        // node in a child group so only this supervisor selects the shutdown
        // mode; otherwise the node can begin a slow fleet handoff before the
        // supervisor asks for the fast same-node preserve path.
        command.as_std_mut().process_group(0);
    }
    #[cfg(target_os = "linux")]
    {
        use std::os::unix::process::CommandExt as _;

        let supervisor = unsafe { libc::getpid() };
        // Drop cannot run after SIGKILL or a supervisor crash. Ask the kernel
        // to kill the node in that case. The parent check closes the race in
        // which the supervisor exits between `fork` and `prctl`.
        unsafe {
            command.as_std_mut().pre_exec(move || {
                if libc::prctl(libc::PR_SET_PDEATHSIG, libc::SIGKILL) == -1 {
                    return Err(std::io::Error::last_os_error());
                }
                if libc::getppid() != supervisor {
                    return Err(std::io::Error::from_raw_os_error(libc::ESRCH));
                }
                Ok(())
            });
        }
    }
    let mut child = command.spawn().context("start the local celld node")?;

    let stdout = child
        .stdout
        .take()
        .expect("the local node was configured with piped output");
    let (internal_tx, internal_rx) = oneshot::channel();
    let (listener_tx, listener_rx) = oneshot::channel();
    let output_console = *console;
    let output = tokio::spawn(async move {
        let mut lines = BufReader::new(stdout).lines();
        let mut internal_tx = Some(internal_tx);
        let mut listener_tx = Some(listener_tx);
        while let Ok(Some(line)) = lines.next_line().await {
            if let Some(origin) = internal_origin(&line) {
                if let Some(sender) = internal_tx.take() {
                    let _ = sender.send(origin);
                }
            }
            if let Some(address) = listener_address(&line) {
                if let Some(sender) = listener_tx.take() {
                    let _ = sender.send(address);
                }
            }
            if logs || line.contains(" ERROR ") {
                output_console.log(&line);
            }
        }
    });
    let internal = match tokio::time::timeout(Duration::from_secs(30), internal_rx).await {
        Ok(Ok(internal)) => internal,
        result => {
            stop_child(&mut child, None, None).await;
            let _ = output.await;
            match result {
                Err(_) => bail!("the local node did not announce its operator listener"),
                Ok(Err(_)) => {
                    bail!("the local node exited before it announced its operator listener")
                }
                Ok(Ok(_)) => unreachable!(),
            }
        }
    };

    // The node writes its token file before it announces the listener, so the
    // file is there now. Only the file: the node ignores the variable.
    let token = match crate::operator_token::OperatorToken::from_file(&token_file) {
        Ok(token) => token,
        Err(error) => {
            stop_child(&mut child, None, None).await;
            let _ = output.await;
            return Err(error.context("read the local node's operator token"));
        }
    };

    // The node prints its public address before its internal one, so by now the
    // line has been read; the channel holds it.
    let listener = match listener_rx.await {
        Ok(address) => address,
        Err(_) => {
            stop_child(&mut child, Some(&internal), token.as_ref()).await;
            let _ = output.await;
            bail!("the local node exited before it announced its listener");
        }
    };
    if let Err(error) = wait_for_node(&mut child, listener).await {
        stop_child(&mut child, Some(&internal), token.as_ref()).await;
        let _ = output.await;
        return Err(error);
    }
    console.ready(&format!("http://{listener}"));
    Ok(RunningNode {
        child,
        listener,
        internal,
        token,
        output,
    })
}

/// The address in the line the node prints once its public listener is bound.
fn listener_address(line: &str) -> Option<SocketAddr> {
    line.strip_prefix("celld listening on ")?
        .split_once(' ')?
        .0
        .parse()
        .ok()
}

fn internal_origin(line: &str) -> Option<String> {
    let address = line
        .strip_prefix("celld internal listening on ")?
        .split_once(' ')?
        .0;
    Some(format!("http://{address}"))
}

#[allow(clippy::disallowed_methods)] // Readiness waits on a real child and wall-clock deadline.
async fn wait_for_node(child: &mut Child, listener: SocketAddr) -> anyhow::Result<()> {
    let client = reqwest::Client::builder()
        .connect_timeout(Duration::from_millis(200))
        .timeout(Duration::from_secs(1))
        .build()?;
    let readiness_host = match listener.ip() {
        IpAddr::V4(address) if address.is_unspecified() => IpAddr::V4(Ipv4Addr::LOCALHOST),
        IpAddr::V6(address) if address.is_unspecified() => IpAddr::V6(Ipv6Addr::LOCALHOST),
        address => address,
    };
    let url = format!(
        "http://{}/.well-known/celld/health",
        SocketAddr::new(readiness_host, listener.port())
    );
    let mut last = "the node did not answer".to_string();
    for _ in 0..300 {
        if let Some(status) = child.try_wait().context("read the local node status")? {
            bail!("the local celld node exited during startup with {status}");
        }
        match client.get(&url).send().await {
            Ok(response) if response.status().is_success() => return Ok(()),
            Ok(response) => last = format!("the readiness endpoint returned {}", response.status()),
            Err(error) => last = error.to_string(),
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    bail!("the local celld node did not become ready: {last}")
}

async fn wait_for_node_event(
    child: &mut Child,
    signals: &mut ShutdownSignals,
    watcher: Option<&mut ProjectWatcher>,
) -> anyhow::Result<NodeEvent> {
    crate::asyncrt::select! {
        status = child.wait() => {
            let status = status.context("wait for the local celld node")?;
            Ok(NodeEvent::Exited(status))
        }
        _ = signals.received() => Ok(NodeEvent::Shutdown),
        changed = wait_for_project_change(watcher) => {
            changed?;
            Ok(NodeEvent::Reload)
        }
    }
}

async fn wait_for_project_change(watcher: Option<&mut ProjectWatcher>) -> anyhow::Result<()> {
    match watcher {
        Some(watcher) => watcher.changed().await,
        None => std::future::pending().await,
    }
}

async fn stop_running_node(mut running: RunningNode) {
    stop_child(
        &mut running.child,
        Some(&running.internal),
        running.token.as_ref(),
    )
    .await;
    let _ = running.output.await;
}

/// The same-node preserve request to the node's internal listener, which
/// requires the node's operator token.
fn shutdown_request(
    client: &reqwest::Client,
    internal: &str,
    token: Option<&crate::operator_token::OperatorToken>,
) -> reqwest::RequestBuilder {
    let request = client.post(format!("{internal}/shutdown?handoff=preserve"));
    match token {
        Some(token) => request.header(reqwest::header::AUTHORIZATION, token.bearer()),
        None => request,
    }
}

#[allow(clippy::disallowed_methods)] // Shutdown deadlines govern a real child process.
async fn stop_child(
    child: &mut Child,
    internal: Option<&str>,
    token: Option<&crate::operator_token::OperatorToken>,
) {
    let requested = if let Some(internal) = internal {
        match reqwest::Client::builder()
            .timeout(Duration::from_secs(2))
            .build()
        {
            Ok(client) => shutdown_request(&client, internal, token)
                .send()
                .await
                .is_ok_and(|response| response.status().is_success()),
            Err(_) => false,
        }
    } else {
        false
    };
    #[cfg(unix)]
    if !requested {
        if let Some(id) = child.id() {
            // The child installs SIGTERM as its graceful durability handoff. The
            // supervisor sends that signal instead of Child::kill, which is a
            // SIGKILL on Unix and can leave the last local transaction unflushed.
            unsafe {
                libc::kill(id as libc::pid_t, libc::SIGTERM);
            }
        }
    }
    let waited = tokio::time::timeout(Duration::from_secs(35), child.wait()).await;
    if waited.is_err() {
        let _ = child.kill().await;
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::disallowed_methods)] // Tests use the host filesystem.
    use super::*;

    fn parse(arguments: &[&str]) -> anyhow::Result<Option<Options>> {
        options_from_arguments(
            arguments
                .iter()
                .map(|argument| (*argument).to_owned())
                .collect(),
        )
    }

    #[test]
    fn port_zero_asks_for_a_free_port() {
        let options = parse(&["--port", "0"]).unwrap().unwrap();
        assert_eq!(options.stack.listener.port(), 0);
    }

    #[test]
    fn the_state_directory_moves_the_parent_of_the_local_state() {
        let options = parse(&["--state-dir", "/var/celld"]).unwrap().unwrap();
        assert_eq!(options.state_dir, Some(PathBuf::from("/var/celld")));
        assert!(parse(&["--state-dir"]).is_err());
    }

    fn resolved(flag: Option<&str>, env: Option<&str>) -> PathBuf {
        DevState::resolve(
            Path::new("/work/app"),
            flag.map(PathBuf::from),
            env.map(OsString::from),
        )
        .unwrap()
        .path()
        .to_path_buf()
    }

    #[test]
    fn the_state_defaults_to_the_project() {
        assert_eq!(resolved(None, None), Path::new("/work/app/.celld/dev"));
        assert_eq!(
            resolved(Some(""), Some("")),
            Path::new("/work/app/.celld/dev")
        );
    }

    #[test]
    fn the_flag_wins_over_the_environment_and_an_empty_flag_falls_back() {
        let hash = project_hash(Path::new("/work/app"));
        let expected = |root: &str| Path::new(root).join(&hash).join(".celld/dev");
        assert_eq!(resolved(Some("/a"), Some("/b")), expected("/a"));
        assert_eq!(resolved(None, Some("/b")), expected("/b"));
        assert_eq!(resolved(Some(""), Some("/b")), expected("/b"));
        assert_eq!(resolved(Some("/a"), Some("")), expected("/a"));
    }

    #[test]
    fn projects_that_share_a_state_directory_get_their_own_state() {
        let state = |project: &str| {
            DevState::resolve(Path::new(project), Some(PathBuf::from("/shared")), None)
                .unwrap()
                .path()
                .to_path_buf()
        };
        let (one, two) = (state("/work/one"), state("/work/two"));
        assert_ne!(one, two);
        assert!(one.starts_with("/shared") && one.ends_with(".celld/dev"));
        assert!(!one.starts_with(&two) && !two.starts_with(&one));
    }

    #[cfg(unix)]
    #[test]
    #[allow(clippy::disallowed_methods)] // Host paths in a temporary directory.
    fn clean_refuses_a_state_directory_reached_through_a_link() {
        let directory = tempfile::tempdir().unwrap();
        let victim = directory.path().join("victim");
        std::fs::create_dir_all(victim.join(".celld/dev")).unwrap();
        std::fs::write(victim.join(".celld/dev/keep"), "data").unwrap();
        let shared = directory.path().join("shared");
        std::fs::create_dir(&shared).unwrap();
        let project = Path::new("/work/app");
        let state = DevState::resolve(project, Some(shared.clone()), None).unwrap();
        std::os::unix::fs::symlink(&victim, shared.join(project_hash(project))).unwrap();
        let error = state.discard().unwrap_err().to_string();
        assert!(error.contains("symbolic link"), "{error}");
        assert!(victim.join(".celld/dev/keep").exists());
    }

    #[cfg(unix)]
    #[test]
    #[allow(clippy::disallowed_methods)] // Host paths in a temporary directory.
    fn the_per_project_directory_is_private_and_a_real_one_is_cleaned() {
        use std::os::unix::fs::PermissionsExt as _;
        let directory = tempfile::tempdir().unwrap();
        let state = DevState::resolve(
            Path::new("/work/app"),
            Some(directory.path().join("shared")),
            None,
        )
        .unwrap();
        state.prepare_shared_root().unwrap();
        let root = state.shared_root.clone().unwrap();
        let mode = std::fs::metadata(&root).unwrap().permissions().mode();
        assert_eq!(mode & 0o777, 0o700);
        assert!(!state.discard().unwrap());
        prepare_state(state.path()).unwrap();
        assert!(state.discard().unwrap());
    }

    #[cfg(unix)]
    #[test]
    #[allow(clippy::disallowed_methods)] // Host paths in a temporary directory.
    fn a_planted_per_project_directory_is_refused_before_it_is_used() {
        let directory = tempfile::tempdir().unwrap();
        let shared = directory.path().join("shared");
        std::fs::create_dir(&shared).unwrap();
        let project = Path::new("/work/app");
        let state = DevState::resolve(project, Some(shared.clone()), None).unwrap();
        let victim = directory.path().join("victim");
        std::fs::create_dir(&victim).unwrap();
        std::os::unix::fs::symlink(&victim, shared.join(project_hash(project))).unwrap();
        let error = state.prepare_shared_root().unwrap_err().to_string();
        assert!(error.contains("symbolic link"), "{error}");
    }

    #[cfg(unix)]
    #[test]
    #[allow(clippy::disallowed_methods)] // Host paths in a temporary directory.
    fn a_state_directory_others_can_write_to_is_refused() {
        use std::os::unix::fs::PermissionsExt as _;
        let directory = tempfile::tempdir().unwrap();
        let project = std::fs::canonicalize(directory.path()).unwrap();
        let shared = project.join("shared");
        for state_dir in [None, Some(shared)] {
            let state = DevState::resolve(&project, state_dir, None).unwrap();
            state.prepare_shared_root().unwrap();
            prepare_state(state.path()).unwrap();
            state.ensure_not_redirected("use").unwrap();
            let levels = [state.path().parent().unwrap(), state.path()];
            for level in levels.into_iter().chain(state.shared_root.as_deref()) {
                for mode in [0o770, 0o707] {
                    std::fs::set_permissions(level, std::fs::Permissions::from_mode(mode)).unwrap();
                    let error = state.ensure_not_redirected("use").unwrap_err().to_string();
                    assert!(
                        error.contains("can be written to by other users"),
                        "{error}"
                    );
                }
                std::fs::set_permissions(level, std::fs::Permissions::from_mode(0o700)).unwrap();
            }
        }
    }

    #[cfg(unix)]
    #[test]
    #[allow(clippy::disallowed_methods)] // Host paths in a temporary directory.
    fn celld_dev_tightens_a_loose_state_but_a_command_that_sends_the_token_refuses_it() {
        use std::os::unix::fs::PermissionsExt as _;
        let directory = tempfile::tempdir().unwrap();
        let project = std::fs::canonicalize(directory.path()).unwrap();
        let state = DevState::resolve(&project, None, None).unwrap();
        std::fs::create_dir_all(state.path()).unwrap();
        for level in [state.path().parent().unwrap(), state.path()] {
            std::fs::set_permissions(level, std::fs::Permissions::from_mode(0o777)).unwrap();
        }
        std::fs::write(state.path().join("objects.sqlite3"), b"").unwrap();
        let error = local_node_files(&project, None).unwrap_err().to_string();
        assert!(
            error.contains("can be written to by other users"),
            "{error}"
        );
        state.prepare_shared_root().unwrap();
        state.ensure_not_redirected("use").unwrap();
        let mode = std::fs::metadata(state.path())
            .unwrap()
            .permissions()
            .mode();
        assert_eq!(mode & 0o777, 0o700);
        // `--clean` tightens the same way.
        std::fs::set_permissions(state.path(), std::fs::Permissions::from_mode(0o777)).unwrap();
        assert!(state.discard().unwrap());
    }

    #[cfg(unix)]
    #[test]
    #[allow(clippy::disallowed_methods)] // Host paths in a temporary directory.
    fn a_project_local_state_that_another_user_owns_is_refused() {
        // SAFETY: `geteuid` takes no arguments and cannot fail.
        if unsafe { libc::geteuid() } == 0 {
            return;
        }
        let directory = tempfile::tempdir().unwrap();
        let project = std::fs::canonicalize(directory.path()).unwrap();
        // `/usr` is owned by root and not writable by others, so only the
        // owner check can refuse it.
        std::os::unix::fs::symlink("/usr", project.join(".celld")).unwrap();
        let state = DevState::resolve(&project, None, None).unwrap();
        let error = state.ensure_not_redirected("use").unwrap_err().to_string();
        assert!(error.contains("owned by another user"), "{error}");

        // A link to a directory the user owns is still fine.
        std::fs::remove_file(project.join(".celld")).unwrap();
        let own = project.join("own");
        std::fs::create_dir(&own).unwrap();
        std::fs::set_permissions(&own, std::os::unix::fs::PermissionsExt::from_mode(0o700))
            .unwrap();
        std::os::unix::fs::symlink(&own, project.join(".celld")).unwrap();
        state.ensure_not_redirected("use").unwrap();
    }

    #[test]
    fn clients_find_the_token_where_the_node_with_the_state_directory_keeps_it() {
        let directory = tempfile::tempdir().unwrap();
        let project = &std::fs::canonicalize(directory.path()).unwrap();
        let inside = operator_token_path_with(project, None).unwrap();
        assert_eq!(inside, project.join(".celld/dev/operator-token"));
        let shared = operator_token_path_with(project, Some("/shared".into())).unwrap();
        assert_eq!(
            shared,
            DevState::resolve(project, None, Some("/shared".into()))
                .unwrap()
                .path()
                .join(crate::operator_token::FILE_NAME)
        );
        assert_ne!(shared, inside);
    }

    #[cfg(unix)]
    #[test]
    fn clients_find_the_token_through_links_and_relative_paths() {
        let directory = tempfile::tempdir().unwrap();
        let project = std::fs::canonicalize(directory.path()).unwrap().join("app");
        std::fs::create_dir(&project).unwrap();
        let link = directory.path().join("link");
        std::os::unix::fs::symlink(&project, &link).unwrap();
        let state = Some(OsString::from("/shared"));
        let expected = operator_token_path_with(&project, state.clone()).unwrap();
        assert_eq!(
            operator_token_path_with(&link, state.clone()).unwrap(),
            expected
        );
        let dotted = project.join("..").join("app");
        assert_eq!(
            operator_token_path_with(&dotted, state.clone()).unwrap(),
            expected
        );
        let relative = pathdiff_from_current(&project);
        assert_eq!(
            operator_token_path_with(&relative, state).unwrap(),
            expected
        );
    }

    /// `path` as a relative path from the current directory.
    fn pathdiff_from_current(path: &Path) -> PathBuf {
        let current = std::fs::canonicalize(std::env::current_dir().unwrap()).unwrap();
        let mut relative = PathBuf::new();
        for _ in current.components().skip(1) {
            relative.push("..");
        }
        relative.join(path.strip_prefix("/").unwrap())
    }

    #[test]
    fn a_relative_state_directory_becomes_absolute() {
        let path = resolved(Some("state"), None);
        assert!(path.is_absolute(), "{}", path.display());
        assert!(path.ends_with(".celld/dev"));
    }

    #[test]
    fn a_port_out_of_range_is_refused() {
        assert!(parse(&["--port", "65536"]).is_err());
        assert!(parse(&["--port", "-1"]).is_err());
    }

    #[test]
    fn the_public_address_is_read_from_the_announce_line() {
        assert_eq!(
            listener_address("celld listening on 127.0.0.1:55421 (ownership=dev)"),
            Some("127.0.0.1:55421".parse().unwrap())
        );
        assert_eq!(
            listener_address("celld listening on [::1]:8080 (ownership=dev)"),
            Some("[::1]:8080".parse().unwrap())
        );
        assert_eq!(
            listener_address("celld internal listening on 127.0.0.1:1 (advertise=x)"),
            None
        );
    }
}

#[cfg(test)]
mod operator_token_tests {
    use super::*;

    #[tokio::test]
    async fn the_shutdown_request_carries_the_operator_token() {
        let stub = crate::test_stub::Recorder::start().await;
        let token =
            crate::operator_token::OperatorToken::parse(std::ffi::OsStr::new("tok-7")).unwrap();
        let _ = shutdown_request(
            &reqwest::Client::new(),
            &format!("http://{}", stub.addr()),
            Some(&token),
        )
        .send()
        .await;
        assert_eq!(
            stub.authorizations(),
            vec![Some("Bearer tok-7".to_string())]
        );
        assert!(stub.heads()[0].starts_with("POST /shutdown?handoff=preserve "));
    }
}
