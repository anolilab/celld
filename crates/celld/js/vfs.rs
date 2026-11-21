// Copyright 2026 Deno Land Inc. Apache-2.0 license.

//! The request's in-memory filesystem: the writable `/tmp` that `node:fs`
//! reads and writes, and the ops behind it. The `/bundle` projection is
//! isolate state and stays with the module loader.
use super::*;

/// The writable directory subset of Workerd's per-request virtual filesystem.
///
/// Keeping the tree on `IoContext` is the isolation mechanism: async
/// continuations for one request share it, while a concurrent or later request
/// receives a different tree. No path can escape into the host filesystem, and
/// the tree is freed with the request.
///
/// The tree lives in memory, so `used` bounds what one request can make the
/// node hold: the capacity of each file plus a fixed charge per entry, so a request cannot
/// get around the cap with many empty files or directories.
pub(super) struct RequestVfs {
    nodes: HashMap<String, VfsNode>,
    /// The names directly inside each directory that has any, so listing a
    /// directory and removing a tree walk only what they touch. An entry's
    /// parent key and name are copies of parts of its path, which `node_cost`
    /// charges for.
    children: HashMap<String, std::collections::BTreeSet<String>>,
    /// Charged memory: every entry's cost plus each file's capacity.
    used: usize,
    /// The capacity files hold beyond their length, summed. A charge that
    /// would fail gives it back, and a write limit counts it as room.
    spare: usize,
    /// How many times a reclaim visited the files.
    #[cfg(test)]
    reclaim_scans: usize,
}

/// The most memory one request's `/tmp` may hold.
const VFS_MAX_BYTES: usize = 64 * 1024 * 1024;

/// Charged for each entry on top of its path and bytes.
const VFS_NODE_OVERHEAD: usize = 64;

/// The least spare capacity an append that outgrows its file claims, and the
/// share of the file (a quarter) it claims once the file is larger than four
/// times this. A fixed step would copy the whole file once per step; a share
/// keeps the copies geometric. The slack also never takes more than half of
/// the room left, and is reclaimed when another write needs the room.
const VFS_APPEND_SLACK: usize = 1024 * 1024;

/// A file has no kind but its bytes; a path-only set would make a file
/// indistinguishable from its parent directory.
enum VfsNode {
    Directory { writable: bool },
    File(Vec<u8>),
}

enum VfsStat {
    Directory { writable: bool },
    File(usize),
}

enum VfsMkdirResult {
    Created(Option<String>),
    Error(&'static str),
}

impl Default for RequestVfs {
    fn default() -> Self {
        Self {
            nodes: [
                ("/", false),
                ("/bundle", false),
                ("/dev", false),
                ("/tmp", true),
            ]
            .into_iter()
            .map(|(path, writable)| (path.to_string(), VfsNode::Directory { writable }))
            .collect(),
            children: HashMap::from([(
                "/".to_string(),
                ["bundle", "dev", "tmp"].map(String::from).into(),
            )]),
            used: 0,
            spare: 0,
            #[cfg(test)]
            reclaim_scans: 0,
        }
    }
}

impl RequestVfs {
    fn stat(&self, path: &str) -> Option<VfsStat> {
        self.nodes.get(path).map(|node| match node {
            VfsNode::Directory { writable } => VfsStat::Directory {
                writable: *writable,
            },
            VfsNode::File(bytes) => VfsStat::File(bytes.len()),
        })
    }

    /// Only `/tmp` is writable; everything else answers `EPERM`.
    fn writable_path(path: &str) -> bool {
        path == "/tmp" || path.starts_with("/tmp/")
    }

    fn parent_of(path: &str) -> &str {
        match path.rsplit_once('/') {
            Some(("", _)) | None => "/",
            Some((parent, _)) => parent,
        }
    }

    /// Make room for `extra` more bytes, or refuse without changing anything.
    /// Room that only unused file capacity holds is reclaimed first, so spare
    /// capacity never turns a write that fits the data away.
    fn charge(&mut self, extra: usize) -> Result<(), &'static str> {
        self.try_charge(extra).or_else(|code| {
            if self.spare == 0 {
                return Err(code);
            }
            self.reclaim_capacity();
            self.try_charge(extra)
        })
    }

    fn try_charge(&mut self, extra: usize) -> Result<(), &'static str> {
        match self.used.checked_add(extra) {
            Some(total) if total <= VFS_MAX_BYTES => {
                self.used = total;
                Ok(())
            }
            _ => Err("ENOSPC"),
        }
    }

    /// Shrink every file's capacity to its length and refund the difference.
    fn reclaim_capacity(&mut self) {
        #[cfg(test)]
        {
            self.reclaim_scans += 1;
        }
        self.spare = 0;
        for node in self.nodes.values_mut() {
            if let VfsNode::File(bytes) = node {
                let before = bytes.capacity();
                bytes.shrink_to_fit();
                self.used -= before - bytes.capacity();
            }
        }
    }

    /// The path is held three times: as the node's key, as the parent key of
    /// the child index, and as the name inside it (each at most the path's
    /// length), so a deep tree of short components is charged for all three.
    fn node_cost(path: &str) -> usize {
        path.len() * 3 + VFS_NODE_OVERHEAD
    }

    /// Add a new entry and list it under its parent.
    fn insert(&mut self, path: String, node: VfsNode) {
        let name = path.rsplit('/').next().unwrap_or_default().to_string();
        self.children
            .entry(Self::parent_of(&path).to_string())
            .or_default()
            .insert(name);
        self.nodes.insert(path, node);
    }

    fn mkdir(&mut self, path: &str, recursive: bool) -> VfsMkdirResult {
        if !Self::writable_path(path) {
            return VfsMkdirResult::Error("EPERM");
        }
        match self.nodes.get(path) {
            Some(VfsNode::File(_)) => return VfsMkdirResult::Error("EEXIST"),
            Some(VfsNode::Directory { .. }) => return VfsMkdirResult::Created(None),
            None => {}
        }
        if recursive {
            let mut current = "/tmp".to_string();
            let mut first_created = None;
            for segment in path.trim_start_matches("/tmp/").split('/') {
                current.push('/');
                current.push_str(segment);
                match self.nodes.get(&current) {
                    Some(VfsNode::File(_)) => return VfsMkdirResult::Error("ENOTDIR"),
                    Some(VfsNode::Directory { .. }) => continue,
                    None => {}
                }
                if let Err(code) = self.charge(Self::node_cost(&current)) {
                    return VfsMkdirResult::Error(code);
                }
                self.insert(current.clone(), VfsNode::Directory { writable: true });
                first_created.get_or_insert_with(|| current.clone());
            }
            return VfsMkdirResult::Created(first_created);
        }
        match self.nodes.get(Self::parent_of(path)) {
            None => return VfsMkdirResult::Error("ENOENT"),
            Some(VfsNode::File(_)) => return VfsMkdirResult::Error("ENOTDIR"),
            Some(VfsNode::Directory { .. }) => {}
        }
        if let Err(code) = self.charge(Self::node_cost(path)) {
            return VfsMkdirResult::Error(code);
        }
        self.insert(path.to_string(), VfsNode::Directory { writable: true });
        VfsMkdirResult::Created(None)
    }

    /// Replace or extend a file. A write that would pass the cap changes
    /// nothing. The buffer moves into the tree for a new file or an overwrite,
    /// so the content is never copied a second time.
    fn write(&mut self, path: &str, data: Vec<u8>, append: bool) -> Result<(), &'static str> {
        if !Self::writable_path(path) {
            return Err("EPERM");
        }
        match self.nodes.get(Self::parent_of(path)) {
            None => return Err("ENOENT"),
            Some(VfsNode::File(_)) => return Err("ENOTDIR"),
            Some(VfsNode::Directory { .. }) => {}
        }
        match self.nodes.get(path) {
            Some(VfsNode::Directory { .. }) => Err("EISDIR"),
            Some(VfsNode::File(old)) => {
                let (mut before, mut after) = self.planned_capacity(old, &data, append);
                if self.charge_capacity(before, after).is_err() {
                    // The spare capacity of the files may be what is in the
                    // way, this file's included, so plan again once it is back.
                    self.reclaim_capacity();
                    let Some(VfsNode::File(old)) = self.nodes.get(path) else {
                        unreachable!("the node was a file a moment ago");
                    };
                    (before, after) = self.planned_capacity(old, &data, append);
                    self.charge_capacity(before, after)?;
                }
                let Some(VfsNode::File(bytes)) = self.nodes.get_mut(path) else {
                    unreachable!("the node was a file a moment ago");
                };
                self.spare -= bytes.capacity() - bytes.len();
                if append {
                    // The capacity was charged above, so grow to exactly it.
                    bytes.reserve_exact(after - bytes.len());
                    bytes.extend_from_slice(&data);
                } else {
                    *bytes = data;
                }
                self.spare += bytes.capacity() - bytes.len();
                Ok(())
            }
            None => {
                self.charge(Self::node_cost(path).saturating_add(data.capacity()))?;
                self.spare += data.capacity() - data.len();
                self.insert(path.to_string(), VfsNode::File(data));
                Ok(())
            }
        }
    }

    /// A file's capacity before and after this write.
    fn planned_capacity(&self, old: &Vec<u8>, data: &Vec<u8>, append: bool) -> (usize, usize) {
        let before = old.capacity();
        if !append {
            return (before, data.capacity());
        }
        // The room this file may take: what is free plus what it already holds.
        let budget = VFS_MAX_BYTES.saturating_sub(self.used) + before;
        (
            before,
            Self::grown_capacity(before, old.len(), data.len(), budget),
        )
    }

    /// The capacity an append needs: unchanged while it fits, otherwise the
    /// bytes needed plus slack: an equal amount while that is small, at least
    /// `VFS_APPEND_SLACK` and a quarter of the bytes needed beyond it, so n
    /// small appends reallocate O(log n) times at every size and one append
    /// to a large file claims a bounded extra. The slack takes at most half of
    /// what is left of `budget`, the most capacity the file may take, after the
    /// bytes needed, so it leaves room for other files and an append whose
    /// bytes fit is never refused for the room its growth would have wanted.
    /// Spare capacity is charged, but it is given back when a charge would
    /// otherwise fail.
    fn grown_capacity(capacity: usize, len: usize, extra: usize, budget: usize) -> usize {
        let needed = len.saturating_add(extra);
        if needed <= capacity {
            capacity
        } else {
            let slack = needed
                .min(VFS_APPEND_SLACK.max(needed / 4))
                .min(budget.saturating_sub(needed) / 2);
            needed.saturating_add(slack)
        }
    }

    /// Charge a file's move from capacity `before` to `after`, or release the
    /// difference when it shrinks.
    fn charge_capacity(&mut self, before: usize, after: usize) -> Result<(), &'static str> {
        if after > before {
            self.try_charge(after - before)
        } else {
            self.used -= before - after;
            Ok(())
        }
    }

    /// The most bytes a write to `path` could carry and still be charged, or
    /// the error the write would answer anyway. Lets a caller refuse an
    /// oversized buffer before copying it. Spare capacity counts as room, since
    /// a charge gives it back; the target's own is counted once.
    fn write_limit(&self, path: &str, append: bool) -> Result<usize, &'static str> {
        if !Self::writable_path(path) {
            return Err("EPERM");
        }
        let remaining = VFS_MAX_BYTES - self.used;
        Ok(match self.nodes.get(path) {
            // The room the old content gives back, and every file's spare.
            Some(VfsNode::File(old)) if !append => remaining
                .saturating_add(old.len())
                .saturating_add(self.spare),
            // An append keeps the file's bytes.
            Some(VfsNode::File(_)) => remaining.saturating_add(self.spare),
            None => remaining
                .saturating_sub(Self::node_cost(path))
                .saturating_add(self.spare),
            Some(_) => remaining.saturating_add(self.spare),
        })
    }

    fn read(&self, path: &str) -> Option<&[u8]> {
        match self.nodes.get(path) {
            Some(VfsNode::File(bytes)) => Some(bytes),
            _ => None,
        }
    }

    /// The names directly inside a directory of the tree, sorted. `/bundle`
    /// is listed from the module tree by `op_vfs_readdir`.
    fn readdir(&self, path: &str) -> Result<Vec<String>, &'static str> {
        match self.nodes.get(path) {
            None => return Err("ENOENT"),
            Some(VfsNode::File(_)) => return Err("ENOTDIR"),
            Some(VfsNode::Directory { .. }) => {}
        }
        Ok(self
            .children
            .get(path)
            .map(|names| names.iter().cloned().collect())
            .unwrap_or_default())
    }

    /// Remove a file, or a directory with everything under it when
    /// `recursive`. A directory without `recursive` is `EISDIR`.
    fn remove(&mut self, path: &str, recursive: bool) -> Result<(), &'static str> {
        if !Self::writable_path(path) || path == "/tmp" {
            return Err("EPERM");
        }
        match self.nodes.get(path) {
            None => return Err("ENOENT"),
            Some(VfsNode::Directory { .. }) if !recursive => return Err("EISDIR"),
            _ => {}
        }
        let mut freed = 0;
        let mut pending = vec![path.to_string()];
        while let Some(entry) = pending.pop() {
            if let Some(names) = self.children.remove(&entry) {
                pending.extend(names.into_iter().map(|name| format!("{entry}/{name}")));
            }
            freed += Self::node_cost(&entry)
                + match self.nodes.remove(&entry) {
                    Some(VfsNode::File(bytes)) => {
                        self.spare -= bytes.capacity() - bytes.len();
                        bytes.capacity()
                    }
                    _ => 0,
                };
        }
        if let Some(names) = self.children.get_mut(Self::parent_of(path)) {
            names.remove(path.rsplit('/').next().unwrap_or_default());
        }
        self.used -= freed;
        Ok(())
    }
}

/// The error code for a `/tmp` change with no request-local tree; the harness
/// words it. `tmp_unavailable_code` tells "no request" from "a request that
/// began before `node:fs` was loaded".
fn no_tmp(scope: &mut v8::PinScope) -> &'static str {
    super::tmp_unavailable_code(scope)
}

fn lock_vfs(context: &IoContext) -> std::sync::MutexGuard<'_, RequestVfs> {
    context
        .vfs
        .get_or_init(|| Mutex::new(RequestVfs::default()))
        .lock()
        .unwrap()
}

/// Run `f` on the current request's virtual `/tmp` tree. Without a request
/// there is no tree to change: `None`.
fn with_vfs<R>(scope: &mut v8::PinScope, f: impl FnOnce(&mut RequestVfs) -> R) -> Option<R> {
    let context = request_context(scope)?;
    let result = f(&mut lock_vfs(&context));
    Some(result)
}

/// Like `with_vfs` for a change: no request-local tree answers `ENOREQUEST`
/// or `ENOTMPTOKEN`.
fn with_vfs_mut(
    scope: &mut v8::PinScope,
    f: impl FnOnce(&mut RequestVfs) -> Result<(), &'static str>,
) -> &'static str {
    match with_vfs(scope, f) {
        Some(Ok(())) => "",
        Some(Err(code)) => code,
        None => no_tmp(scope),
    }
}

/// Read the current request's tree. With no request the tree is empty, as for
/// a request that wrote nothing, and nothing is created for it.
fn read_vfs<R>(scope: &mut v8::PinScope, f: impl FnOnce(&RequestVfs) -> R) -> R {
    match request_context(scope) {
        Some(context) => {
            let result = f(&lock_vfs(&context));
            result
        }
        None => f(&RequestVfs::default()),
    }
}

/// Create one directory in the current request's virtual `/tmp` tree.
/// A created path starts with `/`, an error is a Node code, and an empty string
/// means that no path must be returned. The disjoint representation keeps the
/// result and its meaning together without allocating a JS object for each
/// directory segment.
pub(super) fn op_vfs_mkdir(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    mut rv: v8::ReturnValue<v8::Value>,
) {
    let path = args.get(0).to_rust_string_lossy(scope);
    let recursive = args.get(1).boolean_value(scope);
    let result = match with_vfs(scope, |vfs| vfs.mkdir(&path, recursive)) {
        Some(result) => result,
        None => VfsMkdirResult::Error(no_tmp(scope)),
    };
    let result = match result {
        VfsMkdirResult::Created(Some(path)) => path,
        VfsMkdirResult::Created(None) => String::new(),
        VfsMkdirResult::Error(code) => code.to_string(),
    };
    rv.set(v8::String::new(scope, &result).unwrap().into());
}

/// Write (or, when the third argument is true, append to) a file of the
/// request's `/tmp`. Returns `""` or a Node error code; `ENOSPC` means the
/// request's `/tmp` is full.
pub(super) fn op_vfs_write(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    mut rv: v8::ReturnValue<v8::Value>,
) {
    let path = args.get(0).to_rust_string_lossy(scope);
    let append = args.get(2).boolean_value(scope);
    let view = args.get(1).try_cast::<v8::ArrayBufferView>().ok();
    let len = view.map_or(0, |view| view.byte_length());
    // Refuse a buffer that cannot fit before copying it out of the heap.
    let limit = match with_vfs(scope, |vfs| vfs.write_limit(&path, append)) {
        Some(limit) => limit,
        None => Err(no_tmp(scope)),
    };
    let code = match limit {
        Err(code) => code,
        Ok(limit) if len > limit => "ENOSPC",
        Ok(_) => {
            let mut data = vec![0; len];
            if let Some(view) = view {
                view.copy_contents(&mut data);
            }
            with_vfs_mut(scope, |vfs| vfs.write(&path, data, append))
        }
    };
    rv.set(v8::String::new(scope, code).unwrap().into());
}

/// The entries of a `/bundle` directory, sorted, or the error for a bundle
/// path that is not one. `None` for a path outside the bundle projection, and
/// for `/bundle` itself when the isolate has no bundle.
fn bundle_children(
    scope: &mut v8::PinScope,
    path: &str,
) -> Option<Result<Vec<String>, &'static str>> {
    if path != "/bundle" && !path.starts_with("/bundle/") {
        return None;
    }
    let bundle = scope.get_slot::<Arc<BundleFs>>().cloned()?;
    Some(bundle.tree().list(path))
}

/// The names in a directory as an array, or a Node error code as a string.
pub(super) fn op_vfs_readdir(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    mut rv: v8::ReturnValue<v8::Value>,
) {
    let path = args.get(0).to_rust_string_lossy(scope);
    let listed = match bundle_children(scope, &path) {
        Some(listed) => listed,
        None => read_vfs(scope, |vfs| vfs.readdir(&path)),
    };
    match listed {
        Ok(names) => {
            let array = v8::Array::new(scope, names.len() as i32);
            for (index, name) in names.iter().enumerate() {
                let name = v8::String::new(scope, name).unwrap();
                array.set_index(scope, index as u32, name.into());
            }
            rv.set(array.into());
        }
        Err(code) => rv.set(v8::String::new(scope, code).unwrap().into()),
    }
}

/// Remove a file or directory of the request's `/tmp`. Returns `""` or a
/// Node error code.
pub(super) fn op_vfs_remove(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    mut rv: v8::ReturnValue<v8::Value>,
) {
    let path = args.get(0).to_rust_string_lossy(scope);
    let recursive = args.get(1).boolean_value(scope);
    let code = with_vfs_mut(scope, |vfs| vfs.remove(&path, recursive));
    rv.set(v8::String::new(scope, code).unwrap().into());
}

/// Return `[kind, size]`, where kind is 0 for a missing path, 1 for a
/// read-only directory, 2 for a writable directory, 3 for a read-only
/// file and 4 for a writable file. Only a file has a size; a directory reports
/// 0, matching workerd.
///
/// The pair travels together because a caller that knows a path is a file
/// always needs its size to build `Stats`, and a second lookup could observe a
/// different tree.
pub(super) fn op_vfs_stat(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    mut rv: v8::ReturnValue<v8::Value>,
) {
    let path = args.get(0).to_rust_string_lossy(scope);
    let (kind, size) = match read_vfs(scope, |vfs| vfs.stat(&path)) {
        Some(VfsStat::Directory { writable }) => (if writable { 2 } else { 1 }, 0),
        Some(VfsStat::File(size)) => (4, size),
        // Only a path the writable tree does not claim can be a bundle path,
        // so the request tree stays authoritative for `/tmp`.
        None => match bundle_node(scope, &path) {
            Some(BundleStat::Directory) => (1, 0),
            Some(BundleStat::File(size)) => (3, size),
            None => (0, 0),
        },
    };
    let pair = v8::Array::new(scope, 2);
    let kind = v8::Integer::new(scope, kind).into();
    let size = v8::Number::new(scope, size as f64).into();
    pair.set_index(scope, 0, kind);
    pair.set_index(scope, 1, size);
    rv.set(pair.into());
}

/// Read a whole bundle file. Returns an `ArrayBuffer` of the module's bytes,
/// or `null` when the path is not a bundle file, so the caller raises the
/// `ENOENT` that names the syscall it was performing.
pub(super) fn op_vfs_read_file(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    mut rv: v8::ReturnValue<v8::Value>,
) {
    let path = args.get(0).to_rust_string_lossy(scope);
    if let Some(bytes) = read_vfs(scope, |vfs| vfs.read(&path).map(<[u8]>::to_vec)) {
        let store = v8::ArrayBuffer::new_backing_store_from_vec(bytes).make_shared();
        rv.set(v8::ArrayBuffer::with_backing_store(scope, &store).into());
        return;
    }
    let Some(bundle) = scope.get_slot::<Arc<BundleFs>>().cloned() else {
        return;
    };
    let tree = bundle.tree();
    let Some(BundleNode::File(bytes)) = tree.nodes.get(&path) else {
        return;
    };
    // One copy into the V8 heap. The projected `Bytes` stays shared, so a
    // second read of the same module does not re-copy the config's source.
    let store = v8::ArrayBuffer::new_backing_store_from_vec(bytes.to_vec()).make_shared();
    let buffer = v8::ArrayBuffer::with_backing_store(scope, &store);
    rv.set(buffer.into());
}

#[cfg(test)]
mod tests {
    use super::{IoContext, RequestVfs, VfsMkdirResult, VfsNode, VFS_MAX_BYTES};
    use crate::js::{BundleNode, BundleTree};

    #[test]
    fn a_bundle_lists_recursively_without_scanning_every_node_per_directory() {
        // 10 000 directories of 10 files: a scan of every node for each
        // directory is some 10^9 steps, an index is about 10^5.
        let names: Vec<String> = (0..10_000)
            .flat_map(|d| (0..10).map(move |f| format!("d{d}/f{f}")))
            .collect();
        let tree = BundleTree::from_files(
            names
                .iter()
                .map(|name| (name.as_str(), bytes::Bytes::new())),
        );
        let started = std::time::Instant::now();
        let mut stack = vec!["/bundle".to_string()];
        let mut files = 0;
        while let Some(path) = stack.pop() {
            for name in tree.list(&path).unwrap() {
                let child = format!("{path}/{name}");
                if tree.list(&child).is_ok() {
                    stack.push(child);
                } else {
                    files += 1;
                }
            }
        }
        assert_eq!(files, 100_000);
        // The indexed walk takes well under a second; the scan took about two
        // minutes on a developer machine. The bound only has to tell them apart,
        // so it leaves a slow or busy runner plenty of room.
        assert!(started.elapsed().as_secs() < 60, "{:?}", started.elapsed());
        assert_eq!(tree.list("/bundle/d1/f1"), Err("ENOTDIR"));
        assert_eq!(tree.list("/bundle/none"), Err("ENOENT"));
        assert_eq!(tree.list("/bundle/d1").unwrap().len(), 10);
    }

    #[test]
    fn bundle_module_paths_are_normalised_like_lookups() {
        let tree = BundleTree::from_files(
            ["./foo.js", "./lib/a.js", "x/../y.js"]
                .into_iter()
                .map(|name| (name, bytes::Bytes::new())),
        );
        assert_eq!(tree.list("/bundle").unwrap(), ["foo.js", "lib", "y.js"]);
        assert_eq!(tree.list("/bundle/lib").unwrap(), ["a.js"]);
    }

    #[test]
    fn a_sibling_module_does_not_replace_the_entry_at_the_same_path() {
        let tree = BundleTree::from_files(
            [("worker.js", "entry"), ("./worker.js", "sibling")]
                .into_iter()
                .map(|(name, body)| (name, bytes::Bytes::from(body))),
        );
        match tree.nodes.get("/bundle/worker.js") {
            Some(BundleNode::File(bytes)) => assert_eq!(&bytes[..], b"entry"),
            _ => panic!("worker.js is not a file"),
        }
    }

    #[test]
    fn appended_files_are_charged_their_capacity() {
        let mut vfs = RequestVfs::default();
        let mut expected = 0;
        for index in 0..4 {
            let path = format!("/tmp/f{index}");
            for _ in 0..5000 {
                vfs.write(&path, b"x".to_vec(), true).unwrap();
            }
            let Some(VfsNode::File(bytes)) = vfs.nodes.get(&path) else {
                panic!("{path} is not a file");
            };
            assert_eq!(bytes.len(), 5000);
            expected += RequestVfs::node_cost(&path) + bytes.capacity();
        }
        assert_eq!(vfs.used, expected, "what is charged must be what is held");
    }

    #[test]
    fn many_small_appends_reallocate_logarithmically() {
        let mut vfs = RequestVfs::default();
        let mut capacity = 0;
        let mut reallocations = 0;
        for _ in 0..10_000 {
            vfs.write("/tmp/log", b"x".to_vec(), true).unwrap();
            let Some(VfsNode::File(bytes)) = vfs.nodes.get("/tmp/log") else {
                panic!("/tmp/log is not a file");
            };
            if bytes.capacity() != capacity {
                capacity = bytes.capacity();
                reallocations += 1;
            }
        }
        // Doubling from one byte reaches 10 000 in 14 steps.
        assert!(reallocations <= 16, "{reallocations} reallocations");
    }

    #[test]
    fn appends_to_a_large_file_grow_its_capacity_geometrically() {
        let mut vfs = RequestVfs::default();
        let chunk = vec![b'x'; 1024];
        let mut capacity = 0;
        let mut copies = 0;
        let mut copied = 0;
        // 1 KiB appends up to 40 MiB. A growth step of a fixed 1 MiB copies
        // the file about once per MiB past the first.
        for _ in 0..40 * 1024 {
            vfs.write("/tmp/log", chunk.clone(), true).unwrap();
            let Some(VfsNode::File(bytes)) = vfs.nodes.get("/tmp/log") else {
                panic!("/tmp/log is not a file");
            };
            if bytes.capacity() != capacity {
                // The bytes held before this append are what a move copies.
                copied += bytes.len() - chunk.len();
                capacity = bytes.capacity();
                copies += 1;
            }
        }
        assert!(copies <= 32, "{copies} reallocations");
        assert!(copied <= 6 * 40 * 1024 * 1024, "{copied} bytes copied");
    }

    #[test]
    fn append_slack_does_not_turn_away_a_write_that_fits_the_data() {
        let mut vfs = RequestVfs::default();
        let mib = 1024 * 1024;
        vfs.write("/tmp/a", vec![0; 40 * mib], false).unwrap();
        vfs.write("/tmp/a", b"x".to_vec(), true).unwrap();
        vfs.write("/tmp/b", vec![0; 14 * mib], false)
            .expect("40 MiB and 14 MiB of data fit in 64 MiB");
        assert!(vfs.used <= VFS_MAX_BYTES);
    }

    #[test]
    fn one_append_past_the_slack_budget_leaves_room_for_other_entries() {
        let mut vfs = RequestVfs::default();
        let mib = 1024 * 1024;
        vfs.write("/tmp/a", vec![0; 52 * mib], false).unwrap();
        vfs.write("/tmp/a", b"x".to_vec(), true).unwrap();
        assert!(vfs.used < VFS_MAX_BYTES - mib, "used {}", vfs.used);
        assert!(matches!(
            vfs.mkdir("/tmp/d", false),
            VfsMkdirResult::Created(_)
        ));
        vfs.write("/tmp/d/f", b"small".to_vec(), false).unwrap();
    }

    #[test]
    fn spare_capacity_is_reclaimed_before_a_charge_is_refused() {
        let mut vfs = RequestVfs::default();
        let mib = 1024 * 1024;
        let mut spare = Vec::with_capacity(60 * mib);
        spare.extend_from_slice(&[0; 10]);
        vfs.write("/tmp/spare", spare, false).unwrap();
        vfs.write("/tmp/b", vec![0; 30 * mib], false)
            .expect("the spare capacity is given back");
        assert!(matches!(
            vfs.mkdir("/tmp/d", false),
            VfsMkdirResult::Created(_)
        ));
    }

    /// The spare capacity as a scan of every file finds it.
    fn scanned_spare(vfs: &RequestVfs) -> usize {
        vfs.nodes
            .values()
            .map(|node| match node {
                VfsNode::File(bytes) => bytes.capacity() - bytes.len(),
                VfsNode::Directory { .. } => 0,
            })
            .sum()
    }

    #[test]
    fn the_write_limit_counts_each_files_spare_capacity_once() {
        let mut vfs = RequestVfs::default();
        let mib = 1024 * 1024;
        vfs.write("/tmp/a", vec![0; 20 * mib], false).unwrap();
        vfs.write("/tmp/a", b"x".to_vec(), true).unwrap();
        vfs.write("/tmp/b", vec![0; 10 * mib], false).unwrap();
        vfs.write("/tmp/b", b"x".to_vec(), true).unwrap();
        let spare = scanned_spare(&vfs);
        assert!(spare > 0);
        // What a charge could really find after giving all the spare back.
        let room = VFS_MAX_BYTES - vfs.used + spare;
        assert_eq!(vfs.write_limit("/tmp/a", true), Ok(room));
        let len = 20 * mib + 1;
        assert_eq!(vfs.write_limit("/tmp/a", false), Ok(room + len));
        assert_eq!(
            vfs.write_limit("/tmp/new", false),
            Ok(room - RequestVfs::node_cost("/tmp/new"))
        );
    }

    #[test]
    fn the_spare_total_follows_every_change_to_a_file() {
        let mut vfs = RequestVfs::default();
        let check = |vfs: &RequestVfs| assert_eq!(vfs.spare, scanned_spare(vfs));
        vfs.mkdir("/tmp/d", false);
        for index in 0..50 {
            vfs.write(&format!("/tmp/d/f{index}"), b"ab".to_vec(), true)
                .unwrap();
            vfs.write("/tmp/d/f0", vec![1; 1000 + index], true).unwrap();
        }
        check(&vfs);
        assert!(vfs.spare > 0);
        let mut roomy = Vec::with_capacity(4096);
        roomy.extend_from_slice(b"abc");
        vfs.write("/tmp/roomy", roomy, false).unwrap();
        vfs.write("/tmp/d/f1", vec![0; 10], false).unwrap();
        vfs.write("/tmp/d/f0", b"short".to_vec(), false).unwrap();
        check(&vfs);
        vfs.remove("/tmp/d/f2", false).unwrap();
        vfs.remove("/tmp/roomy", false).unwrap();
        check(&vfs);
        vfs.remove("/tmp/d", true).unwrap();
        check(&vfs);
        assert_eq!(vfs.spare, 0);
        vfs.write("/tmp/g", vec![0; 100], true).unwrap();
        vfs.write("/tmp/g", vec![0; 100], true).unwrap();
        vfs.reclaim_capacity();
        assert_eq!(vfs.spare, 0);
        check(&vfs);
    }

    #[test]
    fn a_refused_charge_with_no_spare_capacity_scans_nothing() {
        let mut vfs = RequestVfs::default();
        let fill = VFS_MAX_BYTES - RequestVfs::node_cost("/tmp/full") - 10;
        vfs.write("/tmp/full", vec![0; fill], false).unwrap();
        assert_eq!(vfs.spare, 0);
        for index in 0..100 {
            assert!(matches!(
                vfs.mkdir(&format!("/tmp/d{index}"), false),
                VfsMkdirResult::Error("ENOSPC")
            ));
        }
        assert_eq!(vfs.reclaim_scans, 0);
        // With spare capacity held, the refusal does look for it.
        vfs.remove("/tmp/full", false).unwrap();
        let mut spare =
            Vec::with_capacity(VFS_MAX_BYTES - RequestVfs::node_cost("/tmp/spare") - 10);
        spare.push(0);
        vfs.write("/tmp/spare", spare, false).unwrap();
        vfs.mkdir("/tmp/d", false);
        assert_eq!(vfs.reclaim_scans, 1);
    }

    #[test]
    fn a_shrinking_overwrite_and_a_removal_give_the_charge_back() {
        let mut vfs = RequestVfs::default();
        vfs.write("/tmp/a", vec![1; 4096], false).unwrap();
        let big = vfs.used;
        vfs.write("/tmp/a", b"ab".to_vec(), false).unwrap();
        assert!(vfs.used < big);
        vfs.remove("/tmp/a", false).unwrap();
        assert_eq!(vfs.used, 0);
    }

    #[test]
    fn write_limit_refuses_what_could_never_be_charged() {
        let mut vfs = RequestVfs::default();
        assert_eq!(vfs.write_limit("/bundle/x", false), Err("EPERM"));
        let new = vfs.write_limit("/tmp/new", false).unwrap();
        assert!(new < VFS_MAX_BYTES && new > VFS_MAX_BYTES - 1024);
        // Replacing a file may use the room the old content gives back.
        vfs.write("/tmp/big", vec![0; VFS_MAX_BYTES / 2], false)
            .unwrap();
        assert!(vfs.write_limit("/tmp/big", false).unwrap() >= VFS_MAX_BYTES - 1024);
        assert!(vfs.write_limit("/tmp/big", true).unwrap() < VFS_MAX_BYTES / 2 + 1024);
    }

    #[test]
    fn a_small_append_near_the_cap_succeeds_when_the_bytes_fit() {
        let mut vfs = RequestVfs::default();
        // Above half the cap, so doubling the capacity would pass it.
        vfs.write("/tmp/big", vec![0; 40 * 1024 * 1024], false)
            .unwrap();
        vfs.write("/tmp/big", b"x".to_vec(), true).unwrap();
        assert!(vfs.used <= VFS_MAX_BYTES);
        let Some(VfsNode::File(bytes)) = vfs.nodes.get("/tmp/big") else {
            panic!("/tmp/big is not a file");
        };
        assert_eq!(bytes.len(), 40 * 1024 * 1024 + 1);
        // What does not fit is still refused, and leaves the file alone.
        assert_eq!(
            vfs.write("/tmp/big", vec![0; VFS_MAX_BYTES], true),
            Err("ENOSPC")
        );
    }

    #[test]
    fn an_append_may_use_the_files_own_unused_capacity() {
        let mut vfs = RequestVfs::default();
        // A file holding 10 MiB in 40 MiB of capacity, and a second file
        // that takes all the room left.
        let mut spare = vec![0; 10 * 1024 * 1024];
        spare.reserve_exact(30 * 1024 * 1024);
        let capacity = spare.capacity();
        vfs.write("/tmp/spare", spare, false).unwrap();
        let room = VFS_MAX_BYTES - vfs.used - RequestVfs::node_cost("/tmp/fill");
        vfs.write("/tmp/fill", vec![0; room], false).unwrap();
        let limit = vfs.write_limit("/tmp/spare", true).unwrap();
        assert!(limit >= capacity - 10 * 1024 * 1024, "limit {limit}");
        let used = vfs.used;
        vfs.write("/tmp/spare", vec![1; 20 * 1024 * 1024], true)
            .unwrap();
        assert_eq!(vfs.used, used, "the spare capacity was already charged");
    }

    #[test]
    fn one_append_claims_a_bounded_slack() {
        let mut vfs = RequestVfs::default();
        let len = 20 * 1024 * 1024;
        vfs.write("/tmp/big", vec![0; len], false).unwrap();
        vfs.write("/tmp/big", b"x".to_vec(), true).unwrap();
        let Some(VfsNode::File(bytes)) = vfs.nodes.get("/tmp/big") else {
            panic!("/tmp/big is not a file");
        };
        assert_eq!(bytes.len(), len + 1);
        assert!(
            bytes.capacity() <= len + 1 + (len + 1) / 4,
            "capacity {}",
            bytes.capacity()
        );
    }

    #[test]
    fn the_root_and_the_read_only_directories_list_as_stat_shows_them() {
        let vfs = RequestVfs::default();
        assert_eq!(vfs.readdir("/").unwrap(), ["bundle", "dev", "tmp"]);
        for name in vfs.readdir("/").unwrap() {
            assert!(matches!(
                vfs.stat(&format!("/{name}")),
                Some(super::VfsStat::Directory { .. })
            ));
        }
        assert_eq!(vfs.readdir("/dev").unwrap(), Vec::<String>::new());
        assert_eq!(vfs.readdir("/nowhere"), Err("ENOENT"));
    }

    #[test]
    fn the_child_index_counts_against_the_cap() {
        let mut vfs = RequestVfs::default();
        let mut path = String::from("/tmp");
        // One-character components: every entry's path is a copy of its
        // parent's key, so the index holds far more than the paths alone.
        loop {
            path.push_str("/a");
            if matches!(vfs.mkdir(&path, false), super::VfsMkdirResult::Error(_)) {
                break;
            }
        }
        let retained: usize = vfs
            .nodes
            .keys()
            .chain(vfs.children.keys())
            .map(String::len)
            .chain(vfs.children.values().flatten().map(String::len))
            .sum();
        assert!(retained <= VFS_MAX_BYTES, "retained {retained} bytes");
        assert!(vfs.used <= VFS_MAX_BYTES);
    }

    #[test]
    fn an_overwrite_moves_its_buffer_into_the_tree() {
        let mut vfs = RequestVfs::default();
        let data = vec![7_u8; 1000];
        let address = data.as_ptr();
        vfs.write("/tmp/new", data, false).unwrap();
        let data = vec![8_u8; 2000];
        let address_again = data.as_ptr();
        vfs.write("/tmp/new", data, false).unwrap();
        let Some(VfsNode::File(bytes)) = vfs.nodes.get("/tmp/new") else {
            panic!("/tmp/new is not a file");
        };
        assert_eq!(bytes.as_ptr(), address_again, "the content was copied");
        assert_ne!(address, address_again);
    }

    #[test]
    fn listing_and_removing_follow_the_child_index() {
        let mut vfs = RequestVfs::default();
        for index in 0..200 {
            vfs.mkdir(&format!("/tmp/d{index}/sub"), true);
            vfs.write(&format!("/tmp/d{index}/sub/f"), vec![1], false)
                .unwrap();
        }
        assert_eq!(vfs.readdir("/tmp").unwrap().len(), 200);
        assert_eq!(vfs.readdir("/tmp/d7").unwrap(), ["sub"]);
        assert_eq!(vfs.readdir("/tmp/d7/sub").unwrap(), ["f"]);
        vfs.remove("/tmp/d7", true).unwrap();
        assert!(!vfs.readdir("/tmp").unwrap().contains(&"d7".to_string()));
        assert_eq!(vfs.readdir("/tmp/d7"), Err("ENOENT"));
        assert!(vfs
            .nodes
            .keys()
            .all(|key| key != "/tmp/d7" && !key.starts_with("/tmp/d7/")));
        for index in (0..200).filter(|index| *index != 7) {
            vfs.remove(&format!("/tmp/d{index}"), true).unwrap();
        }
        assert!(vfs.readdir("/tmp").unwrap().is_empty());
        assert_eq!(vfs.used, 0);
        assert!(vfs
            .children
            .get("/tmp")
            .is_none_or(|names| names.is_empty()));
    }

    #[test]
    fn one_requests_tmp_is_not_visible_to_another() {
        let write = |context: &std::sync::Arc<IoContext>, data: &[u8]| {
            super::lock_vfs(context)
                .write("/tmp/shared.txt", data.to_vec(), false)
                .unwrap();
        };
        let (a, b) = (IoContext::new(), IoContext::new());
        write(&a, b"from a");
        assert!(b.vfs.get().is_none(), "B has no tree until it uses /tmp");
        write(&b, b"from b");
        let read = |context: &std::sync::Arc<IoContext>| {
            super::lock_vfs(context)
                .read("/tmp/shared.txt")
                .unwrap()
                .to_vec()
        };
        assert_eq!(read(&a), b"from a");
        assert_eq!(read(&b), b"from b");
    }
}
