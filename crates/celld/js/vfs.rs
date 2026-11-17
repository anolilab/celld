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
}

/// The most memory one request's `/tmp` may hold.
const VFS_MAX_BYTES: usize = 64 * 1024 * 1024;

/// Charged for each entry on top of its path and bytes.
const VFS_NODE_OVERHEAD: usize = 64;

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
            children: HashMap::new(),
            used: 0,
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
    fn charge(&mut self, extra: usize) -> Result<(), &'static str> {
        match self.used.checked_add(extra) {
            Some(total) if total <= VFS_MAX_BYTES => {
                self.used = total;
                Ok(())
            }
            _ => Err("ENOSPC"),
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
                let before = old.capacity();
                let after = if append {
                    // The room this file may take: what is free plus what it
                    // already holds.
                    let budget = VFS_MAX_BYTES.saturating_sub(self.used) + before;
                    Self::grown_capacity(before, old.len(), data.len(), budget)
                } else {
                    data.capacity()
                };
                self.charge_capacity(before, after)?;
                let Some(VfsNode::File(bytes)) = self.nodes.get_mut(path) else {
                    unreachable!("the node was a file a moment ago");
                };
                if append {
                    // The capacity was charged above, so grow to exactly it.
                    bytes.reserve_exact(after - bytes.len());
                    bytes.extend_from_slice(&data);
                } else {
                    *bytes = data;
                }
                Ok(())
            }
            None => {
                self.charge(Self::node_cost(path).saturating_add(data.capacity()))?;
                self.insert(path.to_string(), VfsNode::File(data));
                Ok(())
            }
        }
    }

    /// The capacity an append needs: unchanged while it fits, otherwise
    /// doubled, so n small appends reallocate O(log n) times. Doubling stops
    /// at `budget`, the most capacity the file may take, so an append whose
    /// bytes fit is never refused for the room its growth would have wanted.
    fn grown_capacity(capacity: usize, len: usize, extra: usize, budget: usize) -> usize {
        let needed = len.saturating_add(extra);
        if needed <= capacity {
            capacity
        } else {
            needed.max(capacity.saturating_mul(2).min(budget))
        }
    }

    /// Charge a file's move from capacity `before` to `after`, or release the
    /// difference when it shrinks.
    fn charge_capacity(&mut self, before: usize, after: usize) -> Result<(), &'static str> {
        if after > before {
            self.charge(after - before)
        } else {
            self.used -= before - after;
            Ok(())
        }
    }

    /// The most bytes a write to `path` could carry and still be charged, or
    /// the error the write would answer anyway. Lets a caller refuse an
    /// oversized buffer before copying it.
    fn write_limit(&self, path: &str, append: bool) -> Result<usize, &'static str> {
        if !Self::writable_path(path) {
            return Err("EPERM");
        }
        let remaining = VFS_MAX_BYTES - self.used;
        Ok(match self.nodes.get(path) {
            Some(VfsNode::File(old)) if !append => remaining.saturating_add(old.capacity()),
            None => remaining.saturating_sub(Self::node_cost(path)),
            Some(_) => remaining,
        })
    }

    fn read(&self, path: &str) -> Option<&[u8]> {
        match self.nodes.get(path) {
            Some(VfsNode::File(bytes)) => Some(bytes),
            _ => None,
        }
    }

    /// The names directly inside a directory of the writable tree, sorted.
    fn readdir(&self, path: &str) -> Result<Vec<String>, &'static str> {
        if !Self::writable_path(path) {
            return Err("ENOENT");
        }
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
                    Some(VfsNode::File(bytes)) => bytes.capacity(),
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

/// The names in a directory of the request's `/tmp` as an array, or a Node
/// error code as a string.
pub(super) fn op_vfs_readdir(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    mut rv: v8::ReturnValue<v8::Value>,
) {
    let path = args.get(0).to_rust_string_lossy(scope);
    match read_vfs(scope, |vfs| vfs.readdir(&path)) {
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
    use super::{IoContext, RequestVfs, VfsNode, VFS_MAX_BYTES};

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
