//! An in-memory filesystem.
//!
//! Useful on its own for scratch space and tests, and useful as a reference for
//! what a [`Filesystem`] implementation looks like when it has to do its own
//! path resolution - which is to say, any backend that is not delegating to a
//! real kernel.
//!
//! The tree is inode-based rather than a nested map, which buys three things
//! that matter for POSIX-shaped guests: hard links, `rename` that does not
//! invalidate open handles, and unlinking a file that is still open.

use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, RwLock};
use std::time::SystemTime;

use async_trait::async_trait;

use crate::spi::{
    DescriptorFlags, DescriptorType, DirEntry, ErrorCode, File, Filesystem, FsPath, FsResult,
    MetadataHash, NewTimestamp, ObjectId, OpenFlags, OpenOptions, Opened, SetTimes, Stat,
};

/// The inode of the root directory.
const ROOT: u64 = 1;

/// How many symbolic links may be traversed while resolving one path.
const SYMLINK_LIMIT: usize = 32;

/// The default content budget: 256 MiB.
pub const DEFAULT_MEMORY_BUDGET: u64 = 256 * 1024 * 1024;

/// The default cap on the number of nodes (files, directories, symlinks).
const DEFAULT_MAX_NODES: usize = 1 << 17;

/// The longest permitted name component, as POSIX `NAME_MAX`.
const NAME_MAX: usize = 255;

/// An in-memory filesystem.
///
/// Cloning is shallow: clones share the same tree.
#[derive(Clone)]
pub struct MemoryFilesystem {
    inner: Arc<RwLock<Tree>>,
}

impl Default for MemoryFilesystem {
    fn default() -> Self {
        Self::new()
    }
}

impl MemoryFilesystem {
    /// Creates an empty filesystem containing only a root directory, with the
    /// default content budget of [`DEFAULT_MEMORY_BUDGET`] bytes.
    pub fn new() -> Self {
        Self::with_budget(DEFAULT_MEMORY_BUDGET)
    }

    /// Creates an empty filesystem whose content - file bytes and symlink
    /// targets - may not exceed `budget` bytes.
    ///
    /// The budget keeps guest-driven growth within known bounds: `set-size`,
    /// `write`, and `append` past it fail with `insufficient-space` instead
    /// of allocating host memory. A cap on node count and POSIX's `NAME_MAX`
    /// bound the bookkeeping around the content.
    pub fn with_budget(budget: u64) -> Self {
        Self::with_limits(budget, DEFAULT_MAX_NODES)
    }

    fn with_limits(budget: u64, max_nodes: usize) -> Self {
        let mut nodes = HashMap::new();
        let mut root = Node::new_dir();
        // The root is never the target of `link`/`unlink`, so its count is
        // fixed here: one name, forever.
        root.links = 1;
        nodes.insert(ROOT, root);
        Self {
            inner: Arc::new(RwLock::new(Tree {
                nodes,
                next_inode: ROOT + 1,
                used: 0,
                budget,
                max_nodes,
            })),
        }
    }

    /// Creates a filesystem seeded with the given files.
    ///
    /// Parent directories are created as needed. Intended for tests and for
    /// small fixed content; anything large belongs in a real backend.
    pub fn with_files<P, C>(files: impl IntoIterator<Item = (P, C)>) -> FsResult<Self>
    where
        P: AsRef<str>,
        C: Into<Vec<u8>>,
    {
        let fs = Self::new();
        {
            let mut tree = fs.write();
            for (path, contents) in files {
                let path = FsPath::new(path.as_ref())?;
                let (parent, name) = tree.resolve_parent_creating(path)?;
                let contents: Vec<u8> = contents.into();
                let len = contents.len();
                tree.charge(len)?;
                let inode = match tree.alloc(Node::new_file(contents)) {
                    Ok(inode) => inode,
                    Err(err) => {
                        tree.credit(len);
                        return Err(err);
                    }
                };
                if let Err(err) = tree.link(parent, &name, inode) {
                    tree.collect(inode);
                    return Err(err);
                }
            }
        }
        Ok(fs)
    }

    fn read(&self) -> std::sync::RwLockReadGuard<'_, Tree> {
        self.inner.read().unwrap_or_else(|e| e.into_inner())
    }

    fn write(&self) -> std::sync::RwLockWriteGuard<'_, Tree> {
        self.inner.write().unwrap_or_else(|e| e.into_inner())
    }
}

#[async_trait]
impl Filesystem for MemoryFilesystem {
    fn summary(&self) -> String {
        "memory".into()
    }

    async fn open(&self, path: &FsPath, opts: OpenOptions) -> FsResult<Opened> {
        let mut tree = self.write();

        let creating = opts.open_flags.contains(OpenFlags::CREATE);
        let want_dir_spelling = path.requires_directory();
        let inode = if creating {
            // O_CREAT|O_EXCL does not follow a final symlink: a link there -
            // even a dangling one - means "the path exists".
            let exclusive = opts.open_flags.contains(OpenFlags::EXCLUSIVE);
            let follow_final = !exclusive && opts.follow_symlinks;
            let components: Vec<&str> = path.components().collect();
            match tree.walk(&components, follow_final, true)? {
                Lookup::Exists(_) if exclusive => return Err(ErrorCode::Exist),
                Lookup::Exists(inode) => inode,
                Lookup::Missing { dir, name } => {
                    if opts.open_flags.contains(OpenFlags::DIRECTORY) {
                        return Err(ErrorCode::NoEntry);
                    }
                    if want_dir_spelling {
                        // `open("newdir/", O_CREAT)` may not create a regular
                        // file; POSIX reports `EISDIR`.
                        return Err(ErrorCode::IsDirectory);
                    }
                    let inode = tree.alloc(Node::new_file(Vec::new()))?;
                    if let Err(err) = tree.link(dir, &name, inode) {
                        tree.collect(inode);
                        return Err(err);
                    }
                    inode
                }
            }
        } else {
            tree.resolve(path, opts.follow_symlinks)?
        };

        let node = tree.node(inode)?;
        match &node.kind {
            NodeKind::Dir(_) => {
                // POSIX: `O_CREAT|O_EXCL` on an existing directory is
                // `EEXIST` (handled by the walk above); plain `O_CREAT` is
                // `EISDIR`. Directories can otherwise be opened for mutation
                // - that is `mutate-directory`, carried in `flags` - but not
                // for writing bytes.
                if creating
                    || opts.flags.contains(DescriptorFlags::WRITE)
                    || opts.open_flags.contains(OpenFlags::TRUNCATE)
                {
                    return Err(ErrorCode::IsDirectory);
                }
                Ok(Opened::Dir)
            }
            _ if want_dir_spelling => Err(ErrorCode::NotDirectory),
            NodeKind::Symlink(_) => {
                // We only get here with `follow_symlinks` false, which is
                // `O_NOFOLLOW`; POSIX reports that as `ELOOP`.
                Err(ErrorCode::Loop)
            }
            NodeKind::File(_) => {
                if opts.open_flags.contains(OpenFlags::DIRECTORY) {
                    return Err(ErrorCode::NotDirectory);
                }
                if opts.open_flags.contains(OpenFlags::TRUNCATE) {
                    let now = SystemTime::now();
                    let node = tree.node_mut(inode)?;
                    let mut freed = 0;
                    if let NodeKind::File(contents) = &mut node.kind {
                        freed = contents.len();
                        contents.clear();
                        contents.shrink_to_fit();
                    }
                    node.mtime = now;
                    node.ctime = now;
                    tree.credit(freed);
                }
                tree.node_mut(inode)?.open_handles += 1;
                Ok(Opened::File(Arc::new(MemoryFile {
                    fs: self.clone(),
                    inode,
                })))
            }
        }
    }

    async fn stat_at(&self, path: &FsPath, follow: bool) -> FsResult<Stat> {
        let tree = self.read();
        let inode = tree.resolve(path, follow)?;
        Ok(tree.node(inode)?.stat())
    }

    async fn set_times_at(&self, path: &FsPath, follow: bool, times: SetTimes) -> FsResult<()> {
        let mut tree = self.write();
        let inode = tree.resolve(path, follow)?;
        tree.node_mut(inode)?.apply_times(times);
        Ok(())
    }

    async fn read_dir(&self, path: &FsPath) -> FsResult<Vec<DirEntry>> {
        let tree = self.read();
        let inode = tree.resolve(path, true)?;
        let NodeKind::Dir(entries) = &tree.node(inode)?.kind else {
            return Err(ErrorCode::NotDirectory);
        };
        entries
            .iter()
            .map(|(name, &child)| {
                Ok(DirEntry {
                    type_: tree.node(child)?.type_(),
                    name: name.clone(),
                })
            })
            .collect()
    }

    async fn create_dir(&self, path: &FsPath) -> FsResult<()> {
        let mut tree = self.write();
        let (parent, name) = tree.resolve_parent(path)?;
        if tree.child(parent, &name)?.is_some() {
            return Err(ErrorCode::Exist);
        }
        let inode = tree.alloc(Node::new_dir())?;
        if let Err(err) = tree.link(parent, &name, inode) {
            tree.collect(inode);
            return Err(err);
        }
        Ok(())
    }

    async fn remove_dir(&self, path: &FsPath) -> FsResult<()> {
        let mut tree = self.write();
        let (parent, name) = tree.resolve_parent(path)?;
        let inode = tree.child(parent, &name)?.ok_or(ErrorCode::NoEntry)?;
        match &tree.node(inode)?.kind {
            NodeKind::Dir(entries) if !entries.is_empty() => return Err(ErrorCode::NotEmpty),
            NodeKind::Dir(_) => {}
            _ => return Err(ErrorCode::NotDirectory),
        }
        tree.unlink(parent, &name)
    }

    async fn unlink(&self, path: &FsPath) -> FsResult<()> {
        let mut tree = self.write();
        let (parent, name) = tree.resolve_parent(path)?;
        let inode = tree.child(parent, &name)?.ok_or(ErrorCode::NoEntry)?;
        if matches!(tree.node(inode)?.kind, NodeKind::Dir(_)) {
            return Err(ErrorCode::IsDirectory);
        }
        tree.unlink(parent, &name)
    }

    async fn rename(&self, from: &FsPath, to: &FsPath) -> FsResult<()> {
        let mut tree = self.write();
        let (from_parent, from_name) = tree.resolve_parent(from)?;
        let (to_parent, to_name) = tree.resolve_parent(to)?;
        let inode = tree
            .child(from_parent, &from_name)?
            .ok_or(ErrorCode::NoEntry)?;

        // POSIX: if both names already refer to the same object, do nothing -
        // and in particular do not remove either name.
        if tree.child(to_parent, &to_name)? == Some(inode) {
            return Ok(());
        }

        // Refuse to move a directory inside itself, which would orphan the
        // subtree and, worse, make it unreachable but still linked. This
        // must precede every mutation: a refused rename may not have
        // side effects, least of all unlinking its destination.
        if matches!(tree.node(inode)?.kind, NodeKind::Dir(_))
            && tree.is_ancestor(inode, to_parent)?
        {
            return Err(ErrorCode::Invalid);
        }

        if let Some(existing) = tree.child(to_parent, &to_name)? {
            let source_is_dir = matches!(tree.node(inode)?.kind, NodeKind::Dir(_));
            match &tree.node(existing)?.kind {
                NodeKind::Dir(entries) => {
                    if !source_is_dir {
                        return Err(ErrorCode::IsDirectory);
                    }
                    if !entries.is_empty() {
                        return Err(ErrorCode::NotEmpty);
                    }
                }
                _ if source_is_dir => return Err(ErrorCode::NotDirectory),
                _ => {}
            }
            tree.unlink(to_parent, &to_name)?;
        }

        tree.detach(from_parent, &from_name)?;
        tree.attach(to_parent, &to_name, inode)?;
        Ok(())
    }

    async fn symlink(&self, target: &str, link: &FsPath) -> FsResult<()> {
        if target.starts_with('/') {
            // WASI: "If `old-path` starts with `/`, the function fails with
            // `error-code::not-permitted`."
            return Err(ErrorCode::NotPermitted);
        }
        let mut tree = self.write();
        let (parent, name) = tree.resolve_parent(link)?;
        if tree.child(parent, &name)?.is_some() {
            return Err(ErrorCode::Exist);
        }
        tree.charge(target.len())?;
        let inode = match tree.alloc(Node::new_symlink(target.to_owned())) {
            Ok(inode) => inode,
            Err(err) => {
                tree.credit(target.len());
                return Err(err);
            }
        };
        if let Err(err) = tree.link(parent, &name, inode) {
            tree.collect(inode);
            return Err(err);
        }
        Ok(())
    }

    async fn readlink(&self, path: &FsPath) -> FsResult<String> {
        let tree = self.read();
        let inode = tree.resolve(path, false)?;
        match &tree.node(inode)?.kind {
            NodeKind::Symlink(target) => Ok(target.clone()),
            _ => Err(ErrorCode::Invalid),
        }
    }

    async fn hard_link(&self, from: &FsPath, follow: bool, to: &FsPath) -> FsResult<()> {
        let mut tree = self.write();
        let inode = tree.resolve(from, follow)?;
        if matches!(tree.node(inode)?.kind, NodeKind::Dir(_)) {
            return Err(ErrorCode::NotPermitted);
        }
        let (parent, name) = tree.resolve_parent(to)?;
        if tree.child(parent, &name)?.is_some() {
            return Err(ErrorCode::Exist);
        }
        tree.link(parent, &name, inode)
    }

    async fn metadata_hash_at(&self, path: &FsPath, follow: bool) -> FsResult<MetadataHash> {
        let tree = self.read();
        let inode = tree.resolve(path, follow)?;
        Ok(MetadataHash::from_stat(&tree.node(inode)?.stat()))
    }

    async fn object_id_at(&self, path: &FsPath, follow: bool) -> FsResult<ObjectId> {
        let tree = self.read();
        Ok(ObjectId(tree.resolve(path, follow)? as u128))
    }
}

/// An open file in a [`MemoryFilesystem`].
struct MemoryFile {
    fs: MemoryFilesystem,
    inode: u64,
}

impl MemoryFile {
    fn contents<'a>(&self, tree: &'a Tree) -> FsResult<&'a Vec<u8>> {
        match &tree.node(self.inode)?.kind {
            NodeKind::File(contents) => Ok(contents),
            _ => Err(ErrorCode::BadDescriptor),
        }
    }
}

impl Drop for MemoryFile {
    fn drop(&mut self) {
        let mut tree = self.fs.write();
        if let Some(node) = tree.nodes.get_mut(&self.inode) {
            node.open_handles = node.open_handles.saturating_sub(1);
        }
        tree.collect(self.inode);
    }
}

#[async_trait]
impl File for MemoryFile {
    async fn read_at(&self, buf: &mut [u8], offset: u64) -> FsResult<usize> {
        let tree = self.fs.read();
        let contents = self.contents(&tree)?;
        let Ok(offset) = usize::try_from(offset) else {
            return Ok(0);
        };
        if offset >= contents.len() {
            return Ok(0);
        }
        let n = buf.len().min(contents.len() - offset);
        buf[..n].copy_from_slice(&contents[offset..offset + n]);
        Ok(n)
    }

    async fn write_at(&self, buf: &[u8], offset: u64) -> FsResult<usize> {
        let mut tree = self.fs.write();
        if buf.is_empty() {
            // POSIX: a zero-length `pwrite` detects errors but neither
            // extends the file nor touches its timestamps.
            self.contents(&tree)?;
            return Ok(0);
        }
        let offset = usize::try_from(offset).map_err(|_| ErrorCode::FileTooLarge)?;
        let end = offset
            .checked_add(buf.len())
            .ok_or(ErrorCode::FileTooLarge)?;
        // Budget first: the charge is refused - and nothing is allocated -
        // when an offset or size would exceed the tree's byte budget.
        let grow = end.saturating_sub(self.contents(&tree)?.len());
        if grow > 0 {
            tree.charge(grow)?;
        }
        let now = SystemTime::now();
        let node = tree.node_mut(self.inode).expect("probed by contents()");
        let NodeKind::File(contents) = &mut node.kind else {
            unreachable!("probed as a file by contents()");
        };
        if grow > 0 && contents.try_reserve(grow).is_err() {
            tree.credit(grow);
            return Err(ErrorCode::InsufficientSpace);
        }
        if end > contents.len() {
            // Writing past the end zero-fills the gap, as WASI requires.
            contents.resize(end, 0);
        }
        contents[offset..end].copy_from_slice(buf);
        node.mtime = now;
        node.ctime = now;
        Ok(buf.len())
    }

    async fn append(&self, buf: &[u8]) -> FsResult<usize> {
        let mut tree = self.fs.write();
        self.contents(&tree)?;
        tree.charge(buf.len())?;
        let now = SystemTime::now();
        let node = tree.node_mut(self.inode).expect("probed by contents()");
        let NodeKind::File(contents) = &mut node.kind else {
            unreachable!("probed as a file by contents()");
        };
        if contents.try_reserve(buf.len()).is_err() {
            tree.credit(buf.len());
            return Err(ErrorCode::InsufficientSpace);
        }
        contents.extend_from_slice(buf);
        node.mtime = now;
        node.ctime = now;
        Ok(buf.len())
    }

    async fn stat(&self) -> FsResult<Stat> {
        let tree = self.fs.read();
        Ok(tree.node(self.inode)?.stat())
    }

    async fn set_size(&self, size: u64) -> FsResult<()> {
        let mut tree = self.fs.write();
        let size = usize::try_from(size).map_err(|_| ErrorCode::FileTooLarge)?;
        let old = self.contents(&tree)?.len();
        if size > old {
            tree.charge(size - old)?;
        }
        let now = SystemTime::now();
        let node = tree.node_mut(self.inode).expect("probed by contents()");
        let NodeKind::File(contents) = &mut node.kind else {
            unreachable!("probed as a file by contents()");
        };
        if size > old {
            if contents.try_reserve(size - old).is_err() {
                tree.credit(size - old);
                return Err(ErrorCode::InsufficientSpace);
            }
            contents.resize(size, 0);
        } else {
            contents.truncate(size);
            // Release the capacity too: the budget tracks logical bytes, so
            // freed bytes must actually be free.
            contents.shrink_to_fit();
        }
        node.mtime = now;
        node.ctime = now;
        if size < old {
            tree.credit(old - size);
        }
        Ok(())
    }

    async fn set_times(&self, times: SetTimes) -> FsResult<()> {
        let mut tree = self.fs.write();
        tree.node_mut(self.inode)?.apply_times(times);
        Ok(())
    }

    async fn sync(&self) -> FsResult<()> {
        Ok(())
    }

    async fn sync_data(&self) -> FsResult<()> {
        Ok(())
    }

    async fn metadata_hash(&self) -> FsResult<MetadataHash> {
        let tree = self.fs.read();
        Ok(MetadataHash::from_stat(&tree.node(self.inode)?.stat()))
    }

    async fn object_id(&self) -> FsResult<ObjectId> {
        Ok(ObjectId(self.inode as u128))
    }
}

/// The tree itself. Everything here runs under the filesystem's lock and does
/// no I/O, so none of it is `async`.
struct Tree {
    nodes: HashMap<u64, Node>,
    next_inode: u64,
    /// Bytes of content - file bytes and symlink targets - currently stored.
    used: u64,
    /// Content bytes the tree may hold; growth beyond this is refused.
    budget: u64,
    /// Nodes the tree may hold, bounding bookkeeping memory.
    max_nodes: usize,
}

/// The result of a [`Tree::walk`].
enum Lookup {
    /// The path names this existing inode.
    Exists(u64),
    /// Everything up to the final component exists; the final component does
    /// not. A create can land as `name` in the directory `dir`.
    Missing { dir: u64, name: String },
}

impl Tree {
    fn node(&self, inode: u64) -> FsResult<&Node> {
        self.nodes.get(&inode).ok_or(ErrorCode::NoEntry)
    }

    fn node_mut(&mut self, inode: u64) -> FsResult<&mut Node> {
        self.nodes.get_mut(&inode).ok_or(ErrorCode::NoEntry)
    }

    /// Reserves `bytes` of the content budget, refusing to overcommit.
    fn charge(&mut self, bytes: usize) -> FsResult<()> {
        let used = self
            .used
            .checked_add(bytes as u64)
            .ok_or(ErrorCode::InsufficientSpace)?;
        if used > self.budget {
            return Err(ErrorCode::InsufficientSpace);
        }
        self.used = used;
        Ok(())
    }

    /// Returns `bytes` of the content budget.
    fn credit(&mut self, bytes: usize) {
        self.used = self.used.saturating_sub(bytes as u64);
    }

    fn alloc(&mut self, node: Node) -> FsResult<u64> {
        if self.nodes.len() >= self.max_nodes {
            return Err(ErrorCode::InsufficientSpace);
        }
        let inode = self.next_inode;
        self.next_inode += 1;
        self.nodes.insert(inode, node);
        Ok(inode)
    }

    fn child(&self, dir: u64, name: &str) -> FsResult<Option<u64>> {
        match &self.node(dir)?.kind {
            NodeKind::Dir(entries) => Ok(entries.get(name).copied()),
            _ => Err(ErrorCode::NotDirectory),
        }
    }

    /// Adds a directory entry and bumps the target's link count.
    fn link(&mut self, dir: u64, name: &str, inode: u64) -> FsResult<()> {
        self.attach(dir, name, inode)?;
        self.node_mut(inode)?.links += 1;
        Ok(())
    }

    /// Removes a directory entry and drops the target's link count, freeing the
    /// node if nothing refers to it any more.
    fn unlink(&mut self, dir: u64, name: &str) -> FsResult<()> {
        let inode = self.detach(dir, name)?;
        let node = self.node_mut(inode)?;
        node.links = node.links.saturating_sub(1);
        self.collect(inode);
        Ok(())
    }

    /// Adds a directory entry without touching link counts.
    fn attach(&mut self, dir: u64, name: &str, inode: u64) -> FsResult<()> {
        if name.len() > NAME_MAX {
            return Err(ErrorCode::NameTooLong);
        }
        let now = SystemTime::now();
        let node = self.node_mut(dir)?;
        let NodeKind::Dir(entries) = &mut node.kind else {
            return Err(ErrorCode::NotDirectory);
        };
        entries.insert(name.to_owned(), inode);
        node.mtime = now;
        node.ctime = now;
        Ok(())
    }

    /// Removes a directory entry without touching link counts.
    fn detach(&mut self, dir: u64, name: &str) -> FsResult<u64> {
        let now = SystemTime::now();
        let node = self.node_mut(dir)?;
        let NodeKind::Dir(entries) = &mut node.kind else {
            return Err(ErrorCode::NotDirectory);
        };
        let inode = entries.remove(name).ok_or(ErrorCode::NoEntry)?;
        node.mtime = now;
        node.ctime = now;
        Ok(inode)
    }

    /// Frees a node once it has no directory entries and no open handles.
    ///
    /// This is what makes unlinking an open file behave like POSIX: the name
    /// goes away immediately, the bytes stick around until the last handle is
    /// dropped.
    fn collect(&mut self, inode: u64) {
        if let Some(node) = self.nodes.get(&inode)
            && node.links == 0
            && node.open_handles == 0
        {
            let node = self.nodes.remove(&inode).expect("observed just above");
            self.credit(node.content_bytes());
        }
    }

    /// True if `ancestor` is `inode` or one of its ancestors.
    fn is_ancestor(&self, ancestor: u64, inode: u64) -> FsResult<bool> {
        // Nodes have no parent pointers, so search downwards from `ancestor`
        // instead. Directories form a tree - hard links to directories are
        // refused - so this terminates.
        let mut stack = vec![ancestor];
        while let Some(current) = stack.pop() {
            if current == inode {
                return Ok(true);
            }
            if let NodeKind::Dir(entries) = &self.node(current)?.kind {
                stack.extend(entries.values().copied());
            }
        }
        Ok(false)
    }

    /// Resolves `path` to an inode, following symbolic links along the way and,
    /// if `follow_final` is set, at the final component too.
    fn resolve(&self, path: &FsPath, follow_final: bool) -> FsResult<u64> {
        let components: Vec<&str> = path.components().collect();
        match self.walk(&components, follow_final, false)? {
            Lookup::Exists(inode) => {
                if path.requires_directory() && !matches!(self.node(inode)?.kind, NodeKind::Dir(_))
                {
                    return Err(ErrorCode::NotDirectory);
                }
                Ok(inode)
            }
            Lookup::Missing { .. } => Err(ErrorCode::NoEntry),
        }
    }

    /// Walks `components` from the root.
    ///
    /// The walk keeps an explicit stack of the directories it has entered, so
    /// `..` is a pop rather than a lexical rewrite. That is what makes
    /// `a/link/..` land where POSIX says it should - in `link`'s target's
    /// parent - and what makes a `..` that would step above the root
    /// detectable rather than silently clamped.
    ///
    /// With `create` set, a missing *final* component is reported as
    /// [`Lookup::Missing`] - the directory and name where a new object would
    /// go - instead of as an error. Because symlink targets are spliced into
    /// the walk, a dangling final symlink yields its *target's* location,
    /// which is where POSIX says `O_CREAT` creates.
    fn walk(&self, components: &[&str], follow_final: bool, create: bool) -> FsResult<Lookup> {
        let mut stack = vec![ROOT];
        // Remaining components, reversed so the next one is a `pop`.
        let mut queue: Vec<String> = components.iter().rev().map(|c| (*c).to_owned()).collect();
        let mut budget = SYMLINK_LIMIT;

        while let Some(component) = queue.pop() {
            let current = *stack.last().expect("stack always holds at least the root");

            if component == ".." {
                // `f/..` must fail like `f/x` would: resolution does not
                // pass *through* a non-directory in either direction.
                if !matches!(self.node(current)?.kind, NodeKind::Dir(_)) {
                    return Err(ErrorCode::NotDirectory);
                }
                if stack.len() == 1 {
                    // Stepping above the root leaves the sandbox.
                    return Err(ErrorCode::NotPermitted);
                }
                stack.pop();
                continue;
            }

            let is_final = queue.is_empty();
            let Some(child) = self.child(current, &component)? else {
                if is_final && create {
                    return Ok(Lookup::Missing {
                        dir: current,
                        name: component,
                    });
                }
                return Err(ErrorCode::NoEntry);
            };

            if let NodeKind::Symlink(target) = &self.node(child)?.kind
                && (!is_final || follow_final)
            {
                if target.starts_with('/') {
                    // A rooted link target would escape the sandbox.
                    return Err(ErrorCode::NotPermitted);
                }
                if budget == 0 {
                    return Err(ErrorCode::Loop);
                }
                budget -= 1;
                // Splice the target's components in ahead of what is left. The
                // link resolves relative to the directory holding it, so the
                // stack does not move.
                for part in target
                    .split('/')
                    .filter(|c| !c.is_empty() && *c != ".")
                    .rev()
                {
                    queue.push(part.to_owned());
                }
                continue;
            }

            stack.push(child);
        }

        Ok(Lookup::Exists(
            *stack.last().expect("stack always holds at least the root"),
        ))
    }

    /// Splits `path` into its parent directory's inode and its final component.
    fn resolve_parent(&self, path: &FsPath) -> FsResult<(u64, String)> {
        let components: Vec<&str> = path.components().collect();
        let Some((name, parents)) = components.split_last() else {
            // The root has no parent, so no operation that needs one applies.
            return Err(ErrorCode::NotPermitted);
        };
        if *name == ".." {
            return Err(ErrorCode::NotPermitted);
        }
        let parent = match self.walk(parents, true, false)? {
            Lookup::Exists(inode) => inode,
            Lookup::Missing { .. } => return Err(ErrorCode::NoEntry),
        };
        if !matches!(self.node(parent)?.kind, NodeKind::Dir(_)) {
            return Err(ErrorCode::NotDirectory);
        }
        Ok((parent, (*name).to_owned()))
    }

    /// Like [`Tree::resolve_parent`], but creates missing intermediate
    /// directories. Only used when seeding a filesystem.
    fn resolve_parent_creating(&mut self, path: &FsPath) -> FsResult<(u64, String)> {
        let components: Vec<String> = path.components().map(ToOwned::to_owned).collect();
        let Some((name, parents)) = components.split_last() else {
            return Err(ErrorCode::NotPermitted);
        };
        let mut current = ROOT;
        for parent in parents {
            current = match self.child(current, parent)? {
                Some(inode) => inode,
                None => {
                    let inode = self.alloc(Node::new_dir())?;
                    if let Err(err) = self.link(current, parent, inode) {
                        self.collect(inode);
                        return Err(err);
                    }
                    inode
                }
            };
        }
        Ok((current, name.clone()))
    }
}

struct Node {
    kind: NodeKind,
    /// Directory entries pointing at this node.
    links: u64,
    /// Live [`MemoryFile`] handles for this node.
    open_handles: u64,
    atime: SystemTime,
    mtime: SystemTime,
    ctime: SystemTime,
}

enum NodeKind {
    Dir(BTreeMap<String, u64>),
    File(Vec<u8>),
    Symlink(String),
}

impl Node {
    /// The bytes this node holds against the content budget.
    fn content_bytes(&self) -> usize {
        match &self.kind {
            NodeKind::Dir(_) => 0,
            NodeKind::File(contents) => contents.len(),
            NodeKind::Symlink(target) => target.len(),
        }
    }
}

impl Node {
    fn new(kind: NodeKind) -> Self {
        let now = SystemTime::now();
        Self {
            kind,
            links: 0,
            open_handles: 0,
            atime: now,
            mtime: now,
            ctime: now,
        }
    }

    fn new_dir() -> Self {
        Self::new(NodeKind::Dir(BTreeMap::new()))
    }

    fn new_file(contents: Vec<u8>) -> Self {
        Self::new(NodeKind::File(contents))
    }

    fn new_symlink(target: String) -> Self {
        Self::new(NodeKind::Symlink(target))
    }

    fn type_(&self) -> DescriptorType {
        match self.kind {
            NodeKind::Dir(_) => DescriptorType::Directory,
            NodeKind::File(_) => DescriptorType::RegularFile,
            NodeKind::Symlink(_) => DescriptorType::SymbolicLink,
        }
    }

    fn size(&self) -> u64 {
        match &self.kind {
            NodeKind::Dir(entries) => entries.len() as u64,
            NodeKind::File(contents) => contents.len() as u64,
            NodeKind::Symlink(target) => target.len() as u64,
        }
    }

    fn stat(&self) -> Stat {
        Stat {
            type_: self.type_(),
            // Truthful even at zero: POSIX reports no links for a file that
            // has been unlinked while a handle keeps it alive.
            link_count: self.links,
            size: self.size(),
            data_access_timestamp: Some(self.atime),
            data_modification_timestamp: Some(self.mtime),
            status_change_timestamp: Some(self.ctime),
        }
    }

    fn apply_times(&mut self, times: SetTimes) {
        let now = SystemTime::now();
        match times.access {
            NewTimestamp::NoChange => {}
            NewTimestamp::Now => self.atime = now,
            NewTimestamp::Timestamp(t) => self.atime = t,
        }
        match times.modification {
            NewTimestamp::NoChange => {}
            NewTimestamp::Now => self.mtime = now,
            NewTimestamp::Timestamp(t) => self.mtime = t,
        }
        if !times.is_noop() {
            self.ctime = now;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn p(path: &str) -> &FsPath {
        FsPath::new(path).unwrap()
    }

    async fn create(fs: &MemoryFilesystem, path: &str) -> Arc<dyn File> {
        match fs
            .open(
                p(path),
                OpenOptions {
                    open_flags: OpenFlags::CREATE,
                    flags: DescriptorFlags::READ | DescriptorFlags::WRITE,
                    follow_symlinks: true,
                },
            )
            .await
            .unwrap()
        {
            Opened::File(file) => file,
            Opened::Dir => panic!("expected a file"),
        }
    }

    /// The case that motivated the budget: a `set-size` far past available
    /// memory returns `insufficient-space` without allocating.
    #[tokio::test]
    async fn oversized_set_size_is_refused_not_allocated() {
        let fs = MemoryFilesystem::with_limits(1024, 16);
        let file = create(&fs, "f").await;
        assert_eq!(
            file.set_size(4 << 50).await.unwrap_err(),
            ErrorCode::InsufficientSpace
        );
        assert_eq!(
            file.set_size(2048).await.unwrap_err(),
            ErrorCode::InsufficientSpace
        );
        file.set_size(512).await.unwrap();
    }

    #[tokio::test]
    async fn oversized_write_offset_is_refused() {
        let fs = MemoryFilesystem::with_limits(1024, 16);
        let file = create(&fs, "f").await;
        assert_eq!(
            file.write_at(b"x", 1 << 40).await.unwrap_err(),
            ErrorCode::InsufficientSpace
        );
        assert_eq!(
            file.append(&[0; 2048]).await.unwrap_err(),
            ErrorCode::InsufficientSpace
        );
    }

    /// POSIX: a zero-length positional write is a no-op - in particular it
    /// does not extend the file to its offset.
    #[tokio::test]
    async fn empty_write_does_not_extend() {
        let fs = MemoryFilesystem::new();
        let file = create(&fs, "f").await;
        assert_eq!(file.write_at(b"", 1000).await.unwrap(), 0);
        assert_eq!(file.stat().await.unwrap().size, 0);
    }

    #[tokio::test]
    async fn budget_is_reclaimed_by_shrink_and_unlink() {
        let fs = MemoryFilesystem::with_limits(1024, 16);
        let file = create(&fs, "a").await;
        file.set_size(1024).await.unwrap();
        // Full: nothing left for another byte anywhere.
        let other = create(&fs, "b").await;
        assert_eq!(
            other.append(b"x").await.unwrap_err(),
            ErrorCode::InsufficientSpace
        );
        // Shrinking frees...
        file.set_size(512).await.unwrap();
        other.append(&[0; 512]).await.unwrap();
        // ...and so does unlinking, once the handle is gone.
        drop(file);
        fs.unlink(p("a")).await.unwrap();
        other.append(&[0; 512]).await.unwrap();
    }

    #[tokio::test]
    async fn symlink_targets_count_against_the_budget() {
        let fs = MemoryFilesystem::with_limits(64, 16);
        fs.symlink(&"t".repeat(65), p("l")).await.unwrap_err();
        fs.symlink(&"t".repeat(64), p("l")).await.unwrap();
        assert_eq!(
            fs.symlink("x", p("m")).await.unwrap_err(),
            ErrorCode::InsufficientSpace
        );
        fs.unlink(p("l")).await.unwrap();
        fs.symlink("x", p("m")).await.unwrap();
    }

    #[tokio::test]
    async fn node_count_is_capped() {
        // Root plus three nodes fit; a fourth does not.
        let fs = MemoryFilesystem::with_limits(1024, 4);
        create(&fs, "a").await;
        fs.create_dir(p("d")).await.unwrap();
        create(&fs, "d/b").await;
        assert_eq!(
            fs.create_dir(p("e")).await.unwrap_err(),
            ErrorCode::InsufficientSpace
        );
        // Reclaiming a node makes room again.
        fs.unlink(p("a")).await.unwrap();
        fs.create_dir(p("e")).await.unwrap();
    }

    #[tokio::test]
    async fn names_longer_than_name_max_are_refused() {
        let fs = MemoryFilesystem::new();
        assert_eq!(
            fs.create_dir(p(&"n".repeat(256))).await.unwrap_err(),
            ErrorCode::NameTooLong
        );
        fs.create_dir(p(&"n".repeat(255))).await.unwrap();
    }
}
