//! A 9P2000 server over any [`Filesystem`].
//!
//! # Model
//!
//! - The [`Filesystem`] answers questions about the *tree*: walk one name,
//!   stat a qid, list a directory, open a file. These calls are synchronous
//!   and run on the connection's task, so they should be quick (in-memory
//!   lookups, snapshots).
//! - Each successful `Topen`/`Tcreate` of a non-directory produces a
//!   [`File`]: one value per open fid, so per-open state (a stream cursor,
//!   the result buffer of a write-then-read query file) lives on it and two
//!   opens of the same file on one connection stay independent. The `File`
//!   is dropped when the fid is clunked; `Drop` is the clunk hook.
//! - [`File::read`] is async and **may park** for as long as it likes: the
//!   server runs every read on its own task, so a blocked read never stalls
//!   other requests on the connection.
//! - `Tflush` aborts the named read, and `Rflush` follows promptly. The
//!   flushed read never answers (unless its reply was already queued, in
//!   which case it is sent before the `Rflush`, as `flush(5)` permits).
//! - Directory reads are handled by the server from [`Filesystem::readdir`]:
//!   whole stat entries only, served from a snapshot taken at offset 0, so a
//!   listing that changes between reads can never tear an entry.
//!
//! Errors are strings ([`Error`]) and reach the client verbatim as `Rerror`.
//! The [`errors`] module has the conventional Plan 9/Inferno texts.

use std::collections::HashMap;
use std::fmt;
use std::future::Future;
use std::io;
use std::sync::Arc;

use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use crate::proto::{
    DecodeError, IOHDRSZ, MAXWELEM, NOFID, OEXEC, ORCLOSE, OTRUNC, Qid, Rmsg, Stat, Tmsg, VERSION,
};

/// Conventional error strings, as Plan 9 and Inferno servers spell them.
pub mod errors {
    /// A walk element or path does not exist.
    pub const NOT_FOUND: &str = "file does not exist";
    /// Access refused.
    pub const PERM: &str = "permission denied";
    /// The fid is not in use.
    pub const BAD_FID: &str = "fid unknown or out of range";
    /// The fid (or newfid) is already in use.
    pub const FID_IN_USE: &str = "fid already in use";
    /// Walk or open of a fid that is already open.
    pub const OPEN: &str = "fid already opened";
    /// I/O on a fid that is not open.
    pub const NOT_OPEN: &str = "fid not open";
    /// Walking from, or creating in, something that is not a directory.
    pub const NOT_DIR: &str = "not a directory";
    /// Writing a directory, or opening one for writing.
    pub const IS_DIR: &str = "is a directory";
    /// The connection is not authenticated and does not need to be.
    pub const NO_AUTH: &str = "authentication not required";
    /// A message this server does not implement.
    pub const UNSUPPORTED: &str = "operation not supported";
    /// A read was abandoned because its fid was clunked.
    pub const CLUNKED: &str = "fid clunked";
}

/// An error for a request; its text is sent to the client as `Rerror`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Error(String);

impl Error {
    /// An error with the given text.
    pub fn new(msg: impl Into<String>) -> Self {
        Self(msg.into())
    }

    /// The text sent to the client.
    pub fn message(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for Error {}

impl From<&str> for Error {
    fn from(s: &str) -> Self {
        Self(s.into())
    }
}

impl From<String> for Error {
    fn from(s: String) -> Self {
        Self(s)
    }
}

impl From<io::Error> for Error {
    fn from(e: io::Error) -> Self {
        Self(e.to_string())
    }
}

/// Result alias for filesystem and file operations.
pub type Result<T> = std::result::Result<T, Error>;

/// Who is asking: the identity from the `Tattach` that the fid descends from.
#[derive(Debug, Clone, Copy)]
#[non_exhaustive]
pub struct Ctx<'a> {
    /// The user name the client attached as.
    pub uname: &'a str,
    /// The tree the client asked to attach to (often empty).
    pub aname: &'a str,
}

/// The tree a server serves. See the [module docs](self) for the model.
///
/// Qids are the filesystem's own: the server never invents them, it only
/// remembers which qid each fid points at.
pub trait Filesystem: Send + Sync + 'static {
    /// The per-open file type.
    type File: File;

    /// A client attaches: return the root qid (or refuse).
    fn attach(&self, ctx: &Ctx<'_>) -> Result<Qid>;

    /// Walk from directory `dir` to its child `name`.
    ///
    /// `name` may be `".."`; at the root, `".."` should return the root.
    /// The server only calls this with a directory qid.
    fn walk1(&self, ctx: &Ctx<'_>, dir: Qid, name: &str) -> Result<Qid>;

    /// Describe a file or directory.
    fn stat(&self, ctx: &Ctx<'_>, qid: Qid) -> Result<Stat>;

    /// List a directory. The server encodes the entries and serves reads of
    /// the directory from that snapshot; return entries in a stable order.
    fn readdir(&self, ctx: &Ctx<'_>, dir: Qid) -> Result<Vec<Stat>>;

    /// Open a non-directory. `mode` is the raw open mode (`OREAD`, `OWRITE`,
    /// `ORDWR`, `OEXEC`, possibly with `OTRUNC`/`ORCLOSE`); permission checks
    /// are the filesystem's job. Directories are opened by the server.
    fn open(&self, ctx: &Ctx<'_>, qid: Qid, mode: u8) -> Result<Self::File>;

    /// Create `name` in directory `dir` and open it. `perm` carries `DMDIR`
    /// for a new directory. For a directory, return a `File` whose methods
    /// will not be called. Defaults to refusing.
    fn create(
        &self,
        ctx: &Ctx<'_>,
        dir: Qid,
        name: &str,
        perm: u32,
        mode: u8,
    ) -> Result<(Qid, Self::File)> {
        let _ = (ctx, dir, name, perm, mode);
        Err(errors::PERM.into())
    }

    /// Remove a file. The fid is clunked whether or not this succeeds.
    /// Defaults to refusing.
    fn remove(&self, ctx: &Ctx<'_>, qid: Qid) -> Result<()> {
        let _ = (ctx, qid);
        Err(errors::PERM.into())
    }

    /// Change a file's metadata. Fields the client is not changing hold
    /// their don't-touch values ([`Stat::dont_touch`]). Defaults to refusing.
    fn wstat(&self, ctx: &Ctx<'_>, qid: Qid, stat: &Stat) -> Result<()> {
        let _ = (ctx, qid, stat);
        Err(errors::PERM.into())
    }
}

/// One open fid. Dropped when the fid is clunked (or the connection ends).
///
/// # Cancellation
///
/// A read is cancelled by `Tflush` (and by clunk, `Tversion`, or
/// disconnect) by dropping its future at whatever `.await` it is parked
/// on. Make reads cancel-safe: commit state (advance a cursor) only once
/// the data to return is in hand, as the last step before returning.
pub trait File: Send + Sync + 'static {
    /// Read up to `count` bytes at `offset`; an empty result is EOF. May
    /// wait indefinitely for data (a blocking file). The server truncates
    /// anything longer than `count`.
    fn read(&self, offset: u64, count: u32) -> impl Future<Output = Result<Vec<u8>>> + Send;

    /// Write `data` at `offset`, returning the bytes accepted. Writes are
    /// answered in order on the connection's task, so they should not park.
    /// Defaults to refusing.
    fn write(&self, offset: u64, data: &[u8]) -> impl Future<Output = Result<u32>> + Send {
        let _ = (offset, data);
        async { Err(errors::PERM.into()) }
    }
}

/// Serves a [`Filesystem`] on any number of connections.
pub struct Server<F> {
    fs: Arc<F>,
    max_msize: u32,
}

impl<F> Clone for Server<F> {
    fn clone(&self) -> Self {
        Self { fs: Arc::clone(&self.fs), max_msize: self.max_msize }
    }
}

/// The default and largest message size this server negotiates.
pub const DEFAULT_MSIZE: u32 = 64 * 1024;
/// The smallest msize accepted from a client.
pub const MIN_MSIZE: u32 = 256;

impl<F: Filesystem> Server<F> {
    /// A server for `fs`.
    pub fn new(fs: F) -> Self {
        Self::from_arc(Arc::new(fs))
    }

    /// A server for a filesystem the caller also holds (to publish into it,
    /// inspect it, or serve it on several listeners).
    pub fn from_arc(fs: Arc<F>) -> Self {
        Self { fs, max_msize: DEFAULT_MSIZE }
    }

    /// Cap the negotiated message size (default [`DEFAULT_MSIZE`]).
    pub fn max_msize(mut self, msize: u32) -> Self {
        self.max_msize = msize.max(MIN_MSIZE);
        self
    }

    /// The filesystem being served.
    pub fn fs(&self) -> &Arc<F> {
        &self.fs
    }

    /// Accept TCP connections forever, serving each on its own task.
    pub async fn serve_tcp(&self, listener: TcpListener) -> io::Result<()> {
        loop {
            let (stream, _) = listener.accept().await?;
            let _ = stream.set_nodelay(true);
            let s = self.clone();
            tokio::spawn(async move { s.serve_conn(stream).await });
        }
    }

    /// Accept Unix-domain connections forever, serving each on its own task.
    #[cfg(unix)]
    pub async fn serve_unix(&self, listener: tokio::net::UnixListener) -> io::Result<()> {
        loop {
            let (stream, _) = listener.accept().await?;
            let s = self.clone();
            tokio::spawn(async move { s.serve_conn(stream).await });
        }
    }

    /// Serve one connection until the client hangs up or breaks framing.
    ///
    /// Returns `Ok(())` on a clean hang-up between messages.
    pub async fn serve_conn<S>(&self, stream: S) -> io::Result<()>
    where
        S: AsyncRead + AsyncWrite + Send + 'static,
    {
        let (mut rd, mut wr) = tokio::io::split(stream);

        // One writer task serialises replies from the connection task and
        // from every in-flight read.
        let (tx, mut rx) = mpsc::channel::<Vec<u8>>(64);
        let writer = tokio::spawn(async move {
            while let Some(buf) = rx.recv().await {
                wr.write_all(&buf).await?;
            }
            wr.shutdown().await
        });

        let mut conn = Conn::<F> {
            fs: Arc::clone(&self.fs),
            max_msize: self.max_msize,
            msize: self.max_msize,
            fids: HashMap::new(),
            inflight: HashMap::new(),
            tx,
        };
        let result = conn.run(&mut rd).await;
        conn.abort_all().await;
        conn.fids.clear();
        drop(conn); // closes the reply channel
        let wrote = writer.await.unwrap_or(Ok(()));
        result.and(
            wrote.or_else(|e| if e.kind() == io::ErrorKind::BrokenPipe { Ok(()) } else { Err(e) }),
        )
    }
}

struct FidState<T> {
    qid: Qid,
    uname: Arc<str>,
    aname: Arc<str>,
    open: Option<Opened<T>>,
    /// Opened with `ORCLOSE`: remove on clunk.
    rclose: bool,
}

enum Opened<T> {
    File(Arc<T>),
    Dir(DirSnapshot),
}

#[derive(Default)]
struct DirSnapshot {
    bytes: Vec<u8>,
    /// End offset of each entry in `bytes`.
    ends: Vec<usize>,
}

struct Inflight {
    fid: u32,
    task: JoinHandle<()>,
}

struct Conn<F: Filesystem> {
    fs: Arc<F>,
    max_msize: u32,
    msize: u32,
    fids: HashMap<u32, FidState<F::File>>,
    inflight: HashMap<u16, Inflight>,
    tx: mpsc::Sender<Vec<u8>>,
}

/// What a request produced: a reply now, or nothing (a read was spawned).
enum Outcome {
    Reply(Rmsg),
    Spawned,
}

impl<F: Filesystem> Conn<F> {
    async fn run<R: AsyncRead + Unpin>(&mut self, rd: &mut R) -> io::Result<()> {
        loop {
            let size = match rd.read_u32_le().await {
                Ok(n) => n,
                Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => return Ok(()),
                Err(e) => return Err(e),
            };
            if size < 7 || size > self.msize {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("9P frame size {size} outside 7..={}", self.msize),
                ));
            }
            let mut buf = vec![0u8; size as usize - 4];
            rd.read_exact(&mut buf).await?;

            self.inflight.retain(|_, r| !r.task.is_finished());

            let (tag, outcome) = match Tmsg::decode(&buf) {
                Ok((tag, msg)) => (tag, self.handle(tag, msg).await),
                Err(DecodeError::UnknownType { tag, .. }) => {
                    (tag, Outcome::Reply(Rmsg::error(errors::UNSUPPORTED)))
                }
                Err(e) => {
                    // The tag is in the first bytes; anything shorter fails
                    // the size check above.
                    let tag = u16::from_le_bytes([buf[1], buf[2]]);
                    (tag, Outcome::Reply(Rmsg::error(e.to_string())))
                }
            };
            if let Outcome::Reply(r) = outcome {
                if self.tx.send(r.encode(tag)).await.is_err() {
                    return Ok(()); // writer gone: the peer hung up
                }
            }
        }
    }

    fn ctx<'a>(f: &'a FidState<F::File>) -> Ctx<'a> {
        Ctx { uname: &f.uname, aname: &f.aname }
    }

    async fn handle(&mut self, tag: u16, msg: Tmsg) -> Outcome {
        use Outcome::Reply;
        let r = match msg {
            Tmsg::Version { msize, version } => self.version(msize, &version).await,
            Tmsg::Auth { .. } => Err(errors::NO_AUTH.into()),
            Tmsg::Attach { fid, afid, uname, aname } => self.attach(fid, afid, uname, aname),
            Tmsg::Flush { oldtag } => {
                if let Some(r) = self.inflight.remove(&oldtag) {
                    r.task.abort();
                    let _ = r.task.await;
                }
                Ok(Rmsg::Flush)
            }
            Tmsg::Walk { fid, newfid, wnames } => self.walk(fid, newfid, &wnames),
            Tmsg::Open { fid, mode } => self.open(fid, mode),
            Tmsg::Create { fid, name, perm, mode } => self.create(fid, &name, perm, mode),
            Tmsg::Read { fid, offset, count } => return self.read(tag, fid, offset, count),
            Tmsg::Write { fid, offset, data } => self.write(fid, offset, &data).await,
            Tmsg::Clunk { fid } => match self.drop_fid(fid).await {
                Some(f) => {
                    if f.rclose {
                        let _ = self.fs.remove(&Self::ctx(&f), f.qid);
                    }
                    Ok(Rmsg::Clunk)
                }
                None => Err(errors::BAD_FID.into()),
            },
            Tmsg::Remove { fid } => match self.drop_fid(fid).await {
                Some(f) => self.fs.remove(&Self::ctx(&f), f.qid).map(|()| Rmsg::Remove),
                None => Err(errors::BAD_FID.into()),
            },
            Tmsg::Stat { fid } => match self.fids.get(&fid) {
                Some(f) => self.fs.stat(&Self::ctx(f), f.qid).map(|stat| Rmsg::Stat { stat }),
                None => Err(errors::BAD_FID.into()),
            },
            Tmsg::Wstat { fid, stat } => match self.fids.get(&fid) {
                Some(f) => self.fs.wstat(&Self::ctx(f), f.qid, &stat).map(|()| Rmsg::Wstat),
                None => Err(errors::BAD_FID.into()),
            },
        };
        Reply(r.unwrap_or_else(|e| Rmsg::Error { ename: e.0 }))
    }

    /// Abort every in-flight read and wait for each to stop.
    async fn abort_all(&mut self) {
        for (_, r) in self.inflight.drain() {
            r.task.abort();
            let _ = r.task.await;
        }
    }

    async fn version(&mut self, msize: u32, version: &str) -> Result<Rmsg> {
        // A Tversion aborts all outstanding I/O (no replies) and clunks
        // every fid.
        self.abort_all().await;
        self.fids.clear();
        if msize < MIN_MSIZE {
            return Err(format!("msize {msize} too small").into());
        }
        let msize = msize.min(self.max_msize);
        self.msize = msize;
        let version =
            if version == VERSION || version.starts_with("9P2000.") { VERSION } else { "unknown" };
        Ok(Rmsg::Version { msize, version: version.into() })
    }

    fn attach(&mut self, fid: u32, afid: u32, uname: String, aname: String) -> Result<Rmsg> {
        if afid != NOFID {
            return Err(errors::NO_AUTH.into());
        }
        if self.fids.contains_key(&fid) {
            return Err(errors::FID_IN_USE.into());
        }
        let qid = self.fs.attach(&Ctx { uname: &uname, aname: &aname })?;
        self.fids.insert(
            fid,
            FidState { qid, uname: uname.into(), aname: aname.into(), open: None, rclose: false },
        );
        Ok(Rmsg::Attach { qid })
    }

    fn walk(&mut self, fid: u32, newfid: u32, wnames: &[String]) -> Result<Rmsg> {
        let f = self.fids.get(&fid).ok_or(errors::BAD_FID)?;
        if f.open.is_some() {
            return Err(errors::OPEN.into());
        }
        if newfid != fid && self.fids.contains_key(&newfid) {
            return Err(errors::FID_IN_USE.into());
        }
        if wnames.len() > MAXWELEM {
            return Err(format!("walk of more than {MAXWELEM} elements").into());
        }

        let ctx = Self::ctx(f);
        let mut cur = f.qid;
        let mut wqids = Vec::with_capacity(wnames.len());
        for (i, name) in wnames.iter().enumerate() {
            let step = if cur.is_dir() {
                self.fs.walk1(&ctx, cur, name)
            } else {
                Err(errors::NOT_DIR.into())
            };
            match step {
                Ok(q) => {
                    cur = q;
                    wqids.push(q);
                }
                // The first element failing is an error; a later one is a
                // partial walk, and newfid is left untouched.
                Err(e) if i == 0 => return Err(e),
                Err(_) => return Ok(Rmsg::Walk { wqids }),
            }
        }

        let (uname, aname) = (Arc::clone(&f.uname), Arc::clone(&f.aname));
        self.fids.insert(newfid, FidState { qid: cur, uname, aname, open: None, rclose: false });
        Ok(Rmsg::Walk { wqids })
    }

    fn iounit(&self) -> u32 {
        self.msize - IOHDRSZ
    }

    fn open(&mut self, fid: u32, mode: u8) -> Result<Rmsg> {
        let iounit = self.iounit();
        let f = self.fids.get_mut(&fid).ok_or(errors::BAD_FID)?;
        if f.open.is_some() {
            return Err(errors::OPEN.into());
        }
        let opened = if f.qid.is_dir() {
            let rw = mode & 3;
            if (rw != 0 && rw != OEXEC) || mode & OTRUNC != 0 {
                return Err(errors::IS_DIR.into());
            }
            Opened::Dir(DirSnapshot::default())
        } else {
            let ctx = Ctx { uname: &f.uname, aname: &f.aname };
            Opened::File(Arc::new(self.fs.open(&ctx, f.qid, mode)?))
        };
        f.open = Some(opened);
        f.rclose = mode & ORCLOSE != 0;
        Ok(Rmsg::Open { qid: f.qid, iounit })
    }

    fn create(&mut self, fid: u32, name: &str, perm: u32, mode: u8) -> Result<Rmsg> {
        let iounit = self.iounit();
        let f = self.fids.get_mut(&fid).ok_or(errors::BAD_FID)?;
        if f.open.is_some() {
            return Err(errors::OPEN.into());
        }
        if !f.qid.is_dir() {
            return Err(errors::NOT_DIR.into());
        }
        if name.is_empty() || name == "." || name == ".." || name.contains('/') {
            return Err(format!("illegal name {name:?}").into());
        }
        let ctx = Ctx { uname: &f.uname, aname: &f.aname };
        let (qid, file) = self.fs.create(&ctx, f.qid, name, perm, mode)?;
        f.qid = qid;
        f.rclose = mode & ORCLOSE != 0;
        f.open = Some(if qid.is_dir() {
            Opened::Dir(DirSnapshot::default())
        } else {
            Opened::File(Arc::new(file))
        });
        Ok(Rmsg::Create { qid, iounit })
    }

    fn read(&mut self, tag: u16, fid: u32, offset: u64, count: u32) -> Outcome {
        let count = count.min(self.iounit());
        let Some(f) = self.fids.get_mut(&fid) else {
            return Outcome::Reply(Rmsg::error(errors::BAD_FID));
        };
        let file = match &mut f.open {
            None => return Outcome::Reply(Rmsg::error(errors::NOT_OPEN)),
            Some(Opened::Dir(snap)) => {
                let ctx = Ctx { uname: &f.uname, aname: &f.aname };
                let r = read_dir(&*self.fs, &ctx, f.qid, snap, offset, count);
                return Outcome::Reply(r.unwrap_or_else(|e| Rmsg::Error { ename: e.0 }));
            }
            Some(Opened::File(file)) => Arc::clone(file),
        };

        let tx = self.tx.clone();
        let task = tokio::spawn(async move {
            let reply = match file.read(offset, count).await {
                Ok(mut data) => {
                    data.truncate(count as usize);
                    Rmsg::Read { data }
                }
                Err(e) => Rmsg::Error { ename: e.0 },
            };
            let _ = tx.send(reply.encode(tag)).await;
        });
        self.inflight.insert(tag, Inflight { fid, task });
        Outcome::Spawned
    }

    async fn write(&mut self, fid: u32, offset: u64, data: &[u8]) -> Result<Rmsg> {
        let f = self.fids.get(&fid).ok_or(errors::BAD_FID)?;
        match &f.open {
            None => Err(errors::NOT_OPEN.into()),
            Some(Opened::Dir(_)) => Err(errors::IS_DIR.into()),
            Some(Opened::File(file)) => {
                let file = Arc::clone(file);
                file.write(offset, data).await.map(|count| Rmsg::Write { count })
            }
        }
    }

    /// Retire a fid: answer its in-flight reads with an error (so no tag is
    /// left hanging), then drop it, which drops its `File`.
    async fn drop_fid(&mut self, fid: u32) -> Option<FidState<F::File>> {
        let f = self.fids.remove(&fid)?;
        let tags: Vec<u16> =
            self.inflight.iter().filter(|(_, r)| r.fid == fid).map(|(t, _)| *t).collect();
        for tag in tags {
            let r = self.inflight.remove(&tag).unwrap();
            r.task.abort();
            if let Err(e) = r.task.await {
                if e.is_cancelled() {
                    let _ = self.tx.send(Rmsg::error(errors::CLUNKED).encode(tag)).await;
                }
            }
        }
        Some(f)
    }
}

/// Serve a directory read from the fid's snapshot: whole entries only,
/// re-listing when the client starts again at offset 0.
fn read_dir<F: Filesystem>(
    fs: &F,
    ctx: &Ctx<'_>,
    dir: Qid,
    snap: &mut DirSnapshot,
    offset: u64,
    count: u32,
) -> Result<Rmsg> {
    if offset == 0 {
        snap.bytes.clear();
        snap.ends.clear();
        for s in fs.readdir(ctx, dir)? {
            s.encode_into(&mut snap.bytes);
            snap.ends.push(snap.bytes.len());
        }
    }
    let start = usize::try_from(offset).unwrap_or(usize::MAX);
    if start != 0 && start != snap.bytes.len() && snap.ends.binary_search(&start).is_err() {
        return Err("bad offset in directory read".into());
    }
    if start >= snap.bytes.len() {
        return Ok(Rmsg::Read { data: Vec::new() });
    }
    let limit = start + count as usize;
    let end = snap.ends.iter().copied().take_while(|&e| e <= limit).last().unwrap_or(start);
    if end <= start {
        return Err("directory entry larger than read count".into());
    }
    Ok(Rmsg::Read { data: snap.bytes[start..end].to_vec() })
}

/// The answer buffer of a write-then-read ("file as RPC") file, such as a
/// query or ctl file: the client writes a request and reads the answer back
/// on the same fid.
///
/// A client that writes and then reads on one open file, without seeking,
/// reads at the offset its write left behind (`{echo q >[1=0]; cat} <>
/// query` in Inferno's sh). `Answer` therefore pages the answer from the
/// offset of the first read after each [`set`](Answer::set), and a read at
/// a lower offset (the seek-to-0 idiom) also starts from the beginning.
#[derive(Debug, Default, Clone)]
pub struct Answer {
    data: Vec<u8>,
    base: Option<u64>,
}

impl Answer {
    /// Replace the answer; the next read starts at its beginning.
    pub fn set(&mut self, data: impl Into<Vec<u8>>) {
        self.data = data.into();
        self.base = None;
    }

    /// Read part of the answer for a `Tread` at `offset`.
    pub fn read(&mut self, offset: u64, count: u32) -> Vec<u8> {
        let base = *self.base.get_or_insert(offset);
        read_at(&self.data, offset.saturating_sub(base), count)
    }
}

/// Slice `data` for a read at `offset` of up to `count` bytes: the usual
/// body of [`File::read`] for a file whose contents are in memory.
pub fn read_at(data: &[u8], offset: u64, count: u32) -> Vec<u8> {
    let start = usize::try_from(offset).unwrap_or(usize::MAX).min(data.len());
    let end = start.saturating_add(count as usize).min(data.len());
    data[start..end].to_vec()
}
