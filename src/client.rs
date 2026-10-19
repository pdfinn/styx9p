//! A small 9P2000 client: one request in flight at a time.
//!
//! Enough to consume a 9P service from Rust (walk, open, read, write,
//! stat, create, remove) and to test servers. It does not multiplex
//! concurrent requests over one connection; open several connections for
//! that, or drive [`crate::proto`] directly.

use std::fmt;
use std::io;

use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::{TcpStream, ToSocketAddrs};

use crate::proto::{IOHDRSZ, MAXWELEM, NOFID, NOTAG, OREAD, Qid, Rmsg, Stat, Tmsg, VERSION};

/// Why a client call failed.
#[derive(Debug)]
pub enum ClientError {
    /// The transport failed.
    Io(io::Error),
    /// The server answered `Rerror` with this text.
    Server(String),
    /// A walk stopped short at this path element (the server answered the
    /// walk, but without reaching it). When the server refuses the *first*
    /// element of a walk with `Rerror`, that is [`ClientError::Server`],
    /// carrying the server's own text.
    NotFound(String),
    /// The server sent something that is not a valid reply to the request.
    Protocol(String),
}

impl fmt::Display for ClientError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ClientError::Io(e) => write!(f, "9p: {e}"),
            ClientError::Server(m) => f.write_str(m),
            ClientError::NotFound(n) => write!(f, "'{n}' file does not exist"),
            ClientError::Protocol(m) => write!(f, "9p protocol error: {m}"),
        }
    }
}

impl std::error::Error for ClientError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            ClientError::Io(e) => Some(e),
            _ => None,
        }
    }
}

impl From<io::Error> for ClientError {
    fn from(e: io::Error) -> Self {
        ClientError::Io(e)
    }
}

/// Result alias for client calls.
pub type Result<T> = std::result::Result<T, ClientError>;

/// A fid allocated by this client. Release it with [`Client::clunk`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Fid(pub u32);

/// The root fid every client attaches as.
pub const ROOT: Fid = Fid(0);

/// A connected, attached 9P client.
pub struct Client<S> {
    stream: S,
    msize: u32,
    tag: u16,
    next_fid: u32,
}

impl Client<TcpStream> {
    /// Connect over TCP, negotiate the version, and attach.
    pub async fn dial(addr: impl ToSocketAddrs, uname: &str, aname: &str) -> Result<Self> {
        let stream = TcpStream::connect(addr).await?;
        stream.set_nodelay(true)?;
        Self::new(stream, uname, aname).await
    }
}

impl<S: AsyncRead + AsyncWrite + Unpin> Client<S> {
    /// Negotiate the version and attach over an existing stream.
    pub async fn new(stream: S, uname: &str, aname: &str) -> Result<Self> {
        Self::with_msize(stream, uname, aname, crate::server::DEFAULT_MSIZE).await
    }

    /// As [`Client::new`], proposing `msize`.
    pub async fn with_msize(stream: S, uname: &str, aname: &str, msize: u32) -> Result<Self> {
        let mut c = Self { stream, msize, tag: 0, next_fid: 1 };
        let v = Tmsg::Version { msize, version: VERSION.into() };
        match c.rpc_tagged(NOTAG, v).await? {
            Rmsg::Version { msize: m, version } if version == VERSION && m <= msize => c.msize = m,
            Rmsg::Version { version, .. } => {
                return Err(ClientError::Protocol(format!("server speaks {version:?}")));
            }
            other => return Err(unexpected("Rversion", &other)),
        }
        let a = Tmsg::Attach { fid: ROOT.0, afid: NOFID, uname: uname.into(), aname: aname.into() };
        match c.rpc(a).await? {
            Rmsg::Attach { .. } => Ok(c),
            other => Err(unexpected("Rattach", &other)),
        }
    }

    /// The negotiated message size.
    pub fn msize(&self) -> u32 {
        self.msize
    }

    /// The largest data payload of one read or write.
    pub fn iounit(&self) -> u32 {
        self.msize - IOHDRSZ
    }

    async fn rpc(&mut self, t: Tmsg) -> Result<Rmsg> {
        self.tag = if self.tag >= NOTAG - 1 { 0 } else { self.tag + 1 };
        self.rpc_tagged(self.tag, t).await
    }

    async fn rpc_tagged(&mut self, tag: u16, t: Tmsg) -> Result<Rmsg> {
        self.stream.write_all(&t.encode(tag)).await?;
        let size = self.stream.read_u32_le().await?;
        if size < 7 || size > self.msize.max(crate::server::DEFAULT_MSIZE) {
            return Err(ClientError::Protocol(format!("bad frame size {size}")));
        }
        let mut buf = vec![0u8; size as usize - 4];
        self.stream.read_exact(&mut buf).await?;
        let (rtag, r) = Rmsg::decode(&buf).map_err(|e| ClientError::Protocol(e.to_string()))?;
        if rtag != tag {
            return Err(ClientError::Protocol(format!("reply tag {rtag}, expected {tag}")));
        }
        match r {
            Rmsg::Error { ename } => Err(ClientError::Server(ename)),
            r => Ok(r),
        }
    }

    fn alloc_fid(&mut self) -> Fid {
        let f = Fid(self.next_fid);
        self.next_fid = self.next_fid.wrapping_add(1).max(1);
        f
    }

    /// Walk from the root along `path`, returning a new fid for the result.
    /// An empty path clones the root.
    pub async fn walk(&mut self, path: &[&str]) -> Result<Fid> {
        self.walk_from(ROOT, path).await
    }

    /// Walk from `from` along `path` (any length; sent in chunks of
    /// [`MAXWELEM`]), returning a new fid.
    pub async fn walk_from(&mut self, from: Fid, path: &[&str]) -> Result<Fid> {
        let newfid = self.alloc_fid();
        let mut chunks: Vec<&[&str]> = path.chunks(MAXWELEM).collect();
        if chunks.is_empty() {
            chunks.push(&[]);
        }
        let mut src = from;
        for (i, chunk) in chunks.iter().enumerate() {
            let wnames: Vec<String> = chunk.iter().map(|s| s.to_string()).collect();
            let r = self.rpc(Tmsg::Walk { fid: src.0, newfid: newfid.0, wnames }).await;
            let missing = match r {
                Ok(Rmsg::Walk { wqids }) if wqids.len() == chunk.len() => None,
                Ok(Rmsg::Walk { wqids }) => Some(chunk[wqids.len()].to_string()),
                Ok(other) => return Err(unexpected("Rwalk", &other)),
                Err(e) => {
                    if i > 0 {
                        let _ = self.clunk(newfid).await;
                    }
                    return Err(e);
                }
            };
            if let Some(name) = missing {
                if i > 0 {
                    let _ = self.clunk(newfid).await;
                }
                return Err(ClientError::NotFound(name));
            }
            src = newfid;
        }
        Ok(newfid)
    }

    /// Open a walked fid. Returns the file's qid and the server's iounit.
    pub async fn open(&mut self, fid: Fid, mode: u8) -> Result<(Qid, u32)> {
        match self.rpc(Tmsg::Open { fid: fid.0, mode }).await? {
            Rmsg::Open { qid, iounit } => Ok((qid, iounit)),
            other => Err(unexpected("Ropen", &other)),
        }
    }

    /// Create `name` in the directory `dir` and open it; `dir` becomes the
    /// new file's fid.
    pub async fn create(&mut self, dir: Fid, name: &str, perm: u32, mode: u8) -> Result<Qid> {
        let t = Tmsg::Create { fid: dir.0, name: name.into(), perm, mode };
        match self.rpc(t).await? {
            Rmsg::Create { qid, .. } => Ok(qid),
            other => Err(unexpected("Rcreate", &other)),
        }
    }

    /// One `Tread` of up to `count` bytes (capped at the iounit).
    pub async fn read(&mut self, fid: Fid, offset: u64, count: u32) -> Result<Vec<u8>> {
        let count = count.min(self.iounit());
        match self.rpc(Tmsg::Read { fid: fid.0, offset, count }).await? {
            Rmsg::Read { data } if data.len() <= count as usize => Ok(data),
            Rmsg::Read { data } => {
                Err(ClientError::Protocol(format!("Rread of {} for count {count}", data.len())))
            }
            other => Err(unexpected("Rread", &other)),
        }
    }

    /// Fill `buf` from `offset`, issuing as many `Tread`s as it takes.
    /// Returns the bytes read, which is less than `buf.len()` only if the
    /// file ended first.
    pub async fn read_full(&mut self, fid: Fid, offset: u64, buf: &mut [u8]) -> Result<usize> {
        let mut done = 0;
        while done < buf.len() {
            let want = (buf.len() - done).min(self.iounit() as usize) as u32;
            let chunk = self.read(fid, offset + done as u64, want).await?;
            if chunk.is_empty() {
                break;
            }
            buf[done..done + chunk.len()].copy_from_slice(&chunk);
            done += chunk.len();
        }
        Ok(done)
    }

    /// Read from `offset` until EOF, stopping with an error past `limit`
    /// bytes. Do not use on blocking files.
    pub async fn read_to_end(&mut self, fid: Fid, limit: usize) -> Result<Vec<u8>> {
        let mut out = Vec::new();
        loop {
            let chunk = self.read(fid, out.len() as u64, self.iounit()).await?;
            if chunk.is_empty() {
                return Ok(out);
            }
            out.extend_from_slice(&chunk);
            if out.len() > limit {
                return Err(ClientError::Protocol(format!("file larger than {limit} bytes")));
            }
        }
    }

    /// One `Twrite` (data beyond the iounit is not sent; see the count).
    pub async fn write(&mut self, fid: Fid, offset: u64, data: &[u8]) -> Result<u32> {
        let n = data.len().min(self.iounit() as usize);
        let t = Tmsg::Write { fid: fid.0, offset, data: data[..n].to_vec() };
        match self.rpc(t).await? {
            Rmsg::Write { count } => Ok(count),
            other => Err(unexpected("Rwrite", &other)),
        }
    }

    /// Write all of `data` at `offset`, in as many `Twrite`s as it takes.
    pub async fn write_all(&mut self, fid: Fid, offset: u64, data: &[u8]) -> Result<()> {
        let mut done = 0;
        while done < data.len() {
            let n = self.write(fid, offset + done as u64, &data[done..]).await? as usize;
            if n == 0 {
                return Err(ClientError::Protocol("server accepted a zero-length write".into()));
            }
            done += n;
        }
        Ok(())
    }

    /// Release a fid.
    pub async fn clunk(&mut self, fid: Fid) -> Result<()> {
        match self.rpc(Tmsg::Clunk { fid: fid.0 }).await? {
            Rmsg::Clunk => Ok(()),
            other => Err(unexpected("Rclunk", &other)),
        }
    }

    /// Remove the file and release the fid.
    pub async fn remove(&mut self, fid: Fid) -> Result<()> {
        match self.rpc(Tmsg::Remove { fid: fid.0 }).await? {
            Rmsg::Remove => Ok(()),
            other => Err(unexpected("Rremove", &other)),
        }
    }

    /// Stat a fid.
    pub async fn stat(&mut self, fid: Fid) -> Result<Stat> {
        match self.rpc(Tmsg::Stat { fid: fid.0 }).await? {
            Rmsg::Stat { stat } => Ok(stat),
            other => Err(unexpected("Rstat", &other)),
        }
    }

    /// Change a fid's metadata; start from [`Stat::dont_touch`].
    pub async fn wstat(&mut self, fid: Fid, stat: Stat) -> Result<()> {
        match self.rpc(Tmsg::Wstat { fid: fid.0, stat }).await? {
            Rmsg::Wstat => Ok(()),
            other => Err(unexpected("Rwstat", &other)),
        }
    }

    // ── path conveniences: walk, act, clunk ─────────────────────────────

    /// Read a whole (non-blocking) file, up to 16 MiB.
    pub async fn read_file(&mut self, path: &[&str]) -> Result<Vec<u8>> {
        let fid = self.walk(path).await?;
        let r = async {
            self.open(fid, OREAD).await?;
            self.read_to_end(fid, 16 << 20).await
        }
        .await;
        let _ = self.clunk(fid).await;
        r
    }

    /// Read a whole (non-blocking) file as UTF-8.
    pub async fn read_string(&mut self, path: &[&str]) -> Result<String> {
        let data = self.read_file(path).await?;
        String::from_utf8(data).map_err(|_| ClientError::Protocol("file is not UTF-8".into()))
    }

    /// List a directory.
    pub async fn read_dir(&mut self, path: &[&str]) -> Result<Vec<Stat>> {
        let data = self.read_file(path).await?;
        Stat::decode_all(&data).map_err(|e| ClientError::Protocol(e.to_string()))
    }

    /// Stat a path.
    pub async fn stat_path(&mut self, path: &[&str]) -> Result<Stat> {
        let fid = self.walk(path).await?;
        let r = self.stat(fid).await;
        let _ = self.clunk(fid).await;
        r
    }
}

fn unexpected(want: &str, got: &Rmsg) -> ClientError {
    ClientError::Protocol(format!("expected {want}, got message type {}", got.msgtype()))
}
