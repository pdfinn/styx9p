//! Protocol conformance suite.
//!
//! Tests 1-6 come from manto-9p's suite, the fleet's original; 7-10 are the
//! one-connection behaviours Inferno relies on (it multiplexes every process
//! using a mount over one connection) that sminez/ninep 0.6.0 failed; the
//! rest pin down the remaining semantics of `intro(5)`..`walk(5)`.
//!
//! Every network wait is bounded, so a non-conformance fails instead of
//! hanging.

use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use styx9p::proto::{self, NOFID, NOTAG, ORCLOSE, OREAD, OWRITE, Rmsg, Tmsg};
use styx9p::{
    Answer, Client, ClientError, Ctx, File, Filesystem, Qid, Result, Server, Stat, errors, read_at,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt, DuplexStream};
use tokio::sync::Notify;
use tokio::time::timeout;

const WAIT: Duration = Duration::from_millis(500);

const ROOT: u64 = 0;
const HELLO: u64 = 1;
const STREAM: u64 = 2;
const SINK: u64 = 3;
const QUERY: u64 = 4;
const SUB: u64 = 5;
const TMP: u64 = 6;

/// `hello` (static), `stream` (blocking: one line per publish, per open),
/// `sink` (write-only), `query` (write an expression, read the answer on the
/// same fid), `sub/` (a directory whose listing grows), `tmp` (removable).
#[derive(Default)]
struct ToyFs {
    seq: AtomicU64,
    notify: Notify,
    sink: Mutex<Vec<u8>>,
    open_files: AtomicUsize,
    sub_entries: AtomicUsize,
    removed: Mutex<Vec<u64>>,
}

impl ToyFs {
    fn publish(&self) {
        self.seq.fetch_add(1, Ordering::SeqCst);
        self.notify.notify_waiters();
    }

    fn sub_listing(&self) -> Vec<Stat> {
        (0..self.sub_entries.load(Ordering::SeqCst))
            .map(|i| Stat::new(format!("entry-{i:03}"), Qid::file(1000 + i as u64), 0o444, 0))
            .collect()
    }
}

struct ToyFile {
    fs: Arc<ToyFs>,
    qid: Qid,
    /// The last `seq` this open delivered (stream).
    cursor: AtomicU64,
    /// The answer to the last query written (query).
    answer: Mutex<Answer>,
}

impl Drop for ToyFile {
    fn drop(&mut self) {
        self.fs.open_files.fetch_sub(1, Ordering::SeqCst);
    }
}

/// The filesystem is shared with the test through an Arc.
struct Shared(Arc<ToyFs>);

impl Filesystem for Shared {
    type File = ToyFile;

    fn attach(&self, ctx: &Ctx) -> Result<Qid> {
        if ctx.uname == "nobody" {
            return Err(errors::PERM.into());
        }
        Ok(Qid::dir(ROOT))
    }

    fn walk1(&self, _: &Ctx, dir: Qid, name: &str) -> Result<Qid> {
        match (dir.path, name) {
            (ROOT, "hello") => Ok(Qid::file(HELLO)),
            (ROOT, "stream") => Ok(Qid::file(STREAM)),
            (ROOT, "sink") => Ok(Qid::file(SINK)),
            (ROOT, "query") => Ok(Qid::file(QUERY)),
            (ROOT, "sub") => Ok(Qid::dir(SUB)),
            (ROOT, "tmp") => Ok(Qid::file(TMP)),
            (ROOT, "..") | (SUB, "..") => Ok(Qid::dir(ROOT)),
            _ => Err(errors::NOT_FOUND.into()),
        }
    }

    fn stat(&self, _: &Ctx, qid: Qid) -> Result<Stat> {
        let (name, perm, len) = match qid.path {
            ROOT => ("/", 0o555, 0),
            HELLO => ("hello", 0o444, 6),
            STREAM => ("stream", 0o444, 0),
            SINK => ("sink", 0o222, 0),
            QUERY => ("query", 0o666, 0),
            SUB => ("sub", 0o555, 0),
            TMP => ("tmp", 0o666, 0),
            _ => return Err(errors::NOT_FOUND.into()),
        };
        Ok(Stat::new(name, qid, perm, len))
    }

    fn readdir(&self, ctx: &Ctx, dir: Qid) -> Result<Vec<Stat>> {
        match dir.path {
            ROOT => [HELLO, STREAM, SINK].iter().map(|&p| self.stat(ctx, Qid::file(p))).collect(),
            SUB => Ok(self.0.sub_listing()),
            _ => Err(errors::NOT_DIR.into()),
        }
    }

    fn open(&self, _: &Ctx, qid: Qid, mode: u8) -> Result<ToyFile> {
        let rw = mode & 3;
        match qid.path {
            HELLO | STREAM if rw != OREAD => return Err(errors::PERM.into()),
            SINK if rw != OWRITE => return Err(errors::PERM.into()),
            _ => {}
        }
        self.0.open_files.fetch_add(1, Ordering::SeqCst);
        Ok(ToyFile {
            fs: Arc::clone(&self.0),
            qid,
            cursor: AtomicU64::new(0),
            answer: Mutex::new(Answer::default()),
        })
    }

    fn remove(&self, _: &Ctx, qid: Qid) -> Result<()> {
        if qid.path != TMP {
            return Err(errors::PERM.into());
        }
        self.0.removed.lock().unwrap().push(qid.path);
        Ok(())
    }
}

impl File for ToyFile {
    async fn read(&self, offset: u64, count: u32) -> Result<Vec<u8>> {
        match self.qid.path {
            HELLO => Ok(read_at(b"hello\n", offset, count)),
            QUERY => Ok(self.answer.lock().unwrap().read(offset, count)),
            STREAM => loop {
                // Register for the wake before checking, so a publish
                // between the check and the await is not lost.
                let notified = self.fs.notify.notified();
                let cur = self.fs.seq.load(Ordering::SeqCst);
                if cur > self.cursor.load(Ordering::SeqCst) {
                    // Commit last: a cancelled read must not skip events.
                    self.cursor.store(cur, Ordering::SeqCst);
                    return Ok(format!("event {cur}\n").into_bytes());
                }
                notified.await;
            },
            _ => Err(errors::PERM.into()),
        }
    }

    async fn write(&self, _offset: u64, data: &[u8]) -> Result<u32> {
        match self.qid.path {
            SINK => self.fs.sink.lock().unwrap().extend_from_slice(data),
            QUERY => {
                let q = String::from_utf8_lossy(data);
                self.answer.lock().unwrap().set(format!("{}\n", q.trim().to_uppercase()));
            }
            _ => return Err(errors::PERM.into()),
        }
        Ok(data.len() as u32)
    }
}

// ── harness ──────────────────────────────────────────────────────────────

fn spawn_server(fs: &Arc<ToyFs>) -> DuplexStream {
    let (client_side, server_side) = tokio::io::duplex(1 << 17);
    let server = Server::new(Shared(Arc::clone(fs)));
    tokio::spawn(async move { server.serve_conn(server_side).await });
    client_side
}

async fn client(fs: &Arc<ToyFs>) -> Client<DuplexStream> {
    Client::new(spawn_server(fs), "glenda", "").await.expect("handshake")
}

/// A raw connection, for tests that need several requests in flight.
struct Raw(DuplexStream);

impl Raw {
    async fn new(fs: &Arc<ToyFs>) -> Self {
        let mut r = Raw(spawn_server(fs));
        r.send(NOTAG, Tmsg::Version { msize: 65536, version: "9P2000".into() }).await;
        assert!(matches!(r.recv().await, Some((_, Rmsg::Version { .. }))));
        r.send(1, Tmsg::Attach { fid: 0, afid: NOFID, uname: "t".into(), aname: "".into() }).await;
        assert!(matches!(r.recv().await, Some((1, Rmsg::Attach { .. }))));
        r
    }

    async fn send(&mut self, tag: u16, t: Tmsg) {
        self.0.write_all(&t.encode(tag)).await.unwrap();
    }

    /// The next reply, or None if nothing arrives within `WAIT`.
    async fn recv(&mut self) -> Option<(u16, Rmsg)> {
        self.recv_within(WAIT).await
    }

    async fn recv_within(&mut self, d: Duration) -> Option<(u16, Rmsg)> {
        timeout(d, async {
            let size = self.0.read_u32_le().await.ok()?;
            let mut buf = vec![0u8; size as usize - 4];
            self.0.read_exact(&mut buf).await.ok()?;
            Some(Rmsg::decode(&buf).expect("server sent an undecodable reply"))
        })
        .await
        .ok()
        .flatten()
    }

    async fn rpc(&mut self, tag: u16, t: Tmsg) -> Rmsg {
        self.send(tag, t).await;
        let (rtag, r) = self.recv().await.expect("no reply");
        assert_eq!(rtag, tag);
        r
    }

    /// Walk fid 0 -> `fid` at `name` and open it OREAD.
    async fn open(&mut self, fid: u32, name: &str) {
        let r = self.rpc(2, Tmsg::Walk { fid: 0, newfid: fid, wnames: vec![name.into()] }).await;
        assert!(matches!(r, Rmsg::Walk { .. }), "{r:?}");
        let r = self.rpc(3, Tmsg::Open { fid, mode: OREAD }).await;
        assert!(matches!(r, Rmsg::Open { .. }), "{r:?}");
    }
}

fn tread(fid: u32) -> Tmsg {
    Tmsg::Read { fid, offset: 0, count: 4096 }
}

fn ename(r: Rmsg) -> String {
    match r {
        Rmsg::Error { ename } => ename,
        other => panic!("expected Rerror, got {other:?}"),
    }
}

// ── 1-6: the manto suite ────────────────────────────────────────────────

#[tokio::test]
async fn t01_static_read_write_stat_and_errors() {
    let fs = Arc::new(ToyFs::default());
    let mut c = client(&fs).await;

    assert_eq!(c.read_string(&["hello"]).await.unwrap(), "hello\n");

    let fid = c.walk(&["hello"]).await.unwrap();
    c.open(fid, OREAD).await.unwrap();
    assert_eq!(c.read(fid, 2, 100).await.unwrap(), b"llo\n");
    assert!(c.read(fid, 100, 100).await.unwrap().is_empty());
    match c.write(fid, 0, b"nope").await {
        Err(ClientError::Server(m)) => assert_eq!(m, errors::PERM),
        other => panic!("expected permission denied, got {other:?}"),
    }
    c.clunk(fid).await.unwrap();

    let fid = c.walk(&["sink"]).await.unwrap();
    c.open(fid, OWRITE).await.unwrap();
    assert_eq!(c.write(fid, 0, b"cmd x\n").await.unwrap(), 6);
    c.clunk(fid).await.unwrap();
    assert_eq!(fs.sink.lock().unwrap().as_slice(), b"cmd x\n");

    let st = c.stat_path(&["hello"]).await.unwrap();
    assert_eq!((st.name.as_str(), st.length, st.mode), ("hello", 6, 0o444));

    // The filesystem's own error text reaches the client; a walk that
    // stops part way names the element it stopped at.
    match c.walk(&["ghost"]).await {
        Err(ClientError::Server(m)) => assert_eq!(m, errors::NOT_FOUND),
        other => panic!("expected the server's error, got {other:?}"),
    }
    match c.walk(&["sub", "ghost"]).await {
        Err(ClientError::NotFound(n)) => assert_eq!(n, "ghost"),
        other => panic!("expected not found, got {other:?}"),
    }

    // read_full fills across Treads and reports a short file.
    let fid = c.walk(&["hello"]).await.unwrap();
    c.open(fid, OREAD).await.unwrap();
    let mut buf = [0u8; 10];
    assert_eq!(c.read_full(fid, 1, &mut buf).await.unwrap(), 5);
    assert_eq!(&buf[..5], b"ello\n");
    c.clunk(fid).await.unwrap();
    let mut raw = Raw::new(&fs).await;
    let r = raw.rpc(5, Tmsg::Walk { fid: 0, newfid: 1, wnames: vec!["ghost".into()] }).await;
    assert_eq!(ename(r), errors::NOT_FOUND);
}

#[tokio::test]
async fn t02_directory_listing_reads_whole_entries() {
    let fs = Arc::new(ToyFs::default());
    let mut c = client(&fs).await;
    let names: Vec<String> = c.read_dir(&[]).await.unwrap().into_iter().map(|s| s.name).collect();
    assert_eq!(names, ["hello", "stream", "sink"]);
}

#[tokio::test]
async fn t03_blocking_read_parks_and_wakes() {
    let fs = Arc::new(ToyFs::default());
    let mut c = client(&fs).await;
    let fid = c.walk(&["stream"]).await.unwrap();
    c.open(fid, OREAD).await.unwrap();

    let fs2 = Arc::clone(&fs);
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(50)).await;
        fs2.publish();
    });
    let start = std::time::Instant::now();
    let data = timeout(WAIT, c.read(fid, 0, 4096)).await.expect("never woke").unwrap();
    assert_eq!(data, b"event 1\n");
    assert!(start.elapsed() >= Duration::from_millis(40), "read did not park");

    let fs2 = Arc::clone(&fs);
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(20)).await;
        fs2.publish();
    });
    let data = timeout(WAIT, c.read(fid, 0, 4096)).await.expect("never woke").unwrap();
    assert_eq!(data, b"event 2\n");

    // Clunk drops the per-open File.
    assert_eq!(fs.open_files.load(Ordering::SeqCst), 1);
    c.clunk(fid).await.unwrap();
    assert_eq!(fs.open_files.load(Ordering::SeqCst), 0);
}

/// A parked read is aborted, Rflush arrives promptly, and the flushed tag
/// never answers.
#[tokio::test]
async fn t04_tflush_cancels_parked_read_promptly() {
    let fs = Arc::new(ToyFs::default());
    let mut raw = Raw::new(&fs).await;
    raw.open(1, "stream").await;

    raw.send(7, tread(1)).await;
    tokio::time::sleep(Duration::from_millis(30)).await;
    let r = raw.rpc(8, Tmsg::Flush { oldtag: 7 }).await;
    assert_eq!(r, Rmsg::Flush);

    fs.publish();
    let r = raw.rpc(9, tread(1)).await; // asserts tag 9, not a late 7
    assert_eq!(r, Rmsg::Read { data: b"event 1\n".to_vec() });
}

#[tokio::test]
async fn t05_tauth_refused_politely() {
    let fs = Arc::new(ToyFs::default());
    let mut raw = Raw(spawn_server(&fs));
    raw.send(NOTAG, Tmsg::Version { msize: 65536, version: "9P2000".into() }).await;
    raw.recv().await.unwrap();
    let r = raw.rpc(2, Tmsg::Auth { afid: 5, uname: "u".into(), aname: "".into() }).await;
    assert_eq!(ename(r), errors::NO_AUTH);
}

#[tokio::test]
async fn t06_tcp_serve_and_dial() {
    let fs = Arc::new(ToyFs::default());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = Server::new(Shared(Arc::clone(&fs)));
    tokio::spawn(async move { server.serve_tcp(listener).await });

    let mut c = Client::dial(addr, "glenda", "").await.expect("dial");
    assert_eq!(c.read_string(&["hello"]).await.unwrap(), "hello\n");
}

// ── 7-10: one connection, many processes (where ninep 0.6.0 failed) ────

/// Two opens of one blocking file keep independent cursors: one publish
/// wakes both readers.
#[tokio::test]
async fn t07_two_opens_of_one_file_keep_independent_state() {
    let fs = Arc::new(ToyFs::default());
    let mut c = client(&fs).await;
    let a = c.walk(&["stream"]).await.unwrap();
    c.open(a, OREAD).await.unwrap();
    let b = c.walk(&["stream"]).await.unwrap();
    c.open(b, OREAD).await.unwrap();

    fs.publish();
    assert_eq!(timeout(WAIT, c.read(a, 0, 4096)).await.expect("a parked").unwrap(), b"event 1\n");
    assert_eq!(timeout(WAIT, c.read(b, 0, 4096)).await.expect("b parked").unwrap(), b"event 1\n");
}

/// Clunking one of two opens leaves the other's state alone.
#[tokio::test]
async fn t08_clunk_of_one_fid_leaves_sibling_state() {
    let fs = Arc::new(ToyFs::default());
    let mut c = client(&fs).await;
    let a = c.walk(&["stream"]).await.unwrap();
    c.open(a, OREAD).await.unwrap();
    fs.publish();
    assert_eq!(timeout(WAIT, c.read(a, 0, 4096)).await.unwrap().unwrap(), b"event 1\n");

    let b = c.walk(&["stream"]).await.unwrap();
    c.open(b, OREAD).await.unwrap();
    c.clunk(b).await.unwrap();

    // a has seen event 1: its next read parks rather than replaying it.
    assert!(timeout(Duration::from_millis(100), c.read(a, 0, 4096)).await.is_err());
}

/// A parked read does not stall other requests on the connection.
#[tokio::test]
async fn t09_parked_read_does_not_stall_connection() {
    let fs = Arc::new(ToyFs::default());
    let mut raw = Raw::new(&fs).await;
    raw.open(1, "stream").await;
    raw.open(2, "hello").await;

    raw.send(7, tread(1)).await; // parks
    let r = raw.rpc(10, tread(2)).await;
    assert_eq!(r, Rmsg::Read { data: b"hello\n".to_vec() });

    fs.publish();
    assert_eq!(raw.recv().await, Some((7, Rmsg::Read { data: b"event 1\n".to_vec() })));
}

/// Directory reads honour count and continue at the returned offset, in
/// whole entries.
#[tokio::test]
async fn t10_directory_read_continues_on_entry_boundaries() {
    let fs = Arc::new(ToyFs::default());
    let mut c = client(&fs).await;
    let fid = c.walk(&[]).await.unwrap();
    c.open(fid, OREAD).await.unwrap();
    let all = c.read(fid, 0, 65535).await.unwrap();
    let first = u16::from_le_bytes([all[0], all[1]]) as usize + 2;

    let one = c.read(fid, 0, first as u32 + 5).await.unwrap();
    assert_eq!(one.len(), first, "split an entry or overran count");
    let rest = c.read(fid, first as u64, 65535).await.unwrap();
    assert_eq!(rest, all[first..]);
    assert!(c.read(fid, all.len() as u64, 65535).await.unwrap().is_empty());
}

// ── the rest of the protocol ────────────────────────────────────────────

/// A listing that grows between reads cannot tear an entry: the fid reads
/// from the snapshot taken at offset 0 until it starts over.
#[tokio::test]
async fn t11_directory_snapshot_survives_a_changing_listing() {
    let fs = Arc::new(ToyFs::default());
    fs.sub_entries.store(200, Ordering::SeqCst);
    let mut c = client(&fs).await;
    let fid = c.walk(&["sub"]).await.unwrap();
    c.open(fid, OREAD).await.unwrap();

    let mut got = Vec::new();
    let mut off = 0u64;
    loop {
        let chunk = c.read(fid, off, 1000).await.unwrap();
        if chunk.is_empty() {
            break;
        }
        off += chunk.len() as u64;
        got.extend(Stat::decode_all(&chunk).expect("torn entry"));
        // Shrink the listing under the reader.
        fs.sub_entries.store(3, Ordering::SeqCst);
    }
    assert_eq!(got.len(), 200);

    // Starting over sees the new listing.
    assert_eq!(Stat::decode_all(&c.read(fid, 0, 65535).await.unwrap()).unwrap().len(), 3);
    // An offset that is not an entry boundary is refused.
    let r = c.read(fid, 1, 100).await;
    assert!(matches!(r, Err(ClientError::Server(_))), "{r:?}");
}

/// Clunking a fid with a parked read answers that read (Rerror) rather than
/// leaving its tag hanging, then answers the clunk.
#[tokio::test]
async fn t12_clunk_answers_parked_reads() {
    let fs = Arc::new(ToyFs::default());
    let mut raw = Raw::new(&fs).await;
    raw.open(1, "stream").await;
    raw.send(7, tread(1)).await;
    tokio::time::sleep(Duration::from_millis(20)).await;
    raw.send(8, Tmsg::Clunk { fid: 1 }).await;
    assert_eq!(raw.recv().await, Some((7, Rmsg::error(errors::CLUNKED))));
    assert_eq!(raw.recv().await, Some((8, Rmsg::Clunk)));
    assert_eq!(fs.open_files.load(Ordering::SeqCst), 0);
}

/// Tversion aborts outstanding reads without replies and clunks every fid.
#[tokio::test]
async fn t13_tversion_resets_the_session() {
    let fs = Arc::new(ToyFs::default());
    let mut raw = Raw::new(&fs).await;
    raw.open(1, "stream").await;
    raw.send(7, tread(1)).await;
    tokio::time::sleep(Duration::from_millis(20)).await;

    raw.send(NOTAG, Tmsg::Version { msize: 8192, version: "9P2000".into() }).await;
    assert_eq!(
        raw.recv().await,
        Some((NOTAG, Rmsg::Version { msize: 8192, version: "9P2000".into() }))
    );
    assert_eq!(fs.open_files.load(Ordering::SeqCst), 0);
    let r = raw.rpc(9, Tmsg::Stat { fid: 1 }).await;
    assert_eq!(ename(r), errors::BAD_FID);
}

/// msize negotiates down, never up; reads are capped by it; other
/// versions get "unknown", and 9P2000.L/.u are answered with plain 9P2000.
#[tokio::test]
async fn t14_version_negotiation() {
    let fs = Arc::new(ToyFs::default());
    let mut raw = Raw(spawn_server(&fs));
    let v = |m: u32, s: &str| Tmsg::Version { msize: m, version: s.into() };

    assert_eq!(
        raw.rpc(NOTAG, v(1 << 20, "9P2000")).await,
        Rmsg::Version { msize: 65536, version: "9P2000".into() }
    );
    assert_eq!(
        raw.rpc(NOTAG, v(4096, "9P2000.L")).await,
        Rmsg::Version { msize: 4096, version: "9P2000".into() }
    );
    assert!(
        matches!(raw.rpc(NOTAG, v(8192, "9P1")).await, Rmsg::Version { version, .. } if version == "unknown")
    );

    // With msize 4096, a big read gets at most 4096 - IOHDRSZ bytes.
    let fs = Arc::new(ToyFs::default());
    fs.sub_entries.store(500, Ordering::SeqCst);
    let mut c = Client::with_msize(spawn_server(&fs), "glenda", "", 4096).await.unwrap();
    assert_eq!(c.msize(), 4096);
    let fid = c.walk(&["sub"]).await.unwrap();
    c.open(fid, OREAD).await.unwrap();
    let n = c.read(fid, 0, 1 << 20).await.unwrap().len();
    assert!(n > 0 && n <= (4096 - proto::IOHDRSZ) as usize, "{n}");
}

/// walk(5): partial walks, clone walks, walking from a file, "..", and fid
/// rules.
#[tokio::test]
async fn t15_walk_semantics() {
    let fs = Arc::new(ToyFs::default());
    let mut raw = Raw::new(&fs).await;
    let w = |fid, newfid, names: &[&str]| Tmsg::Walk {
        fid,
        newfid,
        wnames: names.iter().map(|s| s.to_string()).collect(),
    };

    // Partial walk: Rwalk with the qids that worked; newfid stays unbound.
    let r = raw.rpc(2, w(0, 1, &["sub", "nope"])).await;
    assert_eq!(r, Rmsg::Walk { wqids: vec![Qid::dir(SUB)] });
    assert_eq!(ename(raw.rpc(3, Tmsg::Stat { fid: 1 }).await), errors::BAD_FID);

    // Clone walk.
    assert_eq!(raw.rpc(4, w(0, 1, &[])).await, Rmsg::Walk { wqids: vec![] });
    // newfid in use.
    assert_eq!(ename(raw.rpc(5, w(0, 1, &["hello"])).await), errors::FID_IN_USE);
    // Walk in place (newfid == fid), then "..".
    assert!(matches!(raw.rpc(6, w(1, 1, &["sub"])).await, Rmsg::Walk { .. }));
    assert_eq!(raw.rpc(7, w(1, 1, &[".."])).await, Rmsg::Walk { wqids: vec![Qid::dir(ROOT)] });
    // Walking from a file.
    assert!(matches!(raw.rpc(8, w(0, 2, &["hello"])).await, Rmsg::Walk { .. }));
    assert_eq!(ename(raw.rpc(9, w(2, 3, &["x"])).await), errors::NOT_DIR);
    // Walking an open fid.
    assert!(matches!(raw.rpc(10, Tmsg::Open { fid: 2, mode: OREAD }).await, Rmsg::Open { .. }));
    assert_eq!(ename(raw.rpc(11, w(2, 3, &[])).await), errors::OPEN);
    // More than MAXWELEM elements.
    let many = vec!["sub"; proto::MAXWELEM + 1];
    assert!(matches!(raw.rpc(12, w(0, 4, &many)).await, Rmsg::Error { .. }));
    // Unknown fid.
    assert_eq!(ename(raw.rpc(13, w(99, 4, &[])).await), errors::BAD_FID);
}

/// The file-as-RPC idiom: write a query, read the answer, on the same fid;
/// two fids get two answers.
#[tokio::test]
async fn t16_write_then_read_is_per_fid() {
    let fs = Arc::new(ToyFs::default());
    let mut c = client(&fs).await;
    let a = c.walk(&["query"]).await.unwrap();
    c.open(a, proto::ORDWR).await.unwrap();
    let b = c.walk(&["query"]).await.unwrap();
    c.open(b, proto::ORDWR).await.unwrap();

    c.write_all(a, 0, b"near 1 2\n").await.unwrap();
    c.write_all(b, 0, b"limit 5\n").await.unwrap();
    assert_eq!(c.read(a, 0, 100).await.unwrap(), b"NEAR 1 2\n");
    assert_eq!(c.read(b, 0, 100).await.unwrap(), b"LIMIT 5\n");

    // Inferno's `{echo q >[1=0]; cat} <> query` reads on at the offset the
    // write left (9 here), in small pieces: the answer still starts at 0.
    c.write_all(a, 0, b"near 3 4\n").await.unwrap();
    assert_eq!(c.read(a, 9, 4).await.unwrap(), b"NEAR");
    assert_eq!(c.read(a, 13, 100).await.unwrap(), b" 3 4\n");
    assert!(c.read(a, 18, 100).await.unwrap().is_empty());
    // Seeking back to 0 also starts from the top.
    assert_eq!(c.read(a, 0, 100).await.unwrap(), b"NEAR 3 4\n");
}

/// open(5) rules: directories only for reading, no double open, I/O needs
/// an open fid, and the filesystem's refusals pass through.
#[tokio::test]
async fn t17_open_rules() {
    let fs = Arc::new(ToyFs::default());
    let mut raw = Raw::new(&fs).await;
    raw.rpc(2, Tmsg::Walk { fid: 0, newfid: 1, wnames: vec![] }).await;
    assert_eq!(ename(raw.rpc(3, Tmsg::Open { fid: 1, mode: OWRITE }).await), errors::IS_DIR);
    assert_eq!(ename(raw.rpc(4, tread(1)).await), errors::NOT_OPEN);
    assert!(matches!(raw.rpc(5, Tmsg::Open { fid: 1, mode: OREAD }).await, Rmsg::Open { .. }));
    assert_eq!(ename(raw.rpc(6, Tmsg::Open { fid: 1, mode: OREAD }).await), errors::OPEN);
    let w = Tmsg::Write { fid: 1, offset: 0, data: b"x".to_vec() };
    assert_eq!(ename(raw.rpc(7, w).await), errors::IS_DIR);

    raw.rpc(8, Tmsg::Walk { fid: 0, newfid: 2, wnames: vec!["hello".into()] }).await;
    assert_eq!(ename(raw.rpc(9, Tmsg::Open { fid: 2, mode: OWRITE }).await), errors::PERM);
}

/// remove(5) clunks the fid even when it fails; ORCLOSE removes on clunk;
/// create is refused by default.
#[tokio::test]
async fn t18_remove_orclose_and_create() {
    let fs = Arc::new(ToyFs::default());
    let mut raw = Raw::new(&fs).await;
    raw.rpc(2, Tmsg::Walk { fid: 0, newfid: 1, wnames: vec!["hello".into()] }).await;
    assert_eq!(ename(raw.rpc(3, Tmsg::Remove { fid: 1 }).await), errors::PERM);
    assert_eq!(ename(raw.rpc(4, Tmsg::Clunk { fid: 1 }).await), errors::BAD_FID);

    raw.rpc(5, Tmsg::Walk { fid: 0, newfid: 1, wnames: vec!["tmp".into()] }).await;
    assert_eq!(raw.rpc(6, Tmsg::Remove { fid: 1 }).await, Rmsg::Remove);

    raw.rpc(7, Tmsg::Walk { fid: 0, newfid: 1, wnames: vec!["tmp".into()] }).await;
    raw.rpc(8, Tmsg::Open { fid: 1, mode: proto::ORDWR | ORCLOSE }).await;
    assert_eq!(raw.rpc(9, Tmsg::Clunk { fid: 1 }).await, Rmsg::Clunk);
    assert_eq!(*fs.removed.lock().unwrap(), vec![TMP, TMP]);

    raw.rpc(10, Tmsg::Walk { fid: 0, newfid: 1, wnames: vec![] }).await;
    let c = Tmsg::Create { fid: 1, name: "new".into(), perm: 0o644, mode: OWRITE };
    assert_eq!(ename(raw.rpc(11, c).await), errors::PERM);
}

/// attach(5): the filesystem sees the uname; an afid is refused; a fid in
/// use is refused.
#[tokio::test]
async fn t19_attach_rules() {
    let fs = Arc::new(ToyFs::default());
    let mut raw = Raw(spawn_server(&fs));
    raw.rpc(NOTAG, Tmsg::Version { msize: 8192, version: "9P2000".into() }).await;
    let a = |fid, afid, u: &str| Tmsg::Attach { fid, afid, uname: u.into(), aname: "".into() };
    assert_eq!(ename(raw.rpc(1, a(0, NOFID, "nobody")).await), errors::PERM);
    assert_eq!(ename(raw.rpc(2, a(0, 3, "glenda")).await), errors::NO_AUTH);
    assert!(matches!(raw.rpc(3, a(0, NOFID, "glenda")).await, Rmsg::Attach { .. }));
    assert_eq!(ename(raw.rpc(4, a(0, NOFID, "glenda")).await), errors::FID_IN_USE);
}

/// Unknown message types and malformed bodies get Rerror on their tag; a
/// frame larger than msize closes the connection; wstat defaults to refusal.
#[tokio::test]
async fn t20_bad_input() {
    let fs = Arc::new(ToyFs::default());
    let mut raw = Raw::new(&fs).await;

    raw.0.write_all(&[7, 0, 0, 0, 200, 9, 0]).await.unwrap();
    assert_eq!(raw.recv().await, Some((9, Rmsg::error(errors::UNSUPPORTED))));

    // A Tread missing its count.
    raw.0
        .write_all(&[15, 0, 0, 0, proto::msgtype::TREAD, 10, 0, 1, 0, 0, 0, 0, 0, 0, 0])
        .await
        .unwrap();
    assert!(matches!(raw.recv().await, Some((10, Rmsg::Error { .. }))));

    let r = raw.rpc(11, Tmsg::Wstat { fid: 0, stat: Stat::dont_touch() }).await;
    assert_eq!(ename(r), errors::PERM);

    // Still alive after all that.
    assert!(matches!(raw.rpc(12, Tmsg::Stat { fid: 0 }).await, Rmsg::Stat { .. }));

    raw.0.write_all(&(1u32 << 20).to_le_bytes()).await.unwrap();
    let mut b = [0u8; 1];
    let n = timeout(WAIT, raw.0.read(&mut b)).await.expect("server kept a broken connection");
    assert_eq!(n.unwrap(), 0, "expected EOF");
}

/// Hanging up with a parked read drops every File.
#[tokio::test]
async fn t21_disconnect_drops_everything() {
    let fs = Arc::new(ToyFs::default());
    {
        let mut raw = Raw::new(&fs).await;
        raw.open(1, "stream").await;
        raw.open(2, "hello").await;
        raw.send(7, tread(1)).await;
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert_eq!(fs.open_files.load(Ordering::SeqCst), 2);
    }
    for _ in 0..50 {
        if fs.open_files.load(Ordering::SeqCst) == 0 {
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("{} files still open after disconnect", fs.open_files.load(Ordering::SeqCst));
}

/// Many parked readers across many connections all wake on one publish.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn t22_many_readers_many_connections() {
    let fs = Arc::new(ToyFs::default());
    let mut tasks = Vec::new();
    for _ in 0..16 {
        let mut c = client(&fs).await;
        let mut fids = Vec::new();
        for _ in 0..4 {
            let f = c.walk(&["stream"]).await.unwrap();
            c.open(f, OREAD).await.unwrap();
            fids.push(f);
        }
        tasks.push(tokio::spawn(async move {
            for f in fids {
                assert_eq!(c.read(f, 0, 4096).await.unwrap(), b"event 1\n");
            }
        }));
    }
    tokio::time::sleep(Duration::from_millis(20)).await;
    fs.publish();
    for t in tasks {
        timeout(Duration::from_secs(2), t).await.expect("a reader never woke").unwrap();
    }
}

/// A read that already has its data is never dropped by a Tflush, even when
/// the reply queue is full so the reply is still waiting to be sent (the
/// File advanced its cursor to produce it: dropping it would lose an event).
/// The number of replies queued ahead of the flush is swept so that, for
/// some count, the flush is handled while the woken read is still waiting
/// for queue space.
#[tokio::test]
async fn t23_flush_never_drops_a_read_that_has_its_data() {
    for backlog in 40..110u16 {
        let fs = Arc::new(ToyFs::default());
        let (client_side, server_side) = tokio::io::duplex(64);
        let server = Server::new(Shared(Arc::clone(&fs)));
        tokio::spawn(async move { server.serve_conn(server_side).await });
        let mut raw = Raw(client_side);
        raw.send(NOTAG, Tmsg::Version { msize: 8192, version: "9P2000".into() }).await;
        raw.recv().await.unwrap();
        raw.send(1, Tmsg::Attach { fid: 0, afid: NOFID, uname: "t".into(), aname: "".into() })
            .await;
        raw.recv().await.unwrap();
        raw.open(1, "stream").await;
        raw.open(2, "hello").await;

        let (mut rd, mut wr) = tokio::io::split(raw.0);
        wr.write_all(&tread(1).encode(7)).await.unwrap(); // parks
        // Queue replies faster than we read them, then flush tag 7.
        let fs2 = Arc::clone(&fs);
        let sender = tokio::spawn(async move {
            for t in 0..backlog {
                wr.write_all(&tread(2).encode(100 + t)).await.unwrap();
            }
            fs2.publish(); // tag 7's read now has its data
            tokio::time::sleep(Duration::from_millis(5)).await;
            wr.write_all(&Tmsg::Flush { oldtag: 7 }.encode(8)).await.unwrap();
            wr.write_all(&tread(1).encode(9)).await.unwrap();
            wr
        });
        tokio::time::sleep(Duration::from_millis(20)).await;

        // Drain. Event 1 must arrive exactly once: on tag 7 (before
        // Rflush) or, if tag 7 was cancelled in time, on tag 9.
        let mut events = Vec::new();
        let mut saw_flush = false;
        loop {
            let got = timeout(WAIT, async {
                let size = rd.read_u32_le().await.unwrap();
                let mut buf = vec![0u8; size as usize - 4];
                rd.read_exact(&mut buf).await.unwrap();
                Rmsg::decode(&buf).unwrap()
            })
            .await;
            match got {
                Ok((8, Rmsg::Flush)) => saw_flush = true,
                Ok((tag @ (7 | 9), Rmsg::Read { data })) => {
                    events.push((tag, String::from_utf8(data).unwrap()));
                }
                Ok(_) => {}
                Err(_) => break,
            }
            // Decided: tag 9 answered, or tag 7 answered and was flushed.
            if events.iter().any(|(t, _)| *t == 9) || (saw_flush && !events.is_empty()) {
                break;
            }
        }
        sender.await.unwrap();
        assert!(saw_flush, "backlog {backlog}: no Rflush");
        let ones: Vec<_> = events.iter().filter(|(_, e)| e == "event 1\n").collect();
        assert_eq!(ones.len(), 1, "backlog {backlog}: event 1 lost or duplicated: {events:?}");
    }
}
