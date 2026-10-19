//! A small synthetic file server showing the three kinds of file styx9p is
//! built for:
//!
//! - `hello`: static text;
//! - `ticks`: a blocking stream; each open gets one line per tick, and a
//!   read waits for the next tick;
//! - `query`: write a line, read the answer back on the same fid.
//!
//! ```sh
//! cargo run --example hello              # serves tcp!127.0.0.1!5640
//!
//! # plan9port
//! 9p -a 'tcp!127.0.0.1!5640' ls -l
//! 9p -a 'tcp!127.0.0.1!5640' read hello
//! 9p -a 'tcp!127.0.0.1!5640' read ticks  # one line a second; ^C to stop
//!
//! # Inferno
//! mount -A tcp!127.0.0.1!5640 /n/hello
//! cat /n/hello/ticks                     # interrupt it: Tflush, no hang
//! ```

use std::sync::Mutex;
use std::time::Duration;

use styx9p::{Answer, Ctx, File, Filesystem, Qid, Result, Server, Stat, errors, read_at};
use tokio::sync::watch;

const ROOT: u64 = 0;
const HELLO: u64 = 1;
const TICKS: u64 = 2;
const QUERY: u64 = 3;

struct Hello {
    ticks: watch::Receiver<u64>,
}

enum HelloFile {
    Hello,
    Ticks { rx: watch::Receiver<u64>, seen: Mutex<u64> },
    Query { answer: Mutex<Answer> },
}

impl Filesystem for Hello {
    type File = HelloFile;

    fn attach(&self, _: &Ctx) -> Result<Qid> {
        Ok(Qid::dir(ROOT))
    }

    fn walk1(&self, _: &Ctx, _dir: Qid, name: &str) -> Result<Qid> {
        match name {
            "hello" => Ok(Qid::file(HELLO)),
            "ticks" => Ok(Qid::file(TICKS)),
            "query" => Ok(Qid::file(QUERY)),
            ".." => Ok(Qid::dir(ROOT)),
            _ => Err(errors::NOT_FOUND.into()),
        }
    }

    fn stat(&self, _: &Ctx, qid: Qid) -> Result<Stat> {
        let s = match qid.path {
            ROOT => Stat::new("/", qid, 0o555, 0),
            HELLO => Stat::new("hello", qid, 0o444, 6),
            TICKS => Stat::new("ticks", qid, 0o444, 0),
            QUERY => Stat::new("query", qid, 0o666, 0),
            _ => return Err(errors::NOT_FOUND.into()),
        };
        Ok(s.owned_by("hello"))
    }

    fn readdir(&self, ctx: &Ctx, _dir: Qid) -> Result<Vec<Stat>> {
        [HELLO, TICKS, QUERY].iter().map(|&p| self.stat(ctx, Qid::file(p))).collect()
    }

    fn open(&self, _: &Ctx, qid: Qid, mode: u8) -> Result<HelloFile> {
        let writing = mode & 3 != styx9p::proto::OREAD;
        Ok(match qid.path {
            HELLO | TICKS if writing => return Err(errors::PERM.into()),
            HELLO => HelloFile::Hello,
            TICKS => {
                let rx = self.ticks.clone();
                let seen = Mutex::new(*rx.borrow());
                HelloFile::Ticks { rx, seen }
            }
            QUERY => {
                let mut answer = Answer::default();
                answer.set("write something first\n");
                HelloFile::Query { answer: Mutex::new(answer) }
            }
            _ => return Err(errors::NOT_FOUND.into()),
        })
    }
}

impl File for HelloFile {
    async fn read(&self, offset: u64, count: u32) -> Result<Vec<u8>> {
        match self {
            HelloFile::Hello => Ok(read_at(b"hello\n", offset, count)),
            HelloFile::Query { answer } => Ok(answer.lock().unwrap().read(offset, count)),
            // A stream ignores the offset: every read is the next line.
            HelloFile::Ticks { rx, seen } => {
                let mut rx = rx.clone();
                let last = *seen.lock().unwrap();
                let now = *rx.wait_for(|&t| t > last).await.map_err(|_| "server shutting down")?;
                *seen.lock().unwrap() = now; // commit only once we have the line
                Ok(format!("tick {now}\n").into_bytes())
            }
        }
    }

    async fn write(&self, _offset: u64, data: &[u8]) -> Result<u32> {
        match self {
            HelloFile::Query { answer } => {
                let q = String::from_utf8_lossy(data);
                let words = q.split_whitespace().count();
                answer.lock().unwrap().set(format!("{words} words\n"));
                Ok(data.len() as u32)
            }
            _ => Err(errors::PERM.into()),
        }
    }
}

#[tokio::main]
async fn main() -> std::io::Result<()> {
    let addr = std::env::args().nth(1).unwrap_or_else(|| "127.0.0.1:5640".into());
    let (tx, ticks) = watch::channel(0u64);
    tokio::spawn(async move {
        let mut n = 0;
        loop {
            tokio::time::sleep(Duration::from_secs(1)).await;
            n += 1;
            let _ = tx.send(n);
        }
    });

    let listener = tokio::net::TcpListener::bind(&addr).await?;
    eprintln!("serving on tcp!{}", addr.replace(':', "!"));
    Server::new(Hello { ticks }).serve_tcp(listener).await
}
