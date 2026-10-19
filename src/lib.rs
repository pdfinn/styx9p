//! # styx9p
//!
//! An async [9P2000] server and client for [tokio], faithful to how Plan 9
//! and Inferno actually use the protocol. (Styx is Inferno's name for
//! 9P2000; they have been the same protocol since Inferno's 4th edition.
//! This is **not** 9P2000.L, the Linux variant.)
//!
//! What it gets right that synthetic file servers need:
//!
//! - **Blocking reads.** [`File::read`] is async and may wait as long as it
//!   likes for data; each read runs on its own task, so one parked read
//!   never stalls the connection.
//! - **Prompt `Tflush`.** A flushed read is aborted and `Rflush` is sent at
//!   once, so an interrupted reader on Inferno or Plan 9 unwinds instead of
//!   hanging until the next event.
//! - **Per-open state.** Every open fid gets its own [`File`] value, so two
//!   readers of the same stream on one mount (Inferno multiplexes every
//!   process over one connection) keep independent cursors, and the
//!   write-a-query-then-read-the-answer idiom works per fid.
//! - **Directory reads in whole entries**, from a per-fid snapshot.
//!
//! The only dependency is tokio.
//!
//! ```no_run
//! use styx9p::{Ctx, File, Filesystem, Qid, Result, Server, Stat, errors, read_at};
//!
//! struct Hello;
//! struct HelloFile;
//!
//! impl Filesystem for Hello {
//!     type File = HelloFile;
//!     fn attach(&self, _: &Ctx) -> Result<Qid> { Ok(Qid::dir(0)) }
//!     fn walk1(&self, _: &Ctx, _dir: Qid, name: &str) -> Result<Qid> {
//!         match name {
//!             "hello" => Ok(Qid::file(1)),
//!             ".." => Ok(Qid::dir(0)),
//!             _ => Err(errors::NOT_FOUND.into()),
//!         }
//!     }
//!     fn stat(&self, _: &Ctx, qid: Qid) -> Result<Stat> {
//!         Ok(match qid.path {
//!             0 => Stat::new("/", qid, 0o555, 0),
//!             _ => Stat::new("hello", qid, 0o444, 6),
//!         })
//!     }
//!     fn readdir(&self, ctx: &Ctx, _dir: Qid) -> Result<Vec<Stat>> {
//!         Ok(vec![self.stat(ctx, Qid::file(1))?])
//!     }
//!     fn open(&self, _: &Ctx, _qid: Qid, _mode: u8) -> Result<HelloFile> { Ok(HelloFile) }
//! }
//!
//! impl File for HelloFile {
//!     async fn read(&self, offset: u64, count: u32) -> Result<Vec<u8>> {
//!         Ok(read_at(b"hello\n", offset, count))
//!     }
//! }
//!
//! #[tokio::main]
//! async fn main() -> std::io::Result<()> {
//!     let listener = tokio::net::TcpListener::bind("127.0.0.1:5640").await?;
//!     Server::new(Hello).serve_tcp(listener).await
//! }
//! ```
//!
//! [9P2000]: https://9p.io/magic/man2html/5/intro

pub mod client;
pub mod proto;
pub mod server;

pub use client::{Client, ClientError, Fid};
pub use proto::{Qid, Stat};
pub use server::{Answer, Ctx, Error, File, Filesystem, Result, Server, errors, read_at};
