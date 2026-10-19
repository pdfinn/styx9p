# styx9p

An async **9P2000** server and client for Rust and [tokio], faithful to how
Plan 9 and Inferno actually use the protocol.

> **Styx and 9P2000 are the same protocol.** Styx is Inferno's name for it;
> the two have been identical since Inferno's 4th edition. This crate is
> **not** 9P2000.L (the Linux v9fs variant served by `rs9p`, `p9`, and
> friends). A client asking for `9P2000.L` or `9P2000.u` is answered with
> plain `9P2000`, as `version(5)` prescribes.

9P's best trick is the *synthetic* file server: a program that presents its
state and controls as files — a stream you `cat`, a `ctl` you `echo` into, a
`query` you write and read back. styx9p is built for exactly that, and gets
right the parts that synthetic servers depend on and that are easy to get
wrong:

- **Blocking reads.** `File::read` is `async` and may wait as long as it
  likes for data. Each read runs on its own task, so a parked read never
  stalls the rest of the connection.
- **Prompt `Tflush`.** When a reader is interrupted, its read is aborted and
  `Rflush` goes out at once. The flushed request never answers. (Servers that
  hold the `Rflush` until the read completes leave an interrupted `cat`
  hanging until the next event arrives.)
- **Per-open state.** Every open fid gets its own `File` value. Inferno
  multiplexes every process using a mount over *one* connection, so two
  readers of the same stream need independent cursors, and a
  write-a-query-then-read-the-answer file needs per-fid answers. styx9p hands
  you a value per open; `Drop` is your clunk hook.
- **Directory reads in whole stat entries**, served from a per-fid snapshot
  taken at offset 0, so a listing that changes between reads cannot tear an
  entry.

The only dependency is tokio. No `unsafe`.

## A server

```rust
use styx9p::{Ctx, File, Filesystem, Qid, Result, Server, Stat, errors, read_at};

struct Hello;
struct HelloFile;

impl Filesystem for Hello {
    type File = HelloFile;

    fn attach(&self, _: &Ctx) -> Result<Qid> {
        Ok(Qid::dir(0))
    }
    fn walk1(&self, _: &Ctx, _dir: Qid, name: &str) -> Result<Qid> {
        match name {
            "hello" => Ok(Qid::file(1)),
            ".." => Ok(Qid::dir(0)),
            _ => Err(errors::NOT_FOUND.into()),
        }
    }
    fn stat(&self, _: &Ctx, qid: Qid) -> Result<Stat> {
        Ok(match qid.path {
            0 => Stat::new("/", qid, 0o555, 0),
            _ => Stat::new("hello", qid, 0o444, 6),
        })
    }
    fn readdir(&self, ctx: &Ctx, _dir: Qid) -> Result<Vec<Stat>> {
        Ok(vec![self.stat(ctx, Qid::file(1))?])
    }
    fn open(&self, _: &Ctx, _qid: Qid, _mode: u8) -> Result<HelloFile> {
        Ok(HelloFile)
    }
}

impl File for HelloFile {
    async fn read(&self, offset: u64, count: u32) -> Result<Vec<u8>> {
        Ok(read_at(b"hello\n", offset, count))
    }
}

#[tokio::main]
async fn main() -> std::io::Result<()> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:5640").await?;
    Server::new(Hello).serve_tcp(listener).await
}
```

[`examples/hello.rs`](examples/hello.rs) adds a blocking stream (`ticks`)
and a write-then-read `query` file:

```sh
cargo run --example hello

# plan9port
9p -a 'tcp!127.0.0.1!5640' ls -l
9p -a 'tcp!127.0.0.1!5640' read ticks       # a line a second

# Inferno
mount -A tcp!127.0.0.1!5640 /n/hello
cat /n/hello/ticks                          # interrupt it: no hang
{echo how many words >[1=0]; cat} <> /n/hello/query
```

### The model, briefly

| Piece | Runs | Holds |
|---|---|---|
| `Filesystem` (`attach`, `walk1`, `stat`, `readdir`, `open`, and optionally `create`, `remove`, `wstat`) | synchronously, on the connection's task | the tree |
| `File` (`read`, `write`) | `read`: its own task, cancellable; `write`: in order on the connection | per-open state |

Make `File::read` cancel-safe: a flushed read is dropped at whatever
`.await` it is parked on, so commit state (advance a cursor) only as the
last step before returning. `Answer` is a small helper for write-then-read
files that pages the answer correctly when a client reads on from the
offset its write left behind (as `<>` redirection in Inferno's sh does).

Errors are strings that reach the client verbatim as `Rerror`;
`styx9p::errors` has the conventional Plan 9/Inferno texts.

## A client

```rust
let mut c = styx9p::Client::dial("127.0.0.1:5640", "glenda", "").await?;
print!("{}", c.read_string(&["hello"]).await?);
for s in c.read_dir(&[]).await? {
    println!("{} {}", s.name, s.length);
}
```

The client keeps one request in flight at a time: enough to consume a 9P
service or test a server. For concurrent requests, open more connections or
drive `styx9p::proto` (a complete, I/O-free codec for every 9P2000 message)
directly.

## Conformance

`tests/conformance.rs` covers `version`, `attach`, `walk` (partial, clone,
in-place, `..`, `MAXWELEM`), `open`, `create`, `read`, `write`, `clunk`,
`remove`/`ORCLOSE`, `stat`/`wstat`, `flush` of parked reads, session reset
by `Tversion`, msize negotiation, directory reads, malformed input, and
disconnects with reads outstanding. `scripts/interop.sh` runs the example
against real clients, plan9port's `9p(1)` and Inferno's `mount`, including
killing blocked readers:

```sh
PLAN9=/usr/local/plan9 INFERNO_ROOT=/usr/inferno scripts/interop.sh
```

## Not yet

- Authentication: `Tauth` is answered "authentication not required".
- A multiplexing client.
- 9P2000.u / 9P2000.L. (Out of scope; see the other crates for Linux v9fs.)

## Licence

Licensed under either of [Apache License, Version 2.0](LICENSE-APACHE) or
[MIT license](LICENSE-MIT) at your option. Unless you explicitly state
otherwise, any contribution intentionally submitted for inclusion in this
crate by you, as defined in the Apache-2.0 license, shall be dual licensed
as above, without any additional terms or conditions.

[tokio]: https://tokio.rs
