//! The 9P2000 wire format: message types, `Qid`, `Stat`, and a symmetric
//! little-endian codec for every T- and R-message.
//!
//! This module does no I/O. A frame on the wire is `size[4] type[1] tag[2]
//! body`, where `size` counts itself. [`Tmsg::encode`] and [`Rmsg::encode`]
//! produce whole frames; [`Tmsg::decode`] and [`Rmsg::decode`] take a frame
//! with the leading `size[4]` already stripped (which is how a reader that
//! has just consumed the size prefix holds it).
//!
//! Reference: Plan 9 section 5 manual pages (`intro(5)`, `version(5)`, …),
//! identical to Inferno's `styx(5)` since Inferno's 4th edition.

use std::fmt;

/// The protocol version string this crate speaks.
pub const VERSION: &str = "9P2000";

/// The tag used by `Tversion`.
pub const NOTAG: u16 = 0xFFFF;
/// "No fid": the `afid` of an unauthenticated `Tattach`.
pub const NOFID: u32 = 0xFFFF_FFFF;
/// Maximum path elements in one `Twalk` (Plan 9 `MAXWELEM`).
pub const MAXWELEM: usize = 16;
/// Bytes of an `Rread`/`Twrite` frame that are not data (Plan 9 `IOHDRSZ`).
pub const IOHDRSZ: u32 = 24;

// Message type numbers (Plan 9 fcall.h).
#[allow(missing_docs)]
pub mod msgtype {
    pub const TVERSION: u8 = 100;
    pub const RVERSION: u8 = 101;
    pub const TAUTH: u8 = 102;
    pub const RAUTH: u8 = 103;
    pub const TATTACH: u8 = 104;
    pub const RATTACH: u8 = 105;
    pub const RERROR: u8 = 107;
    pub const TFLUSH: u8 = 108;
    pub const RFLUSH: u8 = 109;
    pub const TWALK: u8 = 110;
    pub const RWALK: u8 = 111;
    pub const TOPEN: u8 = 112;
    pub const ROPEN: u8 = 113;
    pub const TCREATE: u8 = 114;
    pub const RCREATE: u8 = 115;
    pub const TREAD: u8 = 116;
    pub const RREAD: u8 = 117;
    pub const TWRITE: u8 = 118;
    pub const RWRITE: u8 = 119;
    pub const TCLUNK: u8 = 120;
    pub const RCLUNK: u8 = 121;
    pub const TREMOVE: u8 = 122;
    pub const RREMOVE: u8 = 123;
    pub const TSTAT: u8 = 124;
    pub const RSTAT: u8 = 125;
    pub const TWSTAT: u8 = 126;
    pub const RWSTAT: u8 = 127;
}
use msgtype::*;

// Open modes (open(5)).
/// Open for reading.
pub const OREAD: u8 = 0;
/// Open for writing.
pub const OWRITE: u8 = 1;
/// Open for reading and writing.
pub const ORDWR: u8 = 2;
/// Open for execution (read permission, executable check).
pub const OEXEC: u8 = 3;
/// Truncate the file on open.
pub const OTRUNC: u8 = 0x10;
/// Remove the file when the fid is clunked.
pub const ORCLOSE: u8 = 0x40;

// Qid type bits (the high byte of a Stat mode).
/// Qid type: directory.
pub const QTDIR: u8 = 0x80;
/// Qid type: append-only file.
pub const QTAPPEND: u8 = 0x40;
/// Qid type: exclusive-use file.
pub const QTEXCL: u8 = 0x20;
/// Qid type: authentication file.
pub const QTAUTH: u8 = 0x08;
/// Qid type: temporary file (not backed up).
pub const QTTMP: u8 = 0x04;
/// Qid type: plain file.
pub const QTFILE: u8 = 0x00;

// Stat mode bits.
/// Mode bit: directory.
pub const DMDIR: u32 = 0x8000_0000;
/// Mode bit: append-only.
pub const DMAPPEND: u32 = 0x4000_0000;
/// Mode bit: exclusive use.
pub const DMEXCL: u32 = 0x2000_0000;
/// Mode bit: authentication file.
pub const DMAUTH: u32 = 0x0800_0000;
/// Mode bit: temporary.
pub const DMTMP: u32 = 0x0400_0000;

/// The server's unique identity for a file: type bits, version, and path.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub struct Qid {
    /// `QT*` bits.
    pub qtype: u8,
    /// Bumped whenever the file changes (0 for synthetic files is common).
    pub version: u32,
    /// Unique among all files the server serves.
    pub path: u64,
}

impl Qid {
    /// A directory qid with version 0.
    pub const fn dir(path: u64) -> Self {
        Self { qtype: QTDIR, version: 0, path }
    }

    /// A plain-file qid with version 0.
    pub const fn file(path: u64) -> Self {
        Self { qtype: QTFILE, version: 0, path }
    }

    /// True if this qid names a directory.
    pub const fn is_dir(&self) -> bool {
        self.qtype & QTDIR != 0
    }

    fn put(&self, w: &mut Vec<u8>) {
        w.push(self.qtype);
        w.extend_from_slice(&self.version.to_le_bytes());
        w.extend_from_slice(&self.path.to_le_bytes());
    }

    fn get(r: &mut Reader<'_>) -> Result<Self, DecodeError> {
        Ok(Self { qtype: r.u8()?, version: r.u32()?, path: r.u64()? })
    }
}

/// A directory entry, as returned by `Tstat` and by reads of a directory.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Stat {
    /// For kernel use; servers send 0.
    pub typ: u16,
    /// For kernel use; servers send 0.
    pub dev: u32,
    /// The file's qid.
    pub qid: Qid,
    /// Permission bits plus `DM*` flags.
    pub mode: u32,
    /// Last access, seconds since the epoch.
    pub atime: u32,
    /// Last modification, seconds since the epoch.
    pub mtime: u32,
    /// Length in bytes (0 for directories and most synthetic files).
    pub length: u64,
    /// Last path element (`/` for a root).
    pub name: String,
    /// Owner.
    pub uid: String,
    /// Group.
    pub gid: String,
    /// User who last modified the file.
    pub muid: String,
}

impl Stat {
    /// A stat with the given name, qid, mode and length, owned by `none`.
    ///
    /// The `DMDIR` bit is set from the qid, so callers pass permission bits
    /// only (`0o555`, `0o644`, …).
    pub fn new(name: impl Into<String>, qid: Qid, perm: u32, length: u64) -> Self {
        let mode = if qid.is_dir() { perm | DMDIR } else { perm };
        Self {
            qid,
            mode,
            length,
            name: name.into(),
            uid: "none".into(),
            gid: "none".into(),
            muid: "none".into(),
            ..Default::default()
        }
    }

    /// Set uid, gid and muid at once.
    pub fn owned_by(mut self, user: &str) -> Self {
        self.uid = user.into();
        self.gid = user.into();
        self.muid = user.into();
        self
    }

    /// The "don't touch" stat for `Twstat`: every field at its don't-care
    /// value (all-ones integers, empty strings), per `stat(5)`.
    pub fn dont_touch() -> Self {
        Self {
            typ: !0,
            dev: !0,
            qid: Qid { qtype: !0, version: !0, path: !0 },
            mode: !0,
            atime: !0,
            mtime: !0,
            length: !0,
            name: String::new(),
            uid: String::new(),
            gid: String::new(),
            muid: String::new(),
        }
    }

    /// Append the encoded stat, including its leading `size[2]`.
    pub fn encode_into(&self, w: &mut Vec<u8>) {
        let start = w.len();
        w.extend_from_slice(&[0, 0]); // size, patched below
        w.extend_from_slice(&self.typ.to_le_bytes());
        w.extend_from_slice(&self.dev.to_le_bytes());
        self.qid.put(w);
        w.extend_from_slice(&self.mode.to_le_bytes());
        w.extend_from_slice(&self.atime.to_le_bytes());
        w.extend_from_slice(&self.mtime.to_le_bytes());
        w.extend_from_slice(&self.length.to_le_bytes());
        put_str(w, &self.name);
        put_str(w, &self.uid);
        put_str(w, &self.gid);
        put_str(w, &self.muid);
        let size = (w.len() - start - 2) as u16;
        w[start..start + 2].copy_from_slice(&size.to_le_bytes());
    }

    /// The encoded stat, including its leading `size[2]`.
    pub fn encode(&self) -> Vec<u8> {
        let mut w = Vec::with_capacity(64);
        self.encode_into(&mut w);
        w
    }

    /// Decode one stat (with its leading `size[2]`) from the front of `buf`,
    /// returning it and the number of bytes consumed.
    pub fn decode(buf: &[u8]) -> Result<(Self, usize), DecodeError> {
        let mut r = Reader::new(buf);
        let s = Self::get(&mut r)?;
        Ok((s, r.pos))
    }

    /// Decode a directory read's worth of concatenated stats.
    pub fn decode_all(mut buf: &[u8]) -> Result<Vec<Self>, DecodeError> {
        let mut out = Vec::new();
        while !buf.is_empty() {
            let (s, n) = Self::decode(buf)?;
            out.push(s);
            buf = &buf[n..];
        }
        Ok(out)
    }

    fn get(r: &mut Reader<'_>) -> Result<Self, DecodeError> {
        let size = r.u16()? as usize;
        let body = r.take(size)?;
        let mut r = Reader::new(body);
        let s = Self {
            typ: r.u16()?,
            dev: r.u32()?,
            qid: Qid::get(&mut r)?,
            mode: r.u32()?,
            atime: r.u32()?,
            mtime: r.u32()?,
            length: r.u64()?,
            name: r.str()?,
            uid: r.str()?,
            gid: r.str()?,
            muid: r.str()?,
        };
        Ok(s)
    }
}

/// A request, client to server.
#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(missing_docs)]
pub enum Tmsg {
    Version { msize: u32, version: String },
    Auth { afid: u32, uname: String, aname: String },
    Attach { fid: u32, afid: u32, uname: String, aname: String },
    Flush { oldtag: u16 },
    Walk { fid: u32, newfid: u32, wnames: Vec<String> },
    Open { fid: u32, mode: u8 },
    Create { fid: u32, name: String, perm: u32, mode: u8 },
    Read { fid: u32, offset: u64, count: u32 },
    Write { fid: u32, offset: u64, data: Vec<u8> },
    Clunk { fid: u32 },
    Remove { fid: u32 },
    Stat { fid: u32 },
    Wstat { fid: u32, stat: Stat },
}

/// A reply, server to client.
#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(missing_docs)]
pub enum Rmsg {
    Version { msize: u32, version: String },
    Auth { aqid: Qid },
    Error { ename: String },
    Attach { qid: Qid },
    Flush,
    Walk { wqids: Vec<Qid> },
    Open { qid: Qid, iounit: u32 },
    Create { qid: Qid, iounit: u32 },
    Read { data: Vec<u8> },
    Write { count: u32 },
    Clunk,
    Remove,
    Stat { stat: Stat },
    Wstat,
}

impl Tmsg {
    /// The message type number.
    pub fn msgtype(&self) -> u8 {
        match self {
            Tmsg::Version { .. } => TVERSION,
            Tmsg::Auth { .. } => TAUTH,
            Tmsg::Attach { .. } => TATTACH,
            Tmsg::Flush { .. } => TFLUSH,
            Tmsg::Walk { .. } => TWALK,
            Tmsg::Open { .. } => TOPEN,
            Tmsg::Create { .. } => TCREATE,
            Tmsg::Read { .. } => TREAD,
            Tmsg::Write { .. } => TWRITE,
            Tmsg::Clunk { .. } => TCLUNK,
            Tmsg::Remove { .. } => TREMOVE,
            Tmsg::Stat { .. } => TSTAT,
            Tmsg::Wstat { .. } => TWSTAT,
        }
    }

    /// Encode as a complete frame, `size[4]` included.
    pub fn encode(&self, tag: u16) -> Vec<u8> {
        let mut w = frame_start(self.msgtype(), tag);
        match self {
            Tmsg::Version { msize, version } => {
                w.extend_from_slice(&msize.to_le_bytes());
                put_str(&mut w, version);
            }
            Tmsg::Auth { afid, uname, aname } => {
                w.extend_from_slice(&afid.to_le_bytes());
                put_str(&mut w, uname);
                put_str(&mut w, aname);
            }
            Tmsg::Attach { fid, afid, uname, aname } => {
                w.extend_from_slice(&fid.to_le_bytes());
                w.extend_from_slice(&afid.to_le_bytes());
                put_str(&mut w, uname);
                put_str(&mut w, aname);
            }
            Tmsg::Flush { oldtag } => w.extend_from_slice(&oldtag.to_le_bytes()),
            Tmsg::Walk { fid, newfid, wnames } => {
                w.extend_from_slice(&fid.to_le_bytes());
                w.extend_from_slice(&newfid.to_le_bytes());
                w.extend_from_slice(&(wnames.len() as u16).to_le_bytes());
                for n in wnames {
                    put_str(&mut w, n);
                }
            }
            Tmsg::Open { fid, mode } => {
                w.extend_from_slice(&fid.to_le_bytes());
                w.push(*mode);
            }
            Tmsg::Create { fid, name, perm, mode } => {
                w.extend_from_slice(&fid.to_le_bytes());
                put_str(&mut w, name);
                w.extend_from_slice(&perm.to_le_bytes());
                w.push(*mode);
            }
            Tmsg::Read { fid, offset, count } => {
                w.extend_from_slice(&fid.to_le_bytes());
                w.extend_from_slice(&offset.to_le_bytes());
                w.extend_from_slice(&count.to_le_bytes());
            }
            Tmsg::Write { fid, offset, data } => {
                w.extend_from_slice(&fid.to_le_bytes());
                w.extend_from_slice(&offset.to_le_bytes());
                w.extend_from_slice(&(data.len() as u32).to_le_bytes());
                w.extend_from_slice(data);
            }
            Tmsg::Clunk { fid } | Tmsg::Remove { fid } | Tmsg::Stat { fid } => {
                w.extend_from_slice(&fid.to_le_bytes())
            }
            Tmsg::Wstat { fid, stat } => {
                w.extend_from_slice(&fid.to_le_bytes());
                put_nstat(&mut w, stat);
            }
        }
        frame_finish(w)
    }

    /// Decode a frame whose `size[4]` prefix has been stripped. Returns the
    /// tag and the message.
    pub fn decode(buf: &[u8]) -> Result<(u16, Self), DecodeError> {
        let mut r = Reader::new(buf);
        let typ = r.u8()?;
        let tag = r.u16()?;
        let m = match typ {
            TVERSION => Tmsg::Version { msize: r.u32()?, version: r.str()? },
            TAUTH => Tmsg::Auth { afid: r.u32()?, uname: r.str()?, aname: r.str()? },
            TATTACH => {
                Tmsg::Attach { fid: r.u32()?, afid: r.u32()?, uname: r.str()?, aname: r.str()? }
            }
            TFLUSH => Tmsg::Flush { oldtag: r.u16()? },
            TWALK => {
                let fid = r.u32()?;
                let newfid = r.u32()?;
                let n = r.u16()? as usize;
                let mut wnames = Vec::with_capacity(n.min(MAXWELEM));
                for _ in 0..n {
                    wnames.push(r.str()?);
                }
                Tmsg::Walk { fid, newfid, wnames }
            }
            TOPEN => Tmsg::Open { fid: r.u32()?, mode: r.u8()? },
            TCREATE => {
                Tmsg::Create { fid: r.u32()?, name: r.str()?, perm: r.u32()?, mode: r.u8()? }
            }
            TREAD => Tmsg::Read { fid: r.u32()?, offset: r.u64()?, count: r.u32()? },
            TWRITE => {
                let fid = r.u32()?;
                let offset = r.u64()?;
                let n = r.u32()? as usize;
                Tmsg::Write { fid, offset, data: r.take(n)?.to_vec() }
            }
            TCLUNK => Tmsg::Clunk { fid: r.u32()? },
            TREMOVE => Tmsg::Remove { fid: r.u32()? },
            TSTAT => Tmsg::Stat { fid: r.u32()? },
            TWSTAT => {
                let fid = r.u32()?;
                Tmsg::Wstat { fid, stat: get_nstat(&mut r)? }
            }
            other => return Err(DecodeError::UnknownType { typ: other, tag }),
        };
        r.finish()?;
        Ok((tag, m))
    }
}

impl Rmsg {
    /// The message type number.
    pub fn msgtype(&self) -> u8 {
        match self {
            Rmsg::Version { .. } => RVERSION,
            Rmsg::Auth { .. } => RAUTH,
            Rmsg::Error { .. } => RERROR,
            Rmsg::Attach { .. } => RATTACH,
            Rmsg::Flush => RFLUSH,
            Rmsg::Walk { .. } => RWALK,
            Rmsg::Open { .. } => ROPEN,
            Rmsg::Create { .. } => RCREATE,
            Rmsg::Read { .. } => RREAD,
            Rmsg::Write { .. } => RWRITE,
            Rmsg::Clunk => RCLUNK,
            Rmsg::Remove => RREMOVE,
            Rmsg::Stat { .. } => RSTAT,
            Rmsg::Wstat => RWSTAT,
        }
    }

    /// Shorthand for an `Rerror`.
    pub fn error(ename: impl Into<String>) -> Self {
        Rmsg::Error { ename: ename.into() }
    }

    /// Encode as a complete frame, `size[4]` included.
    pub fn encode(&self, tag: u16) -> Vec<u8> {
        let mut w = frame_start(self.msgtype(), tag);
        match self {
            Rmsg::Version { msize, version } => {
                w.extend_from_slice(&msize.to_le_bytes());
                put_str(&mut w, version);
            }
            Rmsg::Auth { aqid: qid } | Rmsg::Attach { qid } => qid.put(&mut w),
            Rmsg::Error { ename } => put_str(&mut w, ename),
            Rmsg::Flush | Rmsg::Clunk | Rmsg::Remove | Rmsg::Wstat => {}
            Rmsg::Walk { wqids } => {
                w.extend_from_slice(&(wqids.len() as u16).to_le_bytes());
                for q in wqids {
                    q.put(&mut w);
                }
            }
            Rmsg::Open { qid, iounit } | Rmsg::Create { qid, iounit } => {
                qid.put(&mut w);
                w.extend_from_slice(&iounit.to_le_bytes());
            }
            Rmsg::Read { data } => {
                w.extend_from_slice(&(data.len() as u32).to_le_bytes());
                w.extend_from_slice(data);
            }
            Rmsg::Write { count } => w.extend_from_slice(&count.to_le_bytes()),
            Rmsg::Stat { stat } => put_nstat(&mut w, stat),
        }
        frame_finish(w)
    }

    /// Decode a frame whose `size[4]` prefix has been stripped. Returns the
    /// tag and the message.
    pub fn decode(buf: &[u8]) -> Result<(u16, Self), DecodeError> {
        let mut r = Reader::new(buf);
        let typ = r.u8()?;
        let tag = r.u16()?;
        let m = match typ {
            RVERSION => Rmsg::Version { msize: r.u32()?, version: r.str()? },
            RAUTH => Rmsg::Auth { aqid: Qid::get(&mut r)? },
            RERROR => Rmsg::Error { ename: r.str()? },
            RATTACH => Rmsg::Attach { qid: Qid::get(&mut r)? },
            RFLUSH => Rmsg::Flush,
            RWALK => {
                let n = r.u16()? as usize;
                let mut wqids = Vec::with_capacity(n.min(MAXWELEM));
                for _ in 0..n {
                    wqids.push(Qid::get(&mut r)?);
                }
                Rmsg::Walk { wqids }
            }
            ROPEN => Rmsg::Open { qid: Qid::get(&mut r)?, iounit: r.u32()? },
            RCREATE => Rmsg::Create { qid: Qid::get(&mut r)?, iounit: r.u32()? },
            RREAD => {
                let n = r.u32()? as usize;
                Rmsg::Read { data: r.take(n)?.to_vec() }
            }
            RWRITE => Rmsg::Write { count: r.u32()? },
            RCLUNK => Rmsg::Clunk,
            RREMOVE => Rmsg::Remove,
            RSTAT => Rmsg::Stat { stat: get_nstat(&mut r)? },
            RWSTAT => Rmsg::Wstat,
            other => return Err(DecodeError::UnknownType { typ: other, tag }),
        };
        r.finish()?;
        Ok((tag, m))
    }
}

/// Why a frame could not be decoded.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DecodeError {
    /// The frame ended before the message did.
    Truncated,
    /// Bytes were left over after the message.
    TrailingBytes,
    /// A string was not valid UTF-8.
    BadUtf8,
    /// An unknown message type. The tag is still known, so a server can
    /// answer with `Rerror`.
    UnknownType {
        /// The type byte received.
        typ: u8,
        /// The request's tag.
        tag: u16,
    },
}

impl fmt::Display for DecodeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DecodeError::Truncated => f.write_str("9P message truncated"),
            DecodeError::TrailingBytes => f.write_str("9P message has trailing bytes"),
            DecodeError::BadUtf8 => f.write_str("9P string is not UTF-8"),
            DecodeError::UnknownType { typ, .. } => write!(f, "unknown 9P message type {typ}"),
        }
    }
}

impl std::error::Error for DecodeError {}

// ── codec primitives ────────────────────────────────────────────────────

fn frame_start(typ: u8, tag: u16) -> Vec<u8> {
    let mut w = Vec::with_capacity(64);
    w.extend_from_slice(&[0, 0, 0, 0]);
    w.push(typ);
    w.extend_from_slice(&tag.to_le_bytes());
    w
}

fn frame_finish(mut w: Vec<u8>) -> Vec<u8> {
    let size = w.len() as u32;
    w[..4].copy_from_slice(&size.to_le_bytes());
    w
}

/// Append a 9P string (`len[2]` + UTF-8). Strings longer than 65535 bytes
/// are cut at the last character boundary that fits.
fn put_str(w: &mut Vec<u8>, s: &str) {
    let mut n = s.len().min(u16::MAX as usize);
    while !s.is_char_boundary(n) {
        n -= 1;
    }
    w.extend_from_slice(&(n as u16).to_le_bytes());
    w.extend_from_slice(&s.as_bytes()[..n]);
}

/// `stat[n]` as carried by `Rstat`/`Twstat`: a `n[2]` count, then a stat
/// that begins with its own `size[2]`.
fn put_nstat(w: &mut Vec<u8>, s: &Stat) {
    let enc = s.encode();
    w.extend_from_slice(&(enc.len() as u16).to_le_bytes());
    w.extend_from_slice(&enc);
}

fn get_nstat(r: &mut Reader<'_>) -> Result<Stat, DecodeError> {
    let n = r.u16()? as usize;
    let mut inner = Reader::new(r.take(n)?);
    let s = Stat::get(&mut inner)?;
    inner.finish()?;
    Ok(s)
}

struct Reader<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl<'a> Reader<'a> {
    fn new(buf: &'a [u8]) -> Self {
        Self { buf, pos: 0 }
    }

    fn take(&mut self, n: usize) -> Result<&'a [u8], DecodeError> {
        let end = self.pos.checked_add(n).ok_or(DecodeError::Truncated)?;
        let s = self.buf.get(self.pos..end).ok_or(DecodeError::Truncated)?;
        self.pos = end;
        Ok(s)
    }

    fn u8(&mut self) -> Result<u8, DecodeError> {
        Ok(self.take(1)?[0])
    }

    fn u16(&mut self) -> Result<u16, DecodeError> {
        Ok(u16::from_le_bytes(self.take(2)?.try_into().unwrap()))
    }

    fn u32(&mut self) -> Result<u32, DecodeError> {
        Ok(u32::from_le_bytes(self.take(4)?.try_into().unwrap()))
    }

    fn u64(&mut self) -> Result<u64, DecodeError> {
        Ok(u64::from_le_bytes(self.take(8)?.try_into().unwrap()))
    }

    fn str(&mut self) -> Result<String, DecodeError> {
        let n = self.u16()? as usize;
        let raw = self.take(n)?;
        String::from_utf8(raw.to_vec()).map_err(|_| DecodeError::BadUtf8)
    }

    fn finish(&self) -> Result<(), DecodeError> {
        if self.pos == self.buf.len() { Ok(()) } else { Err(DecodeError::TrailingBytes) }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_stat() -> Stat {
        Stat { atime: 1, mtime: 2, ..Stat::new("tracks", Qid::dir(7), 0o555, 0).owned_by("glenda") }
    }

    fn t_round_trip(m: Tmsg) {
        let f = m.encode(42);
        assert_eq!(u32::from_le_bytes(f[..4].try_into().unwrap()) as usize, f.len());
        assert_eq!(Tmsg::decode(&f[4..]).unwrap(), (42, m));
    }

    fn r_round_trip(m: Rmsg) {
        let f = m.encode(42);
        assert_eq!(u32::from_le_bytes(f[..4].try_into().unwrap()) as usize, f.len());
        assert_eq!(Rmsg::decode(&f[4..]).unwrap(), (42, m));
    }

    #[test]
    fn every_tmsg_round_trips() {
        t_round_trip(Tmsg::Version { msize: 8192, version: VERSION.into() });
        t_round_trip(Tmsg::Auth { afid: 1, uname: "u".into(), aname: "".into() });
        t_round_trip(Tmsg::Attach { fid: 0, afid: NOFID, uname: "u".into(), aname: "a".into() });
        t_round_trip(Tmsg::Flush { oldtag: 9 });
        t_round_trip(Tmsg::Walk { fid: 0, newfid: 1, wnames: vec!["a".into(), "ü".into()] });
        t_round_trip(Tmsg::Walk { fid: 0, newfid: 1, wnames: vec![] });
        t_round_trip(Tmsg::Open { fid: 1, mode: ORDWR | OTRUNC });
        t_round_trip(Tmsg::Create { fid: 1, name: "n".into(), perm: DMDIR | 0o755, mode: OREAD });
        t_round_trip(Tmsg::Read { fid: 1, offset: u64::MAX, count: 8168 });
        t_round_trip(Tmsg::Write { fid: 1, offset: 3, data: b"hello".to_vec() });
        t_round_trip(Tmsg::Clunk { fid: 1 });
        t_round_trip(Tmsg::Remove { fid: 1 });
        t_round_trip(Tmsg::Stat { fid: 1 });
        t_round_trip(Tmsg::Wstat { fid: 1, stat: Stat::dont_touch() });
        t_round_trip(Tmsg::Wstat { fid: 1, stat: sample_stat() });
    }

    #[test]
    fn every_rmsg_round_trips() {
        r_round_trip(Rmsg::Version { msize: 8192, version: VERSION.into() });
        r_round_trip(Rmsg::Auth { aqid: Qid { qtype: QTAUTH, version: 0, path: 1 } });
        r_round_trip(Rmsg::error("permission denied"));
        r_round_trip(Rmsg::Attach { qid: Qid::dir(0) });
        r_round_trip(Rmsg::Flush);
        r_round_trip(Rmsg::Walk { wqids: vec![Qid::dir(1), Qid::file(2)] });
        r_round_trip(Rmsg::Open { qid: Qid::file(2), iounit: 8168 });
        r_round_trip(Rmsg::Create { qid: Qid::file(3), iounit: 0 });
        r_round_trip(Rmsg::Read { data: vec![0, 1, 2] });
        r_round_trip(Rmsg::Write { count: 3 });
        r_round_trip(Rmsg::Clunk);
        r_round_trip(Rmsg::Remove);
        r_round_trip(Rmsg::Stat { stat: sample_stat() });
        r_round_trip(Rmsg::Wstat);
    }

    /// Byte-exact against a hand-assembled frame, so the codec cannot be
    /// "consistently wrong" in both directions.
    #[test]
    fn rstat_has_the_double_length_prefix() {
        let s = Stat::new("a", Qid::file(1), 0o444, 5);
        let f = Rmsg::Stat { stat: s.clone() }.encode(1);
        let enc = s.encode();
        // size[4] type[1] tag[2] n[2] then stat (size[2] ...)
        assert_eq!(f[7..9], (enc.len() as u16).to_le_bytes());
        assert_eq!(f[9..11], ((enc.len() - 2) as u16).to_le_bytes());
        assert_eq!(&f[9..], &enc[..]);
        // stat body: 2+4+13+4+4+4+8 fixed, then four strings
        assert_eq!(enc.len(), 2 + 39 + (2 + 1) + 3 * (2 + 4));
    }

    #[test]
    fn version_frame_matches_plan9() {
        let f = Tmsg::Version { msize: 8192, version: "9P2000".into() }.encode(NOTAG);
        let want: &[u8] = &[
            19, 0, 0, 0, 100, 0xff, 0xff, 0, 0x20, 0, 0, 6, 0, b'9', b'P', b'2', b'0', b'0', b'0',
        ];
        assert_eq!(f, want);
    }

    #[test]
    fn truncated_and_trailing_are_errors_not_panics() {
        let f = Tmsg::Read { fid: 1, offset: 0, count: 10 }.encode(3);
        for cut in 0..f.len() - 4 {
            assert!(Tmsg::decode(&f[4..4 + cut]).is_err(), "cut at {cut}");
        }
        let mut long = f[4..].to_vec();
        long.push(0);
        assert_eq!(Tmsg::decode(&long), Err(DecodeError::TrailingBytes));
    }

    #[test]
    fn unknown_type_keeps_the_tag() {
        let buf = [200u8, 5, 0];
        assert_eq!(Tmsg::decode(&buf), Err(DecodeError::UnknownType { typ: 200, tag: 5 }));
    }

    #[test]
    fn walk_count_larger_than_buffer_is_truncated_not_oom() {
        let mut b = vec![TWALK, 1, 0];
        b.extend_from_slice(&0u32.to_le_bytes());
        b.extend_from_slice(&1u32.to_le_bytes());
        b.extend_from_slice(&u16::MAX.to_le_bytes());
        assert_eq!(Tmsg::decode(&b), Err(DecodeError::Truncated));
    }

    #[test]
    fn long_strings_are_cut_on_a_char_boundary() {
        let s = "é".repeat(40_000); // 80_000 bytes
        let mut w = Vec::new();
        put_str(&mut w, &s);
        let n = u16::from_le_bytes([w[0], w[1]]) as usize;
        assert!(n <= u16::MAX as usize && n % 2 == 0);
        assert!(std::str::from_utf8(&w[2..]).is_ok());
    }

    #[test]
    fn decode_all_splits_a_directory_read() {
        let a = Stat::new("a", Qid::file(1), 0o444, 0);
        let b = Stat::new("b", Qid::dir(2), 0o555, 0);
        let mut buf = a.encode();
        buf.extend_from_slice(&b.encode());
        assert_eq!(Stat::decode_all(&buf).unwrap(), vec![a, b]);
    }
}
