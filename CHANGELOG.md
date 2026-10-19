# Changelog

## 0.1.0 (unreleased)

First release.

- `proto`: an I/O-free codec for every 9P2000 T- and R-message, `Qid`, and
  `Stat`.
- `Server` over a `Filesystem` with per-open `File`s: async blocking reads
  on their own tasks, prompt `Tflush`, whole-entry directory reads from a
  per-fid snapshot, `Tversion` session reset, `ORCLOSE`, optional
  `create`/`remove`/`wstat`. Serves TCP, Unix sockets, or any stream.
- `Client`: one request at a time; walk, open, create, read, write, stat,
  wstat, remove, and whole-file and directory conveniences.
- `Answer`, a helper for write-then-read query files.
