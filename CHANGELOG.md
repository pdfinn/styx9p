# Changelog

## 0.1.0 (unreleased)

First release.

- `proto`: an I/O-free codec for every 9P2000 T- and R-message, `Qid`, and
  `Stat`.
- `Server` over a `Filesystem` with per-open `File`s: async blocking reads
  on their own tasks, prompt `Tflush`, whole-entry directory reads from a
  per-fid snapshot, `Tversion` session reset, `ORCLOSE`, optional
  `create`/`remove`/`wstat`. Serves TCP, Unix sockets, or any stream.
- `Client`: one request at a time; walk, open, create, read, `read_full`,
  write, stat, wstat, remove, and whole-file and directory conveniences.
  A walk refused on its first element reports the server's own error
  text; one that stops part way reports the element it stopped at.
- `serve_tcp`/`serve_unix` retry failed accepts (e.g. EMFILE) instead of
  returning.
- `Answer`, a helper for write-then-read query files.
