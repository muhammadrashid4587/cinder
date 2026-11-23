# cinder

A Redis-compatible in-memory key-value store, written in C from scratch.
Single-threaded, `epoll`-based event loop for commands, RESP protocol, strings, lists,
sets and hashes with TTLs, approximate LRU eviction, and an append-only file for
durability, plus a small worker thread pool (`--threads`, default 1)
that runs background saves (`BGSAVE`) off the event loop. No dependencies
beyond libc. Real `redis-cli` and any RESP
client library can talk to it.

Version 0.12.0.

## Why

Built as a systems portfolio project: the interesting parts of a database
(hash table, wire protocol, event loop, durability) with nothing hidden
behind a framework.

## Build and run

```sh
make
./cinder --port 6379 --data-dir ./data --aof-fsync everysec
```

`--aof-fsync` is `always` (default) or `everysec`: under `always` every
mutating command is `fdatasync`ed before it is acknowledged, under
`everysec` writes go to the log immediately and the file is synced at
most once a second, so up to a second of acknowledged writes can be
lost on a machine crash. `SAVE` forces a sync under either policy.

Then, with `redis-cli` (or any RESP client):

```
127.0.0.1:6379> SET name cinder
OK
127.0.0.1:6379> GET name
"cinder"
127.0.0.1:6379> SET temp v EX 60
OK
127.0.0.1:6379> INCR counter
(integer) 1
```

## Commands

| Command | Notes |
| --- | --- |
| `PING [msg]`, `ECHO msg` | Connectivity |
| `SET key value [EX s \| PX ms] [NX \| XX]` | String set with TTL and conditional write |
| `GET key` | String get |
| `DEL key [key ...]`, `EXISTS key [key ...]` | Multi-key delete / existence |
| `EXPIRE key s`, `PEXPIRE key ms` | Set a TTL |
| `TTL key`, `PTTL key` | Remaining TTL (`-2` missing, `-1` no expiry) |
| `INCR`, `DECR`, `INCRBY`, `DECRBY` | Integer counters; preserves TTL |
| `LPUSH key e [e ...]`, `RPUSH key e [e ...]` | Push to head / tail; creates the list; returns new length |
| `LPOP key`, `RPOP key` | Pop from head / tail; popping the last element deletes the key |
| `LLEN key` | List length (`0` when missing) |
| `LRANGE key start stop` | Elements, inclusive, negative indexes from the end |
| `SADD key member [member ...]` | Add to set; returns number added (duplicates ignored); creates the set |
| `SREM key member [member ...]` | Remove from set; returns number removed; emptying the set deletes the key |
| `SMEMBERS key` | All members (insertion order) |
| `SISMEMBER key member` | `1` if member, `0` if not |
| `SCARD key` | Set size (`0` when missing) |
| `HSET key field value [field value ...]` | Set fields; returns number of new fields (updates return `0` for that field) |
| `HGET key field` | Field value (`nil` when missing) |
| `HDEL key field [field ...]` | Delete fields; returns number removed; emptying the hash deletes the key |
| `HEXISTS key field` | `1` if the field exists, `0` if not |
| `HLEN key` | Number of fields (`0` when missing) |
| `HGETALL key` | Flat `[field, value, ...]` array in insertion order |
| `KEYS pattern` | Glob match (`*` and `?`) |
| `FLUSHALL`, `DBSIZE`, `INFO` | Admin / introspection |
| `COMMAND`, `SAVE` | `COMMAND` lists supported commands; `SAVE` writes a snapshot of the dataset to `dump.rdb` and forces the AOF to disk |
| `BGSAVE` | Snapshot the dataset to `dump.rdb` in the background: the event loop serializes the store to memory and a worker thread writes the file, so the server keeps answering commands while it saves; answers `Background saving started`, a second `BGSAVE` while one is in progress is an error, and `INFO` reports `bgsave_in_progress` and `last_bgsave_status` |
| `SYNC` | Ask this server for a full sync: it marks the connection a replica, streams a rebuild of the whole dataset (one command per key, with TTLs), then streams every later write live |
| `PSYNC replid offset` | Ask to resume replication from a stream offset; when the replid matches this server's run id and the offset is still in the replication backlog (the last 1 MB of the write stream by default, `--repl-backlog-size`), the primary answers `+CONTINUE <replid> <offset>` and streams only the missed bytes, otherwise `+FULLRESYNC <replid> <offset>` and a full sync |
| `REPLICAOF host port`, `REPLICAOF NO ONE` | Make this server a replica of `host:port` (PSYNC, then streamed writes, applied and AOF-logged locally); `NO ONE` promotes it to primary: the link drops, the replica state (primary run id and stream offset) is forgotten, the replicated data stays, and the node accepts writes as a primary. Reattaching to the old primary afterwards full-syncs |
| `QUIT`, `SHUTDOWN` | Close connection / stop the server |

## Architecture

```
src/
  server.c   TCP listener, epoll event loop, command dispatch
  store.c    hash table (djb2, chaining, resize at 0.75 load), TTLs,
             strings, lists (circular-buffer lists, O(1) push/pop),
             sets (unordered member arrays, dedup on insert) and
             hashes (parallel field/value arrays, insertion order)
  resp.c     RESP parser (array + inline commands) and encoders
  aof.c      append-only log: every mutation is a RESP array, replayed on start;
             fsync policy `always` or `everysec`
  rdb.c      RDB-style snapshots: `SAVE` writes the whole dataset (all
             types, TTLs as absolute expiries) to `dump.rdb` in a compact
             binary format, atomically via `dump.rdb.tmp` + rename; the
             snapshot is loaded on startup before the AOF is replayed;
             `rdb_serialize` renders the same bytes to memory so a
             worker thread can write them for `BGSAVE`
  threadpool.c FIFO job queue with N worker threads (`--threads`); jobs
             touch no shared state unless they take a lock
```

- **Store**: chained hash table, doubling resize, lazy expiry on access plus
  a periodic scan of 20 random buckets per 100 event-loop iterations.
- **Eviction**: Redis-style approximate LRU. When `--max-entries` is set, the
  server samples 5 entries and evicts the least recently used of the sample.
- **Durability**: every mutating command is appended to `appendonly.aof` as a
  RESP array and synced according to `--aof-fsync`: `always` (the default)
  `fdatasync`s after every write; `everysec` writes immediately and syncs at
  most once a second via a timer in the event loop, and always syncs on a
  clean shutdown or `SAVE`. On startup the file is replayed through the
  same dispatcher live clients use, so the log is the dataset. `SAVE`
  additionally writes a snapshot of the whole dataset to `dump.rdb`
  (strings, lists, sets and hashes, with TTLs stored as absolute
  expiries; keys whose TTL already passed are left out), atomically via
  `dump.rdb.tmp`, `fsync`, and rename, so a crash mid-save never leaves
  a torn snapshot. On startup the snapshot is loaded first and the AOF
  is replayed over it — the AOF holds the complete history, so the
  replay reproduces the exact final state either way, and a corrupt
  snapshot is only a warning, never a failed start.
- **Event loop**: one thread, non-blocking sockets, per-client input/output
  buffers. A slow client never blocks other clients. The loop is
  `epoll`-based (level-triggered): the listener and every client fd are
  registered once, and each iteration re-syncs each client's interest
  (`EPOLLOUT` while it has output pending or a replication connect is
  still completing) before `epoll_wait`. Unlike the `poll()` loop it
  replaced, no fd array is allocated per iteration, and the ready set is
  fixed when `epoll_wait` returns, so a client added mid-iteration
  cannot run past the end of the event array.
- **Thread pool**: a FIFO job queue with N worker threads (`--threads`,
  default 1, max 32). Commands still run only on the event-loop thread —
  the store is never touched off it, so the single-threaded correctness
  guarantees are unchanged — and the pool exists for work that is safe
  to move. Today that is `BGSAVE`: the loop serializes the store to
  memory (a consistent picture, taken while the loop owns the store),
  then a worker writes those bytes to `dump.rdb.bgsave.tmp`, fsyncs,
  and renames it over `dump.rdb`, while the server keeps answering
  commands. A second `BGSAVE` while one is in progress is rejected
  rather than queued, and `SAVE` racing a `BGSAVE` uses a different
  temp file, so both snapshots stay valid and the last rename wins.
  On shutdown the loop waits for the pool to drain, so a save that
  already started always lands its snapshot. `INFO` reports
  `threads`, `threadpool_jobs_completed`, `bgsave_in_progress`,
  `last_bgsave_status` (`none`/`ok`/`error`) and `last_bgsave_time_ms`.
- **Replication**: a replica connects with `REPLICAOF host port` (non-blocking
  connect, `PSYNC` sent when it completes) and the primary streams a full
  sync — `FLUSHALL` plus one rebuild command per key (`SET` with `PX` for
  TTL'd strings, `RPUSH`/`SADD`/`HSET` for the other types, `PEXPIRE` after
  the rebuild; keys whose TTL already passed are left out) — followed by
  a `+FULLSYNC` marker. After that, every mutating command the primary
  executes is forwarded verbatim (the raw RESP bytes) to all connected
  replicas. The replica applies streamed commands through the same
  dispatcher live clients use and logs them to its own AOF, so replicated
  data survives a replica restart, and a replica can itself accept `SYNC`
  and forward what it applies, so replicas chain. A replica connection is
  read-only on the primary (only `PING` is accepted); `INFO` reports
  `role:master|replica` and `connected_replicas`. Offsets: the primary
  counts every byte of the live write stream it forwards
  (`master_repl_offset`, under a fresh 40-hex run id per process) and
  reports each replica's offset (`replicaN:ip=...,port=...,offset=...`);
  a replica learns the primary's run id and offset from the `+FULLRESYNC`
  line and counts the stream bytes it applies (`slave_repl_offset`), so a
  caught-up replica's slave offset equals the primary's master offset.
  The primary also keeps a replication backlog — a ring buffer of the
  most recent bytes of the write stream (the last 1 MB by default,
  `--repl-backlog-size`, `0` disables it) — so a `PSYNC` that names the
  primary's run id with an offset still inside the backlog gets a
  partial resync (`+CONTINUE` plus only the missed bytes) instead of
  a full sync; an unknown run id or an offset that has aged out of the
  backlog falls back to `+FULLRESYNC` and a full sync. The replica's
  slave offset is a stream position: it advances for every byte
  received on the replication link, even a command that turns out to
  be a no-op locally, so a later `PSYNC` never replays a command twice.
  **Failover**: `REPLICAOF NO ONE` promotes a replica to primary — the
  link drops, the replica state (primary run id and stream offset) is
  forgotten, and the node keeps the data it replicated and accepts
  writes as a primary; its own run id and stream offset are untouched,
  so replicas chained off it keep working. A promoted node that
  `REPLICAOF`s its old primary again full-syncs, since the stream
  position is gone. A replica that shuts down cleanly also writes its
  stream position to `replication.state` in the data dir, and reloads
  it (consuming the file) on startup, so a clean restart resumes from
  its offset with a partial resync instead of full-syncing; an unclean
  stop leaves no state file, so the next `REPLICAOF` full-syncs rather
  than risk replaying commands from a stale offset.

## Benchmarks

Sequential (one request at a time, no pipelining) on the dev VM, AOF fsync
policy `always`:

| Operation | Throughput |
| --- | --- |
| `SET` | ~25,000 ops/sec |
| `GET` | ~34,000 ops/sec |

`SET` pays one `fdatasync` per write under the `always` policy. On this
dev VM `fdatasync` is cheap, so sequential single-client throughput is
about the same under `everysec` (~22k–33k SET/s either way); the policy
still matters where fsync is expensive, and the sync count is the real
difference — verified with `strace`, 200 sequential SETs cost 201
`fdatasync`s under `always` and 2 under `everysec` (one at the first
write, one on shutdown). Pipelined clients would be substantially
faster under either policy.

## Tests

```sh
make test   # builds, then runs tests/smoke.py: 30+ RESP checks, list
            # checks (push/pop both ends, length, range with negative
            # indexes, wrong-type errors, list TTL expiry), set checks
            # (add/dedup, membership, remove, wrong-type errors, set
            # TTL expiry, emptying a set deletes the key), hash checks
            # (multi-field set, update, get, delete, exists, length,
            # getall in insertion order, wrong-type errors, hash TTL
            # expiry, emptying a hash deletes the key), an AOF
            # restart test (data, a list, a set and a hash survive a
            # SHUTDOWN and reload), and everysec-policy checks (INFO
            # reports the policy, the AOF holds the record before any
            # sync, data survives a SIGKILL + restart, unknown policy
            # is a startup error), and RDB snapshot checks (SAVE writes
            # dump.rdb with all four types, a key whose TTL passed
            # before the save is left out, and a restart with the AOF
            # deleted restores every key from the snapshot alone, with
            # a surviving TTL preserved), and replication checks
            # (REPLICAOF streams a full sync of all four types with a
            # surviving TTL, skips a key whose TTL passed first, then
            # streams live SET/INCR/DEL/LPUSH writes; REPLICAOF NO ONE
            # detaches and later writes stop flowing; a replica restart
            # rebuilds the replicated data from its own AOF), and
            # replication offset checks (the primary's master_repl_offset
            # grows by exactly the bytes of a forwarded command, the
            # replica's slave_repl_offset catches up to it and reports
            # the same run id, INFO lists the replica with its offset,
            # and PSYNC answers +FULLRESYNC with the run id and offset
            # then streams a full sync), and partial resync checks (a
            # replica that shuts down cleanly and restarts resumes from
            # its persisted stream position, replays only the missed
            # writes and keeps a key written directly to it, proving
            # no FLUSHALL ran; with a 128-byte backlog, ten 70-byte
            # writes push the offset out of the backlog, so the
            # reconnect falls back to a full resync and the local-only
            # key is wiped), and failover checks (REPLICAOF NO ONE
            # promotes a replica to primary: INFO reports role:master
            # with no master_host, the replicated data stays, the
            # promoted node accepts writes, the old primary's later
            # writes stop flowing, promoting again is a no-op, and
            # reattaching to the old primary full-syncs, wiping the
            # writes made while promoted), and a concurrent-clients
            # check (64 clients SET/GET at once, exercising the epoll
            # loop with dozens of ready fds in one epoll_wait batch), and
            # background-save checks (`BGSAVE` with `--threads 2` writes
            # `dump.rdb` with all four types, skips a key whose TTL passed,
            # reports `threads:2` and `last_bgsave_status:ok` in `INFO`, a
            # second `BGSAVE` completes after the first, and a restart
            # with the AOF deleted restores every key from the background
            # snapshot alone, with a surviving TTL preserved; `--threads 0`
            # is a startup error)
```

## Roadmap

- [x] Lists (`LPUSH`/`RPUSH`/`LPOP`/`RPOP`/`LLEN`/`LRANGE`, AOF-persisted, TTLs)
- [x] Sets (`SADD`/`SREM`/`SMEMBERS`/`SISMEMBER`/`SCARD`, AOF-persisted, TTLs)
- [x] Hashes (`HSET`/`HGET`/`HDEL`/`HEXISTS`/`HLEN`/`HGETALL`, AOF-persisted, TTLs)
- [x] `everysec` AOF fsync policy
- [x] RDB-style snapshots alongside the AOF (`SAVE` writes `dump.rdb`, loaded on startup)
- [x] Replication (primary/replica over RESP)
- [x] Replication offsets and `PSYNC` handshake (`master_repl_offset`,
      per-replica offsets, run id; always a full resync for now)
- [x] Replication backlog: keep recent stream bytes so `PSYNC` can
      resume from an offset instead of full-syncing (1 MB ring buffer
      by default, `--repl-backlog-size`; `+CONTINUE` partial resync,
      `+FULLRESYNC` fallback when the offset aged out)
- [x] Failover: promote a replica to primary (`REPLICAOF NO ONE`
      detaches, forgets the stream position, keeps the replicated data,
      and reports `role:master` in `INFO`; a clean replica restart
      resumes from its persisted stream position instead of
      full-syncing)
- [x] `epoll` backend for the event loop (replaces `poll()`; listener
      and client fds registered once, interest re-synced per iteration,
      no per-iteration fd array allocation)
- [x] Thread pool for the event loop (FIFO job queue, `--threads`,
      default 1; `BGSAVE` serializes on the loop and a worker writes
      the snapshot off-loop, `INFO` reports pool size and save status)
- [ ] Benchmarks against real Redis on the same hardware
