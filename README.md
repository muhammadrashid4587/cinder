# cinder

A Redis-compatible in-memory key-value store, written in C from scratch.
Single-threaded, `epoll`-based event loop for commands, RESP protocol, strings, lists,
sets, hashes and sorted sets with TTLs, approximate LRU eviction, and an append-only file for
durability, plus a small worker thread pool (`--threads`, default 1)
that runs background saves (`BGSAVE`) off the event loop, and a dedicated
flush thread that does the once-a-second AOF sync under `everysec`,
so a slow disk never stalls the loop. No dependencies
beyond libc. Real `redis-cli` and any RESP
client library can talk to it.

Version 0.45.0.

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
mutating command is `fdatasync`ed by a dedicated flush thread before
it is acknowledged (the reply waits for the sync; see Architecture),
under `everysec` writes go to the log immediately and a background flush
thread syncs the file at most once a second, so the event loop never
waits on the disk and up to a second of acknowledged writes can be
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
| `MSET key value [key value ...]` | Set several string keys at once (overwrites a non-string key with a string) |
| `GET key` | String get |
| `MGET key [key ...]` | Get several string keys at once (`nil` for a missing key or a key holding a non-string value) |
| `GETDEL key` | Return the value and delete the key (`nil` when missing) |
| `GETEX key [EX s \| PX ms \| PERSIST]` | Return the value and set, refresh, or remove its TTL (`nil` when missing; no option leaves the TTL alone) |
| `SETNX key value` | Set only when the key does not exist; returns `1` when set, `0` when the key already exists (whatever type it holds) |
| `SETEX key seconds value` | Set a string with a TTL in seconds (`invalid expire time` when the TTL is not a positive integer; overwrites any type, like `SET`) |
| `GETSET key value` | Return the old value and set the new one (`nil` when missing; clears the TTL) |
| `APPEND key value` | Append to a string, creating the key when missing, and return the new length; preserves the TTL |
| `STRLEN key` | Length of a string (`0` when missing) |
| `GETRANGE key start end` | Substring; negative indexes count from the end, out-of-range ends are clamped, empty string when missing or the range is empty |
| `SETRANGE key offset value` | Overwrite a string starting at `offset`, creating the key zero-padded when missing; returns the new length; preserves the TTL; an empty value changes nothing |
| `SETBIT key offset value` | Set one bit (`0` or `1`, bit 0 is the high bit of byte 0), creating the key zero-padded when missing; returns the old bit; preserves the TTL; setting a bit that already holds the value changes nothing and is not logged |
| `GETBIT key offset` | Bit value (`0` when missing or out of range) |
| `BITCOUNT key [start end]` | Count set bits, optionally over a byte range (negative indexes count from the end, out-of-range ends are clamped); `0` when missing |
| `BITOP op dest key [key ...]` | Bitwise `AND` / `OR` / `XOR` across string keys (`NOT` takes exactly one key); a missing source counts as an empty string, the result is as long as the longest input, it overwrites the destination and clears its TTL; when every source is missing the result is empty and the destination is deleted, returning `0` |
| `BITFIELD key [GET type off] [SET type off value] [INCRBY type off inc] [OVERFLOW WRAP \| SAT \| FAIL]` | Packed integer fields in a string key: types `i1`–`i64` / `u1`–`u63` (`u64` is not supported, RESP integers are signed 64-bit), offsets in bits or `#n` for the n-th field of the width; `GET` reads a field (`0` when missing), `SET` returns the old value, `INCRBY` returns the new value; `OVERFLOW` sets the policy for the `SET`/`INCRBY` operations that follow it (default `WRAP`, `SAT` clamps, `FAIL` answers nil and leaves the field unchanged); a write creates the key zero-padded when missing and preserves the TTL; one reply element per operation, in order |
| `DEL key [key ...]`, `EXISTS key [key ...]` | Multi-key delete / existence |
| `EXPIRE key s`, `PEXPIRE key ms` | Set a TTL |
| `TTL key`, `PTTL key` | Remaining TTL (`-2` missing, `-1` no expiry) |
| `INCR`, `DECR`, `INCRBY`, `DECRBY` | Integer counters; preserves TTL |
| `LPUSH key e [e ...]`, `RPUSH key e [e ...]` | Push to head / tail; creates the list; returns new length |
| `LPOP key`, `RPOP key` | Pop from head / tail; popping the last element deletes the key |
| `LLEN key` | List length (`0` when missing) |
| `LRANGE key start stop` | Elements, inclusive, negative indexes from the end |
| `LINDEX key index` | Element at index, negative indexes count from the end (`nil` when missing or out of range) |
| `LSET key index element` | Set the element at index (`no such key` when missing, `index out of range` when out of range); preserves the TTL; setting an element to the value it already holds changes nothing and is not logged |
| `LINSERT key BEFORE\|AFTER pivot element` | Insert before or after the first occurrence of `pivot`; returns the new length, `0` when the pivot is not in the list, `-1` when the key is missing; preserves the TTL |
| `LPUSHX key e [e ...]`, `RPUSHX key e [e ...]` | Push to head / tail only when the key already holds a list; returns the new length, `0` when the key is missing (creates nothing, logs nothing) |
| `LTRIM key start stop` | Keep only elements from `start` to `stop` inclusive, negative indexes from the end; an empty range deletes the key; preserves the TTL |
| `LMOVE source dest LEFT\|RIGHT LEFT\|RIGHT` | Move the head or tail element of `source` to the head or tail of `dest` (creating `dest` when missing); returns the moved element, `nil` when the source is missing or empty; emptying the source deletes it; same-key moves rotate the list; the destination keeps its TTL |
| `SADD key member [member ...]` | Add to set; returns number added (duplicates ignored); creates the set |
| `SREM key member [member ...]` | Remove from set; returns number removed; emptying the set deletes the key |
| `SMEMBERS key` | All members (insertion order) |
| `SSCAN key cursor [MATCH pattern] [COUNT n]` | Cursor-based iteration over a set's members: start at cursor `0`, repeat with the returned cursor until it comes back `0`; each call returns the next cursor and an array of matching members; `MATCH` filters member names with the same glob as `KEYS`, `COUNT` (default 10) is a hint for how many matching members to return per call |
| `SISMEMBER key member` | `1` if member, `0` if not |
| `SCARD key` | Set size (`0` when missing) |
| `HSET key field value [field value ...]` | Set fields; returns number of new fields (updates return `0` for that field) |
| `MULTI`, `EXEC`, `DISCARD` | Transactions: `MULTI` queues commands (`+QUEUED` per command, nested `MULTI` is an error), `EXEC` runs the queue in order and returns one reply per command (an error inside the transaction takes its place in the array and the rest still run), `DISCARD` drops the queue; `EXEC`/`DISCARD` without `MULTI` are errors. Queued writes are invisible to other clients until `EXEC` runs, and the commands `EXEC` runs are AOF-logged and replicated individually, never `MULTI`/`EXEC` themselves |
| `WATCH key [key ...]`, `UNWATCH` | Optimistic locking for `MULTI`/`EXEC`: `WATCH` records the version each key has right now (0 when it does not exist); if any watched key is created, modified, deleted, expired, or flushed away before `EXEC`, `EXEC` aborts with a nil array and the queue never runs. `UNWATCH` clears the watch set, as do `EXEC` and `DISCARD`; `WATCH` inside `MULTI` is an error, while `UNWATCH` inside `MULTI` queues and runs at `EXEC`, as in real Redis. A write that changes nothing (SADD of a member already present, HSET of an identical value) does not invalidate a watch. Limitation: a watched key that did not exist, was created and deleted again before `EXEC`, reads as version 0 again, so the abort is not detected — the version scheme cannot see a create-plus-delete round trip |
| `HGET key field` | Field value (`nil` when missing) |
| `HDEL key field [field ...]` | Delete fields; returns number removed; emptying the hash deletes the key |
| `HEXISTS key field` | `1` if the field exists, `0` if not |
| `HLEN key` | Number of fields (`0` when missing) |
| `HGETALL key` | Flat `[field, value, ...]` array in insertion order |
| `HMGET key field [field ...]` | Field values in order (`nil` for a missing field or key) |
| `HSETNX key field value` | Set only when the field does not exist; returns `1` when set, `0` when the field already exists |
| `HINCRBY key field increment` | Add to a field's integer value, creating the field and the key when missing; returns the new value; preserves the TTL; a zero increment on an existing field changes nothing and is not logged |
| `HKEYS key` | All field names (insertion order; empty array when missing) |
| `HVALS key` | All field values (insertion order; empty array when missing) |
| `HINCRBYFLOAT key field increment` | Add to a field's float value, creating the field and the key when missing; returns the new value; preserves the TTL; a non-float field is an error, an increment that would produce a non-finite result is an error, and a zero increment on an existing field changes nothing and is not logged |
| `HSCAN key cursor [MATCH pattern] [COUNT n]` | Cursor-based iteration over a hash's fields: start at cursor `0`, repeat with the returned cursor until it comes back `0`; each call returns the next cursor and a flat `[field, value, ...]` array of matching pairs; `MATCH` filters field names with the same glob as `KEYS`, `COUNT` (default 10) is a hint for how many matching pairs to return per call |
| `ZADD key [NX \| XX] [GT \| LT] [CH] [INCR] score member [score member ...]` | Add members with scores; returns number of newly added members (updating a member's score returns `0` for that member; re-adding a member with its current score changes nothing). `NX` only adds new members, `XX` only updates existing ones, `GT`/`LT` only update when the new score is greater/smaller than the current one (new members are still added), `CH` counts changed members instead of only new ones, `INCR` acts like `ZINCRBY` for a single member and returns the new score (nil when a condition blocks the update). `NX` conflicts with `XX`, `GT` and `LT`; `GT` conflicts with `LT`; `INCR` takes exactly one pair; an increment that yields NaN is an error |
| `ZSCORE key member` | Member's score (`nil` when missing) |
| `ZCARD key` | Number of members (`0` when missing) |
| `ZRANGE key start stop [WITHSCORES]` | Members in index range, lowest score first; negative indices count from the end, `WITHSCORES` interleaves each score after its member |
| `ZREVRANGE key start stop [WITHSCORES]` | Same range, highest score first |
| `ZRANGEBYSCORE key min max [WITHSCORES] [LIMIT offset count]` | Members whose scores fall in `[min, max]`; bounds may be `-inf`/`+inf` or exclusive with a leading `(` (e.g. `(5`); `LIMIT` skips `offset` matches and returns at most `count` (negative `count` means all); `WITHSCORES` interleaves each score after its member |
| `ZREVRANGEBYSCORE key max min [WITHSCORES] [LIMIT offset count]` | Same range, highest score first; bounds are `max` first, as in Redis, with the same exclusive/inf/`WITHSCORES`/`LIMIT` handling |
| `ZRANGEBYLEX key min max [LIMIT offset count] [REV]` | Members whose names fall in `[min, max]` lexicographically, regardless of score; bounds are `-` (-inf) / `+` (+inf) or inclusive `[x` / exclusive `(x`; `REV` returns them in reverse member order with the bounds given max-first, as in Redis; `LIMIT` skips `offset` matches and returns at most `count` (negative `count` means all) |
| `ZLEXCOUNT key min max` | Number of members whose names fall in `[min, max]` lexicographically, with the same bounds as `ZRANGEBYLEX` but no `REV` — the count does not depend on order; `0` when missing or the range is empty, including a finite min above the max |
| `ZREM key member [member ...]` | Remove members; returns number removed (the key is deleted when the last member is removed) |
| `ZREMRANGEBYSCORE key min max` | Remove the members whose scores fall in `[min, max]`, with the same bounds as `ZRANGEBYSCORE` (`-inf`/`+inf`, or a leading `(` for an exclusive endpoint); returns the number removed, `0` when missing or the range is empty, including a finite min above the max, and the key is deleted when the last member is removed |
| `ZRANK key member` | 0-based rank of a member, lowest score first (`nil` when missing) |
| `ZREVRANK key member` | 0-based rank of a member, highest score first (`nil` when missing) |
| `ZINCRBY key increment member` | Add to a member's score (creating member and key when missing); returns the new score |
| `ZPOPMIN key [count]`, `ZPOPMAX key [count]` | Remove and return the lowest- / highest-scoring member(s) with their scores, flat `[member, score, ...]`; `count` defaults to 1 and must be positive, a count past the end pops the whole set, an empty array when missing, and popping the last member deletes the key |
| `ZRANDMEMBER key [count [WITHSCORES]]` | A random member (bulk reply, `nil` when missing), or `count` of them as an array; a positive count returns distinct members (at most the whole set), a negative count samples with replacement so members may repeat, `0` returns an empty array, and `WITHSCORES` interleaves each score after its member |
| `ZSCAN key cursor [MATCH pattern] [COUNT n] [WITHSCORES]` | Cursor-based iteration over a sorted set's members, in (score, member) order: start at cursor `0`, repeat with the returned cursor until it comes back `0`; each call returns the next cursor and an array of matching members; `MATCH` filters member names with the same glob as `KEYS`, `COUNT` (default 10) is a hint for how many matching members to return per call, `WITHSCORES` interleaves each score after its member |
| `KEYS pattern` | Glob match (`*` and `?`) |
| `SCAN cursor [MATCH pattern] [COUNT n]` | Cursor-based keyspace iteration: start at cursor `0`, repeat with the returned cursor until it comes back `0`; `MATCH` filters with the same glob as `KEYS`, `COUNT` (default 10) is a hint for how many matching keys to return per call |
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
             sets (unordered member arrays, plus a member-to-index
             hash table for O(1) membership),
             hashes (parallel field/value arrays, insertion order,
             plus a field-to-index hash table for O(1) lookup)
             and sorted sets (skip list ordered by score then
             member, plus a member-to-node hash table for O(1)
             lookup)
  resp.c     RESP parser (array + inline commands) and encoders
  aof.c      append-only log: every mutation is a RESP array, replayed on start;
             fsync policy `always` or `everysec`; under `everysec` a
             dedicated flush thread does the once-a-second `fdatasync`
             off the event loop, `SAVE` forces a sync through it (it
             waits until a sync covers every append made before the
             call), a clean shutdown flushes and joins the thread, and
             `INFO` reports `aof_flush_in_progress`,
             `aof_flushes_completed` and `aof_dirty`. Under `always`
             the same thread does every sync and the reply waits for
             it: the append returns a sequence number, the reply is
             parked in a FIFO queue keyed by that number and released
             in order once the sync covers it, and a later reply to
             the same client parks behind it too, so per-client order
             never breaks. The loop wakes the thread once per
             iteration (`aof_kick`), after the read batch is
             processed, so one pipeline coalesces into one sync
             instead of one sync per command; the thread wakes the
             loop back through an eventfd after each sync, and
             `INFO` reports `aof_pending_appends` and
             `aof_deferred_replies`
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
  RESP array and synced according to `--aof-fsync`: under `always` (the
  default) the flush thread `fdatasync`s after every write and the
  reply is deferred until that sync covers the command's append, so
  no acknowledged write is lost on a crash and the event loop never
  waits on the disk; under `everysec` the thread syncs at most once a
  second, and always syncs on a clean shutdown or `SAVE`. On startup the file is replayed through the
  same dispatcher live clients use, so the log is the dataset. `SAVE`
  additionally writes a snapshot of the whole dataset to `dump.rdb`
  (strings, lists, sets, hashes and sorted sets, with TTLs stored as absolute
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
  cannot run past the end of the event array. The loop also watches the
  AOF flush thread's wakeup eventfd, so a reply deferred on a sync is
  released as soon as that sync lands, and it drains the deferred-reply
  queue once per iteration as a backstop.
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

Measured with `tools/bench.py` (one client, 5,000 requests per
operation, pipeline depth 50, mean of 3 runs each), cinder 0.16.0 and
real Redis 8.12.0 back-to-back on the same dev VM. Redis ran with
persistence off (`--save '' --appendonly no`); cinder ran with its
AOF on. The VM is a shared, noisy host, so treat these as
same-ballpark numbers (±20% run to run), not lab precision. The
Redis column is the mean of 9 runs in the 0.13.0 sessions; the cinder
columns are the 0.16.0 means, re-measured for this release.

| Operation | cinder `always` | cinder `everysec` | Redis 8.12.0 |
| --- | ---: | ---: | ---: |
| `SET` | ~12,000 | ~25,000 | ~35,000 |
| `GET` | ~33,000 | ~35,000 | ~34,000 |
| `INCR` | ~15,000 | ~26,000 | ~34,000 |
| `LPUSH` | ~16,000 | ~26,000 | ~35,000 |
| `HSET` | ~15,000 | ~25,000 | ~35,000 |
| `SET` pipelined | ~131,000 | ~127,000 | ~240,000 |
| `GET` pipelined | ~254,000 | ~231,000 | ~228,000 |

Sequential single-client throughput is latency-bound. The 0.16.0
`always` column is the cost of the deferred-reply slice, measured
honestly: a sequential write's reply now waits on a flush-thread
round trip, and on this dev VM the thread handoff (~50µs) costs more
than the `fdatasync` it replaces (a few µs), so sequential
SET/INCR/LPUSH/HSET measure ~12–16k under `always` against ~25–26k
under `everysec` and ~34k on Redis — down from ~33–36k under the old
inline sync. Pipelined writes are unaffected in kind: one sync covers
the whole batch (the loop wakes the flush thread once per iteration,
after the read batch is processed), so pipelined SET still measures
~131k. The trade is deliberate: the event loop never blocks on the
disk, so a slow disk stalls only the writing client's reply instead
of every client; on this VM the disk is fast, so the handoff dominates
and `always` looks slower than the old inline sync. On a machine where
`fdatasync` costs milliseconds, the old design would stall every
client for milliseconds per write and the deferred one would not.
`SET` still pays one `fdatasync` per write under the `always` policy
— verified with `strace`, 20 sequential SETs cost 21 `fdatasync`s
(the extra one on shutdown), all of them on the flush thread, never
on the event loop — and 2 under `everysec` (one at the first write,
one on shutdown). The `everysec` column moved a little from the
0.13.0 session (~32–35k then, ~25–26k now); the `everysec` path is
unchanged apart from per-iteration housekeeping, so that is host
noise on a shared VM, not a code change. Reproduce with:

```sh
python3 tools/bench.py --port 7777 --compare-port 7780 --label redis
```

## Tests

```sh
make test   # builds, then runs tests/smoke.py: 30+ RESP checks, including
            # INCR checks (INCR preserves the key's TTL, the stored value
            # grows across digit boundaries, 99→100 and 999→1000, and
            # INCR on an expired key restarts at the delta), list
            # checks (push/pop both ends, length, range with negative
            # indexes, wrong-type errors, list TTL expiry), set checks
            # (add/dedup, membership, remove, wrong-type errors, set
            # TTL expiry, emptying a set deletes the key, and a 100-member
            # set exercising the member dict: dict growth, a duplicate add,
            # removing a middle member and finding the member that swapped
            # into its hole, removing the last member, re-adding a removed
            # member, and the full member list), hash checks
            # (multi-field set, update, get, delete, exists, length,
            # getall in insertion order, wrong-type errors, hash TTL
            # expiry, emptying a hash deletes the key), hash-misc
            # checks (`HMGET` in order with `nil` for missing fields,
            # `HSETNX` setting only a new field, `HINCRBY` on an
            # existing, a new, and a missing key's field, negative
            # and zero increments, a non-integer field and a bad
            # increment, overflow, wrong-type and wrong-argument
            # errors, the TTL surviving an `HINCRBY`, `COMMAND`
            # listing all three, a real `HINCRBY` invalidating a
            # `WATCH` while a zero increment does not, and an
            # `HINCRBY` surviving an AOF restart), sorted-set
            # checks (multi-member add, score update, re-add with the
            # same score is a no-op, duplicate member in one call
            # takes the last score, bad-score error applies nothing,
            # wrong-type errors, sorted-set TTL expiry, AOF restart
            # and RDB snapshot and replication full-sync checks for
            # scores), ZADD option checks (`NX`/`XX`/`GT`/`LT`/`CH`/
            # `INCR`, including the conflict errors, nil replies when
            # a condition blocks an `INCR`, `GT`/`LT` comparing the
            # resulting score under `INCR`, and the NaN-result error),
            # and score-range checks (inclusive and exclusive
            # bounds, infinities, WITHSCORES, LIMIT offset/count,
            # reversed bounds, missing key, bad bounds and options),
            # and reversed score-range checks (max-first bounds,
            # exclusive bounds and infinities, WITHSCORES, LIMIT
            # offset/count, ties in reverse member order, missing
            # key, bad bounds and options), and pop/random checks
            # (`ZPOPMIN` / `ZPOPMAX` popping one, several, and past
            # the end, deleting the key when emptied, empty array
            # for a missing key, bad and non-positive counts;
            # `ZRANDMEMBER` single and count forms, distinct and
            # with-replacement sampling, `WITHSCORES` pairing each
            # score with its member, missing key, bad count and
            # option, and wrong-type errors for all three), and
            # `ZSCAN` checks (full iteration in score order, `MATCH`
            # filtering, `COUNT` pagination across three pages,
            # `WITHSCORES` pairing each score with its member, a
            # `COUNT` of 0, a cursor past the end, a missing key,
            # bad cursor and options, a duplicate `WITHSCORES`,
            # and `COMMAND` listing it), and `ZREVRANK` checks
            # (highest member ranks 0, lowest ranks last, `nil`
            # for a missing key or member, wrong-type and
            # wrong-argument errors, and `COMMAND` listing it),
            # an AOF
            # restart test (data, a list, a set, a hash and a sorted set survive a
            # SHUTDOWN and reload), and everysec-policy checks (INFO
            # reports the policy, the AOF holds the record before any
            # sync, data survives a SIGKILL + restart, unknown policy
            # is a startup error), and RDB snapshot checks (SAVE writes
            # dump.rdb with all five types, a key whose TTL passed
            # before the save is left out, and a restart with the AOF
            # deleted restores every key from the snapshot alone, with
            # a surviving TTL preserved), and replication checks
            # (REPLICAOF streams a full sync of all five types with a
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
            # `dump.rdb` with all five types, skips a key whose TTL passed,
            # reports `threads:2` and `last_bgsave_status:ok` in `INFO`, a
            # second `BGSAVE` completes after the first, and a restart
            # with the AOF deleted restores every key from the background
            # snapshot alone, with a surviving TTL preserved; `--threads 0`
            # is a startup error), and a benchmark-tool check
            # (`tools/bench.py` runs against a live server, exits 0, and
            # prints one table row per operation, and `INFO` reports
            # version 0.39.0), and `always`-policy checks (a pipeline
            # mixing writes and reads returns its replies in command
            # order, two clients writing at once each get their own
            # +OK, a QUIT sent in the same packet as a SET still
            # returns both +OKs, `INFO` reports
            # `aof_deferred_replies` and `aof_pending_appends`, and a
            # write acknowledged before a SIGKILL is on disk after the
            # restart), and transaction checks (`EXEC`/`DISCARD`
            # without `MULTI` are errors, nested `MULTI` is an error,
            # queued writes are invisible to a second client until
            # `EXEC` runs, `EXEC` returns one reply per command with
            # an error taking its place in the array and the rest
            # still running, `DISCARD` drops the queue, an empty
            # `EXEC` returns an empty array, a nested array reply
            # comes back nested inside the `EXEC` array, and `COMMAND`
            # lists `MULTI`, `EXEC` and `DISCARD`), and WATCH checks
            # (a clean `EXEC` runs, a `SET`/`DEL`/create/expiry/`FLUSHALL`
            # of a watched key by another client aborts `EXEC` with a
            # nil array and the queued write never runs, `UNWATCH`/`EXEC`/
            # `DISCARD` clear the watch set, `WATCH` inside `MULTI` is
            # an error while `UNWATCH` inside `MULTI` queues and runs
            # at `EXEC`, a no-op `SADD` does not invalidate a watch,
            # and `COMMAND` lists `WATCH` and `UNWATCH`), and string-misc
            # checks (`SETNX` sets only when absent and returns `0`
            # for a key that already exists, even a list; `SETEX`
            # sets the value plus a TTL and rejects a bad TTL;
            # `GETSET` returns the old value and clears the TTL;
            # `APPEND` appends or creates and preserves the TTL;
            # `STRLEN` returns the length, `0` for a missing key,
            # and wrong-type errors for the commands that take one),
            # and string-range checks (`GETRANGE` with negative
            # indexes and out-of-range clamping, empty string for a
            # missing key or an empty range, wrong-type and bad-index
            # errors; `SETRANGE` overwriting, extending, creating a
            # missing key zero-padded, preserving the TTL, an empty
            # value changing nothing and creating nothing, negative
            # and bad offsets; a zero-padded value reads back whole
            # through `GET`/`GETRANGE`/`STRLEN`, `APPEND` appends
            # after the padding, `INCR` on it is not an integer, and
            # the padding survives an AOF restart and an RDB
            # snapshot reload), and string-bit checks (`SETBIT`
            # returns the old bit, creates a missing key zero-padded
            # even for a `0` bit, grows past the end, preserves the
            # TTL, and a no-op set changes nothing; `GETBIT` returns
            # `0` for a missing key or an out-of-range offset;
            # `BITCOUNT` counts the whole value and byte ranges with
            # negative indexes and clamping; bad offsets, bad bit
            # values, wrong-type and wrong-argument errors; `COMMAND`
            # lists all three; and a replication full sync rebuilds
            # bit keys byte-identically, including values ending in a
            # zero byte), and `BITOP` checks (`AND`/`OR`/`XOR`/`NOT`
            # results, a missing source counting as empty, an
            # all-missing result deleting the destination, `NOT` of a
            # missing key, the destination TTL being cleared, the
            # destination also being a source, bad-op and
            # wrong-argument and wrong-type errors, and `COMMAND`
            # listing `BITOP`), and `BITFIELD` checks (`GET` on a
            # missing key reading `0` without creating it, `SET`
            # returning the old value and `GET` reading it back
            # signed and unsigned, `SET` wrapping an out-of-range
            # value, multi-byte fields read as `u16`/`i16`, `#n`
            # offsets addressing the n-th field of the width,
            # `INCRBY` with positive and negative increments, several
            # operations in one call replying in order, `OVERFLOW
            # WRAP`/`SAT`/`FAIL` on `INCRBY` and on `SET`, the policy
            # resetting to `WRAP` on the next call, `i64`/`u63`
            # fields and `u64` rejected, `BITFIELD` preserving the
            # TTL, a syntax error anywhere executing nothing,
            # bad-type/bad-offset/bad-value/wrong-argument/wrong-type
            # errors, the max bit offset, a `BITFIELD` write
            # invalidating a `WATCH` while a no-op write does not,
            # `COMMAND` listing `BITFIELD`, and an AOF restart
            # replaying a `BITFIELD` key), and list-index checks
            # (`LINDEX` reading by positive and negative index,
            # `nil` for a missing key or an out-of-range index;
            # `LSET` setting by positive and negative index,
            # `no such key` and `index out of range` errors, a
            # no-op set changing nothing, and the TTL surviving a
            # set; `LINSERT` inserting `BEFORE`/`AFTER` the first
            # occurrence of a pivot, `0` when the pivot is absent,
            # `-1` when the key is missing, a bad direction error,
            # and wrong-type errors for all three; `COMMAND`
            # listing all three; `LPUSHX` / `RPUSHX` pushing
            # onto an existing list only, `0` and no key created
            # for a missing key; `LTRIM` keeping a middle and a
            # negative range, a whole-list trim changing nothing,
            # an empty range deleting the key, and the TTL
            # surviving a trim; `LMOVE` moving head-to-head and
            # tail-to-tail between lists, emptying the source,
            # `nil` for a missing source, rotating one list when
            # source and destination are the same key, a bad
            # direction error, and wrong-type errors on either
            # key leaving the source untouched; `COMMAND` listing
            # all four)
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
- [x] Benchmarks against real Redis on the same hardware
      (`tools/bench.py`: sequential and pipelined SET/GET/INCR/LPUSH/
      HSET vs Redis 8.12.0, mean of 3 runs each, recorded in the
      Benchmarks section — same ballpark on SET/GET/HSET and
      pipelined GET, ~2x behind on INCR/LPUSH)
- [x] Close the INCR/LPUSH gap vs real Redis (`store_incr` does the
      whole increment in one hash lookup and updates the stored value
      in place, preserving the TTL; LPUSH/RPUSH drop the redundant
      type pre-check — both now measure at parity with Redis 8.12.0
      on the same hardware, mean of 3 interleaved A/B runs each;
      pipelined SET, ~1.4x behind, is the remaining gap)
- [x] Background AOF flush, `everysec` slice (0.15.0): the once-a-second
      `fdatasync` runs on a dedicated flush thread, so a slow disk never
      stalls the event loop; `SAVE` still forces a sync (it waits for a
      sync covering every append before the call) and a clean shutdown
      flushes and joins the thread; `INFO` reports the flush state
- [x] Background AOF flush, `always` slice (0.16.0): the `fdatasync`
      runs on the flush thread and the reply waits for it — deferred
      replies in a FIFO queue keyed by append sequence number, with
      per-client ordering (a later reply parks behind an earlier one),
      a per-iteration kick so one pipeline coalesces into one sync,
      an eventfd wakeup so the reply goes out as soon as its sync
      lands, and `QUIT`/`SHUTDOWN` flushing parked replies before
      closing; `INFO` reports `aof_deferred_replies` and
      `aof_pending_appends`. Sequential writes under `always` now cost
      a thread round trip (~12–16k ops/sec on this VM, down from
      ~33–36k with the inline sync); pipelined writes hold at ~131k
- [x] `MULTI`/`EXEC` transactions (0.17.0): `MULTI` queues commands
      (`+QUEUED` each), `EXEC` runs the queue in order and returns
      one reply per command, `DISCARD` drops the queue; nested
      `MULTI` and `EXEC`/`DISCARD` without `MULTI` are errors, an
      error inside a transaction takes its place in the array and the
      rest still run, queued writes are invisible to other clients
      until `EXEC`, and the commands `EXEC` runs are AOF-logged and
      replicated individually
- [x] Optimistic locking: `WATCH`/`UNWATCH` (0.18.0): every key
      carries a modification version (a global sequence, bumped on
      every real change to a key; a no-op write does not bump it),
      `WATCH` records the version each key has then, and `EXEC`
      aborts with a nil array when any watched key moved — created,
      modified, deleted, expired, or flushed away (`FLUSHALL` bumps
      a separate generation, so even a watch on a key that never
      existed is invalidated). `UNWATCH`, `EXEC` and `DISCARD` clear
      the watch set; `WATCH` inside `MULTI` is an error, `UNWATCH`
      inside `MULTI` queues and runs at `EXEC`, as in real Redis.
      Known limitation: a watched key that did not exist, was created
      and deleted again before `EXEC`, reads as version 0 again, so
      that round trip is not detected
- [x] `GETDEL` / `GETEX` (0.19.0): `GETDEL` returns the value and
      deletes the key, `GETEX` returns the value and sets (`EX`/`PX`),
      refreshes, or removes (`PERSIST`) its TTL; both are
      `WRONGTYPE` on a non-string key, `nil` when missing, AOF-logged
      and replicated only when they actually change state (a missing
      key, a bare `GETEX`, or `PERSIST` on a key with no TTL logs
      nothing)
- [x] `MSET` / `MGET` (0.20.0): `MSET` sets several string keys in one
      call (overwriting a list/set/hash key with a string, as in Redis;
      a duplicate key in one call takes the last value), `MGET` returns
      the values in order with `nil` for a missing key or a key holding
      a non-string value — unlike `GET`, a wrong-type key is `nil`, not
      a `WRONGTYPE` error; `MSET` is AOF-logged and replicated, `MGET`
      is read-only. Limitation: on an out-of-memory failure midway
      through an `MSET`, the pairs set before the failure stay set
      (same partial-apply behaviour as `HSET` in this codebase)
- [x] Sorted sets, basics (0.21.0): `ZADD` / `ZSCORE` / `ZCARD`,
      members kept in a sorted array ordered by (score, member),
      AOF-logged, RDB-persisted (scores as `%.17g` strings, which
      round-trip exactly) and replicated via full sync; `ZADD` parses
      every score before applying any pair, so a bad score changes
      nothing, and re-adding a member with its current score is a
      no-op that neither logs nor invalidates a `WATCH`. Limitations:
      no range-by-score command yet (`ZRANGEBYSCORE`), no `NX`/`XX`/
      `GT`/`LT`/`CH`/`INCR` options on `ZADD`, and member lookup is a
      linear scan, so large sorted sets are slower than a skip list
      would be
- [x] Sorted-set ranges and removal (0.22.0): `ZRANGE` / `ZREVRANGE`
      (negative indices, optional `WITHSCORES`), `ZREM`, `ZRANK`,
      `ZINCRBY`; `ZREM` deletes the key when the last member goes,
      `ZINCRBY` with an increment of 0 on an existing member is a
      no-op like the `ZADD` same-score case. `ZREM` and `ZINCRBY`
      are mutating commands, so they are AOF-logged and replicated
      like `ZADD`; the snapshot and full sync already carried whole
      sorted sets, so they needed no change
- [x] Sorted-set ranges by score (0.23.0): `ZRANGEBYSCORE key min max
      [WITHSCORES] [LIMIT offset count]`, bounds inclusive by default
      and exclusive with a leading `(` (e.g. `(5`), `-inf`/`+inf`
      supported, a negative `LIMIT` count means all matches; a
      read-only command, so no AOF, RDB, or replication change was
      needed. The store scans the already score-sorted member array
      in one pass, so no new data structure. Limitation: member
      lookup is still a linear scan, so large sorted sets are slower
      than a skip list would be
- [x] Sorted-set ranges by score, reversed (0.24.0):
      `ZREVRANGEBYSCORE key max min [WITHSCORES] [LIMIT offset
      count]`, bounds max-first as in Redis, same exclusive/inf/
      `WITHSCORES`/`LIMIT` handling as `ZRANGEBYSCORE`, highest
      score first (on a score tie, the last member first); also
      read-only, so no AOF, RDB, or replication change was needed.
      The reversed scan walks the same sorted array from the top
      down. Limitation: member lookup is still a linear scan, so
      large sorted sets are slower than a skip list would be
- [x] Sorted-set `ZADD` options (0.25.0): `NX` / `XX` / `GT` / `LT` /
      `CH` / `INCR`, following Redis semantics — `NX` only adds new
      members, `XX` only updates existing ones, `GT`/`LT` only update
      when the new score is greater/smaller (new members are still
      added, and `XX` combines with `GT`/`LT`), `CH` counts changed
      members instead of only new ones, `INCR` acts like `ZINCRBY`
      for a single member and returns the new score (nil when a
      condition blocks the update, and `GT`/`LT` compare the
      resulting score). Conflicting options are errors with Redis's
      messages (`NX` with `XX`/`GT`/`LT`, `GT` with `LT`, more than
      one pair with `INCR`, an increment that yields NaN). The
      options are parsed before the first score, every score is
      parsed before any pair is applied, and the raw command is
      AOF-logged and replicated only when something actually changed,
      so the log replays identically. Limitation: member lookup is
      still a linear scan, so large sorted sets are slower than a
      skip list would be
- [x] `SCAN` (0.26.0): cursor-based keyspace iteration,
      `SCAN cursor [MATCH pattern] [COUNT n]`, returning `[cursor,
      [keys...]]` until the cursor comes back `0`. The cursor is a
      bucket index walked in reverse-bit order — the same trick
      Redis's `dictScan` uses — so a table resize mid-iteration still
      visits every bucket exactly once: keys present for the whole
      iteration come back at least once. `MATCH` reuses the `KEYS`
      glob, `COUNT` defaults to 10 and is a hint (one bucket's
      matches are all returned, so a batch can exceed it slightly).
      Read-only, so no AOF, RDB, or replication change was needed.
      Limitation: keys added or deleted mid-iteration may be missed
      or returned twice, as in Redis, and a scan over a mostly-empty
      table with a selective `MATCH` can walk the whole table in one
      call before returning anything
- [x] String misc commands (0.27.0): `SETNX` / `SETEX` / `GETSET` /
      `APPEND` / `STRLEN`. `SETNX` returns `1` only when it sets
      (a key that exists, of any type, answers `0`); `SETEX` sets
      the value plus a TTL in seconds and rejects a non-positive
      TTL; `GETSET` returns the old value (`nil` when missing) and
      clears the TTL; `APPEND` grows the stored string in place
      and preserves the TTL, creating the key when missing; `STRLEN`
      returns the length, `0` for a missing key. All five are
      AOF-logged and replicated when they change state (a `SETNX`
      that does not set logs nothing); the mutating ones bump the
      key's version, so `WATCH` sees them. `COMMAND` lists all five
- [x] String ranges: `GETRANGE` / `SETRANGE` (0.28.0): substring
      read and overwrite with Redis index semantics (negative
      indexes, clamping, empty string for a missing key or an empty
      range); `SETRANGE` creates a missing key zero-padded, extends
      with zero bytes, preserves the TTL, and an empty value changes
      nothing. String values are now length-tracked (`value_len` on
      the entry), so a zero-padded value reads back whole through
      `GET`/`MGET`/`GETDEL`/`GETEX`/`GETSET`/`STRLEN`/`GETRANGE`,
      `APPEND` appends after the padding, `INCR` rejects it as not
      an integer, RDB snapshots store the full bytes, and a
      replication full sync rebuilds it as `SET` plus `SETRANGE`
      per non-zero run (the dispatcher reads argv as C strings, so
      a single `SET` would truncate at the first zero byte). The
      AOF log and live replication carry the raw `SETRANGE` command,
      so those paths needed no change. Limitation: a client-sent
      value holding a zero byte is still truncated at the first zero
      byte by `SET`, as before, because the dispatcher reads argv
      as C strings

- [x] String bit commands: `SETBIT` / `GETBIT` / `BITCOUNT` (0.29.0):
      bit operations on string values, building on the length-tracked
      strings this release added. `SETBIT` returns the old bit, bit 0
      is the high bit of byte 0, the offset caps at 2^32-1 (the last
      bit of a 512 MB string), a missing key is created zero-padded
      even when the bit being set is 0, a write past the end extends
      the value with zero bytes, and the TTL is preserved; setting a
      bit that already holds the requested value changes nothing, so
      it is not AOF-logged or replicated and does not invalidate a
      `WATCH`. `GETBIT` returns `0` for a missing key or an
      out-of-range offset. `BITCOUNT` counts set bits over the whole
      value or an optional byte range (negative indexes, clamping,
      empty range counts 0). `COMMAND` lists all three. A replication
      full sync rebuilds bit values byte-identically: the `SET` /
      `SETRANGE` rebuild cannot carry a trailing zero byte, so a
      value ending in one gets a trailing `SETBIT` of its last bit,
      which zero-pads it back to its full length

- [x] `BITOP` (0.30.0): bitwise `AND` / `OR` / `XOR` / `NOT` across
      string keys, the follow-on to the bit commands. A missing source
      key counts as an empty string, so the result is as long as the
      longest input; the result overwrites the destination key and
      clears its TTL, as in Redis; when every source is missing the
      result is empty and the destination key is deleted, returning 0.
      A result identical to the destination changes nothing, so it is
      not AOF-logged or replicated and does not invalidate a `WATCH`.
      The raw command is logged when it changes state, so AOF replay
      and live replication reproduce it, and the snapshot and full
      sync already carried whole strings, so they needed no change.
      Limitation: the whole result is computed in memory before it is
      written, so a `BITOP` over 512 MB strings transiently holds two
      copies of the longest input

- [x] `BITFIELD` (0.31.0): packed integer fields in a string key,
      the natural follow-on to the bit commands. `GET` / `SET` /
      `INCRBY` over `i1`–`i64` / `u1`–`u63` fields at arbitrary bit
      offsets (`#n` addresses the n-th field of the width), several
      operations in one call replying in order, and `OVERFLOW
      WRAP` / `SAT` / `FAIL` for the `SET` / `INCRBY` operations
      that follow it in the call (default `WRAP`; `SAT` clamps to
      the type's range; `FAIL` answers nil and leaves the field
      unchanged). A write creates the key zero-padded when missing
      and preserves the TTL, as in Redis; a `GET`-only call creates
      nothing. The whole call is parsed before anything executes,
      so a syntax error anywhere changes nothing, and only a real
      change is AOF-logged or replicated or invalidates a `WATCH`.
      Limitation: `u64` is rejected, as in Redis, because RESP
      integers are signed 64-bit, and the whole field is read and
      written bit by bit, so a 64-bit field costs 64 bit operations

- [x] List index commands: `LINDEX` / `LSET` / `LINSERT`
      (0.32.0): read, overwrite, and insert at an index in a list,
      with Redis index semantics (negative indexes count from the
      end). `LINDEX` answers `nil` for a missing key or an
      out-of-range index; `LSET` errors `no such key` and `index
      out of range` and preserves the TTL, and setting an element
      to the value it already holds is a no-op (no AOF log, no
      replication, no `WATCH` invalidation); `LINSERT` inserts
      before or after the first occurrence of a pivot and returns
      the new length, `0` when the pivot is absent, `-1` when
      the key is missing. The list is a circular buffer, so a
      middle insert linearizes it first (a grow already does);
      the mutating two are AOF-logged and replicated as raw
      commands, so the snapshot and full sync needed no change.
      Limitation: a middle insert is O(n) pointer moves, as a
      linked list would not be

- [x] `LPUSHX` / `RPUSHX` / `LTRIM` / `LMOVE` (0.33.0): the
      remaining list writes. `LPUSHX` / `RPUSHX` push only onto
      an existing list and return `0` for a missing key without
      creating it, so a no-op push is not AOF-logged or
      replicated; `LTRIM` keeps the given range (negative
      indexes from the end) and deletes the key on an empty
      range, preserving the TTL; `LMOVE` moves the head or tail
      element between two lists (or rotates one list when
      source and destination are the same key), deletes the
      source when emptied, creates the destination when
      missing, and errors `WRONGTYPE` on either key before
      touching the source, so a bad destination loses nothing.
      All four are AOF-logged and replicated as raw commands
      when they change state, so the snapshot and full sync
      needed no change. Limitation: a trim is O(n) pointer
      moves after linearizing the circular buffer, as a
      middle insert already is

- [x] Sorted-set pop and random member: `ZPOPMIN` / `ZPOPMAX` /
      `ZRANDMEMBER` (0.34.0): `ZPOPMIN` / `ZPOPMAX` remove and
      return the lowest- / highest-scoring member(s) with their
      scores, flat `[member, score, ...]`, with an optional
      positive `count` (default 1, a count past the end pops the
      whole set); popping the last member deletes the key, and
      a missing key answers an empty array. `ZRANDMEMBER`
      returns one random member, or `count` of them: a positive
      count returns distinct members (at most the whole set, by
      a partial Fisher-Yates shuffle), a negative count samples
      with replacement so members may repeat, `0` returns an
      empty array, and `WITHSCORES` interleaves each score
      after its member. The pops are mutating, so they are
      AOF-logged and replicated as raw commands — the replay
      pops the same members, since the sorted order is
      deterministic — and `ZRANDMEMBER` is read-only, so it
      logs nothing. Limitation: member lookup is still a
      linear scan, so large sorted sets are slower than a
      skip list would be

- [x] Sorted-set skip list (0.35.0): the sorted array is gone —
      a sorted set is now a skip list ordered by (score, member)
      with span counts, plus a hash table from member to node,
      so `ZADD` / `ZREM` / `ZSCORE` / `ZRANK` / `ZRANGE` /
      `ZRANGEBYSCORE` / `ZINCRBY` / `ZPOPMIN` / `ZPOPMAX` /
      `ZRANDMEMBER` are O(log n) (member lookup O(1)) instead of
      linear scans with memmoves. The public store API is
      unchanged; the snapshot writer and the replication emitter
      now walk the set through a `zset_foreach` iterator over
      level 0 of the skip list, and RDB load still rebuilds via
      `store_zadd`. This retires the "slower than a skip list"
      limitation on every sorted-set entry above. Limitation:
      sets still use linear scans

- [x] Hash misc commands: `HMGET` / `HSETNX` / `HINCRBY`
      (0.36.0): the remaining core hash family. `HMGET` returns
      the field values in order, `nil` for a missing field or
      key; `HSETNX` sets only when the field does not exist and
      returns `1` when it sets, `0` when the field was already
      there (a no-op set logs nothing and does not invalidate a
      `WATCH`); `HINCRBY` adds to a field's integer value,
      creating the field and the key when missing, and returns
      the new value — a non-integer field is an error, an
      increment that would overflow is an error and changes
      nothing, a zero increment on an existing field is a no-op
      (no log, no `WATCH` invalidation), and the TTL survives,
      as in Redis. The mutating two are AOF-logged and
      replicated as raw commands when they change state, so the
      snapshot and full sync needed no change.

- [x] Hash misc commands, continued: `HKEYS` / `HVALS` /
      `HINCRBYFLOAT` / `HSCAN` (0.37.0): `HKEYS` and `HVALS`
      return the field names or values in insertion order from
      the same flat array `HGETALL` builds; `HINCRBYFLOAT` adds
      to a field's float value like `HINCRBY` does for integers
      — a non-float field is an error, a result that would not
      be finite is an error and changes nothing, a zero
      increment on an existing field is a no-op (no log, no
      `WATCH` invalidation), and the TTL survives; `HSCAN`
      iterates a hash's fields with a cursor that is an index
      into the insertion-ordered fields, with `MATCH` glob
      filtering on field names and `COUNT` as a per-call hint.
      The hash family is now complete for the core commands.
      Limitation: hashes still use a linear scan (retired in
      0.38.0)

- [x] Hash-table-backed hash type (0.38.0): a hash is still its
      fields and values in insertion order, but field lookup no
      longer scans them — a hash table maps each field to its
      index in the arrays, modelled on the sorted set's member
      dict, so `HSET` / `HGET` / `HDEL` / `HEXISTS` / `HSETNX` /
      `HINCRBY` / `HINCRBYFLOAT` are O(1) instead of O(n).
      `HGETALL` / `HKEYS` / `HVALS` / `HSCAN` still walk the
      arrays in insertion order, so their replies are unchanged;
      deleting a field memmoves the tail of the arrays and
      re-indexes the fields that moved, so `HDEL` stays O(n).
      The public store API is unchanged, TTL preservation, AOF
      logging, `WATCH` invalidation and replication needed no
      change — the snapshot writer and the replication emitter
      still walk the same arrays — and RDB load still rebuilds
      via `store_hset`. This retires the "hashes still use a
      linear scan" limitation above. Limitation: sets still use
      a linear scan

- [x] Hash-table-backed set type (0.39.0): a set is still its
      members in an unordered array, but membership no longer
      scans it — a hash table maps each member to its index in
      the array, modelled on the hash type's field dict, so
      `SADD` / `SREM` / `SISMEMBER` are O(1) instead of O(n).
      `SMEMBERS` still walks the array, so its reply is
      unchanged; deleting a member swaps the last member into
      the hole, as before, and re-indexes the member that moved,
      so `SREM` stays correct. The public store API is
      unchanged, TTL preservation, AOF logging, `WATCH`
      invalidation and replication needed no change — the
      snapshot writer and the replication emitter still walk
      the same array — and RDB load still rebuilds via
      `store_sadd`. This retires the "sets still use a linear
      scan" limitation above, so no core type is left on a
      linear scan.

- [x] `SSCAN` (0.40.0): cursor-based iteration over a set's
      members, `SSCAN key cursor [MATCH pattern] [COUNT n]`,
      modelled on `HSCAN` — the cursor is an index into the
      set's members array, `MATCH` filters member names with
      the same glob as `KEYS`, `COUNT` (default 10) is a hint
      for how many matching members to return per call, and a
      missing key answers `["0", []]`. A member added or
      removed between calls may be skipped or returned twice,
      as with `SCAN`. The store gains `store_sscan`; the
      command is read-only, so AOF logging, `WATCH`
      invalidation and replication needed no change.

- [x] `ZSCAN` (0.41.0): cursor-based iteration over a sorted
      set's members, `ZSCAN key cursor [MATCH pattern] [COUNT n]
      [WITHSCORES]`, completing the SCAN family — the cursor is
      a skip-list rank (0-based), so members come back in
      (score, member) order, `MATCH` filters member names with
      the same glob as `KEYS`, `COUNT` (default 10) is a hint
      for how many matching members to return per call, and
      `WITHSCORES` interleaves each score after its member, as
      in `ZRANGE`. A missing key answers `["0", []]`, and a
      member added or removed between calls may be skipped or
      returned twice, as with `SCAN`. The store gains
      `store_zscan`; the command is read-only, so AOF logging,
      `WATCH` invalidation and replication needed no change.

- [x] `ZREVRANK` (0.42.0): 0-based rank of a member, highest
      score first — the reverse-rank path `store_zrank` already
      had (`reverse` counts down from the top) finally gets a
      command on it, mirroring `ZRANK`: `nil` for a missing key
      or member, a `WRONGTYPE` error on a non-sorted-set key,
      listed by `COMMAND`. Read-only, so AOF logging, `WATCH`
      invalidation and replication needed no change.

- [x] `ZRANGEBYLEX` (0.43.0): lexicographic range over sorted-set
      members, `ZRANGEBYLEX key min max [LIMIT offset count]
      [REV]` — the skip list is ordered by (score, member), so a
      lex range cannot walk it in member order: the store's
      `store_zrangebylex` collects the members in range with a
      level-0 scan and sorts them by member, then applies `LIMIT`
      and `REV`. Bounds are `-` / `+` or `[x` / `(x`; a bare
      member or the wrong infinity is a syntax error, and a finite
      min above the max answers an empty array, like
      `ZRANGEBYSCORE`'s reversed bounds. A missing key answers
      `[]`, a non-sorted-set key is a `WRONGTYPE` error, listed by
      `COMMAND`. Read-only, so AOF logging, `WATCH` invalidation
      and replication needed no change.

- [x] `ZLEXCOUNT` (0.44.0): count of the members in a lexicographic
      range, `ZLEXCOUNT key min max`, with the same bounds as
      `ZRANGEBYLEX` (`-` / `+` or `[x` / `(x`) but no `REV` — the
      count does not depend on order. The store gains
      `store_zlexcount`, which counts with a level-0 scan and never
      allocates, so it is O(n) time and O(1) memory where
      `ZRANGEBYLEX` collects and sorts. A missing key answers `0`,
      as does an empty range including a finite min above the max,
      and a non-sorted-set key is a `WRONGTYPE` error, listed by
      `COMMAND`. Read-only, so AOF logging, `WATCH` invalidation
      and replication needed no change.

- [x] `ZREMRANGEBYSCORE` (0.45.0): remove the members in a score
      range, `ZREMRANGEBYSCORE key min max`, with the same bounds
      as `ZRANGEBYSCORE` (`-inf` / `+inf`, or a leading `(` for an
      exclusive endpoint). The store gains
      `store_zremrangebyscore`, which finds the first match with
      the skip list in O(log n) and walks the matching slice once
      at level 0, dropping each member through `zset_remove` (the
      next node is saved before each removal, since removing frees
      the node). A missing key answers `0`, as does an empty range
      including a finite min above the max, and a non-sorted-set
      key is a `WRONGTYPE` error, listed by `COMMAND`. Removing
      the last member deletes the key, as with `ZREM`. A removal
      is logged to the AOF and replicated and invalidates `WATCH`
      on the key; removing nothing is a no-op, so it is none of
      those.
