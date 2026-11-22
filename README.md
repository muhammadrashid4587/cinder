# cinder

A Redis-compatible in-memory key-value store, written in C from scratch.
Single-threaded, `poll()`-based event loop, RESP protocol, strings, lists
and sets with TTLs, approximate LRU eviction, and an append-only file for
durability. No dependencies beyond libc. Real `redis-cli` and any RESP
client library can talk to it.

Version 0.4.0.

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
| `KEYS pattern` | Glob match (`*` and `?`) |
| `FLUSHALL`, `DBSIZE`, `INFO` | Admin / introspection |
| `COMMAND`, `SAVE` | `COMMAND` lists supported commands; `SAVE` forces the AOF to disk |
| `QUIT`, `SHUTDOWN` | Close connection / stop the server |

## Architecture

```
src/
  server.c   TCP listener, poll() event loop, command dispatch
  store.c    hash table (djb2, chaining, resize at 0.75 load), TTLs,
             strings, lists (circular-buffer lists, O(1) push/pop) and
             sets (unordered member arrays, dedup on insert)
  resp.c     RESP parser (array + inline commands) and encoders
  aof.c      append-only log: every mutation is a RESP array, replayed on start;
             fsync policy `always` or `everysec`
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
  same dispatcher live clients use, so the log is the dataset.
- **Event loop**: one thread, non-blocking sockets, per-client input/output
  buffers. A slow client never blocks other clients.

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
            # TTL expiry, emptying a set deletes the key), an AOF
            # restart test (data, a list and a set survive a SHUTDOWN
            # and reload), and everysec-policy checks (INFO reports the
            # policy, the AOF holds the record before any sync, data
            # survives a SIGKILL + restart, unknown policy is a
            # startup error)
```

## Roadmap

- [x] Lists (`LPUSH`/`RPUSH`/`LPOP`/`RPOP`/`LLEN`/`LRANGE`, AOF-persisted, TTLs)
- [x] Sets (`SADD`/`SREM`/`SMEMBERS`/`SISMEMBER`/`SCARD`, AOF-persisted, TTLs)
- [ ] Hashes (data structure beyond strings, lists and sets)
- [x] `everysec` AOF fsync policy
- [ ] RDB-style snapshots alongside the AOF
- [ ] Replication (primary/replica over RESP)
- [ ] `epoll` backend and a thread pool for the event loop
- [ ] Benchmarks against real Redis on the same hardware
