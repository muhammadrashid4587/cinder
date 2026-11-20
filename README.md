# cinder

A Redis-compatible in-memory key-value store, written in C from scratch.
Single-threaded, `poll()`-based event loop, RESP protocol, TTLs, approximate
LRU eviction, and an append-only file for durability. No dependencies beyond
libc. Real `redis-cli` and any RESP client library can talk to it.

## Why

Built as a systems portfolio project: the interesting parts of a database
(hash table, wire protocol, event loop, durability) with nothing hidden
behind a framework.

## Build and run

```sh
make
./cinder --port 6379 --data-dir ./data
```

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
| `KEYS pattern` | Glob match (`*` and `?`) |
| `FLUSHALL`, `DBSIZE`, `INFO` | Admin / introspection |
| `COMMAND`, `SAVE` | `COMMAND` lists supported commands; `SAVE` forces the log to disk |
| `QUIT`, `SHUTDOWN` | Close connection / stop the server |

## Architecture

```
src/
  server.c   TCP listener, poll() event loop, command dispatch
  store.c    hash table (djb2, chaining, resize at 0.75 load), TTLs
  resp.c     RESP parser (array + inline commands) and encoders
  aof.c      append-only log: every mutation is a RESP array, replayed on start
```

- **Store**: chained hash table, doubling resize, lazy expiry on access plus
  a periodic scan of 20 random buckets per 100 event-loop iterations.
- **Eviction**: Redis-style approximate LRU. When `--max-entries` is set, the
  server samples 5 entries and evicts the least recently used of the sample.
- **Durability**: every mutating command is appended to `appendonly.aof` as a
  RESP array and `fdatasync`ed (fsync policy: always). On startup the file is
  replayed through the same dispatcher live clients use, so the log is the
  dataset.
- **Event loop**: one thread, non-blocking sockets, per-client input/output
  buffers. A slow client never blocks other clients.

## Benchmarks

Sequential (one request at a time, no pipelining) on the dev VM, AOF fsync
policy `always`:

| Operation | Throughput |
| --- | --- |
| `SET` | ~25,000 ops/sec |
| `GET` | ~34,000 ops/sec |

`SET` pays one `fdatasync` per write under the `always` policy; pipelined
clients and an `everysec` policy would both be substantially faster.

## Tests

```sh
make test   # builds, then runs tests/smoke.py: 30+ RESP checks plus an
            # AOF restart test (data survives a SHUTDOWN and reloads)
```

## Roadmap

- [ ] Lists, sets, and hashes (data structures beyond strings)
- [ ] `everysec` AOF fsync policy
- [ ] RDB-style snapshots alongside the AOF
- [ ] Replication (primary/replica over RESP)
- [ ] `epoll` backend and a thread pool for the event loop
- [ ] Benchmarks against real Redis on the same hardware
