# cinder

A Redis-compatible in-memory key-value store, written in C from scratch.
Single-threaded, `epoll`-based event loop for commands, RESP protocol, strings, lists,
sets, hashes, sorted sets, HyperLogLog (`PFADD`/`PFCOUNT`/`PFMERGE`), geo
(`GEOADD`/`GEODIST`/`GEOPOS`/`GEOHASH`/`GEOSEARCH`/`GEOSEARCHSTORE`/`GEORADIUS`/
`GEORADIUSBYMEMBER`/`GEORADIUS_RO`/`GEORADIUSBYMEMBER_RO`) and streams
(`XADD` / `XRANGE` / `XREVRANGE` / `XLEN` / `XREAD` / `XGROUP` /
`XREADGROUP` / `XACK` / `XPENDING` / `XCLAIM`) with TTLs, approximate LRU eviction, and an append-only file for
durability, plus a small worker thread pool (`--threads`, default 1)
that runs background saves (`BGSAVE`) off the event loop, and a dedicated
flush thread that does the once-a-second AOF sync under `everysec`,
so a slow disk never stalls the loop. No dependencies
beyond libc. Real `redis-cli` and any RESP
client library can talk to it.

Version 0.70.0.

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
| `PFADD key element [element ...]` | Add elements to the HyperLogLog at `key`, creating it when missing; returns `1` when a register changed, `0` when every element hashed into a register that already held at least that value; a plain string key is an error (`WRONGTYPE Key is not a valid HyperLogLog string value.`), as is any non-string key; preserves the TTL |
| `PFCOUNT key [key ...]` | Estimate the cardinality of the union of the HyperLogLogs at `key` (or several keys, merged register-wise); a missing key counts as empty (`0` for no keys at all); a plain string or non-string key is an error, as for `PFADD` |
| `PFMERGE destkey sourcekey [sourcekey ...]` | Merge the HyperLogLogs at `sourcekey` into `destkey` (registers merged by maximum), creating it when missing and overwriting it when it exists (its TTL is cleared); a missing source counts as empty, so merging only missing sources creates an empty HyperLogLog; sources are never modified, and the destination may itself be a source; a plain string or non-string key at either end is an error, as for `PFADD`; a merge that reproduces the destination exactly changes nothing, so it is not logged, not replicated, and does not invalidate a `WATCH` |
| `GEOADD key [NX \| XX] [CH] longitude latitude member [longitude latitude member ...]` | Add members at coordinates, stored as a sorted set whose scores are 52-bit geohashes (longitude in [-180, 180], latitude in [-85.05112878, 85.05112878]); returns the number of newly added members (moving an existing member answers `0`); `NX` only adds new members, `XX` only updates existing ones, `CH` counts changed members (added plus updated) instead of only new ones, and `NX` with `XX` is an error; every coordinate is parsed and range-checked before any member is added, so a bad triple adds nothing; a re-add at the same coordinates is a no-op, so it is not logged, not replicated, and does not invalidate a `WATCH`; preserves the TTL |
| `GEODIST key member1 member2 [m \| km \| ft \| mi]` | Haversine distance between two members in metres by default (or the named unit), as a bulk string with four decimals (`166274.1516` for Palermo/Catania, as in real Redis); the members' geohash scores are decoded back to the centres of their geohash cells, as in Redis; `nil` when the key or a member is missing; a member of a plain sorted set decodes whatever score it holds, as in Redis |
| `GEOPOS key member [member ...]` | The position of each member as a `[longitude, latitude]` array, each coordinate a bulk string with `%.17g` precision (the centres of their geohash cells, as in Redis); a missing member — or one whose score does not decode as a geohash (NaN, negative, 2^64 or more, or exactly 0) — answers a null array, and a missing key answers one null array per member; a member of a plain sorted set decodes whatever score it holds, as in Redis |
| `GEOHASH key member [member ...]` | The 11-character base32 geohash string of each member's position (`sqc8b49rny0` for Palermo, `sqdtr74hyu0` for Catania, as in real Redis); the score is decoded to the centre of its geohash cell and re-encoded over the standard ranges (longitude [-180, 180], latitude [-90, 90]), because the stored hash uses latitude [-85.05112878, 85.05112878] instead, and the last character is always `0` (52 bits cannot fill the 55 an 11-character string holds); `nil` for a missing member or one whose score does not decode, and a missing key answers one `nil` per member |
| `GEOSEARCH key <FROMMEMBER member \| FROMLONLAT longitude latitude> <BYRADIUS radius <m \| km \| ft \| mi> \| BYBOX width height <m \| km \| ft \| mi>> [ASC \| DESC] [COUNT count [ANY]] [WITHCOORD] [WITHDIST] [WITHHASH]` | The members inside the circle or the box around the centre (the box is `width` east-west by `height` north-south, each converted to degrees at the centre's latitude); distances are haversine over the decoded cell centres, as in Redis, and the search is a full scan of the set — Redis prunes by geohash cell, cinder does not, so it costs O(N) in the size of the set. `ASC` / `DESC` sort by distance (nearest-first is also the default when neither is given; Redis leaves the order unspecified without them), `COUNT` limits the hits (without `ANY` it sorts first, with `ANY` it takes the first N in (score, member) order), `WITHDIST` adds the distance in the shape's unit as a `%.4f` bulk string, `WITHHASH` adds the 52-bit geohash score as an integer, `WITHCOORD` adds the `[longitude, latitude]` cell centre, in that order after the member; a missing key answers an empty array, a missing `FROMMEMBER` member is an error (`could not decode requested zset member`), and members whose scores do not decode as geohashes are skipped |
| `GEOSEARCHSTORE destination source <FROMMEMBER member \| FROMLONLAT longitude latitude> <BYRADIUS radius <m \| km \| ft \| mi> \| BYBOX width height <m \| km \| ft \| mi>> [ASC \| DESC] [COUNT count [ANY]] [STOREDIST]` | The same search as `GEOSEARCH`, stored at the destination as a sorted set instead of returned: the hits keep their 52-bit geohash scores, or with `STOREDIST` their distances in the shape's unit, and the command answers the number of members stored. The destination is overwritten whatever it held and its TTL is cleared, an empty result deletes it, and the destination may be the source key itself; the `WITH*` options are not accepted here, as in Redis |
| `GEORADIUS key longitude latitude radius <m \| km \| ft \| mi> [WITHCOORD] [WITHDIST] [WITHHASH] [COUNT count [ANY]] [ASC \| DESC] [STORE key \| STOREDIST key]`, `GEORADIUSBYMEMBER key member radius <m \| km \| ft \| mi> [same options]` | The legacy radius search, as in Redis: the same search as `GEOSEARCH` with a positional centre (`GEORADIUSBYMEMBER` takes the centre from a member), plus `STORE` (stores the hits at the destination with their geohash scores) and `STOREDIST` (stores them with their distances in the radius unit as scores) — either overwrites the destination whatever it held and clears its TTL, an empty result deletes it, and the destination may be the source key itself; `STORE` / `STOREDIST` answer the number of members stored and cannot be combined with the `WITH*` options |
| `GEORADIUS_RO key longitude latitude radius <m \| km \| ft \| mi> [WITHCOORD] [WITHDIST] [WITHHASH] [COUNT count [ANY]] [ASC \| DESC]`, `GEORADIUSBYMEMBER_RO key member radius <m \| km \| ft \| mi> [same options]` | The read-only variants of the legacy radius search, as in Redis: exactly the same search as `GEORADIUS` / `GEORADIUSBYMEMBER` — same positional centre, same `WITH*` options, `COUNT [ANY]`, and `ASC` / `DESC` — but `STORE` and `STOREDIST` are refused with an error, so the commands never write, never log to the AOF, and never replicate, and are safe to run on a replica |
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
| `HEXPIRE key seconds [NX \| XX \| GT \| LT] FIELDS numfields field [field ...]`, `HPEXPIRE key ms [NX \| XX \| GT \| LT] FIELDS numfields field [field ...]` | Set a per-field expiry on one or more hash fields (seconds / milliseconds from now), with the `NX` / `XX` / `GT` / `LT` conditions (a field with no expiry counts as an infinite TTL for `GT` / `LT`); one integer reply per field: `1` the expiry was set, `0` the condition was not met, `-2` the key or the field does not exist, `2` the field was deleted at once (a zero TTL); a negative TTL is an error; overwriting a field with `HSET` clears its expiry, while `HINCRBY` / `HINCRBYFLOAT` keep it |
| `HEXPIREAT key seconds [NX \| XX \| GT \| LT] FIELDS numfields field [field ...]`, `HPEXPIREAT key ms [NX \| XX \| GT \| LT] FIELDS numfields field [field ...]` | The same per-field expiry, as an absolute Unix time (seconds / milliseconds); an absolute time already past deletes the field at once and answers `2` per field, with the same per-field replies and conditions as `HEXPIRE` |
| `HTTL key FIELDS numfields field [field ...]`, `HPTTL key FIELDS numfields field [field ...]` | Remaining TTL of one or more hash fields, in seconds / milliseconds; one integer reply per field: `-1` the field has no expiry, `-2` the key or the field does not exist |
| `HPERSIST key FIELDS numfields field [field ...]` | Remove the expiry from one or more hash fields; one integer reply per field: `1` an expiry was removed, `-1` the field had none, `-2` the key or the field does not exist |
| `XADD key [MAXLEN [~] threshold] id field value [field value ...]` | Append one entry to a stream, creating it when missing; the ID is `*` (auto: the current ms, sequence 0, or one past the top ID when the ms is not ahead of it), `ms-seq` (explicit), or `ms-*` / `ms-` (partial: the ms given, the sequence auto-filled); answers the new ID as a bulk string. An ID equal to or smaller than the top ID is an error, as is `0-0` and a malformed ID; `MAXLEN` trims the oldest entries until at most `threshold` remain (`~` trims exactly the same way), and trimming the last entry deletes the key. Preserves the TTL, invalidates a `WATCH`, and is AOF-logged with the resolved ID (so a replay reproduces the exact entries) and replicated; a replica re-resolves `*` itself, so the entry order and contents match there but the ms part of an auto ID can differ. The entries are read back with `XRANGE` / `XREVRANGE` / `XLEN` / `XREAD` below |
| `XRANGE key start end [COUNT count]` | The entries whose IDs fall in `[start, end]` inclusive, in ID order, each as `[id, [field, value, ...]]`; bounds are `-` (the minimum ID), `+` (the maximum ID), `ms` (the whole millisecond: `ms-0` as a start bound, `ms-MAX` as an end bound) or `ms-seq`; `COUNT` limits the entries returned (`0` returns none, a negative count is an error). An empty array when the key is missing or the range is empty, including a start above the end. Read-only, so no AOF, RDB, or replication change was needed, and it is safe to run on a replica |
| `XREVRANGE key end start [COUNT count]` | The entries whose IDs fall in `[start, end]` inclusive, in reverse ID order (highest first), each as `[id, [field, value, ...]]`; the bounds are given end-first, as in Redis, and parsed the same way as `XRANGE`'s (`-` / `+` / `ms` / `ms-seq`, a bare ms covering the whole millisecond); `COUNT` takes the highest IDs first (`0` returns none, a negative count is an error). An empty array when the key is missing or the range is empty, including a start above the end. Read-only, so no AOF, RDB, or replication change was needed, and it is safe to run on a replica |
| `XLEN key` | Number of entries in a stream (`0` when missing); read-only |
| `XREAD [COUNT count] [BLOCK milliseconds] STREAMS key [key ...] id [id ...]` | The entries after each ID, one `[key, [[id, [field, value, ...]], ...]]` element per stream that has any (streams with nothing new are left out); IDs are `ms-seq`, a bare `ms` (the whole millisecond: entries after `ms-0`), or `$` (the stream's top ID at read time, resolved once, so only entries added after the read qualify); `>` is an error (it needs `XREADGROUP`, below). `COUNT` limits the entries per stream; `BLOCK` waits for new entries instead of answering at once — an empty array when there is nothing and no `BLOCK`, a nil array when a `BLOCK` times out, `BLOCK 0` waits forever. A blocked client parks with no lock held (the loop re-checks it every iteration, so it wakes within one iteration of the `XADD` that releases it, and a disconnect while parked drops the request) and sends nothing more until its read is answered, so its `EPOLLIN` interest is dropped while it waits. `BLOCK` is refused inside `MULTI`/`EXEC`; a plain `XREAD` queues and runs at `EXEC`. Read-only, so no AOF, RDB, or replication change was needed, and it is safe to run on a replica |
| `XGROUP CREATE key group id [MKSTREAM]` | Create a consumer group on a stream, starting at `id`: `$` (the top ID at creation, so only entries added later are delivered), `0` (from the beginning) or an explicit `ms-seq`; `MKSTREAM` creates the stream empty when it is missing. An existing group name is a `BUSYGROUP` error, a missing stream without `MKSTREAM` is an error, and only `CREATE` exists so far. A real write, so it is AOF-logged and replicated, invalidates a `WATCH`, and is how a group survives a restart (groups are not in RDB snapshots yet) |
| `XREADGROUP GROUP group consumer [COUNT count] [BLOCK milliseconds] STREAMS key [key ...] id [id ...]` | Read a stream as a consumer of a group, one `[key, [[id, [field, value, ...]], ...]]` element per stream that has any, as in `XREAD`. `>` delivers the entries after the group's cursor, records them in the group's pending list (PEL) with a delivery count and time, and advances the cursor; an explicit ID returns the group's pending entries for this consumer after that ID, without changing delivery state. `$` is not valid here. `COUNT` limits the entries per stream; `BLOCK` waits for new entries instead of answering at once — an empty array when there is nothing and no `BLOCK`, a nil array when a `BLOCK` times out, `BLOCK 0` waits forever (a blocked client parks with no lock held, as with `XREAD`'s `BLOCK`, and a disconnect while parked drops the request; `BLOCK` is refused inside `MULTI`/`EXEC`). A `>` read that delivers entries is AOF-logged and replicated like a write (an AOF replay re-delivers, so delivery counts go up by one per replay, and a replayed `BLOCK` read answers at once); a read that delivers nothing, or a pending-only read, is not logged. A missing key or group is a `NOGROUP` error |
| `XCLAIM key group consumer min-idle-time id [id ...] [IDLE ms] [TIME ms-unix-time] [RETRYCOUNT count] [FORCE] [JUSTID]` | Take over pending entries of a group for another consumer, usually because the consumer that held them died: an entry is claimed when it has been idle at least `min-idle-time` since its last delivery, or when `FORCE` is given (which also claims entries that are in the stream but not pending at all, creating a PEL entry for them). A claimed entry is moved to this consumer, its delivery time is set to now (or to `IDLE` ms ago, or to the absolute `TIME` given — `IDLE` and `TIME` cannot be combined) and its delivery count is bumped (or set by `RETRYCOUNT`); `JUSTID` answers only the IDs. A repeated ID in one call is claimed once. A real claim is AOF-logged and replicated like a write (an AOF replay re-claims, so delivery counts go up by one per replay). IDs that are not in the stream (trimmed, or never added), are not idle enough, or are not pending (without `FORCE`) are left out of the reply, where Redis answers nil for them. A missing key or group is a `NOGROUP` error |
| `XACK key group id [id ...]` | Drop entries from a group's pending list, answering how many were dropped (`0` for an ID that was not pending). A missing key or group is a `NOGROUP` error. A real change, so it is AOF-logged and replicated when it drops anything |
| `XPENDING key group [start end count [consumer]]` | Inspect a group's pending list: with no range, the summary — how many entries are pending, the lowest and highest pending IDs (`nil` when the list is empty), and a per-consumer count of pending entries; with `start end count` (and optionally one `consumer`), the pending entries in that ID range, each as `[id, consumer, idle-ms, deliveries]`, where `idle-ms` is the milliseconds since the entry was last delivered and `deliveries` is how many times it has been delivered. Bounds are `-` / `+` / `ms` / `ms-seq`, as in `XRANGE`, and `count` must be a positive integer. A missing key or group is a `NOGROUP` error. Read-only, so no AOF, RDB, or replication change was needed, and it is safe to run on a replica |
| `ZADD key [NX \| XX] [GT \| LT] [CH] [INCR] score member [score member ...]` | Add members with scores; returns number of newly added members (updating a member's score returns `0` for that member; re-adding a member with its current score changes nothing). `NX` only adds new members, `XX` only updates existing ones, `GT`/`LT` only update when the new score is greater/smaller than the current one (new members are still added), `CH` counts changed members instead of only new ones, `INCR` acts like `ZINCRBY` for a single member and returns the new score (nil when a condition blocks the update). `NX` conflicts with `XX`, `GT` and `LT`; `GT` conflicts with `LT`; `INCR` takes exactly one pair; an increment that yields NaN is an error |
| `ZSCORE key member` | Member's score (`nil` when missing) |
| `ZMSCORE key member [member ...]` | Scores of several members at once, in order (`nil` for a missing member, an array of `nil`s when the key is missing) |
| `ZDIFF numkeys key [key ...] [WITHSCORES]` | Members of the first sorted set that are in none of the other sets, lowest score first; `WITHSCORES` interleaves each score after its member; a missing key counts as an empty set |
| `ZINTER numkeys key [key ...] [WEIGHTS w ...] [AGGREGATE SUM\|MIN\|MAX] [WITHSCORES]` | Members present in every input sorted set, lowest score first; each score is weighted per key and then aggregated (`SUM` by default, or `MIN`/`MAX`); `WITHSCORES` interleaves each score after its member; a missing key counts as an empty set |
| `ZUNIONSTORE destination numkeys key [key ...] [WEIGHTS w ...] [AGGREGATE SUM\|MIN\|MAX]` | Store the union of several sorted sets at `destination` and return the number of members stored; each score is weighted per key and then aggregated (`SUM` by default, or `MIN`/`MAX`); the destination is overwritten whatever it held and its TTL is cleared, it may be one of the inputs, and an empty result deletes it; a missing key counts as an empty set |
| `ZINTERSTORE destination numkeys key [key ...] [WEIGHTS w ...] [AGGREGATE SUM\|MIN\|MAX]` | Store the intersection of several sorted sets at `destination` and return the number of members stored; only members present in every input set are stored, each score weighted per key and then aggregated (`SUM` by default, or `MIN`/`MAX`); the destination is overwritten whatever it held and its TTL is cleared, it may be one of the inputs, and an empty result deletes it; a missing key counts as an empty set |
| `ZDIFFSTORE destination numkeys key [key ...]` | Store the difference of several sorted sets at `destination` and return the number of members stored; only members of the first set that are in none of the others are stored, with the scores from the first set (no `WEIGHTS` / `AGGREGATE` / `WITHSCORES` options); the destination is overwritten whatever it held and its TTL is cleared, it may be one of the inputs, and an empty result deletes it; a missing key counts as an empty set |
| `ZCARD key` | Number of members (`0` when missing) |
| `ZRANGE key start stop [WITHSCORES]` | Members in index range, lowest score first; negative indices count from the end, `WITHSCORES` interleaves each score after its member |
| `ZREVRANGE key start stop [WITHSCORES]` | Same range, highest score first |
| `ZRANGEBYSCORE key min max [WITHSCORES] [LIMIT offset count]` | Members whose scores fall in `[min, max]`; bounds may be `-inf`/`+inf` or exclusive with a leading `(` (e.g. `(5`); `LIMIT` skips `offset` matches and returns at most `count` (negative `count` means all); `WITHSCORES` interleaves each score after its member |
| `ZREVRANGEBYSCORE key max min [WITHSCORES] [LIMIT offset count]` | Same range, highest score first; bounds are `max` first, as in Redis, with the same exclusive/inf/`WITHSCORES`/`LIMIT` handling |
| `ZRANGEBYLEX key min max [LIMIT offset count] [REV]` | Members whose names fall in `[min, max]` lexicographically, regardless of score; bounds are `-` (-inf) / `+` (+inf) or inclusive `[x` / exclusive `(x`; `REV` returns them in reverse member order with the bounds given max-first, as in Redis; `LIMIT` skips `offset` matches and returns at most `count` (negative `count` means all) |
| `ZLEXCOUNT key min max` | Number of members whose names fall in `[min, max]` lexicographically, with the same bounds as `ZRANGEBYLEX` but no `REV` — the count does not depend on order; `0` when missing or the range is empty, including a finite min above the max |
| `ZREM key member [member ...]` | Remove members; returns number removed (the key is deleted when the last member is removed) |
| `ZREMRANGEBYSCORE key min max` | Remove the members whose scores fall in `[min, max]`, with the same bounds as `ZRANGEBYSCORE` (`-inf`/`+inf`, or a leading `(` for an exclusive endpoint); returns the number removed, `0` when missing or the range is empty, including a finite min above the max, and the key is deleted when the last member is removed |
| `ZREMRANGEBYRANK key start stop` | Remove the members ranked `[start, stop]` (0-based, negative indices count from the end, as in `ZRANGE`, out-of-range ends clamped); returns the number removed, `0` when missing or the range is empty, and the key is deleted when the last member is removed |
| `ZREMRANGEBYLEX key min max` | Remove the members whose names fall in `[min, max]` lexicographically, with the same bounds as `ZRANGEBYLEX` (`-`/`+` or `[x`/`(x`); returns the number removed, `0` when missing or the range is empty, including a finite min above the max, and the key is deleted when the last member is removed |
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
             plus a field-to-index hash table for O(1) lookup and a
             per-field expiry array for hash field TTLs)
             and sorted sets (skip list ordered by score then
             member, plus a member-to-node hash table for O(1)
             lookup), and HyperLogLogs (a string value with a
             4-byte magic header and 2^14 one-byte registers,
             as in Redis — see `store_pfadd`), and streams
             (append-only entry arrays in ID order, each entry an
             ID plus field/value pairs, with the top ID cached so
             the next auto ID needs no scan)
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
             types, TTLs as absolute expiries, hash field TTLs as one
             absolute expiry per field, streams as one record per
             entry with its explicit ID) to `dump.rdb` in a compact
             binary format, atomically via `dump.rdb.tmp` + rename; the
             snapshot is loaded on startup before the AOF is replayed;
             format version 3, which version 1 and 2 snapshots still
             load from (a version 1 snapshot's hash fields just come
             back without a TTL);
             `rdb_serialize` renders the same bytes to memory so a
             worker thread can write them for `BGSAVE`
  threadpool.c FIFO job queue with N worker threads (`--threads`); jobs
             touch no shared state unless they take a lock
```

- **Store**: chained hash table, doubling resize, lazy expiry on access plus
  a periodic scan of 20 random buckets per 100 event-loop iterations.
  The same laziness covers hash field TTLs: a field whose expiry
  passed is dropped on the next access to its key (and by the same
  periodic scan), and the key goes when its last field does.
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
  (strings, lists, sets, hashes, sorted sets and streams, with TTLs stored as absolute
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
  TTL'd strings, `RPUSH`/`SADD`/`HSET` for the other types, one `XADD`
  with an explicit ID per stream entry, `PEXPIRE` after
  the rebuild; keys whose TTL already passed are left out, and a hash
  rebuild carries only its live fields, each with an absolute
  `HPEXPIREAT` when it has a field TTL) — followed by
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
            # `HINCRBY` surviving an AOF restart), hash field TTL
            # checks (`HEXPIRE` / `HPEXPIRE` / `HEXPIREAT` /
            # `HPEXPIREAT` setting expiries and `HTTL` / `HPTTL`
            # reading them back, `-2` for a missing key or field
            # and `-1` for no expiry, wrong-type and wrong-argument
            # errors, the `NX` / `XX` / `GT` / `LT` conditions
            # including a field with no expiry counting as infinite
            # for `GT` / `LT`, a zero TTL and a past absolute time
            # deleting the field at once with reply `2`, lazy
            # expiry hiding expired fields from `HGET` / `HLEN` /
            # `HGETALL` / `HKEYS` / `HEXISTS` / `HTTL` and deleting
            # the key when its last field goes, `HPERSIST` removing
            # an expiry and `-1` when there was none, `HSET`
            # clearing a field's TTL even for an identical value
            # while `HINCRBY` / `HINCRBYFLOAT` keep it, a real
            # `HEXPIRE` invalidating a `WATCH` while a condition
            # not met does not, a field expiring after the `WATCH`
            # aborting the `EXEC`, `COMMAND` listing all seven,
            # an RDB snapshot carrying a field TTL and skipping an
            # expired field, and a replication full sync carrying
            # a field TTL while skipping an expired field, plus a
            # streamed `HEXPIRE` landing on the replica), sorted-set
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
            # wrong-argument errors, and `COMMAND` listing it), and
            # HyperLogLog checks (`PFADD` returning 1 for a new key,
            # 0 for re-adding the same elements and 1 for a new one,
            # small counts coming back exact, duplicates in one call
            # counting once, `PFCOUNT` of a missing key returning 0,
            # a plain string key and a list key both erroring, the
            # TTL surviving a `PFADD`, a 1000-element estimate within
            # 5% and a 1500-element union of two overlapping sets
            # within 5%, a no-op `PFADD` not invalidating a `WATCH`
            # while a changing one does, and `COMMAND` listing
            # both), and `PFMERGE` checks (merging two HyperLogLogs
            # into a new destination counts the union, sources are
            # untouched, a missing source counts as empty and
            # merging only missing sources creates an empty
            # destination, a plain string or list key errors at
            # either end, merging a destination into itself is a
            # no-op that does not invalidate a `WATCH`, overwriting
            # a destination clears its TTL, and `COMMAND` lists
            # it; the HyperLogLog that survives the AOF restart is
            # itself the product of a `PFMERGE`), and GEO checks
            # (`GEOADD` adding two members and answering the count,
            # `GEODIST` in metres, kilometres, feet and miles
            # matching real Redis's Palermo/Catania example
            # (166274.1516 m), the same member answering 0.0000,
            # `nil` for a missing member or key, a bad unit and the
            # wrong argument count, the scores visible through
            # `ZSCORE` as the 52-bit geohashes, a re-add at the same
            # coordinates and a move both answering 0 with the
            # distance changing after the move and coming back exact
            # after moving back, out-of-range and non-numeric
            # coordinates and a bad triple adding nothing,
            # wrong-type errors on a list key, the TTL surviving a
            # `GEOADD`, a no-op `GEOADD` not invalidating a `WATCH`
            # while a move does, `COMMAND` listing both, and the
            # geo set that survives the AOF restart answering the
            # exact distance again), and `GEOADD` option checks
            # (`NX` adding only new members and skipping an
            # existing one without moving it, `XX` updating only
            # existing members and skipping a new one, `CH`
            # counting added plus updated members including a
            # mixed call and answering 0 for a same-coordinates
            # re-add, `NX` with `CH` and `XX` with `CH`, `NX`
            # with `XX` erroring in either order, options alone
            # or with an incomplete triple erroring, a duplicate
            # member in one call keeping the first coordinates,
            # lowercase options, a wrong-type error under `NX`,
            # and a no-op update under `XX` not invalidating a
            # `WATCH`), and `GEOPOS` / `GEOHASH` checks
            # (`GEOPOS` answering the decoded cell centres as
            # `[longitude, latitude]` bulk-string pairs, a null
            # array for a missing member and one null array per
            # member for a missing key; `GEOHASH` answering
            # `sqc8b49rny0` / `sqdtr74hyu0` for Palermo/Catania,
            # as in real Redis, with the last character always
            # `0`, and `nil` for a missing member or key; both
            # decoding whatever score a member of a plain sorted
            # set holds, while a member with a `0` or negative
            # score answers `nil`; wrong-type and wrong-argument
            # errors, and `COMMAND` listing both), and
            # `GEOSEARCH` / `GEOSEARCHSTORE` / `GEORADIUS` /
            # `GEORADIUSBYMEMBER` / `GEORADIUS_RO` /
            # `GEORADIUSBYMEMBER_RO`
            # checks (`GEOSEARCH` by radius and by box around
            # (15, 37), nearest-first by default and with `ASC` /
            # `DESC`, `COUNT` and `COUNT ... ANY` (the `ANY` form
            # takes the first hit in (score, member) order),
            # `WITHDIST` / `WITHHASH` / `WITHCOORD` in that order
            # after the member, `FROMMEMBER` starting from Palermo
            # itself at `0.0000`, a missing member and a missing
            # key, bad options and units, and wrong-type errors;
            # `GEOSEARCHSTORE` storing the hits with geohash scores
            # or distances (`STOREDIST`), by radius, by box, and
            # from a member, `COUNT` limiting to the nearest hit,
            # `WITHDIST` erroring, the destination's TTL cleared on
            # overwrite, an empty result deleting the destination,
            # the destination allowed to be the source key itself,
            # and wrong-type errors; `GEORADIUS` and
            # `GEORADIUSBYMEMBER` answering the
            # same search, and `GEORADIUS ... STORE` / `STOREDIST`
            # storing the hits with geohash scores / distances,
            # an empty result creating nothing, and `STORE` with a
            # `WITH*` option erroring; `GEORADIUS_RO` and
            # `GEORADIUSBYMEMBER_RO` answering the same search
            # with the `WITH*` options, `COUNT` / `COUNT ... ANY`,
            # and `ASC` / `DESC`, while `STORE` and `STOREDIST`
            # are refused and write nothing; `COMMAND` listing
            # all six), and
            # an AOF
            # restart test (data, a list, a set, a hash, a sorted set,
            # a HyperLogLog and a geo set survive a SHUTDOWN and reload),
            # and everysec-policy checks (INFO
            # reports the policy, the AOF holds the record before any
            # sync, data survives a SIGKILL + restart, unknown policy
            # is a startup error), and RDB snapshot checks (SAVE writes
            # dump.rdb with all six types and a HyperLogLog, a key whose TTL passed
            # before the save is left out, and a restart with the AOF
            # deleted restores every key from the snapshot alone, with
            # a surviving TTL preserved), and replication checks
            # (REPLICAOF streams a full sync of all six types and a
            # HyperLogLog with a surviving TTL, skips a key whose TTL passed first, then
            # streams live SET/INCR/DEL/LPUSH/PFADD writes; REPLICAOF NO ONE
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
            # `dump.rdb` with all six types, skips a key whose TTL passed,
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
            # all four), and stream checks (`XADD` with explicit,
            # partial and auto IDs, the equal-or-smaller, `0-0`
            # and malformed-ID errors, wrong-type and
            # wrong-argument errors, `MAXLEN` exact and `~`
            # forms including `MAXLEN 0` deleting the key, the
            # TTL surviving an `XADD`, a real `XADD` invalidating
            # a `WATCH`, `COMMAND` listing it, and an AOF restart,
            # an RDB snapshot and a replication full sync each
            # carrying a stream's entries and top ID, plus a live
            # streamed `XADD` landing on the replica), and `XRANGE` /
            # `XLEN` checks (the full range, a sub-range, bare-ms
            # start and end bounds, an empty and a reversed range,
            # `COUNT` and `COUNT 0`, a missing key reading as empty,
            # wrong-type, bad-bound, bad-count and bad-option errors,
            # and `COMMAND` listing both), and `XREVRANGE` checks
            # (the full range highest-first, a sub-range, bare-ms
            # bounds, an empty range and a start above the end,
            # `COUNT` taking the highest IDs and `COUNT 0`, a
            # missing key reading as empty, wrong-type, bad-bound,
            # bad-count and bad-option errors, and `COMMAND`
            # listing it), and `XREAD` checks
            # (multi-stream reads from `0`, after an ID, and by
            # bare-ms ID; `$` resolving once per call and seeing
            # only entries added after it; per-stream `COUNT`;
            # a missing key and a stream with nothing new left
            # out of the reply; wrong-type, unbalanced-list, `>`,
            # bad-ID, bad-`COUNT`, bad-`BLOCK`, duplicate-`COUNT`,
            # bad-option and no-`STREAMS` errors; `BLOCK` with
            # data already there answering at once, a `BLOCK`
            # timeout answering nil after ~300 ms, a blocked read
            # waking when another client adds an entry after its
            # `$` (`BLOCK 0` too) with the woken client still
            # usable, a disconnect while parked on `BLOCK 0`
            # leaving the server alive, `BLOCK` refused inside
            # `MULTI` while a plain `XREAD` queues and runs at
            # `EXEC`, and `COMMAND` listing it), and consumer-group
            # checks (`XGROUP CREATE` from `0` and `$`, `MKSTREAM`
            # creating the stream, the `BUSYGROUP` error, a missing
            # stream, wrong-type and bad-ID errors, an unknown
            # subcommand; `XREADGROUP` delivering with `>` and
            # re-reading pending entries with an explicit ID, per
            # consumer, `COUNT` limiting a delivery, multi-stream
            # reads, the `NOGROUP`, wrong-type, `$`, bad-ID,
            # `BLOCK`-not-supported, bad-`COUNT` and unbalanced-list
            # errors; `XACK` dropping one, several, and already-acked
            # entries, with the `NOGROUP` and bad-ID errors; and
            # `XPENDING` summaries for two groups and an empty group,
            # the extended form's ID, consumer, non-negative idle
            # time and delivery count, `COUNT` limiting, an empty
            # range, the one-consumer filter, and the `NOGROUP`,
            # wrong-type, bad-ID, bad-`COUNT` and wrong-argument
            # errors, with `COMMAND` listing all four)
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

- [x] Version fix (0.45.1): `CINDER_VERSION` still said 0.42.0
      while the README was at 0.45.0, so `INFO` reported the stale
      version and the smoke test pinned it. The define and the
      smoke-test expectation now both say 0.45.0; no behaviour
      change.

- [x] Large-command fix (0.45.2): the RESP parser capped the
      argument count at 1024, so a single `ZADD` of 1001 members
      (2004 arguments) was a protocol error and the server closed
      the connection with no reply — the queued `-ERR Protocol
      error` is never flushed before a dead client is removed.
      The cap is now `RESP_MAX_ARGC` (65536): the per-client
      input buffer is 64 KiB and every argument costs at least
      6 bytes, so a command that fits in the buffer can never
      carry more than ~10,900 arguments, and the ceiling bounds
      the argv allocation to 512 KiB. A 1001-member `ZADD` now
      answers `:1001` and the connection stays alive, covered by
      a smoke test on a throwaway client. `CINDER_VERSION` and
      the smoke-test expectation move to 0.45.2 with the README,
      so the three can no longer drift apart.

- [x] `ZREMRANGEBYRANK` (0.46.0): remove the members ranked
      `[start, stop]`, `ZREMRANGEBYRANK key start stop`, with
      0-based ranks and negative indices counting from the end, as
      in `ZRANGE`, and out-of-range ends clamped. The store gains
      `store_zremrangebyrank`, which finds the node at rank
      start + 1 with the skip list in O(log n) and walks the
      slice once at level 0, dropping each member through
      `zset_remove` (the next node is saved before each removal,
      since removing frees the node). A missing key answers `0`,
      as does an empty range including start above stop, and a
      non-sorted-set key is a `WRONGTYPE` error, listed by
      `COMMAND`. Removing the last member deletes the key, as with
      `ZREM`. A removal is logged to the AOF and replicated and
      invalidates `WATCH` on the key; removing nothing is a no-op,
      so it is none of those.

- [x] `ZREMRANGEBYLEX` (0.47.0): remove the members whose names
      fall in `[min, max]` lexicographically, `ZREMRANGEBYLEX key
      min max`, with the same bounds as `ZRANGEBYLEX` (`-` / `+` or
      `[x` / `(x`) but no `REV` — the removed set does not depend
      on order. The store gains `store_zremrangebylex`, which finds
      the matching slice with a level-0 scan (the skip list is
      ordered by (score, member), so a lex range cannot walk it in
      order) and drops each member through `zset_remove` (the next
      node is saved before each removal, since removing frees the
      node). A missing key answers `0`, as does an empty range
      including a finite min above the max, and a non-sorted-set
      key is a `WRONGTYPE` error, listed by `COMMAND`. Removing
      the last member deletes the key, as with `ZREM`. A removal
      is logged to the AOF and replicated and invalidates `WATCH`
      on the key; removing nothing is a no-op, so it is none of
      those.

- [x] `ZMSCORE` (0.48.0): return the scores of several members of a
      sorted set in one command, `ZMSCORE key member [member ...]`,
      in the order requested, `nil` for a member that is not in the
      set and an array of `nil`s when the key is missing, as in real
      Redis. It reuses `store_zscore`, so it needs no new store
      code; the handler pre-checks the key type (missing or sorted
      set, else `WRONGTYPE`) and writes one bulk score or `nil`
      per member, following the `HMGET` pattern. A missing key
      answers an array of `nil`s, not a `nil` array, because that
      is what real Redis returns. It is a read, so it is not
      logged to the AOF, not replicated, and does not invalidate
      `WATCH`, and it is listed by `COMMAND`.

- [x] `ZDIFF` (0.49.0): return the members of the first sorted
      set that are in none of the other sets, `ZDIFF numkeys key
      [key ...] [WITHSCORES]`, lowest score first, with the scores
      from the first set when `WITHSCORES` is given, as in real
      Redis. A missing key counts as an empty set and a
      non-sorted-set key is a `WRONGTYPE` error; the handler checks
      the type of every input key before reading any set. It
      reuses `store_zrange` for the first set and `store_zscore`
      for membership in the others, so it needs no new store code.
      It is a read, so it is not logged to the AOF, not
      replicated, and does not invalidate `WATCH`, and it is
      listed by `COMMAND`.

- [x] `ZUNION` (0.50.0): return the union of several sorted sets,
      `ZUNION numkeys key [key ...] [WITHSCORES]`, lowest score
      first, with a member that is in several sets appearing once
      with the sum of its scores, as in real Redis. A missing key
      counts as an empty set and a non-sorted-set key is a
      `WRONGTYPE` error; the handler checks the type of every
      input key before reading any set. It reuses `store_zrange`
      for each input set and sorts the merged members by score,
      member name breaking ties, so it needs no new store code.
      It is a read, so it is not logged to the AOF, not
      replicated, and does not invalidate `WATCH`, and it is
      listed by `COMMAND`.

- [x] `ZINTER` (0.51.0): return the intersection of several sorted
      sets, `ZINTER numkeys key [key ...] [WEIGHTS w ...]
      [AGGREGATE SUM|MIN|MAX] [WITHSCORES]`, lowest score first,
      with only the members present in every input set and each
      score weighted per key and then aggregated (`SUM` by
      default), as in real Redis. The options may appear in any
      order; a bad weight is a float error, a bad or repeated
      option is a syntax error. A missing key counts as an empty
      set and a non-sorted-set key is a `WRONGTYPE` error; the
      handler checks the type of every input key before reading
      any set. It reuses `store_zrange` for the first set and
      `store_zscore` for membership in the others, and sorts the
      result by score, member name breaking ties, so it needs no
      new store code. It is a read, so it is not logged to the
      AOF, not replicated, and does not invalidate `WATCH`, and
      it is listed by `COMMAND`.

- [x] `ZUNIONSTORE` (0.52.0): store the union of several sorted
      sets at a destination key, `ZUNIONSTORE destination numkeys
      key [key ...] [WEIGHTS w ...] [AGGREGATE SUM|MIN|MAX]`, and
      return the number of members stored, as in real Redis. Each
      score is weighted per key and then aggregated (`SUM` by
      default, or `MIN`/`MAX`), with the options in any order and
      the same float/syntax errors as `ZINTER`. The destination is
      overwritten whatever it held, its TTL is cleared, it may be
      one of the input keys (the union is computed before the
      destination is touched), and an empty result deletes it
      instead of leaving an empty set. A missing key counts as an
      empty set and a non-sorted-set input key is a `WRONGTYPE`
      error; the handler checks the type of every input key
      before reading any set. It reuses `store_zrange` for each
      input set and `store_zadd` for the write, so it needs no new
      store code. Only a real change is written: a `ZUNIONSTORE`
      that reproduces the destination exactly is a no-op, so it
      is not logged to the AOF, not replicated, and does not
      invalidate `WATCH`, like a no-op `ZADD`; a real write is
      all three, and it is listed by `COMMAND`.

- [x] `ZINTERSTORE` (0.53.0): store the intersection of several
      sorted sets at a destination key, `ZINTERSTORE destination
      numkeys key [key ...] [WEIGHTS w ...] [AGGREGATE SUM|MIN|MAX]`,
      and return the number of members stored, as in real Redis.
      Only the members present in every input set are stored, each
      score weighted per key and then aggregated (`SUM` by default,
      or `MIN`/`MAX`), with the options in any order and the same
      float/syntax errors as `ZINTER`. The destination is
      overwritten whatever it held, its TTL is cleared, it may be
      one of the input keys (the intersection is computed before
      the destination is touched), and an empty result deletes it
      instead of leaving an empty set. A missing key counts as an
      empty set and a non-sorted-set input key is a `WRONGTYPE`
      error; the handler checks the type of every input key
      before reading any set. It reuses `store_zrange` for the
      first set and `store_zscore` for membership in the others
      and `store_zadd` for the write, so it needs no new store
      code. Only a real change is written: a `ZINTERSTORE` that
      reproduces the destination exactly is a no-op, so it is
      not logged to the AOF, not replicated, and does not
      invalidate `WATCH`, like a no-op `ZADD`; a real write is
      all three, and it is listed by `COMMAND`.

- [x] `ZDIFFSTORE` (0.54.0): store the difference of several sorted
      sets at a destination key, `ZDIFFSTORE destination numkeys
      key [key ...]`, and return the number of members stored, as in
      real Redis. Only the members of the first set that are in none
      of the other sets are stored, with the scores from the first
      set; there are no `WEIGHTS` / `AGGREGATE` / `WITHSCORES`
      options, so any argument past the input keys is a syntax
      error. The destination is overwritten whatever it held, its
      TTL is cleared, it may be one of the input keys (the
      difference is computed before the destination is touched),
      and an empty result deletes it instead of leaving an empty
      set. A missing key counts as an empty set and a non-sorted-set
      input key is a `WRONGTYPE` error; the handler checks the type
      of every input key before reading any set. It reuses
      `store_zrange` for the first set and `store_zscore` for
      membership in the others and `store_zadd` for the write, so
      it needs no new store code, and the kept members stay in
      score order, so the result needs no resort. Only a real change
      is written: a `ZDIFFSTORE` that reproduces the destination
      exactly is a no-op, so it is not logged to the AOF, not
      replicated, and does not invalidate `WATCH`, like a no-op
      `ZADD`; a real write is all three, and it is listed by
      `COMMAND`. This completes the sorted-set store family
      (`ZUNIONSTORE` / `ZINTERSTORE` / `ZDIFFSTORE`).

- [x] HyperLogLog, first slice (0.55.0): `PFADD` / `PFCOUNT`,
      as in real Redis. A HyperLogLog is stored as a string value
      with a 4-byte magic header and 2^14 one-byte registers
      (MurmurHash64A, fixed seed, so an AOF replay rebuilds exactly
      the same registers), which is the layout Redis itself uses:
      `TYPE` would report "string", RDB snapshots carry it as a
      string, and the replication full sync rebuilds it
      byte-identically through the string path. `PFADD` returns 1
      when a register changed and 0 when it did not, preserves the
      TTL, and a no-op add is not logged, not replicated, and does
      not invalidate `WATCH`; `PFCOUNT` estimates the cardinality
      of one key or the union of several (registers merged by
      maximum), with linear counting for small ranges, and a
      missing key counts as empty. A plain string key is an error
      (`WRONGTYPE Key is not a valid HyperLogLog string value.`),
      as is any non-string key. Second slice (0.56.0): `PFMERGE`,
      which merges the registers of one or more sources into a
      destination by maximum, creating it when missing and
      overwriting it (TTL cleared) when it exists; a missing source
      counts as empty, sources are never modified, and a merge that
      reproduces the destination exactly is a no-op, like a no-op
      `PFADD`.

- [x] Geo, first slice (0.57.0): `GEOADD` / `GEODIST`, as in real
      Redis. A geo set is a sorted set whose scores are 52-bit
      geohashes (26 latitude bits and 26 longitude bits
      interleaved, latitude in the even bit positions, over
      longitude [-180, 180] and latitude [-85.05112878,
      85.05112878]), so `TYPE` reports "zset", `ZRANGE` / `ZSCORE`
      see the geohash scores, RDB snapshots carry them as `%.17g`
      strings (a 52-bit integer round-trips exactly), the
      replication full sync rebuilds them through `ZADD`, and an
      AOF replay re-runs `GEOADD` deterministically. `GEOADD`
      returns the number of newly added members (moving an existing
      member answers `0`); every coordinate is parsed and
      range-checked before any member is added, so a bad triple
      adds nothing, and a re-add at the same coordinates is a no-op
      that is not logged, not replicated, and does not invalidate a
      `WATCH`. `GEODIST` decodes the members' scores back to the
      centres of their geohash cells and measures the haversine
      distance (earth radius 6372797.560856 m, as in Redis),
      answering a bulk string with four decimals in metres by
      default or `km` / `ft` / `mi` — Palermo/Catania answers
      166274.1516 m, exactly as real Redis does — and `nil` for a
      missing key or member. Limitation: no `GEORADIUS` /
      `GEOSEARCH` yet, and `GEOADD` takes no `NX` / `XX` / `CH`
      options (those come with a later slice, as `ZADD`'s options
      did after its first slice).

- [x] Geo, second slice (0.58.0): `GEOPOS` / `GEOHASH`, as in real
      Redis. `GEOPOS` decodes each member's geohash score back to
      the centre of its geohash cell and answers a `[longitude,
      latitude]` array of bulk strings (`%.17g`, the same digits
      Redis's `%.17Lg` prints); a missing member — or one whose
      score does not decode as a geohash (NaN, negative, 2^64 or
      more, or exactly 0) — answers a null array, and a missing
      key answers one null array per member. `GEOHASH` decodes the
      score to the cell centre and re-encodes it over the standard
      ranges (longitude [-180, 180], latitude [-90, 90]), because
      the stored hash uses latitude [-85.05112878, 85.05112878]
      instead — Palermo answers `sqc8b49rny0` and Catania
      `sqdtr74hyu0`, exactly as real Redis does — and the eleventh
      character is always `0`, since 52 bits cannot fill the 55 an
      11-character string holds. Both are read-only (nothing
      logged, nothing replicated), both decode whatever score a
      member of a plain sorted set holds, and both are listed by
      `COMMAND`. Limitation: no `GEORADIUS` / `GEOSEARCH` yet,
      and `GEOADD` takes no `NX` / `XX` / `CH` options.

- [x] Geo, third slice (0.59.0): `GEOSEARCH` / `GEORADIUS` /
      `GEORADIUSBYMEMBER`, as in real Redis. `GEOSEARCH` takes
      its centre from a member (`FROMMEMBER`) or coordinates
      (`FROMLONLAT`) and its shape as a circle (`BYRADIUS`) or a
      box (`BYBOX`, width east-west by height north-south,
      converted to degrees at the centre's latitude), with `ASC` /
      `DESC` sorting by distance, `COUNT [ANY]` limiting the hits,
      and `WITHDIST` / `WITHHASH` / `WITHCOORD` adding the
      distance (in the shape's unit), the 52-bit geohash score,
      and the decoded cell centre after each member, in that
      order. `GEORADIUS` and `GEORADIUSBYMEMBER` are the legacy
      positional forms, with `STORE` (geohash scores) and
      `STOREDIST` (distances in the radius unit) writing the hits
      to a destination sorted set, overwriting it whatever it held
      and clearing its TTL, an empty result deleting it, and the
      destination allowed to be the source key itself. The search
      is a full scan of the set — Redis prunes by geohash cell,
      cinder tests every member's decoded position against the
      shape — so it costs O(N) in the size of the set; the
      results are the same, since the test is the same haversine
      distance (circle) or degree-box test (box). Results are
      nearest-first by default, where Redis leaves the order
      unspecified without `ASC` / `DESC`. A `STORE` / `STOREDIST`
      write is logged to the AOF and replicated like any other
      write, and replays deterministically. Limitation: no
      `GEOSEARCHSTORE` or `GEORADIUS_RO` / `GEORADIUSBYMEMBER_RO`
      yet, and `GEOADD` still takes no `NX` / `XX` / `CH`
      options.

- [x] Geo, fourth slice (0.60.0): `GEOSEARCHSTORE`, as in real
      Redis. It runs the same `GEOSEARCH` search — centre from a
      member (`FROMMEMBER`) or coordinates (`FROMLONLAT`), circle
      (`BYRADIUS`) or box (`BYBOX`), `ASC` / `DESC` sorting,
      `COUNT [ANY]` limiting — and writes the hits to a
      destination sorted set instead of returning them, answering
      the number of members stored. The hits keep their 52-bit
      geohash scores, or with `STOREDIST` their distances in the
      shape's unit; the destination is overwritten whatever it
      held and its TTL is cleared, an empty result deletes it, and
      the destination may be the source key itself. The `WITH*`
      options are a syntax error here, as in Redis. The write is
      logged to the AOF and replicated like any other write, and
      replays deterministically. Limitation: `GEOADD` still takes
      no `NX` / `XX` / `CH` options.

- [x] Geo, fifth slice (0.61.0): `GEORADIUS_RO` /
      `GEORADIUSBYMEMBER_RO`, as in real Redis. They are exactly
      the `GEORADIUS` / `GEORADIUSBYMEMBER` search — same
      positional centre, same `WITHDIST` / `WITHHASH` /
      `WITHCOORD`, `COUNT [ANY]`, and `ASC` / `DESC` — but
      `STORE` and `STOREDIST` are refused with an error, so the
      commands never write, never log to the AOF, and never
      replicate, and are safe to run on a replica. Both are
      listed by `COMMAND`.

- [x] Geo, sixth slice (0.62.0): `GEOADD` `NX` / `XX` / `CH`
      options, as in real Redis. `NX` only adds new members,
      `XX` only updates existing ones, `CH` counts changed
      members (added plus updated) instead of only newly added
      ones, and `NX` with `XX` is an error; the options come
      after the key and before the first longitude, and are
      case-insensitive. A pair the options skip changes nothing
      and is not logged, and a no-op update at the same
      coordinates still neither logs nor invalidates a `WATCH`,
      as before. A duplicate member in one call sees the
      coordinates the earlier triple just set, so `NX` keeps the
      first pair and skips the second. The options are part of
      the logged command, so AOF replay and replication
      reproduce the same result.

- [x] Hash field TTLs (0.63.0): `HEXPIRE` / `HPEXPIRE` /
      `HEXPIREAT` / `HPEXPIREAT` / `HTTL` / `HPTTL` / `HPERSIST` —
      per-field expiries inside a hash, Redis 7.4 style, with the
      same lazy-expiry and AOF/RDB/replication treatment as key
      TTLs, so a field whose TTL passed reads as missing and an
      emptied hash deletes its key. Each field carries its own
      absolute expiry (ms); `HEXPIRE` / `HPEXPIRE` take a relative
      TTL, `HEXPIREAT` / `HPEXPIREAT` an absolute Unix time, all
      four accept the `NX` / `XX` / `GT` / `LT` conditions (a field
      with no expiry counts as an infinite TTL for `GT` / `LT`),
      and each answers one integer per field: `-2` the key or the
      field does not exist, `0` the condition was not met, `1` the
      expiry was set, `2` the field was deleted at once (a zero
      relative TTL, or an absolute time already past). `HTTL` /
      `HPTTL` answer the remaining TTL per field (`-1` no expiry,
      `-2` missing), `HPERSIST` removes it (`1` removed, `-1` had
      none, `-2` missing). Fields expire lazily on access and in
      the periodic sweep, and either expiry invalidates a `WATCH`;
      a set that changes nothing (a condition not met, a field
      already holding exactly that expiry) neither logs nor
      invalidates one. Overwriting a field with `HSET` clears that
      field's expiry, as in Redis, while `HINCRBY` /
      `HINCRBYFLOAT` keep it. Expiries are AOF-logged verbatim (so
      a relative `HEXPIRE` / `HPEXPIRE` re-arms from replay time
      after a restart, exactly as `EXPIRE` does — `HEXPIREAT` /
      `HPEXPIREAT` replay exactly), carried in RDB snapshots
      (format version 2; version 1 snapshots still load, their
      fields just come back without a TTL), and streamed in the
      replication full sync as absolute `HPEXPIREAT`s. All seven
      are listed by `COMMAND`.

- [x] Streams, `XADD` only (0.64.0): append-only streams with
      `ms-seq` IDs — auto (`*`), explicit (`ms-seq`) and partial
      (`ms-*` / `ms-`) — plus `MAXLEN [~] threshold`, which trims
      the oldest entries (and deletes the key when the trim empties
      it). `XADD` preserves the key's TTL, invalidates a `WATCH`,
      and is AOF-logged with the resolved ID, carried in RDB
      snapshots (format version 3; versions 1 and 2 still load)
      and in the replication full sync as one explicit-ID `XADD`
      per entry, with live `XADD`s streamed after the sync. A
      replica re-resolves `*` from the raw bytes it receives, so
      the entry order and contents match but the ms part of an auto
      ID can differ there.
- [x] Stream reads, `XRANGE` / `XLEN` (0.65.0): `XRANGE key start end
      [COUNT count]` returns the entries in `[start, end]` as
      `[id, [field, value, ...]]` pairs, with `-` / `+` / `ms` /
      `ms-seq` bounds (a bare ms covers the whole millisecond) and an
      optional `COUNT`; `XLEN key` counts the entries. Both are
      read-only, so no AOF, RDB, or replication change was needed,
      and they are safe on a replica.
- [x] Multi-stream reads, `XREAD` (0.66.0): `XREAD [COUNT count]
      [BLOCK milliseconds] STREAMS key [key ...] id [id ...]`
      returns the entries after each ID as one `[key, entries]`
      element per stream that has any, with `$` resolved once to
      the top ID at read time and `COUNT` applied per stream.
      `BLOCK` parks the client with no lock held until an entry
      arrives or the timeout passes (nil on timeout, `BLOCK 0`
      waits forever), and is refused inside `MULTI`/`EXEC`.
      Read-only, so no AOF, RDB, or replication change was needed,
      and it is safe on a replica.
- [x] Reverse stream reads, `XREVRANGE` (0.67.0): `XREVRANGE key end
      start [COUNT count]` returns the entries in `[start, end]`
      highest-first, with the bounds given end-first and parsed
      the same way as `XRANGE`'s, and `COUNT` taking the highest
      IDs first. Read-only, so no AOF, RDB, or replication change
      was needed, and it is safe on a replica.
- [x] Consumer groups, `XGROUP` / `XREADGROUP` / `XACK` (0.68.0):
      `XGROUP CREATE key group id [MKSTREAM]` makes a group starting
      at `$` (only later entries), `0` (from the beginning) or an
      explicit ID, creating the stream empty with `MKSTREAM`;
      `XREADGROUP GROUP group consumer [COUNT count] STREAMS key ...
      id ...` delivers new entries with `>` (recorded in the
      group's pending list with delivery count and time, cursor
      advanced) or re-reads pending entries for the consumer with
      an explicit ID, across several streams; `XACK key group id
      ...` drops entries from the pending list. Groups live on the
      stream, so deleting the key drops them with it. A `>` read
      that delivers, a group creation and a real ack are AOF-logged
      and replicated; groups are not in RDB snapshots yet, and an
      AOF replay re-delivers, so delivery counts go up by one per
      replay. `XCLAIM` and `BLOCK` for `XREADGROUP` landed in
      0.70.0, below.
- [x] `XPENDING` for consumer groups (0.69.0): `XPENDING key group`
      answers the summary — how many entries are pending, the
      lowest and highest pending IDs, and a per-consumer count of
      pending entries — and `XPENDING key group start end count
      [consumer]` lists the pending entries in that ID range as
      `[id, consumer, idle-ms, deliveries]`, optionally for one
      consumer only. Read-only, so no AOF, RDB, or replication
      change was needed, and it is safe on a replica.
- [x] `XCLAIM` and `BLOCK` for `XREADGROUP` (0.70.0): `XCLAIM key
      group consumer min-idle-time id ... [IDLE ms] [TIME
      ms-unix-time] [RETRYCOUNT count] [FORCE] [JUSTID]` takes over
      pending entries for another consumer once they have been idle
      long enough (or with `FORCE`, even when they have not, and
      even entries that are not pending at all), moving them to
      this consumer with a fresh delivery time and a bumped (or
      `RETRYCOUNT`-set) delivery count, answering the entries or
      just their IDs with `JUSTID`; `XREADGROUP ... BLOCK ms`
      parks the client until new entries arrive or the timeout
      passes (nil on timeout, `BLOCK 0` waits forever), as
      `XREAD`'s `BLOCK` does, and is refused inside `MULTI`/`EXEC`.
      A real claim and a `>` read that delivers are AOF-logged and
      replicated; an AOF replay re-claims and re-delivers, so
      delivery counts go up by one per replay, and a replayed
      `BLOCK` read answers at once. One documented deviation:
      `XCLAIM` leaves IDs that are not in the stream out of the
      reply, where Redis answers nil for them. Consumer groups are
      now complete; the remaining known gap is that groups are not
      in RDB snapshots yet.
