#ifndef CINDER_STORE_H
#define CINDER_STORE_H

#include <stddef.h>
#include <stdint.h>

#define ENTRY_STRING 0
#define ENTRY_LIST 1
#define ENTRY_SET 2
#define ENTRY_HASH 3
#define ENTRY_ZSET 4
#define ENTRY_STREAM 5

/* A list value: circular buffer of malloc'd strings, so push/pop at
   either end is O(1) amortised. */
typedef struct List {
    char **items;
    size_t cap;
    size_t head; /* index of the first element */
    size_t len;
} List;

/* A set value: unordered array of malloc'd strings (insertion order
   apart from removals, which swap the last member into the hole),
   plus a hash table from member to its index in the array, so
   membership is O(1) instead of a linear scan. The dict is opaque
   here; its definition lives in store.c. SMEMBERS still walks the
   array, so its reply is unchanged; deleting a member re-indexes
   the member that moved into the hole, so SREM stays correct. */
typedef struct Set {
    char **members;
    size_t cap;
    size_t len;
    struct sdict *dict;
} Set;

/* A hash value: fields and values in insertion order (parallel
   arrays of malloc'd strings), plus a hash table from field to its
   index in those arrays, so field lookup is O(1) instead of a
   linear scan. The dict is opaque here; its definition lives in
   store.c. HGETALL / HKEYS / HVALS / HSCAN still walk the arrays
   in insertion order; deleting a field memmoves the tail of the
   arrays (preserving order) and re-indexes the fields that moved,
   so HDEL stays O(n). field_expire is a third parallel array, one
   ms-since-epoch expiry per field (-1 = no expiry), for the
   Redis 7.4 style per-field TTLs (HEXPIRE and friends). */
typedef struct Hash {
    char **fields;
    char **values;
    long long *field_expire;
    size_t cap;
    size_t len;
    struct hdict *dict;
} Hash;

/* A sorted-set value: a skip list ordered by (score, member) —
   score ascending, then member lexicographically, as in Redis —
   plus a hash table from member to skip-list node, so member
   lookup is O(1) and insert, delete, rank and range-by-index are
   O(log n) instead of the linear scans the old sorted array
   needed. The skip list and the dict are opaque here; their
   definitions live in store.c. `len` mirrors the skip list's
   length. */
typedef struct ZSet {
    struct zsl *sl;
    struct zdict *dict;
    size_t len;
} ZSet;

/* One entry of a stream: its ID (ms, seq) plus its field/value
   pairs, in the order they were given to XADD. */
typedef struct StreamEntry {
    uint64_t ms, seq;
    char **fields;
    char **values;
    size_t nfields;
    size_t cap;
} StreamEntry;

/* One pending entry in a consumer group's PEL: the entry ID, the
   consumer it was delivered to, when it was last delivered (ms
   since epoch) and how many times it has been delivered. The PEL
   is kept in delivery (ID) order. */
typedef struct StreamPending {
    uint64_t ms, seq;
    char *consumer; /* owned */
    long long delivery_time;
    long long delivery_count;
} StreamPending;

/* A consumer group on a stream: its name, the last ID delivered
   (the cursor `>` reads from), the consumers that have read from
   it, and its pending entries list (PEL). Consumers come into
   being when they first read, as in Redis. */
typedef struct StreamGroup {
    char *name; /* owned */
    uint64_t last_ms, last_seq;
    char **consumers; /* owned names */
    size_t nconsumers, cap_consumers;
    StreamPending *pel;
    size_t npel, cap_pel;
} StreamGroup;

/* A stream value: entries in insertion (ID) order, plus the top ID
   (last_ms, last_seq), so the next auto or partial ID is derived
   without scanning. New entries append at the end; MAXLEN trims
   from the front. Consumer groups live on the stream too, so
   deleting the key (or trimming it empty) drops them with it. */
typedef struct Stream {
    StreamEntry *entries;
    size_t len;
    size_t cap;
    uint64_t last_ms, last_seq;
    StreamGroup *groups;
    size_t ngroups, cap_groups;
} Stream;

typedef struct Entry {
    char *key;
    int type; /* ENTRY_STRING, ENTRY_LIST, ENTRY_SET or ENTRY_HASH */
    char *value; /* string values (type == ENTRY_STRING) */
    size_t value_len; /* true byte length of a string value, NUL bytes
                         included: string values are length-tracked,
                         so SETRANGE zero-padding survives GET, RDB
                         snapshots and replication. e->value[value_len]
                         is always '\0'; value_len == strlen(value)
                         whenever the value holds no NUL byte */
    List *list; /* list values (type == ENTRY_LIST) */
    Set *set; /* set values (type == ENTRY_SET) */
    Hash *hash; /* hash values (type == ENTRY_HASH) */
    ZSet *zset; /* sorted-set values (type == ENTRY_ZSET) */
    Stream *stream; /* stream values (type == ENTRY_STREAM) */
    long long expire_at; /* ms since epoch, -1 = no expiry */
    unsigned long long last_access; /* logical clock, for approximate LRU */
    unsigned long long version; /* bumped on every real change to this
                                   key (WATCH compares it); 0 is never
                                   a live version, it means "no key" */
    struct Entry *next;
} Entry;

typedef struct Store {
    Entry **buckets;
    size_t cap;
    size_t count;
    unsigned long long clock; /* increments on every access */
    unsigned long long mod_seq; /* increments on every key mutation;
                                   entry versions come from it */
    unsigned long long flush_seq; /* increments on every FLUSHALL, so
                                     a flush invalidates even a watch
                                     on a key that did not exist */
} Store;

long long now_ms(void);

Store *store_new(void);
void store_free(Store *s);

/* Insert or overwrite a key. Value is copied. expire_at is ms epoch, -1 = none.
   Overwriting a list key replaces it with the string. */
int store_set(Store *s, const char *key, const char *value, long long expire_at);
/* Length-aware store_set: value is vlen bytes, NUL bytes included.
   store_set is this with vlen = strlen(value). */
int store_set_len(Store *s, const char *key, const char *value, size_t vlen,
                  long long expire_at);
/* Returns a malloc'd copy of the value, or NULL if missing/expired/wrong type. */
char *store_get(Store *s, const char *key);
/* Length-aware store_get: the copy holds the full value_len bytes
   (NUL bytes included) plus a terminator at [value_len]; *len_out is
   the true length. Returns NULL when missing/expired/wrong type. */
char *store_get_len(Store *s, const char *key, size_t *len_out);
/* Substring of a string value, Redis GETRANGE semantics: negative
   indexes count from the end, out-of-range ends are clamped, and an
   empty range (or a missing key) returns an empty string, not nil.
   Returns 0 on success (*out is a malloc'd buffer of *n_out bytes,
   NUL-terminated at [*n_out]; the caller frees it), -1 = wrong type,
   -2 = out of memory. */
int store_getrange(Store *s, const char *key, long long start, long long stop,
                   char **out, size_t *n_out);
/* Overwrite a string value starting at offset, Redis SETRANGE
   semantics: creates the key when missing, zero-padding up to the
   offset, and extends the value with zero bytes when the write runs
   past the end. Preserves the key's TTL, as in Redis. An empty value
   changes nothing and creates nothing. Returns the new length,
   -1 = wrong type, -2 = out of memory, -3 = negative offset. */
long long store_setrange(Store *s, const char *key, long long offset,
                         const char *value, size_t vlen);
/* Set the bit at offset in a string value, Redis SETBIT semantics:
   bit 0 is the high bit of byte 0; creates the key zero-padded when
   missing (even when the bit being set is 0, the key comes into
   being), extends the value with zero bytes when the offset runs
   past the end, and preserves the key's TTL, as in Redis. Returns 0
   on success (*old_out is the bit's previous value, 0 or 1;
   *changed_out is 1 when the stored bytes actually changed, 0 when
   the bit already held the requested value), -1 = wrong type,
   -2 = out of memory, -3 = offset out of range (negative or
   >= 2^32, the largest offset in a 512 MB string). */
int store_setbit(Store *s, const char *key, long long offset, int value,
                 int *old_out, int *changed_out);
/* The bit at offset: 0 for a missing key or an offset past the end
   of the value. Returns 0 on success (*out is 0 or 1),
   -1 = wrong type, -3 = offset out of range. */
int store_getbit(Store *s, const char *key, long long offset, int *out);
/* Count set bits in a string value, Redis BITCOUNT semantics: when
   has_range is set, start/end are byte indexes (negative counts
   from the end, out-of-range ends are clamped); an empty range
   counts 0. Returns 0 on success (*out is the count),
   -1 = wrong type. */
int store_bitcount(Store *s, const char *key, long long start, long long end,
                   int has_range, long long *out);
/* Bitwise operations across string values. */
#define BITOP_AND 0
#define BITOP_OR 1
#define BITOP_XOR 2
#define BITOP_NOT 3
/* Bitwise operation across string values, Redis BITOP semantics:
   AND/OR/XOR take two or more source keys, NOT takes exactly one.
   A missing source key counts as an empty string, so the result is
   as long as the longest source. The result overwrites the
   destination key, whatever it held, and clears its TTL, as in
   Redis. When every source is missing the result is empty and the
   destination key is deleted, as in Redis. Returns 0 on success
   (*len_out is the result length in bytes; *changed_out is 1 when
   the stored state actually changed, 0 when the destination already
   held exactly the result, or the result was empty and there was no
   destination to delete), -1 = wrong type (the destination or a
   source holds a non-string), -2 = out of memory. */
int store_bitop(Store *s, int op, const char *dest, const char **keys, int nkeys,
                long long *len_out, int *changed_out);
/* BITFIELD: packed integer fields in a string value, Redis BITFIELD
   semantics. Encodings are i1..i64 (signed, two's complement) and
   u1..u63 (unsigned; u64 is not supported because RESP integers are
   signed 64-bit, as in Redis). Offsets are bit offsets, or #n for
   the n-th field of the encoding's width (bit offset n*bits).
   GET reads a field (0 for a missing key or bits past the end),
   SET writes one (wrapping, saturating, or failing on overflow per
   the overflow policy in effect) and returns the old value, INCRBY
   adds an increment and returns the new value. OVERFLOW WRAP is the
   default; SAT clamps to the type's range; FAIL returns nil for
   that operation and leaves the field unchanged. A SET or INCRBY
   creates the key zero-padded when missing and grows it with zero
   bytes when the field runs past the end, preserving the TTL, as
   in Redis; a GET-only call creates nothing. */
#define BITFIELD_GET 0
#define BITFIELD_SET 1
#define BITFIELD_INCRBY 2
#define BITFIELD_OVERFLOW_WRAP 0
#define BITFIELD_OVERFLOW_SAT 1
#define BITFIELD_OVERFLOW_FAIL 2
typedef struct BitfieldOp {
    int kind; /* BITFIELD_GET / SET / INCRBY */
    int is_signed; /* 1 = i, 0 = u */
    int bits; /* 1..64 signed, 1..63 unsigned */
    unsigned long long offset; /* bit offset; offset + bits <= 2^32 */
    long long value; /* SET value or INCRBY increment */
    int overflow; /* WRAP / SAT / FAIL in effect for this op */
} BitfieldOp;
/* One BITFIELD result: raw holds the field value as raw bits
   (interpret as signed per the op's encoding when is_signed);
   is_nil is 1 when an INCRBY or SET overflowed under OVERFLOW
   FAIL, in which case the field was left unchanged. */
typedef struct BitfieldResult {
    unsigned long long raw;
    int is_nil;
} BitfieldResult;
/* Execute the ops in order against the key. Returns 0 on success
   (results[i] holds one reply per op, *changed_out is 1 when the
   stored state actually changed — a key created, the value grown,
   or a byte different — 0 when every write was a no-op),
   -1 = wrong type, -2 = out of memory, -3 = a field runs past the
   end of the largest legal string (offset + bits > 2^32). */
/* The raw bits of a signed BITFIELD result as a signed value. */
long long store_bitfield_to_signed(unsigned long long raw, int bits);
int store_bitfield(Store *s, const char *key, const BitfieldOp *ops, int nops,
                   BitfieldResult *results, int *changed_out);
/* Increment a string key's integer value by delta, in one hash lookup.
   Creates the key when missing (or expired). The TTL is untouched, so
   INCR preserves it, as in Redis. Returns 0 on success (*out is the
   new value), -1 = wrong type, -2 = value is not an integer,
   -3 = out of memory. */
int store_incr(Store *s, const char *key, long long delta, long long *out);
int store_del(Store *s, const char *key);
/* Append value to a string key's value, creating the key when missing.
   Preserves the key's TTL, as in Redis. Returns the new length,
   -1 = wrong type, -2 = out of memory. */
long long store_append(Store *s, const char *key, const char *value);
int store_exists(Store *s, const char *key);
int store_expire(Store *s, const char *key, long long expire_at);
/* -2 = no such key, -1 = key exists with no expiry, otherwise ms remaining. */
long long store_ttl_ms(Store *s, const char *key);
size_t store_size(Store *s);
void store_flush(Store *s);
/* Version of a key as WATCH sees it: the entry's version, or 0 when
   the key is missing or expired. */
unsigned long long store_key_version(Store *s, const char *key);
/* How many FLUSHALLs have run; WATCH records it alongside the key
   version so a flush of a key that did not exist still invalidates
   the watch. */
unsigned long long store_flush_seq(Store *s);
/* Delete expired entries found in a sample of buckets. Returns count removed. */
size_t store_expire_scan(Store *s, size_t sample_buckets);
/* Evict approximately-least-recently-used entries until count <= max_entries. */
size_t store_evict_lru(Store *s, size_t max_entries);
/* Collect all non-expired keys matching a glob pattern ('*' and '?').
   Returns the count; *keys_out is a malloc'd array of malloc'd strings
   (NULL and 0 when nothing matches). Caller frees both. */
size_t store_keys_matching(Store *s, const char *pattern, char ***keys_out);

/* Cursor-based keyspace iteration, Redis SCAN semantics. The cursor
   is a bucket index walked in reverse-bit order (the trick Redis's
   dictScan uses), so a table resize mid-iteration still visits every
   bucket exactly once: keys present for the whole iteration are
   returned at least once, and the iteration ends when the returned
   cursor is 0. Any unsigned cursor is valid (it is masked to the
   table size); start with 0. Each call scans buckets until `count`
   matching non-expired keys have been collected (the default Redis
   COUNT is 10) or the cursor wraps to 0; one bucket's matches are
   all returned, so a batch can exceed `count` slightly. Expired
   keys are skipped, not deleted: SCAN is read-only. Returns the
   next cursor (0 = iteration complete); *keys_out is a malloc'd
   array of malloc'd strings (NULL and 0 when nothing matched).
   Caller frees both. */
unsigned long long store_scan(Store *s, unsigned long long cursor,
                              const char *pattern, size_t count,
                              char ***keys_out, size_t *n_out);

/* Iterate over all non-expired entries in bucket order. The callback
   receives each entry (read-only) and arg; returning non-zero stops
   the iteration. Expired entries are skipped, not deleted. Returns 0
   when every entry was visited, 1 if stopped early. */
typedef int (*store_foreach_fn)(const Entry *e, void *arg);
int store_foreach(Store *s, store_foreach_fn fn, void *arg);

/* 0 = no such key, 1 = string, 2 = list, 3 = set, 4 = hash, 5 = zset,
   6 = stream. */
int store_type(Store *s, const char *key);

/* Push one element. Returns the new length, -1 = key holds a string,
   -2 = out of memory. Creates the list when the key is missing. */
long long store_lpush(Store *s, const char *key, const char *elem);
long long store_rpush(Store *s, const char *key, const char *elem);
/* Pop one element from either end. Returns a malloc'd element, or NULL
   when the key is missing or the list is empty. *wrongtype is set to 1
   when the key holds a string. Popping the last element deletes the key. */
char *store_lpop(Store *s, const char *key, int *wrongtype);
char *store_rpop(Store *s, const char *key, int *wrongtype);
/* Length of the list: 0 when missing, -1 when the key holds a string. */
long long store_llen(Store *s, const char *key);
/* Elements from start to stop inclusive; negative indexes count from the
   end. Returns 0 on success (out may be empty), -1 = wrong type,
   -2 = out of memory. *out is a malloc'd array of malloc'd strings;
   the caller frees both. */
int store_lrange(Store *s, const char *key, long long start, long long stop,
                 char ***out, size_t *n_out);

/* The element at index, negative indexes count from the end, as in
   Redis. Returns 0 on success (*out is a malloc'd copy of the
   element, or NULL when the key is missing or the index is out of
   range), -1 = wrong type, -2 = out of memory. Read-only: it does
   not create the key. */
int store_lindex(Store *s, const char *key, long long index, char **out);
/* Set the element at index, negative indexes count from the end, as
   in Redis. Preserves the key's TTL. Returns 0 when the element
   changed, 1 when the element already held exactly that value (no
   change, so no WATCH invalidation and nothing to log), -1 = wrong
   type, -2 = out of memory, -3 = no such key, -4 = index out of
   range. */
int store_lset(Store *s, const char *key, long long index, const char *elem);
/* Insert elem before (before = 1) or after (before = 0) the first
   occurrence of pivot, as in Redis. Preserves the key's TTL.
   Returns the new length, 0 when the pivot is not in the list,
   -1 = no such key, -2 = wrong type, -3 = out of memory. */
long long store_linsert(Store *s, const char *key, int before,
                        const char *pivot, const char *elem);
/* Push one element onto an existing list only: a missing key is a
   no-op. Returns the new length, 0 when the key is missing,
   -1 = wrong type, -2 = out of memory. */
long long store_lpushx(Store *s, const char *key, const char *elem);
long long store_rpushx(Store *s, const char *key, const char *elem);
/* Keep only the elements from start to stop inclusive; negative
   indexes count from the end. An empty range deletes the key, as in
   Redis. Preserves the key's TTL. Returns 0 when the key existed
   (trimmed, or deleted by an empty range), 1 when the key is
   missing, -1 = wrong type, -2 = out of memory. */
int store_ltrim(Store *s, const char *key, long long start, long long stop);
/* Move one element from source to dest: from_head picks the head
   (LEFT) or the tail (RIGHT) of the source, to_head the head or
   tail of the destination. The source is deleted when emptied; the
   destination keeps its TTL, a new destination has none. Returns 1
   on success (*out is a malloc'd copy of the moved element), 0 when
   the source is missing or empty, -1 = wrong type on either key,
   -2 = out of memory. */
int store_lmove(Store *s, const char *source, const char *dest,
                int from_head, int to_head, char **out);

/* Add one member. Returns the number added (1 new, 0 already present),
   -1 = key holds a string or list, -2 = out of memory. Creates the set
   when the key is missing. */
long long store_sadd(Store *s, const char *key, const char *member);
/* Remove one member. Returns 1 if removed, 0 if absent or key missing,
   -1 = key holds a string or list. Removing the last member deletes
   the key, as in Redis. */
long long store_srem(Store *s, const char *key, const char *member);
/* 1 = member present, 0 = absent or key missing, -1 = wrong type. */
int store_sismember(Store *s, const char *key, const char *member);
/* Number of members: 0 when missing, -1 when the key holds a string
   or list. */
long long store_scard(Store *s, const char *key);
/* All members, in insertion order. Returns 0 on success (out may be
   empty), -1 = wrong type, -2 = out of memory. *out is a malloc'd
   array of malloc'd strings; the caller frees both. */
int store_smembers(Store *s, const char *key, char ***out, size_t *n_out);
/* Cursor-based iteration over a set's members, Redis SSCAN
   semantics: start with cursor 0, repeat with the returned cursor
   until it comes back 0. The cursor is an index into the set's
   members array, so a member added or removed between calls may be
   skipped or returned twice, as with SCAN. `pattern` is the same
   glob as KEYS/SCAN, matched against member names; `count` is a
   hint for how many matching members to return per call (0 returns
   none and cursor 0). Returns 0 on success (*n_out is the member
   count, 0 for a missing key, and *next_out is the next cursor),
   -1 = wrong type, -2 = out of memory. *out is a malloc'd array of
   malloc'd strings; the caller frees both. */
int store_sscan(Store *s, const char *key, unsigned long long cursor,
                const char *pattern, size_t count, char ***out,
                size_t *n_out, unsigned long long *next_out);

/* Set one field. Returns 1 when the field is new, 0 when it overwrote
   an existing field, -1 = key holds a string, list or set, -2 = out of
   memory. Creates the hash when the key is missing. */
long long store_hset(Store *s, const char *key, const char *field,
                     const char *value);
/* Returns a malloc'd copy of the field's value, or NULL when the key or
   the field is missing. Caller must check the type first: NULL is also
   returned for a wrong-type key. */
char *store_hget(Store *s, const char *key, const char *field);
/* Remove one field. Returns 1 if removed, 0 if absent or key missing,
   -1 = wrong type. Removing the last field deletes the key, as in
   Redis. */
long long store_hdel(Store *s, const char *key, const char *field);
/* 1 = field present, 0 = absent or key missing, -1 = wrong type. */
int store_hexists(Store *s, const char *key, const char *field);
/* Number of fields: 0 when missing, -1 = wrong type. */
long long store_hlen(Store *s, const char *key);
/* All fields and values as a flat array [field, value, field, value,
   ...] in insertion order. Returns 0 on success (out may be empty),
   -1 = wrong type, -2 = out of memory. *out is a malloc'd array of
   malloc'd strings; the caller frees both. */
int store_hgetall(Store *s, const char *key, char ***out, size_t *n_out);
/* Add delta to a hash field's integer value, creating the field (with
   value delta) and the hash when missing. Returns 1 when the field
   is new, 0 when an existing field changed, 2 when nothing changed
   (increment 0 on a field already there), -1 = wrong type,
   -2 = field value is not an integer, -3 = increment would overflow,
   -4 = out of memory. Preserves the key's TTL, as in Redis.
   *out is the field's new value. */
int store_hincrby(Store *s, const char *key, const char *field,
                  long long delta, long long *out);
/* Add delta to a hash field's float value, creating the field (with
   value delta) and the hash when missing. Returns 1 when the field
   is new, 0 when an existing field changed, 2 when nothing changed
   (increment 0 on a field already there), -1 = wrong type,
   -2 = field value is not a float, -3 = increment would overflow
   (the result is not a finite number), -4 = out of memory.
   Preserves the key's TTL, as in Redis. *out is the field's new
   value, stored formatted with %.17g. */
int store_hincrbyfloat(Store *s, const char *key, const char *field,
                       double delta, double *out);
/* Cursor-based iteration over a hash's fields, Redis HSCAN
   semantics: start with cursor 0, repeat with the returned cursor
   until it comes back 0. The cursor is an index into the hash's
   insertion-ordered fields, so a field added or removed between
   calls may be skipped or returned twice, as with SCAN. `pattern`
   is the same glob as KEYS/SCAN, matched against field names;
   `count` is a hint for how many matching pairs to return per call
   (0 returns none and cursor 0). Returns 0 on success (*n_out is
   the flat [field, value, ...] count, 0 for a missing key, and
   *next_out is the next cursor), -1 = wrong type, -2 = out of
   memory. *out is a malloc'd array of malloc'd strings; the caller
   frees both. */
int store_hscan(Store *s, const char *key, unsigned long long cursor,
                const char *pattern, size_t count, char ***out,
                size_t *n_out, unsigned long long *next_out);

/* Hash field TTLs (HEXPIRE / HPEXPIRE / HEXPIREAT / HPEXPIREAT,
   HTTL / HPTTL, HPERSIST), Redis 7.4 style: each field of a hash
   may carry its own expiry (ms since epoch, -1 = none). A field
   whose expiry passes is deleted lazily on the next access to its
   key and by the periodic expiry sweep; when the last field goes
   the key goes with it, as in Redis. A key-level TTL still bounds
   every field: when the key expires, all its fields go. Overwriting
   a field with HSET clears that field's expiry, as in Redis, while
   HINCRBY / HINCRBYFLOAT leave it alone. */
#define HFIELD_EXPIRE_NONE 0
#define HFIELD_EXPIRE_NX 1
#define HFIELD_EXPIRE_XX 2
#define HFIELD_EXPIRE_GT 3
#define HFIELD_EXPIRE_LT 4
/* Set one field's expiry to the absolute ms time expire_at, under
   the condition cond (NONE always sets; NX only when the field has
   no expiry; XX only when it has one; GT only when the new time is
   later than the current one, a field with no expiry counting as
   infinite; LT only when the new time is earlier, a field with no
   expiry counting as infinite). Returns 1 when the expiry was set,
   0 when the condition was not met, 2 when expire_at had already
   passed and the field was deleted instead, -1 = wrong type,
   -2 = the key or the field does not exist. Setting an expiry the
   field already has answers 1 and changes nothing, so it does not
   invalidate a WATCH; any other real change does. */
int store_hexpire_field(Store *s, const char *key, const char *field,
                        long long expire_at, int cond);
/* Remaining TTL of one field in ms: -2 = the key or the field does
   not exist, -1 = the field has no expiry, otherwise ms left.
   Returns -1 for a wrong-type key too; the caller checks the type
   first, as the TTL is only meaningful for a hash. */
long long store_hfield_ttl_ms(Store *s, const char *key, const char *field);
/* Remove one field's expiry. Returns 1 when an expiry was removed,
   -1 when the field had none, -2 = the key or the field does not
   exist. Returns -1 for a wrong-type key too; the caller checks the
   type first, as the reply code is only meaningful for a hash. */
int store_hfield_persist(Store *s, const char *key, const char *field);

/* Add or update one member's score. Returns 1 when the member is
   new, 0 when it overwrote an existing member's score, 2 when the
   member already had exactly that score (no change), -1 = key holds
   a string, list, set or hash, -2 = out of memory. Creates the
   sorted set when the key is missing. */
long long store_zadd(Store *s, const char *key, const char *member,
                     double score);
/* Score of a member: 1 = found (*out is the score), 0 = key or member
   missing, -1 = wrong type. */
int store_zscore(Store *s, const char *key, const char *member,
                 double *out);
/* Number of members: 0 when missing, -1 = wrong type. */
long long store_zcard(Store *s, const char *key);
/* Remove one member. Returns 1 removed, 0 when the key or member
   is missing, -1 = wrong type. The key is deleted when the last
   member is removed, as in Redis. */
long long store_zrem(Store *s, const char *key, const char *member);
/* Rank of a member: 1 = found (*out is the 0-based rank), 0 = key or
   member missing, -1 = wrong type. reverse ranks from the highest
   score down, as ZREVRANK would. */
int store_zrank(Store *s, const char *key, const char *member, int reverse,
                long long *out);
/* Members in [start, stop] (inclusive), ascending by score, or
   descending when reverse is set. Negative indices count from the
   end, as in Redis, and out-of-range ends are clamped. Returns 0 on
   success (*n_out is 0 for a missing key or an empty range),
   -1 = wrong type, -2 = out of memory. *members_out is a malloc'd
   array of malloc'd strings and *scores_out a malloc'd array of the
   matching scores; the caller frees both. */
int store_zrange(Store *s, const char *key, long long start, long long stop,
                 int reverse, char ***members_out, double **scores_out,
                 size_t *n_out);
/* Members whose scores fall in [min, max], ascending by score, or
   descending when reverse is set, with an endpoint excluded when
   its flag is set. `offset` matches are skipped and at most `count`
   are returned (`count < 0` means all). Returns 0 on success
   (*n_out is 0 for a missing key or an empty range, including
   min > max), -1 = wrong type, -2 = out of memory.
   *members_out is a malloc'd array of malloc'd strings and
   *scores_out a malloc'd array of the matching scores; the caller
   frees both. */
int store_zrangebyscore(Store *s, const char *key, double min, int min_excl,
                        double max, int max_excl, long long offset,
                        long long count, int reverse,
                        char ***members_out, double **scores_out,
                        size_t *n_out);
/* Members whose names fall in [min, max] lexicographically, in
   member order, or reverse member order when reverse is set. A NULL
   min means -inf, a NULL max means +inf; an endpoint is excluded
   when its flag is set. `offset` matches are skipped and at most
   `count` are returned (`count < 0` means all). The skip list is
   ordered by (score, member), so a lex range cannot walk it in
   order: the matching members are collected with a level-0 scan
   and sorted by member, which is fine at the scale this store
   targets. Returns 0 on success (*n_out is 0 for a missing key or
   an empty range, including min > max), -1 = wrong type,
   -2 = out of memory. *members_out is a malloc'd array of
   malloc'd strings; the caller frees both. */
int store_zrangebylex(Store *s, const char *key, const char *min, int min_excl,
                      const char *max, int max_excl, long long offset,
                      long long count, int reverse,
                      char ***members_out, size_t *n_out);
/* Count the members whose names fall in [min, max]
   lexicographically, with the same bound conventions as
   store_zrangebylex (NULL min = -inf, NULL max = +inf, an endpoint
   excluded when its flag is set). Returns the count (0 for a missing
   key or an empty range, including min > max), -1 = wrong type.
   Counts with a level-0 scan and never allocates, so it cannot run
   out of memory. */
long long store_zlexcount(Store *s, const char *key, const char *min,
                          int min_excl, const char *max, int max_excl);
/* Remove the members whose scores fall in [min, max], with an
   endpoint excluded when its flag is set, as in
   store_zrangebyscore. Returns the number removed (0 for a missing
   key or an empty range, including min > max), -1 = wrong type.
   The key is deleted when the last member is removed, as with
   store_zrem; a removal touches the key's version, a no-op does
   not. The skip list finds the first match in O(log n) and the
   matching slice is walked once at level 0. */
long long store_zremrangebyscore(Store *s, const char *key, double min,
                                 int min_excl, double max, int max_excl);
/* Remove the members ranked [start, stop] (inclusive, 0-based),
   with negative indices counting from the end, as in
   store_zrange, and out-of-range ends clamped. Returns the number
   removed (0 for a missing key or an empty range, including start
   above stop after normalisation), -1 = wrong type. The key is
   deleted when the last member is removed, as with store_zrem; a
   removal touches the key's version, a no-op does not. The node at
   rank start + 1 is found in O(log n) and the slice is walked once
   at level 0. */
long long store_zremrangebyrank(Store *s, const char *key, long long start,
                                long long stop);
/* Remove the members whose names fall in [min, max]
   lexicographically, with the same bound conventions as
   store_zrangebylex (NULL min = -inf, NULL max = +inf, an endpoint
   excluded when its flag is set). Returns the number removed (0 for
   a missing key or an empty range, including min > max), -1 = wrong
   type. The key is deleted when the last member is removed, as with
   store_zrem; a removal touches the key's version, a no-op does
   not. The skip list is ordered by (score, member), so a lex range
   cannot walk it in order: the matching slice is found with a
   level-0 scan and dropped through zset_remove, which keeps the
   dict and the skip list in step. */
long long store_zremrangebylex(Store *s, const char *key, const char *min,
                              int min_excl, const char *max, int max_excl);
/* Add increment to a member's score, creating the member (with score
   = increment) and the key when missing. Returns 1 when the member
   is new, 0 when an existing member's score changed, 2 when nothing
   changed (increment 0 on a member already there), -1 = wrong type,
   -2 = out of memory. *out is the member's new score. */
int store_zincrby(Store *s, const char *key, const char *member,
                  double incr, double *out);
/* Pop `count` members from the lowest-score end (from_max = 0) or the
   highest-score end (from_max = 1), in score order — the lowest (or
   highest) member first, as in Redis. Returns 0 on success (*n_out
   is 0 for a missing key; the key is deleted when the last member
   is popped), -1 = wrong type, -2 = out of memory. *members_out is
   a malloc'd array of malloc'd strings and *scores_out a malloc'd
   array of the popped scores; the caller frees both. */
int store_zpop(Store *s, const char *key, long long count, int from_max,
               char ***members_out, double **scores_out, size_t *n_out);
/* `count` random members: count > 0 returns distinct members (at most
   the whole set), count < 0 returns -count members sampled with
   replacement (duplicates possible), count == 0 returns none. The
   single-member form is this with count 1. Returns 0 on success
   (*n_out is 0 for a missing key), -1 = wrong type, -2 = out of
   memory. Same ownership as store_zpop. */
int store_zrandmember(Store *s, const char *key, long long count,
                      char ***members_out, double **scores_out,
                      size_t *n_out);
/* Cursor-based iteration over a sorted set's members, Redis ZSCAN
   semantics: start with cursor 0, repeat with the returned cursor
   until it comes back 0. The cursor is a skip-list rank (0-based),
   so members are visited in (score, member) order, and a member
   added or removed between calls may be skipped or returned twice,
   as with SCAN. `pattern` is the same glob as KEYS/SCAN, matched
   against member names; `count` is a hint for how many matching
   members to return per call (0 returns none and cursor 0).
   Returns 0 on success (*n_out is the member count, 0 for a
   missing key, and *next_out is the next cursor), -1 = wrong type,
   -2 = out of memory. *members_out is a malloc'd array of
   malloc'd strings and *scores_out a malloc'd array of the
   matching scores; the caller frees both. */
int store_zscan(Store *s, const char *key, unsigned long long cursor,
                const char *pattern, size_t count,
                char ***members_out, double **scores_out,
                size_t *n_out, unsigned long long *next_out);
/* Visit every member of a sorted set in (score, member) order — the
   order the skip list keeps. Returns 0 on success, -1 = wrong type
   (or no such key, which the caller can treat the same way: no
   members are visited). The callback receives each member and its
   score; returning non-zero stops the iteration and that value is
   returned. The members are owned by the set and must not be freed
   or kept past the call. Used by the snapshot writer and the
   replication emitter, which only need the set in order. */
typedef int (*zset_foreach_fn)(const char *member, double score, void *arg);
int zset_foreach(const ZSet *z, zset_foreach_fn fn, void *arg);

/* HyperLogLog (PFADD / PFCOUNT), stored as a string value: a 4-byte
   magic header followed by 2^14 one-byte registers — the same layout
   Redis uses, so TYPE reports "string", RDB snapshots carry it as a
   string, AOF replay re-runs PFADD deterministically, and the
   replication full sync rebuilds it byte-identically through the
   string path. A string key that is not a valid HyperLogLog value
   (wrong length or header) is an error, not a silent overwrite. */
#define HLL_P 14
#define HLL_REGISTERS 16384
#define HLL_MAGIC "HLL1"
#define HLL_LEN (4 + HLL_REGISTERS)
/* Add elements to the HyperLogLog at key, creating it when missing.
   Preserves the key's TTL, as in Redis. Returns 0 on success
   (*changed_out is 1 when a register changed, 0 when every element
   hashed into a register that already held at least that value),
   -1 = key holds a non-string, -2 = out of memory, -3 = key holds a
   string that is not a valid HyperLogLog value. A call that changes
   nothing touches neither the version (so WATCH survives it) nor
   the AOF. */
int store_pfadd(Store *s, const char *key, const char **elems, int nelems,
                int *changed_out);
/* Estimate the cardinality of the union of the HyperLogLogs at
   keys (element-wise maximum of the registers, then the standard
   HyperLogLog estimate with linear counting for small ranges).
   A missing key counts as empty. Returns 0 on success (*out is the
   estimate), -1 = a key holds a non-string, -2 = out of memory,
   -3 = a key holds a string that is not a valid HyperLogLog value. */
int store_pfcount(Store *s, const char **keys, int nkeys, long long *out);
/* Merge the HyperLogLogs at sources into dest (element-wise maximum
   of the registers), creating dest when missing and overwriting it
   when it exists — its TTL is cleared, as in Redis. A missing
   source counts as empty, so merging only missing sources creates
   an empty HyperLogLog at dest; sources are never modified, and
   dest may itself be one of the sources. Returns 0 on success
   (*changed_out is 1 when dest was created or its registers
   changed, 0 when the merge reproduces dest exactly), -1 = dest
   or a source holds a non-string, -2 = out of memory, -3 = dest or
   a source holds a string that is not a valid HyperLogLog value.
   A call that changes nothing touches neither the version (so
   WATCH survives it) nor the AOF. */
int store_pfmerge(Store *s, const char *dest, const char **sources,
                  int nsources, int *changed_out);

/* GEO (GEOADD / GEODIST), stored as a sorted set whose scores are
   52-bit geohashes — the layout Redis uses, so TYPE reports "zset",
   ZRANGE / ZSCORE see the geohash scores, RDB snapshots carry them
   as %.17g strings (a 52-bit integer round-trips exactly), the
   replication full sync rebuilds them through ZADD, and an AOF
   replay re-runs GEOADD deterministically. The hash interleaves 26
   latitude bits and 26 longitude bits (latitude in the even bit
   positions), over longitude [-180, 180] and latitude
   [-85.05112878, 85.05112878], as in Redis. */
#define GEO_LAT_MIN -85.05112878
#define GEO_LAT_MAX 85.05112878
#define GEO_LON_MIN -180.0
#define GEO_LON_MAX 180.0
/* Add one member at longitude/latitude. Returns 1 when the member
   is new, 0 when an existing member moved, 2 when the member
   already sat at exactly those coordinates (no change, so no
   WATCH invalidation and nothing to log), -1 = key holds a
   non-sorted-set, -2 = out of memory. Preserves the key's TTL,
   as in Redis. The coordinates are assumed in range; the handler
   checks them first. */
long long store_geoadd(Store *s, const char *key, double lon, double lat,
                       const char *member);
/* Distance in metres between two members, decoded from their
   geohash scores (the centres of their geohash cells, as in
   Redis) with the haversine formula over a 6372797.560856 m
   earth. Returns 1 when both members are present (*out is the
   distance), 0 when the key or a member is missing, -1 = wrong
   type. A member of a plain sorted set decodes whatever score
   it holds, as in Redis. */
int store_geodist(Store *s, const char *key, const char *m1, const char *m2,
                  double *out);
/* The position of one member, decoded from its geohash score (the
   centre of its geohash cell, as in Redis). Returns 1 when the
   member is present (lon_out and lat_out set), 0 when the key or the
   member is missing or the member's score does not decode as a
   geohash (NaN, negative, 2^64 or more, or exactly 0), -1 = wrong
   type. A member of a plain sorted set decodes whatever score it
   holds, as in Redis. */
int store_geopos(Store *s, const char *key, const char *member,
                 double *lon_out, double *lat_out);
/* The 11-character base32 geohash string of one member's position,
   as in Redis: the score is decoded to the centre of its geohash
   cell and re-encoded over the standard ranges (longitude
   [-180, 180], latitude [-90, 90]), because the stored hash uses
   latitude [-85.05112878, 85.05112878] instead; the last character
   is always '0', since 52 bits cannot fill the 55 an 11-character
   string holds. Returns 1 when the member is present (out holds
   the NUL-terminated string), 0 when the key or the member is
   missing or the member's score does not decode, -1 = wrong type. */
int store_geohash(Store *s, const char *key, const char *member,
                  char out[12]);

/* One GEOSEARCH / GEORADIUS hit: the member name (malloc'd), its
   distance from the centre in metres, its decoded position (the
   centre of its geohash cell, as in Redis), and its 52-bit geohash
   score as an integer. */
typedef struct {
    char *member;
    double dist_m;
    double lon, lat;
    uint64_t hash;
} GeoSearchHit;
/* Search the geo set at key for the members inside the shape
   centred at (clon, clat): a circle of radius_m metres when
   is_box is 0, or a box width_m east-west by height_m north-south
   (each converted to degrees at the centre's latitude) when
   is_box is 1. Members whose scores do not decode as geohashes
   have no position and are skipped, as in Redis. The hits are
   sorted by distance from the centre — nearest first, or farthest
   first when sort_desc — with ties broken by member name; when
   count_any is set the hits instead keep (score, member) order
   and are not sorted, as with COUNT ... ANY in Redis. count
   limits the hits returned (-1 for no limit). Returns 0 on
   success (*hits_out is a malloc'd array of *n_out hits, 0 for a
   missing key; the caller frees each member and the array),
   -1 = wrong type, -2 = out of memory. The search is a full scan
   of the set — Redis prunes by geohash cell, cinder does not —
   so it costs O(N) in the size of the set. */
int store_geosearch(Store *s, const char *key, double clon, double clat,
                    int is_box, double radius_m, double width_m,
                    double height_m, int sort_desc, long long count,
                    int count_any, GeoSearchHit **hits_out, size_t *n_out);

/* Streams: a stream is an append-only log of
   entries, each with an ID "ms-seq" (milliseconds since epoch, then
   a sequence number within that millisecond) and field/value pairs.
   The ID argument is "*" (auto: the current ms, sequence 0, or one
   past the top ID when the ms is not ahead of it), "ms-seq"
   (explicit), or "ms-*" / "ms-" (partial: the ms given, the sequence
   auto-filled as for "*"). Appending always changes the key, so it
   always bumps the version (a WATCH on the key is invalidated) and
   preserves the key's TTL, as in Redis. When maxlen >= 0, entries
   are trimmed from the front until at most maxlen remain (the
   MAXLEN option; "~" is accepted by the caller and trims exactly
   the same way). Returns 0 on success (*id_out holds the new ID as
   a "ms-seq" string, at least 48 bytes of room), -1 = the key holds
   a non-stream value, -2 = out of memory, -3 = the ID is not a
   valid stream ID, -4 = the ID is equal to or smaller than the top
   ID, -5 = the ID is 0-0. */
int store_xadd(Store *s, const char *key, const char *id,
               char *const *pairs, int nfields, long long maxlen,
               char *id_out);
/* XLEN: the number of entries in a stream (0 when the key is
   missing), -1 = the key holds a non-stream value. A read, so it
   changes nothing and bumps only the key's last-access clock. */
long long store_xlen(Store *s, const char *key);
/* The top ID of a stream (0-0 when the key is missing), for
   resolving "$" in XREAD: the entries XREAD returns are the ones
   after this ID. Returns 0 on success, -1 = the key holds a
   non-stream value. A read, so it changes nothing and bumps only
   the key's last-access clock. */
int store_stream_top(Store *s, const char *key, uint64_t *ms_out,
                     uint64_t *seq_out);
/* Parse an XRANGE bound: "-" (the minimum ID, 0-0), "+" (the
   maximum ID), "ms" (ms-0 for a start bound, ms-MAX for an end
   bound, so a bare ms covers every sequence in that millisecond)
   or "ms-seq". Returns 0 on success, -1 = not a valid bound. */
int stream_parse_bound(const char *s, int is_end, uint64_t *ms_out,
                       uint64_t *seq_out);
/* XRANGE: the entries whose IDs fall in [start, end] inclusive, in
   ID order, at most count of them (count < 0 means no limit). The
   returned entries are copies owned by the caller; free them with
   stream_entries_free. Returns 0 on success (a missing key answers
   no entries), -1 = the key holds a non-stream value,
   -2 = out of memory. */
int store_xrange(Store *s, const char *key, uint64_t start_ms,
                 uint64_t start_seq, uint64_t end_ms, uint64_t end_seq,
                 long long count, StreamEntry **entries_out,
                 size_t *n_out);
void stream_entries_free(StreamEntry *entries, size_t n);
/* XREVRANGE: the entries whose IDs fall in [start, end] inclusive,
   in reverse ID order (highest first), at most count of them
   (count < 0 means no limit). Same ownership and return codes as
   store_xrange. */
int store_xrevrange(Store *s, const char *key, uint64_t start_ms,
                    uint64_t start_seq, uint64_t end_ms, uint64_t end_seq,
                    long long count, StreamEntry **entries_out,
                    size_t *n_out);
/* XGROUP CREATE: make a consumer group on a stream, starting at
   (ms, seq) — the last-delivered ID, so `>` reads deliver what
   comes after it. With mkstream set, a missing stream is created
   empty first. Creating a group changes the key, so it bumps the
   version (a WATCH on the key is invalidated). Returns 0 on
   success, -1 = the key holds a non-stream value, -2 = out of
   memory, -3 = the group already exists, -4 = the stream does not
   exist. */
int store_xgroup_create(Store *s, const char *key, const char *group,
                        uint64_t ms, uint64_t seq, int mkstream);
/* XGROUP SETID: move a consumer group's last-delivered ID to
   (ms, seq), so the next `>` read delivers what comes after it.
   The pending list is untouched: entries already delivered stay
   pending for their consumers, and entries after the new cursor
   are delivered again on the next `>` read. A set to the ID the
   group already holds changes nothing, so it bumps no version
   (a WATCH on the key survives it) and the caller need not log
   it; a real move bumps the version. Returns 1 when the cursor
   moved, 0 when it was already that ID, -1 = the key holds a
   non-stream value, -3 = no such key or consumer group. */
int store_xgroup_setid(Store *s, const char *key, const char *group,
                       uint64_t ms, uint64_t seq);
/* XREADGROUP: deliver entries of one stream to a consumer of a
   group. When new_only is set, the entries after the group's
   cursor, which are added to the PEL (delivery count 1, stamped
   with the current time) and advance the cursor; otherwise the
   group's pending entries for this consumer with an ID after
   (ms, seq), returned without changing delivery state. Entries
   trimmed from the stream are skipped on a pending read. At most
   count entries (count < 0 means no limit). The returned entries
   are copies owned by the caller; free them with
   stream_entries_free. Returns 0 on success, -1 = the key holds a
   non-stream value, -2 = out of memory, -3 = no such key or
   consumer group. */
int store_xreadgroup(Store *s, const char *key, const char *group,
                     const char *consumer, int new_only, uint64_t ms,
                     uint64_t seq, long long count,
                     StreamEntry **entries_out, size_t *n_out);
/* XACK: drop the given IDs from a group's PEL. Returns the number
   removed, -1 = the key holds a non-stream value, -2 = out of
   memory, -3 = no such key or consumer group. */
long long store_xack(Store *s, const char *key, const char *group,
                     const uint64_t *ms, const uint64_t *seq, size_t n);
/* XCLAIM: take over pending entries of a group for another
   consumer. An entry is claimed when it has been idle at least
   min_idle_ms since its last delivery, or when force is set (which
   also claims entries that are in the stream but not in the PEL at
   all, creating a PEL entry for them). A claimed entry is moved to
   this consumer, its delivery time is set to delivery_time and its
   delivery count is bumped by one, or set to retrycount when that
   is >= 0. IDs that are not in the stream, are not idle enough, or
   are not pending (without FORCE) are left out of the reply, and a
   repeated ID in one call is claimed only once. The claimed
   entries are returned as copies in request order; free them with
   stream_entries_free. Returns 0 on success, -1 = the key holds a
   non-stream value, -2 = out of memory, -3 = no such key or
   consumer group. A claim changes delivery state, so the caller
   logs and replicates it like a write. */
int store_xclaim(Store *s, const char *key, const char *group,
                 const char *consumer, long long min_idle_ms,
                 const uint64_t *ms, const uint64_t *seq, size_t n,
                 int force, long long retrycount, long long delivery_time,
                 StreamEntry **entries_out, size_t *n_out);
/* One pending entry as XPENDING reports it: the entry ID, the
   consumer it was last delivered to, how long (ms) since it was
   last delivered, and how many times it has been delivered. */
typedef struct StreamPendingInfo {
    uint64_t ms, seq;
    char *consumer; /* owned */
    long long idle_ms;
    long long delivery_count;
} StreamPendingInfo;
void stream_pending_info_free(StreamPendingInfo *infos, size_t n);
/* One consumer's share of a group's pending entries, as the
   XPENDING summary reports it. */
typedef struct StreamConsumerCount {
    char *consumer; /* owned */
    long long count;
} StreamConsumerCount;
void stream_consumer_counts_free(StreamConsumerCount *counts, size_t n);
/* XPENDING summary: the group's pending entries at a glance. On
   success *total_out is the PEL size, (min_ms, min_seq) and
   (max_ms, max_seq) are the lowest and highest pending IDs (0-0
   when the PEL is empty), and *counts_out is an array of
   per-consumer counts, in the order the consumers first read,
   leaving out consumers with nothing pending; free it with
   stream_consumer_counts_free. Returns 0 on success, -1 = the key
   holds a non-stream value, -2 = out of memory, -3 = no such key
   or consumer group. A read, so it changes nothing but the key's
   last-access clock. */
int store_xpending_summary(Store *s, const char *key, const char *group,
                           long long *total_out, uint64_t *min_ms,
                           uint64_t *min_seq, uint64_t *max_ms,
                           uint64_t *max_seq,
                           StreamConsumerCount **counts_out,
                           size_t *ncounts_out);
/* XPENDING range: the pending entries with IDs in [start, end]
   inclusive, in ID order, at most count of them (count < 0 means
   no limit), optionally only those delivered to `consumer` (NULL
   means all). The returned entries are copies owned by the
   caller; free them with stream_pending_info_free. Same return
   codes as store_xpending_summary, and likewise a read. */
int store_xpending_range(Store *s, const char *key, const char *group,
                         uint64_t start_ms, uint64_t start_seq,
                         uint64_t end_ms, uint64_t end_seq,
                         long long count, const char *consumer,
                         StreamPendingInfo **infos_out, size_t *n_out);
/* Visit every entry of a stream in ID order. Returns 0 on success,
   -1 = wrong type (or no such key, which the caller can treat the
   same way: no entries are visited). The callback receives each
   entry's ID parts and its field/value pairs; returning non-zero
   stops the iteration and that value is returned. The pairs are
   owned by the stream and must not be freed or kept past the call.
   Used by the snapshot writer and the replication emitter, which
   only need the stream in order. */
typedef int (*stream_foreach_fn)(uint64_t ms, uint64_t seq,
                                 char *const *fields, char *const *values,
                                 size_t nfields, void *arg);
int stream_foreach(const Stream *st, stream_foreach_fn fn, void *arg);
/* stream_foreach in reverse ID order (highest first). Same
   callback contract: returning non-zero stops the iteration and
   that value is returned. */
int stream_foreach_rev(const Stream *st, stream_foreach_fn fn, void *arg);
/* Visit every consumer group of a stream, in creation order. The
   callback receives the group itself (owned by the stream, must
   not be freed or kept past the call); returning non-zero stops
   the iteration and that value is returned. Used by the snapshot
   writer, which needs each group's name, cursor, consumers, and
   pending list. */
typedef int (*stream_group_foreach_fn)(const StreamGroup *g, void *arg);
int stream_group_foreach(const Stream *st, stream_group_foreach_fn fn,
                         void *arg);
/* Restore one consumer group exactly as a snapshot recorded it:
   name, last-delivered ID, consumer names, and pending entries
   with their recorded delivery times and counts (not "now", so a
   reloaded group reports the same idle times and delivery counts
   it had when saved). Used by the RDB loader; the stream must
   already exist and the group must not. Returns 0 on success,
   -1 = the key holds a non-stream value, -2 = out of memory,
   -3 = the group already exists, -4 = the stream does not exist.
   A duplicate pending ID in the snapshot is treated as out of
   memory (-2): it cannot occur in a file the writer produced. */
int store_xgroup_restore(Store *s, const char *key, const char *group,
                         uint64_t last_ms, uint64_t last_seq,
                         char *const *consumers, size_t nconsumers,
                         const StreamPending *pel, size_t npel);

#endif
