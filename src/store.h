#ifndef CINDER_STORE_H
#define CINDER_STORE_H

#include <stddef.h>

#define ENTRY_STRING 0
#define ENTRY_LIST 1
#define ENTRY_SET 2
#define ENTRY_HASH 3

/* A list value: circular buffer of malloc'd strings, so push/pop at
   either end is O(1) amortised. */
typedef struct List {
    char **items;
    size_t cap;
    size_t head; /* index of the first element */
    size_t len;
} List;

/* A set value: unordered array of malloc'd strings, membership by
   linear scan. Simple and fast enough at this scale; a hash table
   would be the next step if sets grow large. */
typedef struct Set {
    char **members;
    size_t cap;
    size_t len;
} Set;

/* A hash value: parallel arrays of malloc'd fields and values, in
   insertion order; lookup by linear scan, like the set type. */
typedef struct Hash {
    char **fields;
    char **values;
    size_t cap;
    size_t len;
} Hash;

typedef struct Entry {
    char *key;
    int type; /* ENTRY_STRING, ENTRY_LIST, ENTRY_SET or ENTRY_HASH */
    char *value; /* string values (type == ENTRY_STRING) */
    List *list; /* list values (type == ENTRY_LIST) */
    Set *set; /* set values (type == ENTRY_SET) */
    Hash *hash; /* hash values (type == ENTRY_HASH) */
    long long expire_at; /* ms since epoch, -1 = no expiry */
    unsigned long long last_access; /* logical clock, for approximate LRU */
    struct Entry *next;
} Entry;

typedef struct Store {
    Entry **buckets;
    size_t cap;
    size_t count;
    unsigned long long clock; /* increments on every access */
} Store;

long long now_ms(void);

Store *store_new(void);
void store_free(Store *s);

/* Insert or overwrite a key. Value is copied. expire_at is ms epoch, -1 = none.
   Overwriting a list key replaces it with the string. */
int store_set(Store *s, const char *key, const char *value, long long expire_at);
/* Returns a malloc'd copy of the value, or NULL if missing/expired/wrong type. */
char *store_get(Store *s, const char *key);
/* Increment a string key's integer value by delta, in one hash lookup.
   Creates the key when missing (or expired). The TTL is untouched, so
   INCR preserves it, as in Redis. Returns 0 on success (*out is the
   new value), -1 = wrong type, -2 = value is not an integer,
   -3 = out of memory. */
int store_incr(Store *s, const char *key, long long delta, long long *out);
int store_del(Store *s, const char *key);
int store_exists(Store *s, const char *key);
int store_expire(Store *s, const char *key, long long expire_at);
/* -2 = no such key, -1 = key exists with no expiry, otherwise ms remaining. */
long long store_ttl_ms(Store *s, const char *key);
size_t store_size(Store *s);
void store_flush(Store *s);
/* Delete expired entries found in a sample of buckets. Returns count removed. */
size_t store_expire_scan(Store *s, size_t sample_buckets);
/* Evict approximately-least-recently-used entries until count <= max_entries. */
size_t store_evict_lru(Store *s, size_t max_entries);
/* Collect all non-expired keys matching a glob pattern ('*' and '?').
   Returns the count; *keys_out is a malloc'd array of malloc'd strings
   (NULL and 0 when nothing matches). Caller frees both. */
size_t store_keys_matching(Store *s, const char *pattern, char ***keys_out);

/* Iterate over all non-expired entries in bucket order. The callback
   receives each entry (read-only) and arg; returning non-zero stops
   the iteration. Expired entries are skipped, not deleted. Returns 0
   when every entry was visited, 1 if stopped early. */
typedef int (*store_foreach_fn)(const Entry *e, void *arg);
int store_foreach(Store *s, store_foreach_fn fn, void *arg);

/* 0 = no such key, 1 = string, 2 = list, 3 = set, 4 = hash. */
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

#endif
