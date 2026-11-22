#ifndef CINDER_STORE_H
#define CINDER_STORE_H

#include <stddef.h>

#define ENTRY_STRING 0
#define ENTRY_LIST 1

/* A list value: circular buffer of malloc'd strings, so push/pop at
   either end is O(1) amortised. */
typedef struct List {
    char **items;
    size_t cap;
    size_t head; /* index of the first element */
    size_t len;
} List;

typedef struct Entry {
    char *key;
    int type; /* ENTRY_STRING or ENTRY_LIST */
    char *value; /* string values (type == ENTRY_STRING) */
    List *list; /* list values (type == ENTRY_LIST) */
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

/* 0 = no such key, 1 = string, 2 = list. */
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

#endif
