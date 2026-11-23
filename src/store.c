#define _GNU_SOURCE
#include "store.h"

#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/time.h>

#define INITIAL_CAP 1024
#define LOAD_FACTOR_NUM 3 /* resize when count * 4 > cap * 3 */
#define LOAD_FACTOR_DEN 4

long long now_ms(void) {
    struct timeval tv;
    gettimeofday(&tv, NULL);
    return (long long)tv.tv_sec * 1000 + tv.tv_usec / 1000;
}

static unsigned long hash_key(const char *key) {
    /* djb2 */
    unsigned long h = 5381;
    int c;
    while ((c = (unsigned char)*key++))
        h = ((h << 5) + h) + (unsigned long)c;
    return h;
}

static int entry_expired(const Entry *e) {
    return e->expire_at != -1 && now_ms() >= e->expire_at;
}

Store *store_new(void) {
    Store *s = calloc(1, sizeof(Store));
    if (!s) return NULL;
    s->cap = INITIAL_CAP;
    s->buckets = calloc(s->cap, sizeof(Entry *));
    if (!s->buckets) {
        free(s);
        return NULL;
    }
    return s;
}

static void list_free(List *l) {
    if (!l) return;
    for (size_t i = 0; i < l->len; i++)
        free(l->items[(l->head + i) % l->cap]);
    free(l->items);
    free(l);
}

static void set_free(Set *st) {
    if (!st) return;
    for (size_t i = 0; i < st->len; i++)
        free(st->members[i]);
    free(st->members);
    free(st);
}

static void hash_free(Hash *h) {
    if (!h) return;
    for (size_t i = 0; i < h->len; i++) {
        free(h->fields[i]);
        free(h->values[i]);
    }
    free(h->fields);
    free(h->values);
    free(h);
}

static void entry_free(Entry *e) {
    free(e->key);
    if (e->type == ENTRY_LIST) list_free(e->list);
    else if (e->type == ENTRY_SET) set_free(e->set);
    else if (e->type == ENTRY_HASH) hash_free(e->hash);
    else free(e->value);
    free(e);
}

static Hash *hash_new(void) {
    Hash *h = calloc(1, sizeof(Hash));
    if (!h) return NULL;
    h->cap = 8;
    h->fields = calloc(h->cap, sizeof(char *));
    h->values = calloc(h->cap, sizeof(char *));
    if (!h->fields || !h->values) {
        hash_free(h);
        return NULL;
    }
    return h;
}

/* Index of field, or -1 when absent. */
static long long hash_find(Hash *h, const char *field) {
    for (size_t i = 0; i < h->len; i++)
        if (strcmp(h->fields[i], field) == 0) return (long long)i;
    return -1;
}

/* Set field to value, adding it if absent. Returns 1 new, 0 updated,
   -1 = OOM. */
static int hash_put(Hash *h, const char *field, const char *value) {
    long long idx = hash_find(h, field);
    if (idx >= 0) {
        char *v = strdup(value);
        if (!v) return -1;
        free(h->values[idx]);
        h->values[idx] = v;
        return 0;
    }
    char *f = strdup(field);
    char *v = strdup(value);
    if (!f || !v) {
        free(f);
        free(v);
        return -1;
    }
    if (h->len == h->cap) {
        size_t ncap = h->cap * 2;
        char **nf = realloc(h->fields, ncap * sizeof(char *));
        char **nv = realloc(h->values, ncap * sizeof(char *));
        if (!nf || !nv) {
            free(f);
            free(v);
            return -1;
        }
        h->fields = nf;
        h->values = nv;
        h->cap = ncap;
    }
    h->fields[h->len] = f;
    h->values[h->len] = v;
    h->len++;
    return 1;
}

/* Remove field if present, preserving insertion order of the rest.
   Returns 1 removed, 0 absent. */
static int hash_remove(Hash *h, const char *field) {
    long long idx = hash_find(h, field);
    if (idx < 0) return 0;
    free(h->fields[idx]);
    free(h->values[idx]);
    for (size_t i = (size_t)idx + 1; i < h->len; i++) {
        h->fields[i - 1] = h->fields[i];
        h->values[i - 1] = h->values[i];
    }
    h->len--;
    h->fields[h->len] = NULL;
    h->values[h->len] = NULL;
    return 1;
}

static Set *set_new(void) {
    Set *st = calloc(1, sizeof(Set));
    if (!st) return NULL;
    st->cap = 8;
    st->members = calloc(st->cap, sizeof(char *));
    if (!st->members) {
        free(st);
        return NULL;
    }
    return st;
}

/* Index of member, or -1 when absent. */
static long long set_find(Set *st, const char *member) {
    for (size_t i = 0; i < st->len; i++)
        if (strcmp(st->members[i], member) == 0) return (long long)i;
    return -1;
}

/* Add member if absent. Returns 1 added, 0 already present, -1 = OOM. */
static int set_add(Set *st, const char *member) {
    if (set_find(st, member) >= 0) return 0;
    char *m = strdup(member);
    if (!m) return -1;
    if (st->len == st->cap) {
        size_t ncap = st->cap * 2;
        char **nm = realloc(st->members, ncap * sizeof(char *));
        if (!nm) {
            free(m);
            return -1;
        }
        st->members = nm;
        st->cap = ncap;
    }
    st->members[st->len++] = m;
    return 1;
}

/* Remove member if present. Returns 1 removed, 0 absent. */
static int set_remove(Set *st, const char *member) {
    long long idx = set_find(st, member);
    if (idx < 0) return 0;
    free(st->members[idx]);
    st->members[idx] = st->members[--st->len];
    st->members[st->len] = NULL;
    return 1;
}

static List *list_new(void) {
    List *l = calloc(1, sizeof(List));
    if (!l) return NULL;
    l->cap = 8;
    l->items = calloc(l->cap, sizeof(char *));
    if (!l->items) {
        free(l);
        return NULL;
    }
    return l;
}

/* Double capacity, re-laying elements out from index 0. */
static int list_grow(List *l) {
    size_t ncap = l->cap * 2;
    char **ni = calloc(ncap, sizeof(char *));
    if (!ni) return 0;
    for (size_t i = 0; i < l->len; i++)
        ni[i] = l->items[(l->head + i) % l->cap];
    free(l->items);
    l->items = ni;
    l->cap = ncap;
    l->head = 0;
    return 1;
}

static int list_push_head(List *l, const char *elem) {
    char *e = strdup(elem);
    if (!e) return 0;
    if (l->len == l->cap && !list_grow(l)) {
        free(e);
        return 0;
    }
    l->head = (l->head + l->cap - 1) % l->cap;
    l->items[l->head] = e;
    l->len++;
    return 1;
}

static int list_push_tail(List *l, const char *elem) {
    char *e = strdup(elem);
    if (!e) return 0;
    if (l->len == l->cap && !list_grow(l)) {
        free(e);
        return 0;
    }
    l->items[(l->head + l->len) % l->cap] = e;
    l->len++;
    return 1;
}

static char *list_pop_head(List *l) {
    if (l->len == 0) return NULL;
    char *e = l->items[l->head];
    l->items[l->head] = NULL;
    l->head = (l->head + 1) % l->cap;
    l->len--;
    return e;
}

static char *list_pop_tail(List *l) {
    if (l->len == 0) return NULL;
    size_t idx = (l->head + l->len - 1) % l->cap;
    char *e = l->items[idx];
    l->items[idx] = NULL;
    l->len--;
    return e;
}

void store_free(Store *s) {
    if (!s) return;
    store_flush(s);
    free(s->buckets);
    free(s);
}

static void store_resize(Store *s) {
    size_t ncap = s->cap * 2;
    Entry **nb = calloc(ncap, sizeof(Entry *));
    if (!nb) return;
    for (size_t i = 0; i < s->cap; i++) {
        Entry *e = s->buckets[i];
        while (e) {
            Entry *next = e->next;
            size_t idx = hash_key(e->key) % ncap;
            e->next = nb[idx];
            nb[idx] = e;
            e = next;
        }
    }
    free(s->buckets);
    s->buckets = nb;
    s->cap = ncap;
}

/* Find entry; if found but expired, delete it and return NULL. */
static Entry *store_find(Store *s, const char *key, Entry ***prev_out) {
    size_t idx = hash_key(key) % s->cap;
    Entry **prev = &s->buckets[idx];
    Entry *e = *prev;
    while (e) {
        if (strcmp(e->key, key) == 0) {
            if (entry_expired(e)) {
                *prev = e->next;
                entry_free(e);
                s->count--;
                return NULL;
            }
            if (prev_out) *prev_out = prev;
            return e;
        }
        prev = &e->next;
        e = e->next;
    }
    return NULL;
}

int store_set(Store *s, const char *key, const char *value, long long expire_at) {
    Entry *e = store_find(s, key, NULL);
    if (e) {
        char *nv = strdup(value);
        if (!nv) return 0;
        if (e->type == ENTRY_LIST) {
            /* SET replaces the key wholesale, whatever it held. */
            list_free(e->list);
            e->list = NULL;
            e->type = ENTRY_STRING;
        } else if (e->type == ENTRY_SET) {
            set_free(e->set);
            e->set = NULL;
            e->type = ENTRY_STRING;
        } else if (e->type == ENTRY_HASH) {
            hash_free(e->hash);
            e->hash = NULL;
            e->type = ENTRY_STRING;
        }
        free(e->value);
        e->value = nv;
        e->expire_at = expire_at;
        e->last_access = ++s->clock;
        return 1;
    }
    if (s->count * LOAD_FACTOR_DEN > s->cap * LOAD_FACTOR_NUM)
        store_resize(s);
    e = calloc(1, sizeof(Entry));
    if (!e) return 0;
    e->key = strdup(key);
    e->value = strdup(value);
    if (!e->key || !e->value) {
        entry_free(e);
        return 0;
    }
    e->type = ENTRY_STRING;
    e->expire_at = expire_at;
    e->last_access = ++s->clock;
    size_t idx = hash_key(key) % s->cap;
    e->next = s->buckets[idx];
    s->buckets[idx] = e;
    s->count++;
    return 1;
}

char *store_get(Store *s, const char *key) {
    Entry *e = store_find(s, key, NULL);
    if (!e || e->type != ENTRY_STRING) return NULL;
    e->last_access = ++s->clock;
    return strdup(e->value);
}

/* Strict integer parse: the whole string must be one integer. */
static int parse_ll(const char *s, long long *out) {
    if (!s || !*s) return 0;
    char *end = NULL;
    errno = 0;
    long long v = strtoll(s, &end, 10);
    if (errno != 0 || end == s || *end != '\0') return 0;
    *out = v;
    return 1;
}

int store_incr(Store *s, const char *key, long long delta, long long *out) {
    Entry *e = store_find(s, key, NULL);
    long long v = 0;
    if (e) {
        if (e->type != ENTRY_STRING) return -1;
        if (!parse_ll(e->value, &v)) return -2;
    }
    v += delta;
    char vbuf[32];
    snprintf(vbuf, sizeof(vbuf), "%lld", v);
    if (e) {
        /* Update the stored string in place when it fits; grow the
           allocation only when the new decimal form is longer than
           the old one. The expiry is untouched, so the TTL survives. */
        size_t nlen = strlen(vbuf);
        if (nlen > strlen(e->value)) {
            char *nv = realloc(e->value, nlen + 1);
            if (!nv) return -3;
            e->value = nv;
        }
        memcpy(e->value, vbuf, nlen + 1);
        e->last_access = ++s->clock;
    } else {
        if (s->count * LOAD_FACTOR_DEN > s->cap * LOAD_FACTOR_NUM)
            store_resize(s);
        e = calloc(1, sizeof(Entry));
        if (!e) return -3;
        e->key = strdup(key);
        e->value = strdup(vbuf);
        if (!e->key || !e->value) {
            entry_free(e);
            return -3;
        }
        e->type = ENTRY_STRING;
        e->expire_at = -1;
        e->last_access = ++s->clock;
        size_t idx = hash_key(key) % s->cap;
        e->next = s->buckets[idx];
        s->buckets[idx] = e;
        s->count++;
    }
    *out = v;
    return 0;
}

int store_del(Store *s, const char *key) {
    Entry *prev = NULL;
    (void)prev;
    Entry **pp = NULL;
    Entry *e = store_find(s, key, &pp);
    if (!e) return 0;
    *pp = e->next;
    entry_free(e);
    s->count--;
    return 1;
}

int store_exists(Store *s, const char *key) {
    return store_find(s, key, NULL) != NULL;
}

int store_expire(Store *s, const char *key, long long expire_at) {
    Entry *e = store_find(s, key, NULL);
    if (!e) return 0;
    e->expire_at = expire_at;
    return 1;
}

long long store_ttl_ms(Store *s, const char *key) {
    Entry *e = store_find(s, key, NULL);
    if (!e) return -2;
    if (e->expire_at == -1) return -1;
    long long rem = e->expire_at - now_ms();
    return rem < 0 ? -2 : rem;
}

size_t store_size(Store *s) {
    return s->count;
}

int store_foreach(Store *s, store_foreach_fn fn, void *arg) {
    for (size_t i = 0; i < s->cap; i++) {
        Entry *e = s->buckets[i];
        while (e) {
            Entry *next = e->next;
            if (!entry_expired(e) && fn(e, arg) != 0) return 1;
            e = next;
        }
    }
    return 0;
}

void store_flush(Store *s) {
    for (size_t i = 0; i < s->cap; i++) {
        Entry *e = s->buckets[i];
        while (e) {
            Entry *next = e->next;
            entry_free(e);
            e = next;
        }
        s->buckets[i] = NULL;
    }
    s->count = 0;
}

size_t store_expire_scan(Store *s, size_t sample_buckets) {
    size_t removed = 0;
    if (s->cap == 0) return 0;
    for (size_t n = 0; n < sample_buckets; n++) {
        size_t idx = (size_t)rand() % s->cap;
        Entry **pp = &s->buckets[idx];
        Entry *e = *pp;
        while (e) {
            Entry *next = e->next;
            if (entry_expired(e)) {
                *pp = next;
                entry_free(e);
                s->count--;
                removed++;
            } else {
                pp = &e->next;
            }
            e = next;
        }
    }
    return removed;
}

/* Glob match supporting '*' and '?'. */
static int glob_match(const char *pat, const char *str) {
    while (*pat) {
        if (*pat == '*') {
            while (*pat == '*') pat++;
            if (!*pat) return 1;
            while (*str) {
                if (glob_match(pat, str)) return 1;
                str++;
            }
            return 0;
        }
        if (*pat == '?') {
            if (!*str) return 0;
            pat++;
            str++;
            continue;
        }
        if (*pat != *str) return 0;
        pat++;
        str++;
    }
    return *str == '\0';
}

size_t store_keys_matching(Store *s, const char *pattern, char ***keys_out) {
    *keys_out = NULL;
    size_t n = 0, cap = 0;
    char **keys = NULL;
    for (size_t i = 0; i < s->cap; i++) {
        Entry **pp = &s->buckets[i];
        Entry *e = *pp;
        while (e) {
            Entry *next = e->next;
            if (entry_expired(e)) {
                *pp = next;
                entry_free(e);
                s->count--;
            } else {
                if (glob_match(pattern, e->key)) {
                    if (n >= cap) {
                        cap = cap ? cap * 2 : 16;
                        char **nk = realloc(keys, cap * sizeof(char *));
                        if (!nk) goto done;
                        keys = nk;
                    }
                    keys[n] = strdup(e->key);
                    if (!keys[n]) goto done;
                    n++;
                }
                pp = &e->next;
            }
            e = next;
        }
    }
done:
    *keys_out = keys;
    return n;
}

size_t store_evict_lru(Store *s, size_t max_entries) {
    size_t evicted = 0;
    while (s->count > max_entries) {
        /* Redis-style approximate LRU: sample a few entries, evict the
           least recently used of the sample. */
        Entry *victim = NULL;
        Entry **victim_pp = NULL;
        for (int n = 0; n < 5; n++) {
            size_t idx = (size_t)rand() % s->cap;
            Entry *e = s->buckets[idx];
            if (e && (!victim || e->last_access < victim->last_access)) {
                victim = e;
                victim_pp = &s->buckets[idx];
            }
        }
        if (!victim) break; /* sampled only empty buckets; table is sparse */
        *victim_pp = victim->next;
        entry_free(victim);
        s->count--;
        evicted++;
    }
    return evicted;
}

/* ---------- lists ---------- */

int store_type(Store *s, const char *key) {
    Entry *e = store_find(s, key, NULL);
    if (!e) return 0;
    if (e->type == ENTRY_LIST) return 2;
    if (e->type == ENTRY_SET) return 3;
    if (e->type == ENTRY_HASH) return 4;
    return 1;
}

/* Push onto an existing list, or create the list when the key is missing. */
static long long store_list_push(Store *s, const char *key, const char *elem,
                                 int at_head) {
    Entry *e = store_find(s, key, NULL);
    if (e) {
        if (e->type != ENTRY_LIST) return -1;
        if (!(at_head ? list_push_head(e->list, elem)
                      : list_push_tail(e->list, elem)))
            return -2;
        e->last_access = ++s->clock;
        return (long long)e->list->len;
    }
    if (s->count * LOAD_FACTOR_DEN > s->cap * LOAD_FACTOR_NUM)
        store_resize(s);
    e = calloc(1, sizeof(Entry));
    if (!e) return -2;
    e->key = strdup(key);
    e->list = list_new();
    if (!e->key || !e->list) {
        entry_free(e);
        return -2;
    }
    e->type = ENTRY_LIST;
    e->expire_at = -1;
    e->last_access = ++s->clock;
    if (!(at_head ? list_push_head(e->list, elem)
                  : list_push_tail(e->list, elem))) {
        entry_free(e);
        return -2;
    }
    size_t idx = hash_key(key) % s->cap;
    e->next = s->buckets[idx];
    s->buckets[idx] = e;
    s->count++;
    return 1;
}

long long store_lpush(Store *s, const char *key, const char *elem) {
    return store_list_push(s, key, elem, 1);
}

long long store_rpush(Store *s, const char *key, const char *elem) {
    return store_list_push(s, key, elem, 0);
}

static char *store_list_pop(Store *s, const char *key, int *wrongtype,
                            int from_head) {
    if (wrongtype) *wrongtype = 0;
    Entry **pp = NULL;
    Entry *e = store_find(s, key, &pp);
    if (!e) return NULL;
    if (e->type != ENTRY_LIST) {
        if (wrongtype) *wrongtype = 1;
        return NULL;
    }
    char *el = from_head ? list_pop_head(e->list) : list_pop_tail(e->list);
    if (!el) return NULL;
    if (e->list->len == 0) {
        /* A list emptied by popping is gone, as in Redis. */
        *pp = e->next;
        entry_free(e);
        s->count--;
    } else {
        e->last_access = ++s->clock;
    }
    return el;
}

char *store_lpop(Store *s, const char *key, int *wrongtype) {
    return store_list_pop(s, key, wrongtype, 1);
}

char *store_rpop(Store *s, const char *key, int *wrongtype) {
    return store_list_pop(s, key, wrongtype, 0);
}

long long store_llen(Store *s, const char *key) {
    Entry *e = store_find(s, key, NULL);
    if (!e) return 0;
    if (e->type != ENTRY_LIST) return -1;
    e->last_access = ++s->clock;
    return (long long)e->list->len;
}

int store_lrange(Store *s, const char *key, long long start, long long stop,
                 char ***out, size_t *n_out) {
    *out = NULL;
    *n_out = 0;
    Entry *e = store_find(s, key, NULL);
    if (!e) return 0;
    if (e->type != ENTRY_LIST) return -1;
    e->last_access = ++s->clock;
    List *l = e->list;
    long long len = (long long)l->len;
    if (start < 0) start += len;
    if (stop < 0) stop += len;
    if (start < 0) start = 0;
    if (stop >= len) stop = len - 1;
    if (start > stop || start >= len) return 0;
    size_t n = (size_t)(stop - start + 1);
    char **res = malloc(n * sizeof(char *));
    if (!res) return -2;
    for (size_t i = 0; i < n; i++) {
        res[i] = strdup(l->items[(l->head + (size_t)start + i) % l->cap]);
        if (!res[i]) {
            for (size_t j = 0; j < i; j++) free(res[j]);
            free(res);
            return -2;
        }
    }
    *out = res;
    *n_out = n;
    return 0;
}

/* ---------- sets ---------- */

long long store_sadd(Store *s, const char *key, const char *member) {
    Entry *e = store_find(s, key, NULL);
    if (e) {
        if (e->type != ENTRY_SET) return -1;
        int rc = set_add(e->set, member);
        if (rc < 0) return -2;
        if (rc) e->last_access = ++s->clock;
        return rc;
    }
    if (s->count * LOAD_FACTOR_DEN > s->cap * LOAD_FACTOR_NUM)
        store_resize(s);
    e = calloc(1, sizeof(Entry));
    if (!e) return -2;
    e->key = strdup(key);
    e->set = set_new();
    if (!e->key || !e->set) {
        entry_free(e);
        return -2;
    }
    e->type = ENTRY_SET;
    e->expire_at = -1;
    e->last_access = ++s->clock;
    if (set_add(e->set, member) < 0) {
        entry_free(e);
        return -2;
    }
    size_t idx = hash_key(key) % s->cap;
    e->next = s->buckets[idx];
    s->buckets[idx] = e;
    s->count++;
    return 1;
}

long long store_srem(Store *s, const char *key, const char *member) {
    Entry **pp = NULL;
    Entry *e = store_find(s, key, &pp);
    if (!e) return 0;
    if (e->type != ENTRY_SET) return -1;
    if (!set_remove(e->set, member)) return 0;
    if (e->set->len == 0) {
        /* A set emptied by removing is gone, as in Redis. */
        *pp = e->next;
        entry_free(e);
        s->count--;
    } else {
        e->last_access = ++s->clock;
    }
    return 1;
}

int store_sismember(Store *s, const char *key, const char *member) {
    Entry *e = store_find(s, key, NULL);
    if (!e) return 0;
    if (e->type != ENTRY_SET) return -1;
    e->last_access = ++s->clock;
    return set_find(e->set, member) >= 0 ? 1 : 0;
}

long long store_scard(Store *s, const char *key) {
    Entry *e = store_find(s, key, NULL);
    if (!e) return 0;
    if (e->type != ENTRY_SET) return -1;
    e->last_access = ++s->clock;
    return (long long)e->set->len;
}

int store_smembers(Store *s, const char *key, char ***out, size_t *n_out) {
    *out = NULL;
    *n_out = 0;
    Entry *e = store_find(s, key, NULL);
    if (!e) return 0;
    if (e->type != ENTRY_SET) return -1;
    e->last_access = ++s->clock;
    Set *st = e->set;
    if (st->len == 0) return 0;
    char **res = malloc(st->len * sizeof(char *));
    if (!res) return -2;
    for (size_t i = 0; i < st->len; i++) {
        res[i] = strdup(st->members[i]);
        if (!res[i]) {
            for (size_t j = 0; j < i; j++) free(res[j]);
            free(res);
            return -2;
        }
    }
    *out = res;
    *n_out = st->len;
    return 0;
}

/* ---------- hashes ---------- */

long long store_hset(Store *s, const char *key, const char *field,
                     const char *value) {
    Entry *e = store_find(s, key, NULL);
    if (e) {
        if (e->type != ENTRY_HASH) return -1;
        int rc = hash_put(e->hash, field, value);
        if (rc < 0) return -2;
        e->last_access = ++s->clock;
        return rc;
    }
    if (s->count * LOAD_FACTOR_DEN > s->cap * LOAD_FACTOR_NUM)
        store_resize(s);
    e = calloc(1, sizeof(Entry));
    if (!e) return -2;
    e->key = strdup(key);
    e->hash = hash_new();
    if (!e->key || !e->hash) {
        entry_free(e);
        return -2;
    }
    e->type = ENTRY_HASH;
    e->expire_at = -1;
    e->last_access = ++s->clock;
    if (hash_put(e->hash, field, value) < 0) {
        entry_free(e);
        return -2;
    }
    size_t idx = hash_key(key) % s->cap;
    e->next = s->buckets[idx];
    s->buckets[idx] = e;
    s->count++;
    return 1;
}

char *store_hget(Store *s, const char *key, const char *field) {
    Entry *e = store_find(s, key, NULL);
    if (!e || e->type != ENTRY_HASH) return NULL;
    e->last_access = ++s->clock;
    long long idx = hash_find(e->hash, field);
    if (idx < 0) return NULL;
    return strdup(e->hash->values[idx]);
}

long long store_hdel(Store *s, const char *key, const char *field) {
    Entry **pp = NULL;
    Entry *e = store_find(s, key, &pp);
    if (!e) return 0;
    if (e->type != ENTRY_HASH) return -1;
    if (!hash_remove(e->hash, field)) return 0;
    if (e->hash->len == 0) {
        /* A hash emptied by removing is gone, as in Redis. */
        *pp = e->next;
        entry_free(e);
        s->count--;
    } else {
        e->last_access = ++s->clock;
    }
    return 1;
}

int store_hexists(Store *s, const char *key, const char *field) {
    Entry *e = store_find(s, key, NULL);
    if (!e) return 0;
    if (e->type != ENTRY_HASH) return -1;
    e->last_access = ++s->clock;
    return hash_find(e->hash, field) >= 0 ? 1 : 0;
}

long long store_hlen(Store *s, const char *key) {
    Entry *e = store_find(s, key, NULL);
    if (!e) return 0;
    if (e->type != ENTRY_HASH) return -1;
    e->last_access = ++s->clock;
    return (long long)e->hash->len;
}

int store_hgetall(Store *s, const char *key, char ***out, size_t *n_out) {
    *out = NULL;
    *n_out = 0;
    Entry *e = store_find(s, key, NULL);
    if (!e) return 0;
    if (e->type != ENTRY_HASH) return -1;
    e->last_access = ++s->clock;
    Hash *h = e->hash;
    if (h->len == 0) return 0;
    char **res = malloc(h->len * 2 * sizeof(char *));
    if (!res) return -2;
    for (size_t i = 0; i < h->len; i++) {
        res[2 * i] = strdup(h->fields[i]);
        res[2 * i + 1] = strdup(h->values[i]);
        if (!res[2 * i] || !res[2 * i + 1]) {
            for (size_t j = 0; j <= 2 * i + 1; j++) free(res[j]);
            free(res);
            return -2;
        }
    }
    *out = res;
    *n_out = h->len * 2;
    return 0;
}
