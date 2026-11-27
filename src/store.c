#define _GNU_SOURCE
#include "store.h"

#include <errno.h>
#include <math.h>
#include <stdint.h>
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

/* Forward declarations: the lazy field-expiry purge below runs from
   store_find, which sits above the definitions of both. */
static void entry_touch(Store *s, Entry *e);
static int entry_purge_expired_fields(Store *s, Entry *e, Entry **pp);
static void stream_free(Stream *st);

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

static void sdict_free(struct sdict *d);

static void set_free(Set *st) {
    if (!st) return;
    for (size_t i = 0; i < st->len; i++)
        free(st->members[i]);
    free(st->members);
    sdict_free(st->dict);
    free(st);
}

/* ---------- hash field dict ---------- */

/* A hash value keeps its fields in insertion order (the parallel
   arrays) and a hash table from field to its index in those arrays,
   so field lookup is O(1) instead of a linear scan. The dict
   entries borrow the field strings owned by the arrays, so freeing
   the dict frees only the entries, never the strings. Deleting a
   field memmoves the tail of the arrays (preserving insertion
   order) and re-indexes the fields that moved, so HDEL stays O(n).
   Modelled on the zdict the sorted set uses, but keyed by field
   with an index as the value instead of a skip-list node. */

#define HDICT_INIT_CAP 16

typedef struct hdictEntry {
    char *field; /* borrowed from the Hash arrays, not owned */
    size_t index;
    struct hdictEntry *next;
} hdictEntry;

typedef struct hdict {
    hdictEntry **buckets;
    size_t cap;
    size_t count;
} hdict;

static hdict *hdict_new(void) {
    hdict *d = calloc(1, sizeof(hdict));
    if (!d) return NULL;
    d->cap = HDICT_INIT_CAP;
    d->buckets = calloc(d->cap, sizeof(hdictEntry *));
    if (!d->buckets) {
        free(d);
        return NULL;
    }
    return d;
}

static void hdict_free(hdict *d) {
    if (!d) return;
    for (size_t i = 0; i < d->cap; i++) {
        hdictEntry *en = d->buckets[i];
        while (en) {
            hdictEntry *next = en->next;
            free(en);
            en = next;
        }
    }
    free(d->buckets);
    free(d);
}

/* Index of field in the hash's arrays, or -1 when absent. */
static long long hdict_find(hdict *d, const char *field) {
    size_t idx = hash_key(field) % d->cap;
    for (hdictEntry *en = d->buckets[idx]; en; en = en->next)
        if (strcmp(en->field, field) == 0) return (long long)en->index;
    return -1;
}

static int hdict_grow(hdict *d) {
    size_t ncap = d->cap * 2;
    hdictEntry **nb = calloc(ncap, sizeof(hdictEntry *));
    if (!nb) return 0;
    for (size_t i = 0; i < d->cap; i++) {
        hdictEntry *en = d->buckets[i];
        while (en) {
            hdictEntry *next = en->next;
            size_t idx = hash_key(en->field) % ncap;
            en->next = nb[idx];
            nb[idx] = en;
            en = next;
        }
    }
    free(d->buckets);
    d->buckets = nb;
    d->cap = ncap;
    return 1;
}

/* Point the field's dict entry at index, adding the entry when the
   field is new. Returns 0 on OOM; re-indexing an existing field
   cannot fail, it only updates the stored index. */
static int hdict_set(hdict *d, char *field, size_t index) {
    size_t idx = hash_key(field) % d->cap;
    for (hdictEntry *en = d->buckets[idx]; en; en = en->next)
        if (strcmp(en->field, field) == 0) {
            en->index = index;
            return 1;
        }
    if (d->count + 1 > d->cap * 3 / 4 && !hdict_grow(d)) return 0;
    idx = hash_key(field) % d->cap;
    hdictEntry *en = malloc(sizeof(hdictEntry));
    if (!en) return 0;
    en->field = field;
    en->index = index;
    en->next = d->buckets[idx];
    d->buckets[idx] = en;
    d->count++;
    return 1;
}

static void hdict_remove(hdict *d, const char *field) {
    size_t idx = hash_key(field) % d->cap;
    hdictEntry **pp = &d->buckets[idx];
    while (*pp) {
        if (strcmp((*pp)->field, field) == 0) {
            hdictEntry *dead = *pp;
            *pp = dead->next;
            free(dead);
            d->count--;
            return;
        }
        pp = &(*pp)->next;
    }
}

/* ---------- set member dict ---------- */

/* A set value keeps its members in an unordered array and a hash
   table from member to its index in that array, so membership is
   O(1) instead of a linear scan. The dict entries borrow the member
   strings owned by the array, so freeing the dict frees only the
   entries, never the strings. Removing a member swaps the last
   member into the hole (the array is unordered) and re-indexes the
   member that moved, so SREM stays correct. Modelled on the hdict
   the hash type uses, but keyed by member. */

#define SDICT_INIT_CAP 16

typedef struct sdictEntry {
    char *member; /* borrowed from the Set array, not owned */
    size_t index;
    struct sdictEntry *next;
} sdictEntry;

typedef struct sdict {
    sdictEntry **buckets;
    size_t cap;
    size_t count;
} sdict;

static sdict *sdict_new(void) {
    sdict *d = calloc(1, sizeof(sdict));
    if (!d) return NULL;
    d->cap = SDICT_INIT_CAP;
    d->buckets = calloc(d->cap, sizeof(sdictEntry *));
    if (!d->buckets) {
        free(d);
        return NULL;
    }
    return d;
}

static void sdict_free(sdict *d) {
    if (!d) return;
    for (size_t i = 0; i < d->cap; i++) {
        sdictEntry *en = d->buckets[i];
        while (en) {
            sdictEntry *next = en->next;
            free(en);
            en = next;
        }
    }
    free(d->buckets);
    free(d);
}

/* Index of member in the set's array, or -1 when absent. */
static long long sdict_find(sdict *d, const char *member) {
    size_t idx = hash_key(member) % d->cap;
    for (sdictEntry *en = d->buckets[idx]; en; en = en->next)
        if (strcmp(en->member, member) == 0) return (long long)en->index;
    return -1;
}

static int sdict_grow(sdict *d) {
    size_t ncap = d->cap * 2;
    sdictEntry **nb = calloc(ncap, sizeof(sdictEntry *));
    if (!nb) return 0;
    for (size_t i = 0; i < d->cap; i++) {
        sdictEntry *en = d->buckets[i];
        while (en) {
            sdictEntry *next = en->next;
            size_t idx = hash_key(en->member) % ncap;
            en->next = nb[idx];
            nb[idx] = en;
            en = next;
        }
    }
    free(d->buckets);
    d->buckets = nb;
    d->cap = ncap;
    return 1;
}

/* Point the member's dict entry at index, adding the entry when the
   member is new. Returns 0 on OOM; re-indexing an existing member
   cannot fail, it only updates the stored index. */
static int sdict_set(sdict *d, char *member, size_t index) {
    size_t idx = hash_key(member) % d->cap;
    for (sdictEntry *en = d->buckets[idx]; en; en = en->next)
        if (strcmp(en->member, member) == 0) {
            en->index = index;
            return 1;
        }
    if (d->count + 1 > d->cap * 3 / 4 && !sdict_grow(d)) return 0;
    idx = hash_key(member) % d->cap;
    sdictEntry *en = malloc(sizeof(sdictEntry));
    if (!en) return 0;
    en->member = member;
    en->index = index;
    en->next = d->buckets[idx];
    d->buckets[idx] = en;
    d->count++;
    return 1;
}

static void sdict_remove(sdict *d, const char *member) {
    size_t idx = hash_key(member) % d->cap;
    sdictEntry **pp = &d->buckets[idx];
    while (*pp) {
        if (strcmp((*pp)->member, member) == 0) {
            sdictEntry *dead = *pp;
            *pp = dead->next;
            free(dead);
            d->count--;
            return;
        }
        pp = &(*pp)->next;
    }
}

static void hash_free(Hash *h) {
    if (!h) return;
    for (size_t i = 0; i < h->len; i++) {
        free(h->fields[i]);
        free(h->values[i]);
    }
    free(h->fields);
    free(h->values);
    free(h->field_expire);
    hdict_free(h->dict);
    free(h);
}

/* ---------- sorted sets: skip list + member dict ---------- */

/* A skip list orders the set by (score, member) — score ascending,
   then member lexicographically, as in Redis — so insert, delete,
   rank and range-by-index are O(log n). A hash table maps each
   member to its skip-list node, so member lookup is O(1). The old
   sorted array did all of these with linear scans and memmoves. */

#define ZSL_MAXLEVEL 32
#define ZSL_P 0.25

typedef struct zslLevel {
    struct zslNode *forward;
    unsigned long span;
} zslLevel;

typedef struct zslNode {
    char *member; /* owned by the node */
    double score;
    struct zslNode *backward;
    zslLevel level[]; /* sized to the node's level */
} zslNode;

typedef struct zsl {
    zslNode *header;
    zslNode *tail;
    unsigned long length;
    int level;
} zsl;

typedef struct zdictEntry {
    zslNode *node; /* the key is node->member */
    struct zdictEntry *next;
} zdictEntry;

typedef struct zdict {
    zdictEntry **buckets;
    size_t cap;
    size_t count;
} zdict;

/* Ordering: score ascending, then member lexicographically. */
static int zset_cmp(double s1, const char *m1, double s2, const char *m2) {
    if (s1 < s2) return -1;
    if (s1 > s2) return 1;
    return strcmp(m1, m2);
}

static int zsl_random_level(void) {
    int level = 1;
    while ((double)rand() / (double)RAND_MAX < ZSL_P &&
           level < ZSL_MAXLEVEL)
        level++;
    return level;
}

static zslNode *zsl_create_node(int level, double score,
                                const char *member) {
    zslNode *n = malloc(sizeof(*n) + (size_t)level * sizeof(zslLevel));
    if (!n) return NULL;
    n->member = strdup(member);
    if (!n->member) {
        free(n);
        return NULL;
    }
    n->score = score;
    n->backward = NULL;
    return n;
}

static zsl *zsl_create(void) {
    zsl *sl = malloc(sizeof(*sl));
    if (!sl) return NULL;
    sl->level = 1;
    sl->length = 0;
    sl->header = zsl_create_node(ZSL_MAXLEVEL, 0, "");
    if (!sl->header) {
        free(sl);
        return NULL;
    }
    for (int i = 0; i < ZSL_MAXLEVEL; i++) {
        sl->header->level[i].forward = NULL;
        sl->header->level[i].span = 0;
    }
    sl->header->backward = NULL;
    sl->tail = NULL;
    return sl;
}

static void zsl_free(zsl *sl) {
    if (!sl) return;
    zslNode *n = sl->header->level[0].forward;
    free(sl->header->member);
    free(sl->header);
    while (n) {
        zslNode *next = n->level[0].forward;
        free(n->member);
        free(n);
        n = next;
    }
    free(sl);
}

/* Insert a new node. The (score, member) pair must not already be
   in the list. Returns the node, or NULL on OOM. */
static zslNode *zsl_insert(zsl *sl, double score, const char *member) {
    zslNode *update[ZSL_MAXLEVEL];
    unsigned long rank[ZSL_MAXLEVEL];
    zslNode *x = sl->header;
    for (int i = sl->level - 1; i >= 0; i--) {
        rank[i] = i == sl->level - 1 ? 0 : rank[i + 1];
        while (x->level[i].forward &&
               zset_cmp(x->level[i].forward->score,
                        x->level[i].forward->member, score, member) < 0) {
            rank[i] += x->level[i].span;
            x = x->level[i].forward;
        }
        update[i] = x;
    }
    int level = zsl_random_level();
    if (level > sl->level) {
        for (int i = sl->level; i < level; i++) {
            rank[i] = 0;
            update[i] = sl->header;
            sl->header->level[i].span = sl->length;
        }
        sl->level = level;
    }
    zslNode *n = zsl_create_node(level, score, member);
    if (!n) return NULL;
    for (int i = 0; i < level; i++) {
        n->level[i].forward = update[i]->level[i].forward;
        update[i]->level[i].forward = n;
        n->level[i].span = update[i]->level[i].span - (rank[0] - rank[i]);
        update[i]->level[i].span = (rank[0] - rank[i]) + 1;
    }
    for (int i = level; i < sl->level; i++)
        update[i]->level[i].span++;
    n->backward = update[0] == sl->header ? NULL : update[0];
    if (n->level[0].forward)
        n->level[0].forward->backward = n;
    else
        sl->tail = n;
    sl->length++;
    return n;
}

/* Unlink the node for (score, member) and return it detached — the
   caller frees the node and its member string — or NULL when the
   pair is not in the list. */
static zslNode *zsl_delete(zsl *sl, double score, const char *member) {
    zslNode *update[ZSL_MAXLEVEL];
    zslNode *x = sl->header;
    for (int i = sl->level - 1; i >= 0; i--) {
        while (x->level[i].forward &&
               zset_cmp(x->level[i].forward->score,
                        x->level[i].forward->member, score, member) < 0)
            x = x->level[i].forward;
        update[i] = x;
    }
    x = x->level[0].forward;
    if (!x || x->score != score || strcmp(x->member, member) != 0)
        return NULL;
    for (int i = 0; i < sl->level; i++) {
        if (update[i]->level[i].forward == x) {
            update[i]->level[i].span += x->level[i].span - 1;
            update[i]->level[i].forward = x->level[i].forward;
        } else {
            update[i]->level[i].span -= 1;
        }
    }
    if (x->level[0].forward)
        x->level[0].forward->backward = x->backward;
    else
        sl->tail = x->backward;
    while (sl->level > 1 &&
           sl->header->level[sl->level - 1].forward == NULL)
        sl->level--;
    sl->length--;
    return x;
}

/* 1-based rank of (score, member), or 0 when it is not in the list. */
static unsigned long zsl_get_rank(zsl *sl, double score,
                                  const char *member) {
    unsigned long rank = 0;
    zslNode *x = sl->header;
    for (int i = sl->level - 1; i >= 0; i--) {
        while (x->level[i].forward &&
               zset_cmp(x->level[i].forward->score,
                        x->level[i].forward->member, score, member) <= 0) {
            rank += x->level[i].span;
            x = x->level[i].forward;
        }
        /* The header's member is "" with score 0, so it must not
           match a real member of the same name and score. */
        if (x != sl->header && x->score == score &&
            strcmp(x->member, member) == 0)
            return rank;
    }
    return 0;
}

/* The node at 1-based rank, or NULL when rank is out of range. */
static zslNode *zsl_get_by_rank(zsl *sl, unsigned long rank) {
    if (rank == 0 || rank > sl->length) return NULL;
    unsigned long traversed = 0;
    zslNode *x = sl->header;
    for (int i = sl->level - 1; i >= 0; i--) {
        while (x->level[i].forward &&
               traversed + x->level[i].span <= rank) {
            traversed += x->level[i].span;
            x = x->level[i].forward;
        }
        if (traversed == rank) return x;
    }
    return NULL;
}

/* First node whose score lies in the range: score >= min (or
   > min when min_excl), and score <= max (or < max when max_excl).
   Returns NULL when no node matches. The caller must have checked
   min <= max. */
static zslNode *zsl_first_in_range(zsl *sl, double min, int min_excl,
                                   double max, int max_excl) {
    zslNode *x = sl->header;
    for (int i = sl->level - 1; i >= 0; i--) {
        while (x->level[i].forward &&
               (min_excl ? x->level[i].forward->score <= min
                         : x->level[i].forward->score < min))
            x = x->level[i].forward;
    }
    x = x->level[0].forward;
    if (!x) return NULL;
    if (max_excl ? x->score >= max : x->score > max) return NULL;
    return x;
}

#define ZDICT_INIT_CAP 16

static zdict *zdict_new(void) {
    zdict *d = calloc(1, sizeof(zdict));
    if (!d) return NULL;
    d->cap = ZDICT_INIT_CAP;
    d->buckets = calloc(d->cap, sizeof(zdictEntry *));
    if (!d->buckets) {
        free(d);
        return NULL;
    }
    return d;
}

static void zdict_free(zdict *d) {
    if (!d) return;
    for (size_t i = 0; i < d->cap; i++) {
        zdictEntry *en = d->buckets[i];
        while (en) {
            zdictEntry *next = en->next;
            free(en);
            en = next;
        }
    }
    free(d->buckets);
    free(d);
}

static zslNode *zdict_find(zdict *d, const char *member) {
    size_t idx = hash_key(member) % d->cap;
    for (zdictEntry *en = d->buckets[idx]; en; en = en->next)
        if (strcmp(en->node->member, member) == 0) return en->node;
    return NULL;
}

static int zdict_grow(zdict *d) {
    size_t ncap = d->cap * 2;
    zdictEntry **nb = calloc(ncap, sizeof(zdictEntry *));
    if (!nb) return 0;
    for (size_t i = 0; i < d->cap; i++) {
        zdictEntry *en = d->buckets[i];
        while (en) {
            zdictEntry *next = en->next;
            size_t idx = hash_key(en->node->member) % ncap;
            en->next = nb[idx];
            nb[idx] = en;
            en = next;
        }
    }
    free(d->buckets);
    d->buckets = nb;
    d->cap = ncap;
    return 1;
}

/* Point the member's dict entry at node, adding the entry when the
   member is new. Returns 0 on OOM; replacing an existing entry
   cannot fail. */
static int zdict_set(zdict *d, zslNode *node) {
    size_t idx = hash_key(node->member) % d->cap;
    for (zdictEntry *en = d->buckets[idx]; en; en = en->next)
        if (strcmp(en->node->member, node->member) == 0) {
            en->node = node;
            return 1;
        }
    if (d->count + 1 > d->cap * 3 / 4 && !zdict_grow(d)) return 0;
    idx = hash_key(node->member) % d->cap;
    zdictEntry *en = malloc(sizeof(zdictEntry));
    if (!en) return 0;
    en->node = node;
    en->next = d->buckets[idx];
    d->buckets[idx] = en;
    d->count++;
    return 1;
}

static void zdict_remove(zdict *d, const char *member) {
    size_t idx = hash_key(member) % d->cap;
    zdictEntry **pp = &d->buckets[idx];
    while (*pp) {
        if (strcmp((*pp)->node->member, member) == 0) {
            zdictEntry *dead = *pp;
            *pp = dead->next;
            free(dead);
            d->count--;
            return;
        }
        pp = &(*pp)->next;
    }
}

static void zset_free(ZSet *z) {
    if (!z) return;
    zdict_free(z->dict);
    zsl_free(z->sl);
    free(z);
}

static void entry_free(Entry *e) {
    free(e->key);
    if (e->type == ENTRY_LIST) list_free(e->list);
    else if (e->type == ENTRY_SET) set_free(e->set);
    else if (e->type == ENTRY_HASH) hash_free(e->hash);
    else if (e->type == ENTRY_ZSET) zset_free(e->zset);
    else if (e->type == ENTRY_STREAM) stream_free(e->stream);
    else free(e->value);
    free(e);
}

static ZSet *zset_new(void) {
    ZSet *z = calloc(1, sizeof(ZSet));
    if (!z) return NULL;
    z->sl = zsl_create();
    z->dict = zdict_new();
    if (!z->sl || !z->dict) {
        zset_free(z);
        return NULL;
    }
    return z;
}

/* The node for member, or NULL when absent. O(1) via the dict. */
static zslNode *zset_find(ZSet *z, const char *member) {
    return zdict_find(z->dict, member);
}

/* Set member to score. Returns 1 new, 0 updated (score changed),
   2 no-op (member already had exactly that score), -1 = OOM. */
static int zset_put(ZSet *z, const char *member, double score) {
    zslNode *node = zset_find(z, member);
    if (!node) {
        zslNode *n = zsl_insert(z->sl, score, member);
        if (!n) return -1;
        if (!zdict_set(z->dict, n)) {
            zslNode *gone = zsl_delete(z->sl, score, member);
            if (gone) {
                free(gone->member);
                free(gone);
            }
            return -1;
        }
        z->len++;
        return 1;
    }
    if (node->score == score) return 2;
    /* A changed score can move the member: insert at the new
       position first, then drop the old node, so an OOM on the
       insert leaves the set untouched. */
    zslNode *n = zsl_insert(z->sl, score, member);
    if (!n) return -1;
    zslNode *old = zsl_delete(z->sl, node->score, member);
    zdict_set(z->dict, n);
    if (old) {
        free(old->member);
        free(old);
    }
    return 0;
}

/* Remove a member. Returns 1 removed, 0 when the member was absent. */
static int zset_remove(ZSet *z, const char *member) {
    zslNode *node = zset_find(z, member);
    if (!node) return 0;
    zdict_remove(z->dict, member);
    zslNode *gone = zsl_delete(z->sl, node->score, member);
    if (gone) {
        free(gone->member);
        free(gone);
    }
    z->len--;
    return 1;
}

/* Visit every member in (score, member) order: level 0 of the skip
   list is the set in order. */
int zset_foreach(const ZSet *z, zset_foreach_fn fn, void *arg) {
    if (!z || !z->sl) return -1;
    for (zslNode *n = z->sl->header->level[0].forward; n;
         n = n->level[0].forward) {
        int rc = fn(n->member, n->score, arg);
        if (rc) return rc;
    }
    return 0;
}

static Hash *hash_new(void) {
    Hash *h = calloc(1, sizeof(Hash));
    if (!h) return NULL;
    h->cap = 8;
    h->fields = calloc(h->cap, sizeof(char *));
    h->values = calloc(h->cap, sizeof(char *));
    h->field_expire = malloc(h->cap * sizeof(long long));
    h->dict = hdict_new();
    if (!h->fields || !h->values || !h->field_expire || !h->dict) {
        hash_free(h);
        return NULL;
    }
    for (size_t i = 0; i < h->cap; i++) h->field_expire[i] = -1;
    return h;
}

/* Index of field, or -1 when absent. */
static long long hash_find(Hash *h, const char *field) {
    return hdict_find(h->dict, field);
}

/* Set field to value, adding it if absent. Returns 1 new, 0 updated,
   -1 = OOM. clear_ttl mirrors Redis: an HSET that overwrites a field
   clears that field's expiry, while an increment (HINCRBY and
   friends) leaves it alone. A new field never has an expiry. */
static int hash_put(Hash *h, const char *field, const char *value,
                    int clear_ttl) {
    long long idx = hash_find(h, field);
    if (idx >= 0) {
        char *v = strdup(value);
        if (!v) return -1;
        free(h->values[idx]);
        h->values[idx] = v;
        if (clear_ttl) h->field_expire[idx] = -1;
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
        long long *ne = realloc(h->field_expire, ncap * sizeof(long long));
        if (!nf || !nv || !ne) {
            free(f);
            free(v);
            return -1;
        }
        h->fields = nf;
        h->values = nv;
        h->field_expire = ne;
        h->cap = ncap;
    }
    h->fields[h->len] = f;
    h->values[h->len] = v;
    h->field_expire[h->len] = -1;
    h->len++;
    if (!hdict_set(h->dict, f, h->len - 1)) {
        /* The field is appended but unindexed; roll the append back
           rather than leave a field no lookup can find. */
        h->len--;
        free(h->fields[h->len]);
        free(h->values[h->len]);
        h->fields[h->len] = NULL;
        h->values[h->len] = NULL;
        h->field_expire[h->len] = -1;
        return -1;
    }
    return 1;
}

/* Remove field if present, preserving insertion order of the rest.
   Returns 1 removed, 0 absent. */
static int hash_remove(Hash *h, const char *field) {
    long long idx = hash_find(h, field);
    if (idx < 0) return 0;
    hdict_remove(h->dict, field);
    free(h->fields[idx]);
    free(h->values[idx]);
    for (size_t i = (size_t)idx + 1; i < h->len; i++) {
        h->fields[i - 1] = h->fields[i];
        h->values[i - 1] = h->values[i];
        h->field_expire[i - 1] = h->field_expire[i];
    }
    h->len--;
    h->fields[h->len] = NULL;
    h->values[h->len] = NULL;
    h->field_expire[h->len] = -1;
    /* The fields that moved down one slot need their dict indexes
       moved with them; re-indexing an existing field cannot fail. */
    for (size_t i = (size_t)idx; i < h->len; i++)
        hdict_set(h->dict, h->fields[i], i);
    return 1;
}

/* Remove every field whose expiry has passed. Returns the number
   removed. Walked from the end so a removal never shifts the
   indexes of the fields still to visit. */
static int hash_purge_expired(Hash *h, long long now) {
    int removed = 0;
    for (long long i = (long long)h->len - 1; i >= 0; i--) {
        if (h->field_expire[i] != -1 && now >= h->field_expire[i]) {
            hash_remove(h, h->fields[i]);
            removed++;
        }
    }
    return removed;
}

/* Lazy field expiry for one hash entry: drop the fields whose time
   is up, and delete the entry itself when its last field goes, as
   in Redis. pp is the bucket link pointing at e. Returns 1 when
   the entry was deleted, 0 otherwise. A purge that removes fields
   is a real change, so it bumps the version and invalidates a
   WATCH, exactly as a key expiring does. */
static int entry_purge_expired_fields(Store *s, Entry *e, Entry **pp) {
    if (e->type != ENTRY_HASH) return 0;
    if (hash_purge_expired(e->hash, now_ms()) == 0) return 0;
    if (e->hash->len == 0) {
        *pp = e->next;
        entry_free(e);
        s->count--;
        return 1;
    }
    entry_touch(s, e);
    return 0;
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
    st->dict = sdict_new();
    if (!st->dict) {
        free(st->members);
        free(st);
        return NULL;
    }
    return st;
}

/* Index of member, or -1 when absent. O(1) via the member dict. */
static long long set_find(Set *st, const char *member) {
    return sdict_find(st->dict, member);
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
    st->members[st->len] = m;
    if (!sdict_set(st->dict, m, st->len)) {
        /* The dict insert failed (OOM): roll the array append back
           so the set and its dict stay in agreement. */
        free(m);
        return -1;
    }
    st->len++;
    return 1;
}

/* Remove member if present. Returns 1 removed, 0 absent. The last
   member swaps into the hole, as before, and is re-indexed in the
   dict so a later lookup finds it at its new slot. */
static int set_remove(Set *st, const char *member) {
    long long idx = set_find(st, member);
    if (idx < 0) return 0;
    sdict_remove(st->dict, member);
    free(st->members[idx]);
    st->len--;
    if ((size_t)idx != st->len) {
        st->members[idx] = st->members[st->len];
        sdict_set(st->dict, st->members[idx], (size_t)idx);
    }
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

/* Find entry; if found but expired, delete it and return NULL.
   A hash found here also has its expired fields purged first, so
   every access — read, write, EXISTS, TYPE, TTL, WATCH — sees
   only live fields, and a hash emptied by field expiry reads as
   a missing key. */
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
            if (e->type == ENTRY_HASH &&
                entry_purge_expired_fields(s, e, prev))
                return NULL;
            if (prev_out) *prev_out = prev;
            return e;
        }
        prev = &e->next;
        e = e->next;
    }
    return NULL;
}

/* Stamp an entry with the next modification sequence number: every
   real change to a key calls this, so WATCH can tell the key moved.
   A write that changes nothing (SADD of a member already present,
   HSET of an identical value) does not touch the version. */
static void entry_touch(Store *s, Entry *e) {
    e->version = ++s->mod_seq;
}

int store_set(Store *s, const char *key, const char *value, long long expire_at) {
    return store_set_len(s, key, value, strlen(value), expire_at);
}

int store_set_len(Store *s, const char *key, const char *value, size_t vlen,
                  long long expire_at) {
    Entry *e = store_find(s, key, NULL);
    if (e) {
        char *nv = malloc(vlen + 1);
        if (!nv) return 0;
        memcpy(nv, value, vlen);
        nv[vlen] = '\0';
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
        } else if (e->type == ENTRY_ZSET) {
            zset_free(e->zset);
            e->zset = NULL;
            e->type = ENTRY_STRING;
        } else if (e->type == ENTRY_STREAM) {
            stream_free(e->stream);
            e->stream = NULL;
            e->type = ENTRY_STRING;
        }
        free(e->value);
        e->value = nv;
        e->value_len = vlen;
        e->expire_at = expire_at;
        e->last_access = ++s->clock;
        entry_touch(s, e);
        return 1;
    }
    if (s->count * LOAD_FACTOR_DEN > s->cap * LOAD_FACTOR_NUM)
        store_resize(s);
    e = calloc(1, sizeof(Entry));
    if (!e) return 0;
    e->key = strdup(key);
    e->value = malloc(vlen + 1);
    if (!e->key || !e->value) {
        entry_free(e);
        return 0;
    }
    memcpy(e->value, value, vlen);
    e->value[vlen] = '\0';
    e->value_len = vlen;
    e->type = ENTRY_STRING;
    e->expire_at = expire_at;
    e->last_access = ++s->clock;
    e->version = ++s->mod_seq;
    size_t idx = hash_key(key) % s->cap;
    e->next = s->buckets[idx];
    s->buckets[idx] = e;
    s->count++;
    return 1;
}

char *store_get(Store *s, const char *key) {
    size_t len;
    return store_get_len(s, key, &len);
}

char *store_get_len(Store *s, const char *key, size_t *len_out) {
    Entry *e = store_find(s, key, NULL);
    if (!e || e->type != ENTRY_STRING) return NULL;
    e->last_access = ++s->clock;
    char *copy = malloc(e->value_len + 1);
    if (!copy) return NULL;
    memcpy(copy, e->value, e->value_len);
    copy[e->value_len] = '\0';
    if (len_out) *len_out = e->value_len;
    return copy;
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

/* Strict double parse: the whole string must be one number, and NaN
   is rejected (Redis refuses NaN field values too). Infinities are
   accepted here; the caller decides whether an infinite result is
   an overflow. */
static int parse_double(const char *s, double *out) {
    if (!s || !*s) return 0;
    char *end = NULL;
    errno = 0;
    double v = strtod(s, &end);
    if (errno != 0 || end == s || *end != '\0' || isnan(v)) return 0;
    *out = v;
    return 1;
}

int store_incr(Store *s, const char *key, long long delta, long long *out) {
    Entry *e = store_find(s, key, NULL);
    long long v = 0;
    if (e) {
        if (e->type != ENTRY_STRING) return -1;
        /* A value holding a NUL byte is not an integer, even when it
           starts with digits: parse_ll would stop at the NUL. */
        if (e->value_len != strlen(e->value) || !parse_ll(e->value, &v)) return -2;
    }
    v += delta;
    char vbuf[32];
    snprintf(vbuf, sizeof(vbuf), "%lld", v);
    if (e) {
        /* Update the stored string in place when it fits; grow the
           allocation only when the new decimal form is longer than
           the old one. The expiry is untouched, so the TTL survives. */
        size_t nlen = strlen(vbuf);
        if (nlen > e->value_len) {
            char *nv = realloc(e->value, nlen + 1);
            if (!nv) return -3;
            e->value = nv;
        }
        memcpy(e->value, vbuf, nlen + 1);
        e->value_len = nlen;
        e->last_access = ++s->clock;
        entry_touch(s, e);
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
        e->value_len = strlen(vbuf);
        e->type = ENTRY_STRING;
        e->expire_at = -1;
        e->last_access = ++s->clock;
        e->version = ++s->mod_seq;
        size_t idx = hash_key(key) % s->cap;
        e->next = s->buckets[idx];
        s->buckets[idx] = e;
        s->count++;
    }
    *out = v;
    return 0;
}

long long store_append(Store *s, const char *key, const char *value) {
    Entry *e = store_find(s, key, NULL);
    if (e) {
        if (e->type != ENTRY_STRING) return -1;
        /* Grow the stored string in place; the expiry is untouched,
           so APPEND preserves the TTL, as in Redis. The old length
           is value_len, not strlen: a value SETRANGE padded with
           zero bytes appends after the padding, not inside it. */
        size_t old = e->value_len;
        size_t add = strlen(value);
        char *nv = realloc(e->value, old + add + 1);
        if (!nv) return -2;
        e->value = nv;
        memcpy(e->value + old, value, add + 1);
        e->value_len = old + add;
        e->last_access = ++s->clock;
        entry_touch(s, e);
        return (long long)(old + add);
    }
    if (s->count * LOAD_FACTOR_DEN > s->cap * LOAD_FACTOR_NUM)
        store_resize(s);
    e = calloc(1, sizeof(Entry));
    if (!e) return -2;
    e->key = strdup(key);
    e->value = strdup(value);
    if (!e->key || !e->value) {
        entry_free(e);
        return -2;
    }
    e->value_len = strlen(value);
    e->type = ENTRY_STRING;
    e->expire_at = -1;
    e->last_access = ++s->clock;
    e->version = ++s->mod_seq;
    size_t idx = hash_key(key) % s->cap;
    e->next = s->buckets[idx];
    s->buckets[idx] = e;
    s->count++;
    return (long long)strlen(value);
}

int store_getrange(Store *s, const char *key, long long start, long long stop,
                   char **out, size_t *n_out) {
    Entry *e = store_find(s, key, NULL);
    *out = NULL;
    *n_out = 0;
    if (!e) return 0; /* missing key: empty string, as in Redis */
    if (e->type != ENTRY_STRING) return -1;
    e->last_access = ++s->clock;
    long long len = (long long)e->value_len;
    if (start < 0) start += len;
    if (stop < 0) stop += len;
    if (start < 0) start = 0;
    if (stop >= len) stop = len - 1;
    if (start > stop || start >= len) {
        /* Empty range: still a valid (empty) reply. */
        char *empty = malloc(1);
        if (!empty) return -2;
        empty[0] = '\0';
        *out = empty;
        *n_out = 0;
        return 0;
    }
    size_t n = (size_t)(stop - start + 1);
    char *buf = malloc(n + 1);
    if (!buf) return -2;
    memcpy(buf, e->value + start, n);
    buf[n] = '\0';
    *out = buf;
    *n_out = n;
    return 0;
}

long long store_setrange(Store *s, const char *key, long long offset,
                         const char *value, size_t vlen) {
    if (offset < 0) return -3;
    if (vlen == 0) {
        /* An empty write changes nothing and creates nothing, as in
           Redis: just report the current length. */
        Entry *e = store_find(s, key, NULL);
        if (!e) return 0;
        if (e->type != ENTRY_STRING) return -1;
        return (long long)e->value_len;
    }
    Entry *e = store_find(s, key, NULL);
    if (e) {
        if (e->type != ENTRY_STRING) return -1;
        /* Grow to cover the write, zero-filling the gap; the expiry
           is untouched, so SETRANGE preserves the TTL, as in Redis. */
        size_t off = (size_t)offset;
        size_t newlen = off + vlen > e->value_len ? off + vlen : e->value_len;
        char *nv = realloc(e->value, newlen + 1);
        if (!nv) return -2;
        e->value = nv;
        if (off > e->value_len)
            memset(e->value + e->value_len, 0, off - e->value_len);
        memcpy(e->value + off, value, vlen);
        e->value[newlen] = '\0';
        e->value_len = newlen;
        e->last_access = ++s->clock;
        entry_touch(s, e);
        return (long long)newlen;
    }
    if (s->count * LOAD_FACTOR_DEN > s->cap * LOAD_FACTOR_NUM)
        store_resize(s);
    e = calloc(1, sizeof(Entry));
    if (!e) return -2;
    e->key = strdup(key);
    size_t off = (size_t)offset;
    size_t newlen = off + vlen;
    e->value = malloc(newlen + 1);
    if (!e->key || !e->value) {
        entry_free(e);
        return -2;
    }
    /* A missing key is created zero-padded up to the offset. */
    if (off > 0) memset(e->value, 0, off);
    memcpy(e->value + off, value, vlen);
    e->value[newlen] = '\0';
    e->value_len = newlen;
    e->type = ENTRY_STRING;
    e->expire_at = -1;
    e->last_access = ++s->clock;
    e->version = ++s->mod_seq;
    size_t idx = hash_key(key) % s->cap;
    e->next = s->buckets[idx];
    s->buckets[idx] = e;
    s->count++;
    return (long long)newlen;
}

/* Bit offsets are byte-major: bit 0 is the high bit of byte 0, as in
   Redis. The largest legal offset is 2^32-1 bits, the last bit of a
   512 MB string, which is Redis's maximum string size. */
#define BIT_MAX_OFFSET 4294967295LL

int store_setbit(Store *s, const char *key, long long offset, int value,
                 int *old_out, int *changed_out) {
    if (offset < 0 || offset > BIT_MAX_OFFSET) return -3;
    size_t byte = (size_t)(offset / 8);
    unsigned char mask = (unsigned char)(0x80 >> (offset % 8));
    Entry *e = store_find(s, key, NULL);
    if (e) {
        if (e->type != ENTRY_STRING) return -1;
        if (byte >= e->value_len) {
            /* The bit is past the end, so it reads 0, and setting it
               grows the value with zero bytes; the expiry is
               untouched, so SETBIT preserves the TTL, as in Redis. */
            size_t newlen = byte + 1;
            char *nv = realloc(e->value, newlen + 1);
            if (!nv) return -2;
            e->value = nv;
            memset(e->value + e->value_len, 0, newlen - e->value_len);
            e->value[newlen] = '\0';
            e->value_len = newlen;
            if (value) e->value[byte] |= (char)mask;
            e->last_access = ++s->clock;
            entry_touch(s, e);
            *old_out = 0;
            *changed_out = 1;
            return 0;
        }
        int old = (((unsigned char)e->value[byte] & mask) != 0);
        *old_out = old;
        if (old == value) {
            /* The bit already holds the requested value: nothing
               changes, so no version bump and no log — a no-op write
               does not invalidate a WATCH, as with SADD of a member
               already present. */
            *changed_out = 0;
            e->last_access = ++s->clock;
            return 0;
        }
        if (value) e->value[byte] |= (char)mask;
        else e->value[byte] &= (char)~mask;
        e->last_access = ++s->clock;
        entry_touch(s, e);
        *changed_out = 1;
        return 0;
    }
    /* A missing key is created zero-padded up to the byte, as in
       Redis — even when the bit being set is 0, the key comes into
       being, holding a string of zero bytes. */
    if (s->count * LOAD_FACTOR_DEN > s->cap * LOAD_FACTOR_NUM)
        store_resize(s);
    e = calloc(1, sizeof(Entry));
    if (!e) return -2;
    e->key = strdup(key);
    size_t newlen = byte + 1;
    e->value = calloc(newlen + 1, 1);
    if (!e->key || !e->value) {
        entry_free(e);
        return -2;
    }
    if (value) e->value[byte] |= (char)mask;
    e->value_len = newlen;
    e->type = ENTRY_STRING;
    e->expire_at = -1;
    e->last_access = ++s->clock;
    e->version = ++s->mod_seq;
    size_t idx = hash_key(key) % s->cap;
    e->next = s->buckets[idx];
    s->buckets[idx] = e;
    s->count++;
    *old_out = 0;
    *changed_out = 1;
    return 0;
}

int store_getbit(Store *s, const char *key, long long offset, int *out) {
    if (offset < 0 || offset > BIT_MAX_OFFSET) return -3;
    Entry *e = store_find(s, key, NULL);
    if (!e) {
        *out = 0;
        return 0;
    }
    if (e->type != ENTRY_STRING) return -1;
    e->last_access = ++s->clock;
    size_t byte = (size_t)(offset / 8);
    if (byte >= e->value_len) {
        *out = 0;
        return 0;
    }
    *out = (((unsigned char)e->value[byte] >> (7 - (offset % 8))) & 1);
    return 0;
}

int store_bitcount(Store *s, const char *key, long long start, long long end,
                   int has_range, long long *out) {
    Entry *e = store_find(s, key, NULL);
    *out = 0;
    if (!e) return 0; /* missing key: 0, as in Redis */
    if (e->type != ENTRY_STRING) return -1;
    e->last_access = ++s->clock;
    long long len = (long long)e->value_len;
    long long lo = 0, hi = len - 1;
    if (has_range) {
        /* Byte indexes, Redis GETRANGE-style: negative counts from
           the end, out-of-range ends are clamped. */
        lo = start < 0 ? start + len : start;
        hi = end < 0 ? end + len : end;
        if (lo < 0) lo = 0;
        if (hi >= len) hi = len - 1;
        if (lo > hi || lo >= len) return 0;
    }
    long long n = 0;
    for (long long i = lo; i <= hi; i++)
        n += __builtin_popcount((unsigned char)e->value[i]);
    *out = n;
    return 0;
}

int store_bitop(Store *s, int op, const char *dest, const char **keys, int nkeys,
                long long *len_out, int *changed_out) {
    *len_out = 0;
    *changed_out = 0;
    /* A non-string destination is an error even when the result
       would be empty, as in Redis. */
    Entry *de = store_find(s, dest, NULL);
    if (de && de->type != ENTRY_STRING) return -1;
    char **vals = calloc((size_t)nkeys, sizeof(char *));
    size_t *lens = calloc((size_t)nkeys, sizeof(size_t));
    if (!vals || !lens) {
        free(vals);
        free(lens);
        return -2;
    }
    size_t maxlen = 0;
    for (int i = 0; i < nkeys; i++) {
        Entry *e = store_find(s, keys[i], NULL);
        if (e && e->type != ENTRY_STRING) {
            for (int j = 0; j < i; j++) free(vals[j]);
            free(vals);
            free(lens);
            return -1;
        }
        if (!e) continue; /* a missing source counts as empty */
        vals[i] = store_get_len(s, keys[i], &lens[i]);
        if (!vals[i]) {
            for (int j = 0; j < i; j++) free(vals[j]);
            free(vals);
            free(lens);
            return -2;
        }
        if (lens[i] > maxlen) maxlen = lens[i];
    }
    *len_out = (long long)maxlen;
    if (maxlen == 0) {
        /* Every source is missing: the result is empty and the
           destination key is deleted, as in Redis. */
        free(vals);
        free(lens);
        if (de) {
            store_del(s, dest);
            *changed_out = 1;
        }
        return 0;
    }
    char *res = calloc(maxlen, 1);
    if (!res) {
        for (int i = 0; i < nkeys; i++) free(vals[i]);
        free(vals);
        free(lens);
        return -2;
    }
    switch (op) {
    case BITOP_AND:
        if (vals[0]) memcpy(res, vals[0], lens[0]);
        for (int i = 1; i < nkeys; i++) {
            for (size_t b = 0; b < lens[i]; b++) res[b] &= vals[i][b];
            /* Bytes past the end of this source count as zero, so
               AND zeroes them in the result — a missing source
               zeroes the whole result, as in Redis. */
            for (size_t b = lens[i]; b < maxlen; b++) res[b] = 0;
        }
        break;
    case BITOP_OR:
        for (int i = 0; i < nkeys; i++)
            for (size_t b = 0; b < lens[i]; b++) res[b] |= vals[i][b];
        break;
    case BITOP_XOR:
        for (int i = 0; i < nkeys; i++)
            for (size_t b = 0; b < lens[i]; b++) res[b] ^= vals[i][b];
        break;
    case BITOP_NOT: /* exactly one source, enforced by the caller */
        for (size_t b = 0; b < lens[0]; b++) res[b] = (char)~vals[0][b];
        break;
    }
    for (int i = 0; i < nkeys; i++) free(vals[i]);
    free(vals);
    free(lens);
    /* A result identical to what the destination already holds
       changes nothing: no version bump, no log, as with SETBIT. */
    if (de && de->value_len == maxlen && memcmp(de->value, res, maxlen) == 0) {
        free(res);
        return 0;
    }
    /* Overwrites the destination wholesale and clears its TTL, like
       a SET; the sources were all copied first, so the destination
       may also be one of them. */
    if (!store_set_len(s, dest, res, maxlen, -1)) {
        free(res);
        return -2;
    }
    free(res);
    *changed_out = 1;
    return 0;
}

/* BITFIELD helpers. Bit 0 is the high bit of byte 0, as in SETBIT,
   and the first bit of a field is its most significant bit, as in
   Redis: a field's bits run from the highest to the lowest, so the
   raw value is assembled by shifting each bit in from the right. */
static unsigned long long bitfield_get_bits(const char *buf, size_t len,
                                            unsigned long long offset, int bits) {
    unsigned long long raw = 0;
    for (int i = 0; i < bits; i++) {
        unsigned long long pos = offset + (unsigned long long)i;
        size_t byte = (size_t)(pos / 8);
        int bit = 0;
        if (byte < len)
            bit = ((unsigned char)buf[byte] >> (7 - (pos % 8))) & 1;
        raw = (raw << 1) | (unsigned long long)bit;
    }
    return raw;
}

static void bitfield_set_bits(char *buf, unsigned long long offset, int bits,
                              unsigned long long raw) {
    for (int i = 0; i < bits; i++) {
        unsigned long long pos = offset + (unsigned long long)i;
        size_t byte = (size_t)(pos / 8);
        int bit = (int)((raw >> (bits - 1 - i)) & 1ULL);
        if (bit) buf[byte] |= (char)(0x80 >> (pos % 8));
        else buf[byte] &= (char)~(0x80 >> (pos % 8));
    }
}

/* The raw bits of a signed field as a signed value. */
long long store_bitfield_to_signed(unsigned long long raw, int bits) {
    if (bits == 64) return (long long)raw;
    if (raw & (1ULL << (bits - 1)))
        return (long long)(raw - (1ULL << bits));
    return (long long)raw;
}

/* Apply the overflow policy to a SET candidate value `v` for a field
   of this type. Returns 0 and stores the raw bits to write in
   *raw_out, or 1 when the policy is FAIL and v is out of range (the
   caller replies nil and writes nothing). */
static int bitfield_set_value(long long v, int is_signed, int bits,
                              int overflow, unsigned long long *raw_out) {
    unsigned long long mask = bits == 64 ? ~0ULL : ((1ULL << bits) - 1);
    if (is_signed) {
        long long min = bits == 64 ? INT64_MIN : -(1LL << (bits - 1));
        long long max = bits == 64 ? INT64_MAX : (1LL << (bits - 1)) - 1;
        if (overflow == BITFIELD_OVERFLOW_FAIL) {
            if (v < min || v > max) return 1;
        } else if (overflow == BITFIELD_OVERFLOW_SAT) {
            if (v < min) v = min;
            if (v > max) v = max;
        }
    } else {
        unsigned long long max = (1ULL << bits) - 1; /* bits <= 63 */
        if (overflow == BITFIELD_OVERFLOW_FAIL) {
            if (v < 0 || (unsigned long long)v > max) return 1;
        } else if (overflow == BITFIELD_OVERFLOW_SAT) {
            if (v < 0) v = 0;
            if ((unsigned long long)v > max) v = (long long)max;
        }
    }
    *raw_out = (unsigned long long)v & mask;
    return 0;
}

/* Apply the overflow policy to an INCRBY: the field currently holds
   `old_raw`, and `inc` is added to it. Returns 0 and stores the raw
   bits to write in *raw_out, or 1 when the policy is FAIL and the
   sum is out of range (the caller replies nil and writes nothing).
   No wide arithmetic: WRAP adds in the unsigned domain, where the
   addition is defined modulo 2^64 and the mask keeps the low `bits`
   bits of the true sum; SAT and FAIL detect the range crossing
   first, with the int64 overflow checks keeping the signed addition
   exact (a sum past INT64_MAX is past any signed field's max, and a
   sum below INT64_MIN is below any signed field's min). */
static int bitfield_incr_value(unsigned long long old_raw, long long inc,
                               int is_signed, int bits, int overflow,
                               unsigned long long *raw_out) {
    unsigned long long mask = bits == 64 ? ~0ULL : ((1ULL << bits) - 1);
    if (overflow == BITFIELD_OVERFLOW_WRAP) {
        *raw_out = (old_raw + (unsigned long long)inc) & mask;
        return 0;
    }
    if (is_signed) {
        long long old = store_bitfield_to_signed(old_raw, bits);
        long long min = bits == 64 ? INT64_MIN : -(1LL << (bits - 1));
        long long max = bits == 64 ? INT64_MAX : (1LL << (bits - 1)) - 1;
        int over = 0, under = 0;
        long long sum = 0;
        if (inc > 0 && old > INT64_MAX - inc) over = 1;
        else if (inc < 0 && old < INT64_MIN - inc) under = 1;
        else {
            sum = old + inc;
            if (sum > max) over = 1;
            else if (sum < min) under = 1;
        }
        if (over || under) {
            if (overflow == BITFIELD_OVERFLOW_FAIL) return 1;
            sum = over ? max : min;
        }
        *raw_out = (unsigned long long)sum & mask;
        return 0;
    } else {
        unsigned long long max = (1ULL << bits) - 1; /* bits <= 63 */
        int over = 0, under = 0;
        unsigned long long sum = 0;
        if (inc >= 0) {
            sum = old_raw + (unsigned long long)inc; /* at most 2^64-2 */
            if (sum > max) over = 1;
        } else {
            unsigned long long mag = 0 - (unsigned long long)inc;
            if (old_raw < mag) under = 1;
            else sum = old_raw - mag;
        }
        if (over || under) {
            if (overflow == BITFIELD_OVERFLOW_FAIL) return 1;
            sum = over ? max : 0;
        }
        *raw_out = sum & mask;
        return 0;
    }
}

int store_bitfield(Store *s, const char *key, const BitfieldOp *ops, int nops,
                   BitfieldResult *results, int *changed_out) {
    *changed_out = 0;
    for (int i = 0; i < nops; i++) results[i].is_nil = 0, results[i].raw = 0;
    Entry *e = store_find(s, key, NULL);
    if (e && e->type != ENTRY_STRING) return -1;
    int has_write = 0;
    size_t needed = 0;
    for (int i = 0; i < nops; i++) {
        if (ops[i].kind == BITFIELD_GET) continue;
        if (ops[i].offset + (unsigned long long)ops[i].bits > 4294967296ULL)
            return -3;
        has_write = 1;
        size_t end = (size_t)((ops[i].offset + (unsigned long long)ops[i].bits + 7) / 8);
        if (end > needed) needed = end;
    }
    /* A write creates the key zero-padded when missing and grows it
       with zero bytes when a field runs past the end, preserving
       the TTL, as in Redis — even a write that then fails under
       OVERFLOW FAIL leaves the grown string behind, as in Redis. */
    int created = 0;
    if (has_write) {
        if (!e) {
            if (s->count * LOAD_FACTOR_DEN > s->cap * LOAD_FACTOR_NUM)
                store_resize(s);
            e = calloc(1, sizeof(Entry));
            if (!e) return -2;
            e->key = strdup(key);
            e->value = calloc(needed + 1, 1);
            if (!e->key || !e->value) {
                entry_free(e);
                return -2;
            }
            e->value_len = needed;
            e->type = ENTRY_STRING;
            e->expire_at = -1;
            e->last_access = ++s->clock;
            e->version = ++s->mod_seq;
            size_t idx = hash_key(key) % s->cap;
            e->next = s->buckets[idx];
            s->buckets[idx] = e;
            s->count++;
            created = 1;
        } else if (needed > e->value_len) {
            char *nv = realloc(e->value, needed + 1);
            if (!nv) return -2;
            e->value = nv;
            memset(e->value + e->value_len, 0, needed - e->value_len);
            e->value[needed] = '\0';
            e->value_len = needed;
        }
    }
    /* Remember the bytes before the ops run, so a write that leaves
       every byte identical is a no-op: no version bump, no log, as
       with SETBIT and BITOP. */
    size_t old_len = e ? e->value_len : 0;
    char *old_bytes = NULL;
    if (e && old_len > 0) {
        old_bytes = malloc(old_len);
        if (!old_bytes) return -2;
        memcpy(old_bytes, e->value, old_len);
    }
    if (e) e->last_access = ++s->clock;
    for (int i = 0; i < nops; i++) {
        const BitfieldOp *op = &ops[i];
        if (op->kind == BITFIELD_GET) {
            /* A missing key, or bits past the end, read 0. */
            results[i].raw = e ? bitfield_get_bits(e->value, e->value_len,
                                                   op->offset, op->bits) : 0;
            continue;
        }
        unsigned long long old_raw =
            bitfield_get_bits(e->value, e->value_len, op->offset, op->bits);
        unsigned long long new_raw = 0;
        if (op->kind == BITFIELD_SET) {
            if (bitfield_set_value(op->value, op->is_signed, op->bits,
                                   op->overflow, &new_raw)) {
                results[i].is_nil = 1;
                results[i].raw = old_raw;
                continue;
            }
            bitfield_set_bits(e->value, op->offset, op->bits, new_raw);
            results[i].raw = old_raw; /* SET returns the old value */
        } else { /* INCRBY */
            if (bitfield_incr_value(old_raw, op->value, op->is_signed,
                                    op->bits, op->overflow, &new_raw)) {
                results[i].is_nil = 1;
                results[i].raw = old_raw;
                continue;
            }
            bitfield_set_bits(e->value, op->offset, op->bits, new_raw);
            results[i].raw = new_raw; /* INCRBY returns the new value */
        }
    }
    if (created) {
        /* The key came into being, even if every byte is zero. */
        *changed_out = 1;
    } else if (e && (e->value_len != old_len ||
                     (old_len > 0 && memcmp(e->value, old_bytes, old_len) != 0))) {
        *changed_out = 1;
    }
    free(old_bytes);
    if (*changed_out) entry_touch(s, e);
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
    entry_touch(s, e);
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
    /* Every key is gone at once, so every watch is dirty — even a
       watch on a key that did not exist, whose version reads 0
       before and after. */
    s->flush_seq++;
}

unsigned long long store_key_version(Store *s, const char *key) {
    Entry *e = store_find(s, key, NULL);
    return e ? e->version : 0;
}

unsigned long long store_flush_seq(Store *s) {
    return s->flush_seq;
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
            } else if (e->type == ENTRY_HASH &&
                       entry_purge_expired_fields(s, e, pp)) {
                /* The hash lost its last field to expiry: the key
                   is gone, so it counts as removed too. A purge
                   that leaves fields behind only bumps the version. */
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
            } else if (e->type == ENTRY_HASH &&
                       entry_purge_expired_fields(s, e, pp)) {
                /* The hash lost its last field to expiry, so the
                   key is gone and matches nothing. */
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

/* Reverse every bit of v, the way Redis's dictScan reverses its
   cursor: the scan walks buckets in reverse-bit order, so a table
   that doubles mid-iteration still has every bucket visited exactly
   once. */
static unsigned long long rev_bits(unsigned long long v) {
    unsigned long long s = 8 * sizeof(v); /* bit size */
    unsigned long long mask = ~0ULL;
    while ((s >>= 1) > 0) {
        mask ^= (mask << s);
        v = ((v >> s) & mask) | ((v << s) & ~mask);
    }
    return v;
}

unsigned long long store_scan(Store *s, unsigned long long cursor,
                              const char *pattern, size_t count,
                              char ***keys_out, size_t *n_out) {
    *keys_out = NULL;
    *n_out = 0;
    size_t n = 0, cap = 0;
    char **keys = NULL;
    /* The table cannot resize during this call (single-threaded
       event loop), so the mask is fixed for the whole call; the
       next call re-reads it, which is what makes a resize between
       calls safe. */
    size_t m0 = s->cap - 1;
    unsigned long long v = cursor;
    do {
        Entry *e = s->buckets[(size_t)(v & m0)];
        while (e) {
            if (!entry_expired(e) && glob_match(pattern, e->key)) {
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
            e = e->next;
        }
        /* Advance the cursor in reverse-bit order. Setting the bits
           above the table size first is what keeps the walk correct
           when the table has grown since the cursor was issued. */
        v |= ~((unsigned long long)m0);
        v = rev_bits(v);
        v++;
        v = rev_bits(v);
    } while (v != 0 && n < count);
done:
    *keys_out = keys;
    *n_out = n;
    return v;
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
    if (e->type == ENTRY_ZSET) return 5;
    if (e->type == ENTRY_STREAM) return 6;
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
        entry_touch(s, e);
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
    e->version = ++s->mod_seq;
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
        entry_touch(s, e);
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

/* Re-lay a circular list out from index 0, keeping the capacity: the
   middle-insert in store_linsert shifts pointers with memmove, which
   only works on a linear layout. */
static int list_linearize(List *l) {
    if (l->head == 0) return 1;
    char **ni = calloc(l->cap, sizeof(char *));
    if (!ni) return 0;
    for (size_t i = 0; i < l->len; i++)
        ni[i] = l->items[(l->head + i) % l->cap];
    free(l->items);
    l->items = ni;
    l->head = 0;
    return 1;
}

int store_lindex(Store *s, const char *key, long long index, char **out) {
    *out = NULL;
    Entry *e = store_find(s, key, NULL);
    if (!e) return 0;
    if (e->type != ENTRY_LIST) return -1;
    e->last_access = ++s->clock;
    List *l = e->list;
    long long len = (long long)l->len;
    if (index < 0) index += len;
    if (index < 0 || index >= len) return 0;
    *out = strdup(l->items[(l->head + (size_t)index) % l->cap]);
    return *out ? 0 : -2;
}

int store_lset(Store *s, const char *key, long long index, const char *elem) {
    Entry *e = store_find(s, key, NULL);
    if (!e) return -3;
    if (e->type != ENTRY_LIST) return -1;
    List *l = e->list;
    long long len = (long long)l->len;
    if (index < 0) index += len;
    if (index < 0 || index >= len) return -4;
    size_t pos = (l->head + (size_t)index) % l->cap;
    if (strcmp(l->items[pos], elem) == 0) return 1; /* no-op: same value */
    char *ne = strdup(elem);
    if (!ne) return -2;
    free(l->items[pos]);
    l->items[pos] = ne;
    e->last_access = ++s->clock;
    entry_touch(s, e);
    return 0;
}

long long store_linsert(Store *s, const char *key, int before,
                        const char *pivot, const char *elem) {
    Entry *e = store_find(s, key, NULL);
    if (!e) return -1;
    if (e->type != ENTRY_LIST) return -2;
    List *l = e->list;
    size_t pos = l->len; /* stays len when the pivot is absent */
    for (size_t i = 0; i < l->len; i++) {
        if (strcmp(l->items[(l->head + i) % l->cap], pivot) == 0) {
            pos = i;
            break;
        }
    }
    if (pos == l->len) return 0;
    if (!before) pos++; /* insert after the pivot */
    char *ne = strdup(elem);
    if (!ne) return -3;
    if (l->len == l->cap && !list_grow(l)) {
        free(ne);
        return -3;
    }
    if (!list_linearize(l)) {
        free(ne);
        return -3;
    }
    memmove(&l->items[pos + 1], &l->items[pos],
            (l->len - pos) * sizeof(char *));
    l->items[pos] = ne;
    l->len++;
    e->last_access = ++s->clock;
    entry_touch(s, e);
    return (long long)l->len;
}

/* Push onto an existing list only: a missing key is a no-op, as in
   Redis. Returns the new length, 0 when the key is missing,
   -1 = wrong type, -2 = out of memory. */
static long long store_list_pushx(Store *s, const char *key, const char *elem,
                                  int at_head) {
    Entry *e = store_find(s, key, NULL);
    if (!e) return 0;
    if (e->type != ENTRY_LIST) return -1;
    if (!(at_head ? list_push_head(e->list, elem)
                  : list_push_tail(e->list, elem)))
        return -2;
    e->last_access = ++s->clock;
    entry_touch(s, e);
    return (long long)e->list->len;
}

long long store_lpushx(Store *s, const char *key, const char *elem) {
    return store_list_pushx(s, key, elem, 1);
}

long long store_rpushx(Store *s, const char *key, const char *elem) {
    return store_list_pushx(s, key, elem, 0);
}

/* Keep only the elements from start to stop inclusive, negative
   indexes count from the end, as in Redis. An empty range deletes
   the key. Preserves the key's TTL. Returns 0 when the key existed
   (trimmed, or deleted by an empty range), 1 when the key is
   missing, -1 = wrong type, -2 = out of memory. */
int store_ltrim(Store *s, const char *key, long long start, long long stop) {
    Entry **pp = NULL;
    Entry *e = store_find(s, key, &pp);
    if (!e) return 1;
    if (e->type != ENTRY_LIST) return -1;
    List *l = e->list;
    long long len = (long long)l->len;
    if (start < 0) start += len;
    if (stop < 0) stop += len;
    if (start < 0) start = 0;
    if (stop >= len) stop = len - 1;
    if (start > stop || start >= len) {
        /* Empty range: the key is gone, as in Redis. */
        *pp = e->next;
        entry_free(e);
        s->count--;
        return 0;
    }
    size_t keep = (size_t)(stop - start + 1);
    if (keep == l->len) {
        e->last_access = ++s->clock;
        entry_touch(s, e);
        return 0;
    }
    if (!list_linearize(l)) return -2;
    for (size_t i = 0; i < (size_t)start; i++) free(l->items[i]);
    for (size_t i = (size_t)stop + 1; i < l->len; i++) free(l->items[i]);
    if (start > 0)
        memmove(l->items, &l->items[start], keep * sizeof(char *));
    l->len = keep;
    l->head = 0;
    e->last_access = ++s->clock;
    entry_touch(s, e);
    return 0;
}

/* Move one element from source to dest, as in Redis: from_head picks
   the head (LEFT) or the tail (RIGHT) of the source, to_head the
   head or tail of the destination. The source is deleted when
   emptied; the destination keeps its TTL, a new destination key has
   none; the source keeps its TTL. Returns 1 on success (*out is a
   malloc'd copy of the moved element), 0 when the source is missing
   or empty, -1 = wrong type on either key, -2 = out of memory. */
int store_lmove(Store *s, const char *source, const char *dest,
                int from_head, int to_head, char **out) {
    *out = NULL;
    Entry *se = store_find(s, source, NULL);
    if (!se) return 0;
    if (se->type != ENTRY_LIST) return -1;
    List *sl = se->list;
    if (sl->len == 0) return 0;
    /* Copy the element, then push the copy onto the destination
       first: the destination is created when missing, and a
       wrong-type destination errors before the source is touched.
       The copy is ours to hand back, so a grow of the source list
       during the push cannot leave *out dangling. */
    char *el = strdup(sl->items[(sl->head + (from_head ? 0 : sl->len - 1))
                                % sl->cap]);
    if (!el) return -2;
    long long n = to_head ? store_lpush(s, dest, el)
                          : store_rpush(s, dest, el);
    if (n < 0) {
        free(el);
        return (int)n; /* -1 wrong type, -2 out of memory */
    }
    /* The push succeeded: now take the element off the source. When
       source and destination are the same key this is a rotation. */
    int wrongtype = 0;
    char *moved = from_head ? store_lpop(s, source, &wrongtype)
                            : store_rpop(s, source, &wrongtype);
    if (!moved) {
        /* Unreachable: the source was a non-empty list above, and
           the destination push only added to it (same key or not). */
        free(el);
        return -2;
    }
    free(moved);
    *out = el;
    return 1;
}

/* ---------- sets ---------- */

long long store_sadd(Store *s, const char *key, const char *member) {
    Entry *e = store_find(s, key, NULL);
    if (e) {
        if (e->type != ENTRY_SET) return -1;
        int rc = set_add(e->set, member);
        if (rc < 0) return -2;
        if (rc) {
            e->last_access = ++s->clock;
            entry_touch(s, e);
        }
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
    e->version = ++s->mod_seq;
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
        entry_touch(s, e);
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

int store_sscan(Store *s, const char *key, unsigned long long cursor,
                const char *pattern, size_t count, char ***out,
                size_t *n_out, unsigned long long *next_out) {
    *out = NULL;
    *n_out = 0;
    *next_out = 0;
    Entry *e = store_find(s, key, NULL);
    if (!e) return 0;
    if (e->type != ENTRY_SET) return -1;
    e->last_access = ++s->clock;
    Set *st = e->set;
    if (count == 0 || cursor >= st->len) return 0;
    char **res = NULL;
    size_t n = 0, cap = 0;
    size_t i = (size_t)cursor;
    while (i < st->len) {
        if (glob_match(pattern, st->members[i])) {
            if (n + 1 > cap) {
                size_t ncap = cap ? cap * 2 : 16;
                char **nres = realloc(res, ncap * sizeof(char *));
                if (!nres) {
                    for (size_t j = 0; j < n; j++) free(res[j]);
                    free(res);
                    return -2;
                }
                res = nres;
                cap = ncap;
            }
            res[n] = strdup(st->members[i]);
            if (!res[n]) {
                for (size_t j = 0; j < n; j++) free(res[j]);
                free(res);
                return -2;
            }
            n++;
            if (n >= count) {
                i++;
                break;
            }
        }
        i++;
    }
    *out = res;
    *n_out = n;
    *next_out = (i >= st->len) ? 0 : (unsigned long long)i;
    return 0;
}

/* ---------- hashes ---------- */

long long store_hset(Store *s, const char *key, const char *field,
                     const char *value) {
    Entry *e = store_find(s, key, NULL);
    if (e) {
        if (e->type != ENTRY_HASH) return -1;
        /* A field set to the value it already holds changes nothing,
           so it does not touch the version; a new field, a changed
           value, or an overwritten field whose expiry is cleared by
           the write does. */
        long long idx = hash_find(e->hash, field);
        int had_expire = idx >= 0 && e->hash->field_expire[idx] != -1;
        int changed = idx < 0 || strcmp(e->hash->values[idx], value) != 0 ||
                      had_expire;
        int rc = hash_put(e->hash, field, value, 1);
        if (rc < 0) return -2;
        if (changed) {
            e->last_access = ++s->clock;
            entry_touch(s, e);
        }
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
    e->version = ++s->mod_seq;
    if (hash_put(e->hash, field, value, 1) < 0) {
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
        entry_touch(s, e);
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

int store_hincrby(Store *s, const char *key, const char *field,
                  long long delta, long long *out) {
    Entry *e = store_find(s, key, NULL);
    if (e && e->type != ENTRY_HASH) return -1;
    long long v = 0;
    int have_field = 0;
    if (e) {
        long long idx = hash_find(e->hash, field);
        if (idx >= 0) {
            have_field = 1;
            if (!parse_ll(e->hash->values[idx], &v)) return -2;
            if (delta == 0) {
                /* A zero increment on an existing field changes
                   nothing, so it does not touch the version. */
                *out = v;
                return 2;
            }
            long long nv;
            if (__builtin_add_overflow(v, delta, &nv)) return -3;
            v = nv;
        } else {
            v = delta;
        }
    } else {
        v = delta;
    }
    char vbuf[32];
    snprintf(vbuf, sizeof(vbuf), "%lld", v);
    if (!e) {
        if (store_hset(s, key, field, vbuf) < 0) return -4;
        *out = v;
        return 1;
    }
    if (hash_put(e->hash, field, vbuf, 0) < 0) return -4;
    e->last_access = ++s->clock;
    entry_touch(s, e);
    *out = v;
    return have_field ? 0 : 1;
}

int store_hincrbyfloat(Store *s, const char *key, const char *field,
                       double delta, double *out) {
    Entry *e = store_find(s, key, NULL);
    if (e && e->type != ENTRY_HASH) return -1;
    double v = 0;
    int have_field = 0;
    if (e) {
        long long idx = hash_find(e->hash, field);
        if (idx >= 0) {
            have_field = 1;
            if (!parse_double(e->hash->values[idx], &v)) return -2;
            if (delta == 0) {
                /* A zero increment on an existing field changes
                   nothing, so it does not touch the version. */
                *out = v;
                return 2;
            }
            v += delta;
            if (isnan(v) || isinf(v)) return -3;
            /* -0.0 prints as "-0" with %.17g; Redis answers "0". */
            if (v == 0) v = 0;
        } else {
            v = delta;
            if (isnan(v) || isinf(v)) return -3;
            if (v == 0) v = 0;
        }
    } else {
        v = delta;
        if (isnan(v) || isinf(v)) return -3;
        if (v == 0) v = 0;
    }
    char vbuf[32];
    snprintf(vbuf, sizeof(vbuf), "%.17g", v);
    if (!e) {
        if (store_hset(s, key, field, vbuf) < 0) return -4;
        *out = v;
        return 1;
    }
    if (hash_put(e->hash, field, vbuf, 0) < 0) return -4;
    e->last_access = ++s->clock;
    entry_touch(s, e);
    *out = v;
    return have_field ? 0 : 1;
}

int store_hscan(Store *s, const char *key, unsigned long long cursor,
                const char *pattern, size_t count, char ***out,
                size_t *n_out, unsigned long long *next_out) {
    *out = NULL;
    *n_out = 0;
    *next_out = 0;
    Entry *e = store_find(s, key, NULL);
    if (!e) return 0;
    if (e->type != ENTRY_HASH) return -1;
    e->last_access = ++s->clock;
    Hash *h = e->hash;
    if (count == 0 || cursor >= h->len) return 0;
    char **res = NULL;
    size_t n = 0, cap = 0;
    size_t i = (size_t)cursor;
    while (i < h->len) {
        if (glob_match(pattern, h->fields[i])) {
            if (n + 2 > cap) {
                size_t ncap = cap ? cap * 2 : 16;
                char **nres = realloc(res, ncap * sizeof(char *));
                if (!nres) {
                    for (size_t j = 0; j < n; j++) free(res[j]);
                    free(res);
                    return -2;
                }
                res = nres;
                cap = ncap;
            }
            res[n] = strdup(h->fields[i]);
            res[n + 1] = strdup(h->values[i]);
            if (!res[n] || !res[n + 1]) {
                free(res[n]);
                free(res[n + 1]);
                for (size_t j = 0; j < n; j++) free(res[j]);
                free(res);
                return -2;
            }
            n += 2;
            if (n / 2 >= count) {
                i++;
                break;
            }
        }
        i++;
    }
    *out = res;
    *n_out = n;
    *next_out = (i >= h->len) ? 0 : (unsigned long long)i;
    return 0;
}

/* ---------- hash field TTLs ---------- */

int store_hexpire_field(Store *s, const char *key, const char *field,
                        long long expire_at, int cond) {
    Entry **pp = NULL;
    Entry *e = store_find(s, key, &pp);
    if (!e) return -2;
    if (e->type != ENTRY_HASH) return -1;
    long long idx = hash_find(e->hash, field);
    if (idx < 0) return -2;
    long long cur = e->hash->field_expire[idx];
    /* The NX / XX / GT / LT condition is checked first, against the
       current expiry: a field with no expiry counts as an infinite
       TTL, so GT never accepts it and LT always does. */
    switch (cond) {
    case HFIELD_EXPIRE_NX:
        if (cur != -1) return 0;
        break;
    case HFIELD_EXPIRE_XX:
        if (cur == -1) return 0;
        break;
    case HFIELD_EXPIRE_GT:
        if (cur == -1 || expire_at <= cur) return 0;
        break;
    case HFIELD_EXPIRE_LT:
        if (cur != -1 && expire_at >= cur) return 0;
        break;
    default:
        break;
    }
    if (expire_at <= now_ms()) {
        /* An expiry in the past deletes the field at once, as in
           Redis (HEXPIRE with 0, HEXPIREAT with a past time). */
        hash_remove(e->hash, field);
        if (e->hash->len == 0) {
            *pp = e->next;
            entry_free(e);
            s->count--;
        } else {
            e->last_access = ++s->clock;
            entry_touch(s, e);
        }
        return 2;
    }
    if (cur == expire_at) {
        /* The field already has exactly this expiry: nothing
           changes, so no WATCH invalidation, but the command still
           answers 1, as in Redis. */
        return 1;
    }
    e->hash->field_expire[idx] = expire_at;
    e->last_access = ++s->clock;
    entry_touch(s, e);
    return 1;
}

long long store_hfield_ttl_ms(Store *s, const char *key, const char *field) {
    Entry *e = store_find(s, key, NULL);
    if (!e) return -2;
    if (e->type != ENTRY_HASH) return -1;
    long long idx = hash_find(e->hash, field);
    if (idx < 0) return -2;
    long long exp = e->hash->field_expire[idx];
    if (exp == -1) return -1;
    long long rem = exp - now_ms();
    return rem < 0 ? -2 : rem;
}

int store_hfield_persist(Store *s, const char *key, const char *field) {
    Entry *e = store_find(s, key, NULL);
    if (!e) return -2;
    if (e->type != ENTRY_HASH) return -1;
    long long idx = hash_find(e->hash, field);
    if (idx < 0) return -2;
    if (e->hash->field_expire[idx] == -1) return -1;
    e->hash->field_expire[idx] = -1;
    e->last_access = ++s->clock;
    entry_touch(s, e);
    return 1;
}

/* ---------- sorted sets ---------- */

long long store_zadd(Store *s, const char *key, const char *member,
                     double score) {
    Entry *e = store_find(s, key, NULL);
    if (e) {
        if (e->type != ENTRY_ZSET) return -1;
        int rc = zset_put(e->zset, member, score);
        if (rc < 0) return -2;
        if (rc != 2) {
            /* A new member or a changed score is a real change; an
               identical score re-add touches nothing, so a WATCH on
               the key is not invalidated by it. */
            e->last_access = ++s->clock;
            entry_touch(s, e);
        }
        return rc;
    }
    if (s->count * LOAD_FACTOR_DEN > s->cap * LOAD_FACTOR_NUM)
        store_resize(s);
    e = calloc(1, sizeof(Entry));
    if (!e) return -2;
    e->key = strdup(key);
    e->zset = zset_new();
    if (!e->key || !e->zset) {
        entry_free(e);
        return -2;
    }
    e->type = ENTRY_ZSET;
    e->expire_at = -1;
    e->last_access = ++s->clock;
    e->version = ++s->mod_seq;
    if (zset_put(e->zset, member, score) < 0) {
        entry_free(e);
        return -2;
    }
    size_t idx = hash_key(key) % s->cap;
    e->next = s->buckets[idx];
    s->buckets[idx] = e;
    s->count++;
    return 1;
}

int store_zscore(Store *s, const char *key, const char *member,
                 double *out) {
    Entry *e = store_find(s, key, NULL);
    if (!e) return 0;
    if (e->type != ENTRY_ZSET) return -1;
    e->last_access = ++s->clock;
    zslNode *node = zset_find(e->zset, member);
    if (!node) return 0;
    *out = node->score;
    return 1;
}

long long store_zcard(Store *s, const char *key) {
    Entry *e = store_find(s, key, NULL);
    if (!e) return 0;
    if (e->type != ENTRY_ZSET) return -1;
    e->last_access = ++s->clock;
    return (long long)e->zset->len;
}

long long store_zrem(Store *s, const char *key, const char *member) {
    Entry **pp = NULL;
    Entry *e = store_find(s, key, &pp);
    if (!e) return 0;
    if (e->type != ENTRY_ZSET) return -1;
    if (!zset_remove(e->zset, member)) return 0;
    if (e->zset->len == 0) {
        /* A sorted set emptied by removing is gone, as in Redis. */
        *pp = e->next;
        entry_free(e);
        s->count--;
    } else {
        e->last_access = ++s->clock;
        entry_touch(s, e);
    }
    return 1;
}

/* Rank of a member: 1 = found (*out is the 0-based rank), 0 = key or
   member missing, -1 = wrong type. reverse ranks from the highest
   score down, as ZREVRANK would. */
int store_zrank(Store *s, const char *key, const char *member, int reverse,
                long long *out) {
    Entry *e = store_find(s, key, NULL);
    if (!e) return 0;
    if (e->type != ENTRY_ZSET) return -1;
    e->last_access = ++s->clock;
    zslNode *node = zset_find(e->zset, member);
    if (!node) return 0;
    /* zsl_get_rank is 1-based; a 0-based rank is one less, and the
       reversed rank counts down from the top. */
    long long rank = (long long)zsl_get_rank(e->zset->sl, node->score,
                                            member);
    *out = reverse ? (long long)e->zset->len - rank : rank - 1;
    return 1;
}

/* Members in [start, stop] (inclusive), in ascending score order, or
   descending when reverse is set. Negative indices count from the
   end, as in Redis, and out-of-range ends are clamped. Returns 0 on
   success (*n_out is 0 for a missing key or an empty range),
   -1 = wrong type, -2 = out of memory. *members_out is a malloc'd
   array of malloc'd strings and *scores_out a malloc'd array of the
   matching scores; the caller frees both. */
int store_zrange(Store *s, const char *key, long long start, long long stop,
                 int reverse, char ***members_out, double **scores_out,
                 size_t *n_out) {
    *members_out = NULL;
    *scores_out = NULL;
    *n_out = 0;
    Entry *e = store_find(s, key, NULL);
    if (!e) return 0;
    if (e->type != ENTRY_ZSET) return -1;
    e->last_access = ++s->clock;
    ZSet *z = e->zset;
    long long len = (long long)z->len;
    if (start < 0) start += len;
    if (stop < 0) stop += len;
    if (start < 0) start = 0;
    if (stop >= len) stop = len - 1;
    if (start > stop || start >= len) return 0;
    size_t n = (size_t)(stop - start + 1);
    char **members = malloc(n * sizeof(char *));
    double *scores = malloc(n * sizeof(double));
    if (!members || !scores) {
        free(members);
        free(scores);
        return -2;
    }
    for (size_t i = 0; i < n; i++) {
        /* Reversed ranges index into the reversed ordering, as in
           Redis: ZREVRANGE -2 -1 is the last two members of the
           highest-first sequence, not the ascending slice reversed.
           Ranks are 1-based, so the i-th element of the range is
           rank start+i+1 ascending, or len-start-i descending. */
        unsigned long rank = (unsigned long)(reverse
            ? len - start - (long long)i
            : start + (long long)i + 1);
        zslNode *node = zsl_get_by_rank(z->sl, rank);
        members[i] = strdup(node->member);
        if (!members[i]) {
            for (size_t j = 0; j < i; j++) free(members[j]);
            free(members);
            free(scores);
            return -2;
        }
        scores[i] = node->score;
    }
    *members_out = members;
    *scores_out = scores;
    *n_out = n;
    return 0;
}

/* Cursor-based iteration over a sorted set's members, in
   (score, member) order. The cursor is a 0-based rank into the
   skip list, so the scan starts at the node at rank cursor + 1
   and walks level 0 forward, collecting up to `count` members
   whose names match the glob. The next cursor is the rank after
   the last member returned, or 0 when the end was reached. */
int store_zscan(Store *s, const char *key, unsigned long long cursor,
                const char *pattern, size_t count,
                char ***members_out, double **scores_out,
                size_t *n_out, unsigned long long *next_out) {
    *members_out = NULL;
    *scores_out = NULL;
    *n_out = 0;
    *next_out = 0;
    Entry *e = store_find(s, key, NULL);
    if (!e) return 0;
    if (e->type != ENTRY_ZSET) return -1;
    e->last_access = ++s->clock;
    ZSet *z = e->zset;
    if (count == 0 || cursor >= z->len) return 0;
    char **members = NULL;
    double *scores = NULL;
    size_t n = 0, cap = 0;
    /* Ranks are 1-based, so the cursor's node is rank cursor + 1. */
    zslNode *node = zsl_get_by_rank(z->sl, (unsigned long)cursor + 1);
    size_t i = (size_t)cursor;
    while (node) {
        if (glob_match(pattern, node->member)) {
            if (n + 1 > cap) {
                size_t ncap = cap ? cap * 2 : 16;
                char **nm = realloc(members, ncap * sizeof(char *));
                if (!nm) {
                    for (size_t j = 0; j < n; j++) free(members[j]);
                    free(members);
                    free(scores);
                    return -2;
                }
                members = nm;
                double *ns = realloc(scores, ncap * sizeof(double));
                if (!ns) {
                    for (size_t j = 0; j < n; j++) free(members[j]);
                    free(members);
                    free(scores);
                    return -2;
                }
                scores = ns;
                cap = ncap;
            }
            members[n] = strdup(node->member);
            if (!members[n]) {
                for (size_t j = 0; j < n; j++) free(members[j]);
                free(members);
                free(scores);
                return -2;
            }
            scores[n] = node->score;
            n++;
            if (n >= count) {
                node = node->level[0].forward;
                i++;
                break;
            }
        }
        node = node->level[0].forward;
        i++;
    }
    *members_out = members;
    *scores_out = scores;
    *n_out = n;
    *next_out = (i >= z->len) ? 0 : (unsigned long long)i;
    return 0;
}

/* Members whose scores fall in [min, max], ascending by score, or
   descending when reverse is set, with an endpoint excluded when
   its flag is set. Skips `offset` matches and returns at most
   `count` (`count < 0` means all). The skip list finds the first
   match in O(log n) and level 0 walks the matching slice; a
   reversed scan walks it from the top down. */
int store_zrangebyscore(Store *s, const char *key, double min, int min_excl,
                        double max, int max_excl, long long offset,
                        long long count, int reverse,
                        char ***members_out, double **scores_out,
                        size_t *n_out) {
    *members_out = NULL;
    *scores_out = NULL;
    *n_out = 0;
    Entry *e = store_find(s, key, NULL);
    if (!e) return 0;
    if (e->type != ENTRY_ZSET) return -1;
    e->last_access = ++s->clock;
    ZSet *z = e->zset;
    if (min > max) return 0;
    zslNode *first = zsl_first_in_range(z->sl, min, min_excl, max, max_excl);
    if (!first) return 0;
    /* Count the matches first so the result arrays are sized once. */
    size_t total = 0;
    for (zslNode *node = first; node; node = node->level[0].forward) {
        if (max_excl ? node->score >= max : node->score > max) break;
        total++;
    }
    if ((long long)total <= offset) return 0;
    size_t avail = total - (size_t)offset;
    size_t n = count < 0 || (long long)avail <= count ? avail : (size_t)count;
    char **members = malloc(n * sizeof(char *));
    double *scores = malloc(n * sizeof(double));
    if (!members || !scores) {
        free(members);
        free(scores);
        return -2;
    }
    size_t seen = 0, out = 0;
    zslNode *node;
    if (reverse) {
        /* Start at the last match and walk backwards, so the
           highest scores (and, on a tie, the last member) come
           first, as in Redis. */
        node = first;
        for (size_t i = 1; i < total; i++) node = node->level[0].forward;
        while (node && out < n) {
            /* Walking backwards the scores only fall, so stop
               once the lower bound is passed. */
            if (min_excl ? node->score <= min : node->score < min) break;
            if ((long long)seen++ < offset) {
                node = node->backward;
                continue;
            }
            members[out] = strdup(node->member);
            if (!members[out]) {
                for (size_t j = 0; j < out; j++) free(members[j]);
                free(members);
                free(scores);
                return -2;
            }
            scores[out] = node->score;
            out++;
            node = node->backward;
        }
    } else {
        node = first;
        while (node && out < n) {
            if (max_excl ? node->score >= max : node->score > max) break;
            if ((long long)seen++ < offset) {
                node = node->level[0].forward;
                continue;
            }
            members[out] = strdup(node->member);
            if (!members[out]) {
                for (size_t j = 0; j < out; j++) free(members[j]);
                free(members);
                free(scores);
                return -2;
            }
            scores[out] = node->score;
            out++;
            node = node->level[0].forward;
        }
    }
    *members_out = members;
    *scores_out = scores;
    *n_out = n;
    return 0;
}

/* qsort comparator for member-name strings. */
static int cmp_str(const void *a, const void *b) {
    return strcmp(*(char *const *)a, *(char *const *)b);
}

/* Members whose names fall in [min, max] lexicographically, in
   member order, or reverse member order when reverse is set. A NULL
   min means -inf, a NULL max means +inf; an endpoint is excluded
   when its flag is set. Skips `offset` matches and returns at most
   `count` (`count < 0` means all). The skip list is ordered by
   (score, member), so a lex range cannot walk it in member order:
   the matches are collected with a level-0 scan and sorted by
   member, which is fine at the scale this store targets; a finite
   min above a finite max matches nothing, as with ZRANGEBYSCORE's
   reversed bounds. */
int store_zrangebylex(Store *s, const char *key, const char *min, int min_excl,
                      const char *max, int max_excl, long long offset,
                      long long count, int reverse,
                      char ***members_out, size_t *n_out) {
    *members_out = NULL;
    *n_out = 0;
    Entry *e = store_find(s, key, NULL);
    if (!e) return 0;
    if (e->type != ENTRY_ZSET) return -1;
    e->last_access = ++s->clock;
    ZSet *z = e->zset;
    if (min && max && strcmp(min, max) > 0) return 0;
    char **members = NULL;
    size_t n = 0, cap = 0;
    for (zslNode *node = z->sl->header->level[0].forward; node;
         node = node->level[0].forward) {
        const char *m = node->member;
        if (min) {
            int c = strcmp(m, min);
            if (c < 0 || (c == 0 && min_excl)) continue;
        }
        if (max) {
            int c = strcmp(m, max);
            if (c > 0 || (c == 0 && max_excl)) continue;
        }
        if (n + 1 > cap) {
            size_t ncap = cap ? cap * 2 : 16;
            char **nm = realloc(members, ncap * sizeof(char *));
            if (!nm) {
                for (size_t j = 0; j < n; j++) free(members[j]);
                free(members);
                return -2;
            }
            members = nm;
            cap = ncap;
        }
        members[n] = strdup(m);
        if (!members[n]) {
            for (size_t j = 0; j < n; j++) free(members[j]);
            free(members);
            return -2;
        }
        n++;
    }
    if (n == 0) {
        free(members);
        return 0;
    }
    qsort(members, n, sizeof(char *), cmp_str);
    if (reverse) {
        /* Reverse the whole sorted slice first, so LIMIT applies
           to the reversed order, as in Redis. */
        for (size_t i = 0, j = n - 1; i < j; i++, j--) {
            char *tmp = members[i];
            members[i] = members[j];
            members[j] = tmp;
        }
    }
    if ((long long)n <= offset) {
        for (size_t j = 0; j < n; j++) free(members[j]);
        free(members);
        return 0;
    }
    size_t avail = n - (size_t)offset;
    size_t out_n = count < 0 || (long long)avail <= count ? avail
                                                          : (size_t)count;
    if (out_n == 0) {
        for (size_t j = 0; j < n; j++) free(members[j]);
        free(members);
        return 0;
    }
    /* Drop everything outside [offset, offset + out_n). */
    for (size_t j = 0; j < (size_t)offset; j++) free(members[j]);
    for (size_t j = (size_t)offset + out_n; j < n; j++) free(members[j]);
    memmove(members, members + offset, out_n * sizeof(char *));
    *members_out = members;
    *n_out = out_n;
    return 0;
}

/* Count the members whose names fall in [min, max]
   lexicographically, with the same bound conventions as
   store_zrangebylex. Like that function, the skip list is ordered
   by (score, member), so the count is a level-0 scan; unlike it,
   no members are collected or sorted, so the work is O(n) time
   and O(1) memory. */
long long store_zlexcount(Store *s, const char *key, const char *min,
                          int min_excl, const char *max, int max_excl) {
    Entry *e = store_find(s, key, NULL);
    if (!e) return 0;
    if (e->type != ENTRY_ZSET) return -1;
    e->last_access = ++s->clock;
    ZSet *z = e->zset;
    if (min && max && strcmp(min, max) > 0) return 0;
    long long n = 0;
    for (zslNode *node = z->sl->header->level[0].forward; node;
         node = node->level[0].forward) {
        const char *m = node->member;
        if (min) {
            int c = strcmp(m, min);
            if (c < 0 || (c == 0 && min_excl)) continue;
        }
        if (max) {
            int c = strcmp(m, max);
            if (c > 0 || (c == 0 && max_excl)) continue;
        }
        n++;
    }
    return n;
}

/* Remove the members whose scores fall in [min, max], with an
   endpoint excluded when its flag is set. The skip list finds the
   first match in O(log n); the matching slice is walked once at
   level 0, each member dropped through zset_remove, which keeps
   the dict and the skip list in step. The next node is saved before
   the removal, since removing frees the node being stood on. */
long long store_zremrangebyscore(Store *s, const char *key, double min,
                                 int min_excl, double max, int max_excl) {
    Entry **pp = NULL;
    Entry *e = store_find(s, key, &pp);
    if (!e) return 0;
    if (e->type != ENTRY_ZSET) return -1;
    if (min > max) return 0;
    ZSet *z = e->zset;
    zslNode *node = zsl_first_in_range(z->sl, min, min_excl, max, max_excl);
    if (!node) return 0;
    long long removed = 0;
    while (node) {
        if (max_excl ? node->score >= max : node->score > max) break;
        zslNode *next = node->level[0].forward;
        if (zset_remove(z, node->member)) removed++;
        node = next;
    }
    if (removed == 0) return 0;
    if (z->len == 0) {
        /* A sorted set emptied by removing is gone, as in Redis. */
        *pp = e->next;
        entry_free(e);
        s->count--;
    } else {
        e->last_access = ++s->clock;
        entry_touch(s, e);
    }
    return removed;
}

/* Remove the members ranked [start, stop] (inclusive, 0-based),
   with negative indices counting from the end, as in ZRANGE, and
   out-of-range ends clamped. The node at rank start + 1 is found in
   O(log n); the slice is walked once at level 0, each member
   dropped through zset_remove, which keeps the dict and the skip
   list in step. The next node is saved before the removal, since
   removing frees the node being stood on. */
long long store_zremrangebyrank(Store *s, const char *key, long long start,
                                long long stop) {
    Entry **pp = NULL;
    Entry *e = store_find(s, key, &pp);
    if (!e) return 0;
    if (e->type != ENTRY_ZSET) return -1;
    ZSet *z = e->zset;
    long long len = (long long)z->len;
    if (start < 0) start += len;
    if (stop < 0) stop += len;
    if (start < 0) start = 0;
    if (stop >= len) stop = len - 1;
    if (start > stop || start >= len) return 0;
    zslNode *node = zsl_get_by_rank(z->sl, (unsigned long)start + 1);
    long long removed = 0;
    while (node && removed <= stop - start) {
        zslNode *next = node->level[0].forward;
        if (zset_remove(z, node->member)) removed++;
        node = next;
    }
    if (removed == 0) return 0;
    if (z->len == 0) {
        /* A sorted set emptied by removing is gone, as in Redis. */
        *pp = e->next;
        entry_free(e);
        s->count--;
    } else {
        e->last_access = ++s->clock;
        entry_touch(s, e);
    }
    return removed;
}

/* Remove the members whose names fall in [min, max]
   lexicographically, with the same bound conventions as
   store_zrangebylex. The skip list is ordered by (score, member),
   so a lex range cannot walk it in order: the matching slice is
   found with a level-0 scan and each member is dropped through
   zset_remove, which keeps the dict and the skip list in step. The
   next node is saved before the removal, since removing frees the
   node being stood on. */
long long store_zremrangebylex(Store *s, const char *key, const char *min,
                              int min_excl, const char *max, int max_excl) {
    Entry **pp = NULL;
    Entry *e = store_find(s, key, &pp);
    if (!e) return 0;
    if (e->type != ENTRY_ZSET) return -1;
    if (min && max && strcmp(min, max) > 0) return 0;
    ZSet *z = e->zset;
    long long removed = 0;
    for (zslNode *node = z->sl->header->level[0].forward; node;) {
        const char *m = node->member;
        zslNode *next = node->level[0].forward;
        if (min) {
            int c = strcmp(m, min);
            if (c < 0 || (c == 0 && min_excl)) {
                node = next;
                continue;
            }
        }
        if (max) {
            int c = strcmp(m, max);
            if (c > 0 || (c == 0 && max_excl)) {
                node = next;
                continue;
            }
        }
        if (zset_remove(z, m)) removed++;
        node = next;
    }
    if (removed == 0) return 0;
    if (z->len == 0) {
        /* A sorted set emptied by removing is gone, as in Redis. */
        *pp = e->next;
        entry_free(e);
        s->count--;
    } else {
        e->last_access = ++s->clock;
        entry_touch(s, e);
    }
    return removed;
}

/* Add increment to a member's score, creating the member (with score
   = increment) and the key when missing. Returns 1 when the member
   is new, 0 when an existing member's score changed, 2 when nothing
   changed (increment 0 on a member already there), -1 = wrong type,
   -2 = out of memory. *out is the member's new score. */
int store_zincrby(Store *s, const char *key, const char *member,
                  double incr, double *out) {
    Entry *e = store_find(s, key, NULL);
    if (e) {
        if (e->type != ENTRY_ZSET) return -1;
        zslNode *node = zset_find(e->zset, member);
        if (node) {
            double cur = node->score;
            if (incr == 0) {
                *out = cur;
                return 2;
            }
            double next = cur + incr;
            int rc = zset_put(e->zset, member, next);
            if (rc < 0) return -2;
            e->last_access = ++s->clock;
            entry_touch(s, e);
            *out = next;
            return 0;
        }
        int rc = zset_put(e->zset, member, incr);
        if (rc < 0) return -2;
        e->last_access = ++s->clock;
        entry_touch(s, e);
        *out = incr;
        return 1;
    }
    if (s->count * LOAD_FACTOR_DEN > s->cap * LOAD_FACTOR_NUM)
        store_resize(s);
    e = calloc(1, sizeof(Entry));
    if (!e) return -2;
    e->key = strdup(key);
    e->zset = zset_new();
    if (!e->key || !e->zset) {
        entry_free(e);
        return -2;
    }
    e->type = ENTRY_ZSET;
    e->expire_at = -1;
    e->last_access = ++s->clock;
    e->version = ++s->mod_seq;
    if (zset_put(e->zset, member, incr) < 0) {
        entry_free(e);
        return -2;
    }
    size_t idx = hash_key(key) % s->cap;
    e->next = s->buckets[idx];
    s->buckets[idx] = e;
    s->count++;
    *out = incr;
    return 1;
}

/* Pop `count` members from the lowest-score end (from_max = 0) or the
   highest-score end (from_max = 1). The popped members come back in
   score order — the lowest (or highest) first, as in Redis — and the
   key is deleted when the last member is popped, as with ZREM. */
int store_zpop(Store *s, const char *key, long long count, int from_max,
               char ***members_out, double **scores_out, size_t *n_out) {
    *members_out = NULL;
    *scores_out = NULL;
    *n_out = 0;
    Entry **pp = NULL;
    Entry *e = store_find(s, key, &pp);
    if (!e) return 0;
    if (e->type != ENTRY_ZSET) return -1;
    ZSet *z = e->zset;
    size_t n = (size_t)count < z->len ? (size_t)count : z->len;
    if (n == 0) return 0;
    char **members = malloc(n * sizeof(char *));
    double *scores = malloc(n * sizeof(double));
    if (!members || !scores) {
        free(members);
        free(scores);
        return -2;
    }
    /* Collect the members to pop first, so an OOM while copying
       leaves the set untouched. Popping from the top returns the
       highest score first, as in Redis; popping from the bottom
       returns the lowest first. The cursor walks from the end
       without removing anything yet. */
    zslNode *cursor = from_max ? z->sl->tail
                               : z->sl->header->level[0].forward;
    for (size_t i = 0; i < n; i++) {
        members[i] = strdup(cursor->member);
        if (!members[i]) {
            for (size_t j = 0; j < i; j++) free(members[j]);
            free(members);
            free(scores);
            return -2;
        }
        scores[i] = cursor->score;
        cursor = from_max ? cursor->backward : cursor->level[0].forward;
    }
    /* The copies are safe in members[] now; remove the originals. */
    for (size_t i = 0; i < n; i++) {
        zdict_remove(z->dict, members[i]);
        zslNode *gone = zsl_delete(z->sl, scores[i], members[i]);
        if (gone) {
            free(gone->member);
            free(gone);
        }
        z->len--;
    }
    if (z->len == 0) {
        /* A sorted set emptied by popping is gone, as in Redis. */
        *pp = e->next;
        entry_free(e);
        s->count--;
    } else {
        e->last_access = ++s->clock;
        entry_touch(s, e);
    }
    *members_out = members;
    *scores_out = scores;
    *n_out = n;
    return 0;
}

/* `count` random members: count > 0 returns distinct members (at most
   the whole set), count < 0 returns -count members sampled with
   replacement, count == 0 returns none. Distinct sampling is a
   partial Fisher-Yates shuffle over an array of 1-based ranks, so
   no member comes back twice and no retry loop is needed. */
int store_zrandmember(Store *s, const char *key, long long count,
                      char ***members_out, double **scores_out,
                      size_t *n_out) {
    *members_out = NULL;
    *scores_out = NULL;
    *n_out = 0;
    Entry *e = store_find(s, key, NULL);
    if (!e) return 0;
    if (e->type != ENTRY_ZSET) return -1;
    e->last_access = ++s->clock;
    ZSet *z = e->zset;
    if (count == 0 || z->len == 0) return 0;
    size_t n;
    int with_replacement = count < 0;
    if (with_replacement) {
        /* -count, computed in unsigned arithmetic so LLONG_MIN does
           not overflow; a huge count just fails the allocation
           below and answers out of memory. */
        unsigned long long want = (unsigned long long)(-(count + 1)) + 1;
        n = want > (unsigned long long)SIZE_MAX ? SIZE_MAX : (size_t)want;
    } else {
        n = (size_t)count < z->len ? (size_t)count : z->len;
    }
    char **members = malloc(n * sizeof(char *));
    double *scores = malloc(n * sizeof(double));
    if (!members || !scores) {
        free(members);
        free(scores);
        return -2;
    }
    if (with_replacement) {
        for (size_t i = 0; i < n; i++) {
            /* Ranks are 1-based; a random rank is O(log n) in the
               skip list. */
            unsigned long rank = (unsigned long)((size_t)rand() % z->len) + 1;
            zslNode *node = zsl_get_by_rank(z->sl, rank);
            members[i] = strdup(node->member);
            if (!members[i]) {
                for (size_t j = 0; j < i; j++) free(members[j]);
                free(members);
                free(scores);
                return -2;
            }
            scores[i] = node->score;
        }
    } else {
        size_t *idx = malloc(z->len * sizeof(size_t));
        if (!idx) {
            free(members);
            free(scores);
            return -2;
        }
        for (size_t i = 0; i < z->len; i++) idx[i] = i + 1; /* ranks */
        for (size_t i = 0; i < n; i++) {
            size_t j = i + (size_t)rand() % (z->len - i);
            size_t tmp = idx[i];
            idx[i] = idx[j];
            idx[j] = tmp;
            zslNode *node = zsl_get_by_rank(z->sl, idx[i]);
            members[i] = strdup(node->member);
            if (!members[i]) {
                for (size_t k = 0; k < i; k++) free(members[k]);
                free(members);
                free(scores);
                free(idx);
                return -2;
            }
            scores[i] = node->score;
        }
        free(idx);
    }
    *members_out = members;
    *scores_out = scores;
    *n_out = n;
    return 0;
}

/* ---------- HyperLogLog ---------- */

/* MurmurHash64A (MurmurHash2, 64-bit): a uniform 64-bit hash of the
   element, so the register index and the leading-zero count the
   HyperLogLog derives from it are unbiased. Fixed seed 0, so a
   PFADD replayed from the AOF or a replication stream rebuilds
   exactly the same registers. The 8-byte chunks are copied with
   memcpy, never read unaligned. */
static uint64_t hll_hash64(const char *data, size_t len) {
    const uint64_t m = 0xc6a4a7935bd1e995ULL;
    const int r = 47;
    uint64_t h = 0x8445d61a4e774912ULL ^ (len * m);
    size_t nblocks = len / 8;
    for (size_t i = 0; i < nblocks; i++) {
        uint64_t k;
        memcpy(&k, data + i * 8, 8);
        k *= m;
        k ^= k >> r;
        k *= m;
        h ^= k;
        h *= m;
    }
    const unsigned char *tail = (const unsigned char *)data + nblocks * 8;
    switch (len & 7) {
    case 7: h ^= (uint64_t)tail[6] << 48; /* fall through */
    case 6: h ^= (uint64_t)tail[5] << 40; /* fall through */
    case 5: h ^= (uint64_t)tail[4] << 32; /* fall through */
    case 4: h ^= (uint64_t)tail[3] << 24; /* fall through */
    case 3: h ^= (uint64_t)tail[2] << 16; /* fall through */
    case 2: h ^= (uint64_t)tail[1] << 8;  /* fall through */
    case 1: h ^= (uint64_t)tail[0];
        h *= m;
    default: break;
    }
    h ^= h >> r;
    h *= m;
    h ^= h >> r;
    return h;
}

/* The position of the first 1-bit in the hash after the register
   index, 1-based: the top HLL_P bits are the index, the remaining
   64 - HLL_P bits carry the run of leading zeros. At most
   64 - HLL_P + 1 (51) when the rest is all zeros. */
static int hll_rho(uint64_t h) {
    uint64_t w = h << HLL_P;
    if (w == 0) return 64 - HLL_P + 1;
    return __builtin_clzll(w) + 1;
}

/* The standard HyperLogLog estimate for one register set: the
   harmonic-mean estimator, linear counting when the raw estimate
   is small and registers are still empty, and the large-range
   correction above 2^32 / 30 (unreachable with a 64-bit hash at
   this scale, kept for the formula's sake). Rounded to the
   nearest integer: the linear-counting value for a handful of
   elements lands just under the integer (2.9999 for three
   distinct elements), and truncating would report one less. */
static long long hll_estimate(const unsigned char *regs) {
    double sum = 0;
    long long zeros = 0;
    for (size_t i = 0; i < HLL_REGISTERS; i++) {
        sum += 1.0 / (double)(1ULL << regs[i]);
        if (regs[i] == 0) zeros++;
    }
    double alpha = 0.7213 / (1.0 + 1.079 / (double)HLL_REGISTERS);
    double est = alpha * (double)HLL_REGISTERS * (double)HLL_REGISTERS / sum;
    if (est <= 2.5 * (double)HLL_REGISTERS && zeros > 0)
        est = (double)HLL_REGISTERS * log((double)HLL_REGISTERS / (double)zeros);
    else if (est > 143165576.5) /* 2^32 / 30 */
        est = -4294967296.0 * log(1.0 - est / 4294967296.0);
    return (long long)(est + 0.5);
}

/* The registers of a stored HyperLogLog value, or NULL when the
   entry is not a valid one: the caller has already checked the
   entry exists and holds a string. */
static unsigned char *hll_regs(Entry *e) {
    if (e->value_len != HLL_LEN || memcmp(e->value, HLL_MAGIC, 4) != 0)
        return NULL;
    return (unsigned char *)e->value + 4;
}

int store_pfadd(Store *s, const char *key, const char **elems, int nelems,
                int *changed_out) {
    Entry *e = store_find(s, key, NULL);
    unsigned char *regs = NULL;
    char *buf = NULL;
    if (e) {
        if (e->type != ENTRY_STRING) return -1;
        regs = hll_regs(e);
        if (!regs) return -3;
    } else {
        buf = calloc(1, HLL_LEN);
        if (!buf) return -2;
        memcpy(buf, HLL_MAGIC, 4);
        regs = (unsigned char *)buf + 4;
    }
    int changed = 0;
    for (int i = 0; i < nelems; i++) {
        uint64_t h = hll_hash64(elems[i], strlen(elems[i]));
        size_t idx = (size_t)(h >> (64 - HLL_P));
        int rho = hll_rho(h);
        if (rho > regs[idx]) {
            regs[idx] = (unsigned char)rho;
            changed = 1;
        }
    }
    if (buf) {
        /* A new key: any element changes a register (rho >= 1 over
           zeroed registers), so changed is 1 here. */
        if (!store_set_len(s, key, buf, HLL_LEN, -1)) {
            free(buf);
            return -2;
        }
        free(buf);
    } else if (changed) {
        e->last_access = ++s->clock;
        entry_touch(s, e);
    }
    *changed_out = changed;
    return 0;
}

int store_pfcount(Store *s, const char **keys, int nkeys, long long *out) {
    unsigned char *merged = calloc(HLL_REGISTERS, 1);
    if (!merged) return -2;
    for (int k = 0; k < nkeys; k++) {
        Entry *e = store_find(s, keys[k], NULL);
        if (!e) continue; /* missing counts as empty */
        if (e->type != ENTRY_STRING) {
            free(merged);
            return -1;
        }
        unsigned char *regs = hll_regs(e);
        if (!regs) {
            free(merged);
            return -3;
        }
        e->last_access = ++s->clock;
        for (size_t i = 0; i < HLL_REGISTERS; i++)
            if (regs[i] > merged[i]) merged[i] = regs[i];
    }
    *out = hll_estimate(merged);
    free(merged);
    return 0;
}

int store_pfmerge(Store *s, const char *dest, const char **sources,
                  int nsources, int *changed_out) {
    /* The destination's own registers, when it already holds a
       HyperLogLog: the merge is compared against them afterwards,
       so reproducing dest exactly is a no-op. A plain string or a
       non-string at dest is an error, never a silent overwrite. */
    Entry *de = store_find(s, dest, NULL);
    unsigned char *dregs = NULL;
    if (de) {
        if (de->type != ENTRY_STRING) return -1;
        dregs = hll_regs(de);
        if (!dregs) return -3;
    }
    unsigned char *merged = calloc(HLL_REGISTERS, 1);
    if (!merged) return -2;
    for (int k = 0; k < nsources; k++) {
        Entry *e = store_find(s, sources[k], NULL);
        if (!e) continue; /* missing counts as empty */
        if (e->type != ENTRY_STRING) {
            free(merged);
            return -1;
        }
        unsigned char *regs = hll_regs(e);
        if (!regs) {
            free(merged);
            return -3;
        }
        e->last_access = ++s->clock;
        for (size_t i = 0; i < HLL_REGISTERS; i++)
            if (regs[i] > merged[i]) merged[i] = regs[i];
    }
    if (dregs && memcmp(merged, dregs, HLL_REGISTERS) == 0) {
        /* The merge reproduces dest exactly: change nothing, so
           the version (and WATCH) and the AOF are untouched. */
        free(merged);
        *changed_out = 0;
        return 0;
    }
    char *buf = malloc(HLL_LEN);
    if (!buf) {
        free(merged);
        return -2;
    }
    memcpy(buf, HLL_MAGIC, 4);
    memcpy(buf + 4, merged, HLL_REGISTERS);
    free(merged);
    /* store_set_len with expire_at -1 clears the TTL, as in Redis,
       and stamps the new version; it also replaces any string
       value wholesale, which is exactly the overwrite PFMERGE
       needs (the type checks above already rejected the rest). */
    if (!store_set_len(s, dest, buf, HLL_LEN, -1)) {
        free(buf);
        return -2;
    }
    free(buf);
    *changed_out = 1;
    return 0;
}

/* ---------- GEO (GEOADD / GEODIST) ----------
 * The 52-bit geohash Redis stores as a sorted-set score: 26
 * latitude bits and 26 longitude bits interleaved, latitude in
 * the even bit positions, longitude in the odd ones — the same
 * bit-spreading trick Redis's geohash.c uses. */

#define GEO_STEP 26 /* 26 bits per axis, 52 bits total */
#define GEO_EARTH_RADIUS 6372797.560856 /* metres, as in Redis */

/* Spread the low 32 bits of xlo to the even bit positions and of
   ylo to the odd ones. */
static uint64_t geo_interleave64(uint32_t xlo, uint32_t ylo) {
    static const uint64_t B[] = {0x5555555555555555ULL, 0x3333333333333333ULL,
                                 0x0F0F0F0F0F0F0F0FULL, 0x00FF00FF00FF00FFULL,
                                 0x0000FFFF0000FFFFULL};
    uint64_t x = xlo;
    uint64_t y = ylo;
    x = (x | (x << 16)) & B[4];
    y = (y | (y << 16)) & B[4];
    x = (x | (x << 8)) & B[3];
    y = (y | (y << 8)) & B[3];
    x = (x | (x << 4)) & B[2];
    y = (y | (y << 4)) & B[2];
    x = (x | (x << 2)) & B[1];
    y = (y | (y << 2)) & B[1];
    x = (x | (x << 1)) & B[0];
    y = (y | (y << 1)) & B[0];
    return x | (y << 1);
}

/* Undo geo_interleave64: the low 32 bits of the result are the
   even-position bits, the high 32 the odd-position ones. */
static uint64_t geo_deinterleave64(uint64_t interleaved) {
    static const uint64_t B[] = {0x5555555555555555ULL, 0x3333333333333333ULL,
                                 0x0F0F0F0F0F0F0F0FULL, 0x00FF00FF00FF00FFULL,
                                 0x0000FFFF0000FFFFULL, 0x00000000FFFFFFFFULL};
    uint64_t x = interleaved;
    uint64_t y = interleaved >> 1;
    x = (x | (x >> 0)) & B[0];
    y = (y | (y >> 0)) & B[0];
    x = (x | (x >> 1)) & B[1];
    y = (y | (y >> 1)) & B[1];
    x = (x | (x >> 2)) & B[2];
    y = (y | (y >> 2)) & B[2];
    x = (x | (x >> 4)) & B[3];
    y = (y | (y >> 4)) & B[3];
    x = (x | (x >> 8)) & B[4];
    y = (y | (y >> 8)) & B[4];
    x = (x | (x >> 16)) & B[5];
    y = (y | (y >> 16)) & B[5];
    return x | (y << 32);
}

/* The 52-bit geohash of a coordinate pair over a given latitude
   range: each axis is scaled to a 26-bit fixed-point value over its
   range (truncated, as in Redis), then interleaved with latitude in
   the even positions. */
static uint64_t geo_encode_range(double lon, double lat,
                                 double lat_min, double lat_max) {
    double lat_offset = (lat - lat_min) / (lat_max - lat_min);
    double lon_offset = (lon - GEO_LON_MIN) / (GEO_LON_MAX - GEO_LON_MIN);
    uint32_t ilat = (uint32_t)(lat_offset * (double)(1ULL << GEO_STEP));
    uint32_t ilon = (uint32_t)(lon_offset * (double)(1ULL << GEO_STEP));
    return geo_interleave64(ilat, ilon);
}

static uint64_t geo_encode(double lon, double lat) {
    return geo_encode_range(lon, lat, GEO_LAT_MIN, GEO_LAT_MAX);
}

/* The centre of the geohash cell a hash names, as in Redis's
   geohashDecode: the decoded coordinate is the cell centre, not
   the coordinate that was encoded. */
static void geo_decode(uint64_t hash, double *lon_out, double *lat_out) {
    uint64_t sep = geo_deinterleave64(hash);
    uint32_t ilat = (uint32_t)(sep & 0xFFFFFFFFULL);
    uint32_t ilon = (uint32_t)(sep >> 32);
    *lat_out = GEO_LAT_MIN + ((double)ilat + 0.5) *
               (GEO_LAT_MAX - GEO_LAT_MIN) / (double)(1ULL << GEO_STEP);
    *lon_out = GEO_LON_MIN + ((double)ilon + 0.5) *
               (GEO_LON_MAX - GEO_LON_MIN) / (double)(1ULL << GEO_STEP);
}

/* A sorted-set score as a geohash: scores GEOADD writes are
   52-bit integers, exactly representable, but a plain sorted set
   can hold any double, and a double outside [0, 2^52) has no
   defined conversion to uint64_t, so clamp before the cast. */
static uint64_t geo_score_to_hash(double score) {
    if (!(score >= 0.0)) return 0;
    if (score >= 4503599627370496.0) return 4503599627370495ULL;
    return (uint64_t)score;
}

/* Decode a sorted-set score as a geohash, as in Redis's
   decodeGeohash: NaN, a negative score, a score of 2^64 or more,
   and a score of exactly 0 (an all-zero hash) all fail to decode,
   so a member holding such a score has no position. Returns 1 on
   success, 0 when the score does not decode. */
static int geo_decode_score(double score, double *lon_out, double *lat_out) {
    if (!(score >= 0.0) || score >= 18446744073709551616.0 || score == 0.0)
        return 0;
    geo_decode((uint64_t)score, lon_out, lat_out);
    return 1;
}

/* Haversine distance in metres, as in Redis's geohashGetDistance.
   The sqrt argument is clamped to 1: antipodal points can round
   a hair past it, and asin of that is NaN. */
static double geo_distance_m(double lon1, double lat1,
                             double lon2, double lat2) {
    double lat1r = lat1 * M_PI / 180.0;
    double lon1r = lon1 * M_PI / 180.0;
    double lat2r = lat2 * M_PI / 180.0;
    double lon2r = lon2 * M_PI / 180.0;
    double u = sin((lat2r - lat1r) / 2.0);
    double v = sin((lon2r - lon1r) / 2.0);
    double a = u * u + cos(lat1r) * cos(lat2r) * v * v;
    if (a > 1.0) a = 1.0;
    return 2.0 * GEO_EARTH_RADIUS * asin(sqrt(a));
}

long long store_geoadd(Store *s, const char *key, double lon, double lat,
                       const char *member) {
    /* The score is the 52-bit geohash, exactly representable as a
       double, so store_zadd carries it like any other score — and
       a re-add at the same coordinates is the same-score no-op,
       which neither logs nor invalidates a WATCH, and the TTL
       survives, as with ZADD. */
    double score = (double)geo_encode(lon, lat);
    return store_zadd(s, key, member, score);
}

int store_geodist(Store *s, const char *key, const char *m1, const char *m2,
                  double *out) {
    double s1, s2;
    int r1 = store_zscore(s, key, m1, &s1);
    if (r1 == -1) return -1;
    if (r1 == 0) return 0;
    int r2 = store_zscore(s, key, m2, &s2);
    if (r2 == -1) return -1;
    if (r2 == 0) return 0;
    double lon1, lat1, lon2, lat2;
    geo_decode(geo_score_to_hash(s1), &lon1, &lat1);
    geo_decode(geo_score_to_hash(s2), &lon2, &lat2);
    *out = geo_distance_m(lon1, lat1, lon2, lat2);
    return 1;
}

int store_geopos(Store *s, const char *key, const char *member,
                 double *lon_out, double *lat_out) {
    double score;
    int rc = store_zscore(s, key, member, &score);
    if (rc == -1) return -1;
    if (rc == 0) return 0;
    if (!geo_decode_score(score, lon_out, lat_out)) return 0;
    return 1;
}

/* The 11-character base32 geohash string of a 52-bit hash, as in
   Redis's GEOHASH: the first ten characters take five bits each
   from the top of the hash, and the eleventh is always '0' — 52
   bits cannot fill 55, so Redis assumes zero for the last five. */
static void geo_hash_string(uint64_t hash, char out[12]) {
    static const char *alphabet = "0123456789bcdefghjkmnpqrstuvwxyz";
    for (int i = 0; i < 10; i++)
        out[i] = alphabet[(hash >> (52 - (i + 1) * 5)) & 0x1f];
    out[10] = '0';
    out[11] = '\0';
}

int store_geohash(Store *s, const char *key, const char *member,
                  char out[12]) {
    /* The stored hash uses latitude [-85.05112878, 85.05112878],
       while the standard geohash string uses [-90, 90], so the
       score is decoded back to the centre of its geohash cell and
       re-encoded over the standard ranges first, as in Redis. */
    double lon, lat;
    int rc = store_geopos(s, key, member, &lon, &lat);
    if (rc != 1) return rc;
    geo_hash_string(geo_encode_range(lon, lat, -90.0, 90.0), out);
    return 1;
}

/* ---------- GEOSEARCH / GEORADIUS ----------
 * The search is a full scan of the set: every member's score is
 * decoded back to the centre of its geohash cell and tested
 * against the shape. Redis prunes by geohash cell instead, so
 * this costs O(N) in the size of the set; the results are the
 * same, since the test itself is the same haversine distance
 * (for a circle) or the same degree-box test (for a box). */

struct geo_collect_arg {
    double clon, clat;
    int is_box;
    double radius_m, width_m, height_m;
    GeoSearchHit *hits;
    size_t n, cap;
    int oom;
};

/* qsort comparators for the collected hits: by distance from the
   centre, ties broken by member name so the order is deterministic
   (Redis leaves ties unspecified). */
static int cmp_geo_hit_asc(const void *a, const void *b) {
    const GeoSearchHit *x = a, *y = b;
    if (x->dist_m < y->dist_m) return -1;
    if (x->dist_m > y->dist_m) return 1;
    return strcmp(x->member, y->member);
}

static int cmp_geo_hit_desc(const void *a, const void *b) {
    const GeoSearchHit *x = a, *y = b;
    if (x->dist_m > y->dist_m) return -1;
    if (x->dist_m < y->dist_m) return 1;
    return strcmp(x->member, y->member);
}

static int geo_collect_cb(const char *member, double score, void *arg) {
    struct geo_collect_arg *a = arg;
    double lon, lat;
    /* A score that does not decode as a geohash has no position,
       so the member can never be inside the shape. */
    if (!geo_decode_score(score, &lon, &lat)) return 0;
    double dist = geo_distance_m(a->clon, a->clat, lon, lat);
    int inside;
    if (a->is_box) {
        /* The box is width_m east-west by height_m north-south,
           centred on (clon, clat): a point is inside when its
           latitude is within height/2 of the centre and its
           longitude within width/2, each converted to degrees at
           the centre's latitude (a degree of longitude is
           cos(latitude) of a degree of latitude). Longitude
           wraps, so the difference is normalised to [-180, 180]. */
        double m_per_deg = M_PI * GEO_EARTH_RADIUS / 180.0;
        double lat_half = (a->height_m / 2.0) / m_per_deg;
        double lon_half = (a->width_m / 2.0) /
                          (m_per_deg * cos(a->clat * M_PI / 180.0));
        double dlat = fabs(lat - a->clat);
        double dlon = fabs(lon - a->clon);
        if (dlon > 180.0) dlon = 360.0 - dlon;
        inside = dlat <= lat_half && dlon <= lon_half;
    } else {
        inside = dist <= a->radius_m;
    }
    if (!inside) return 0;
    if (a->n == a->cap) {
        size_t ncap = a->cap ? a->cap * 2 : 16;
        GeoSearchHit *grown = realloc(a->hits, ncap * sizeof(*grown));
        if (!grown) {
            a->oom = 1;
            return 1;
        }
        a->hits = grown;
        a->cap = ncap;
    }
    char *copy = strdup(member);
    if (!copy) {
        a->oom = 1;
        return 1;
    }
    a->hits[a->n].member = copy;
    a->hits[a->n].dist_m = dist;
    a->hits[a->n].lon = lon;
    a->hits[a->n].lat = lat;
    /* The hash is the stored score as an integer, as in Redis's
       WITHHASH; the score decoded fine, so it is below 2^64 and
       the cast is defined. */
    a->hits[a->n].hash = (uint64_t)score;
    a->n++;
    return 0;
}

int store_geosearch(Store *s, const char *key, double clon, double clat,
                    int is_box, double radius_m, double width_m,
                    double height_m, int sort_desc, long long count,
                    int count_any, GeoSearchHit **hits_out, size_t *n_out) {
    *hits_out = NULL;
    *n_out = 0;
    Entry *e = store_find(s, key, NULL);
    if (!e) return 0;
    if (e->type != ENTRY_ZSET) return -1;
    e->last_access = ++s->clock;
    struct geo_collect_arg arg;
    memset(&arg, 0, sizeof(arg));
    arg.clon = clon;
    arg.clat = clat;
    arg.is_box = is_box;
    arg.radius_m = radius_m;
    arg.width_m = width_m;
    arg.height_m = height_m;
    zset_foreach(e->zset, geo_collect_cb, &arg);
    if (arg.oom) {
        for (size_t i = 0; i < arg.n; i++) free(arg.hits[i].member);
        free(arg.hits);
        return -2;
    }
    if (!count_any)
        qsort(arg.hits, arg.n, sizeof(*arg.hits),
              sort_desc ? cmp_geo_hit_desc : cmp_geo_hit_asc);
    if (count >= 0 && (unsigned long long)count < (unsigned long long)arg.n) {
        for (size_t i = (size_t)count; i < arg.n; i++)
            free(arg.hits[i].member);
        arg.n = (size_t)count;
    }
    *hits_out = arg.hits;
    *n_out = arg.n;
    return 0;
}

/* ---------- streams ---------- */

static void stream_entry_free(StreamEntry *en) {
    for (size_t i = 0; i < en->nfields; i++) {
        free(en->fields[i]);
        free(en->values[i]);
    }
    free(en->fields);
    free(en->values);
    memset(en, 0, sizeof(*en));
}

static void stream_free(Stream *st) {
    if (!st) return;
    for (size_t i = 0; i < st->len; i++) stream_entry_free(&st->entries[i]);
    free(st->entries);
    free(st);
}

static Stream *stream_new(void) {
    return calloc(1, sizeof(Stream));
}

/* Strict unsigned parse: digits only, no sign, no overflow. */
static int stream_parse_u64(const char *s, size_t n, uint64_t *out) {
    if (n == 0) return 0;
    uint64_t v = 0;
    for (size_t i = 0; i < n; i++) {
        if (s[i] < '0' || s[i] > '9') return 0;
        if (v > (UINT64_MAX - (uint64_t)(s[i] - '0')) / 10) return 0;
        v = v * 10 + (uint64_t)(s[i] - '0');
    }
    *out = v;
    return 1;
}

/* Resolve the ID argument against the stream's top ID. "*" is fully
   automatic; "ms-seq" is explicit; "ms-*" or "ms-" is partial (the
   ms is given, the sequence is automatic). On success *ms_out and
   *seq_out hold the new ID. Returns 0 on success, -3 = not a valid
   stream ID, -4 = the ID is equal to or smaller than the top ID,
   -5 = the ID is 0-0. */
static int stream_resolve_id(const char *id, const Stream *st,
                             uint64_t *ms_out, uint64_t *seq_out) {
    uint64_t last_ms = st ? st->last_ms : 0;
    uint64_t last_seq = st ? st->last_seq : 0;
    int have_top = st && st->len > 0;
    if (strcmp(id, "*") == 0) {
        uint64_t ms = (uint64_t)now_ms();
        if (have_top && ms <= last_ms) {
            *ms_out = last_ms;
            *seq_out = last_seq + 1;
        } else {
            *ms_out = ms;
            *seq_out = 0;
        }
        return 0;
    }
    const char *dash = strchr(id, '-');
    if (!dash) return -3;
    size_t ms_len = (size_t)(dash - id);
    const char *seq_s = dash + 1;
    size_t seq_len = strlen(seq_s);
    uint64_t ms;
    if (!stream_parse_u64(id, ms_len, &ms)) return -3;
    if (seq_len == 0 || strcmp(seq_s, "*") == 0) {
        /* Partial ID: the sequence is automatic for this ms. */
        if (have_top && ms < last_ms) return -4;
        if (have_top && ms == last_ms) {
            *ms_out = ms;
            *seq_out = last_seq + 1;
        } else {
            *ms_out = ms;
            *seq_out = 0;
        }
        return 0;
    }
    uint64_t seq;
    if (!stream_parse_u64(seq_s, seq_len, &seq)) return -3;
    if (ms == 0 && seq == 0) return -5;
    if (have_top && (ms < last_ms || (ms == last_ms && seq <= last_seq)))
        return -4;
    *ms_out = ms;
    *seq_out = seq;
    return 0;
}

int store_xadd(Store *s, const char *key, const char *id,
               char *const *pairs, int nfields, long long maxlen,
               char *id_out) {
    Entry *e = store_find(s, key, NULL);
    if (e && e->type != ENTRY_STREAM) return -1;
    Stream *st = e ? e->stream : NULL;
    uint64_t ms, seq;
    int rc = stream_resolve_id(id, st, &ms, &seq);
    if (rc != 0) return rc;
    /* Build the entry first, so an OOM leaves the stream untouched. */
    StreamEntry en;
    memset(&en, 0, sizeof(en));
    en.ms = ms;
    en.seq = seq;
    en.cap = (size_t)nfields > 0 ? (size_t)nfields : 1;
    en.fields = malloc(en.cap * sizeof(char *));
    en.values = malloc(en.cap * sizeof(char *));
    if (!en.fields || !en.values) {
        free(en.fields);
        free(en.values);
        return -2;
    }
    for (int i = 0; i < nfields; i++) {
        en.fields[i] = strdup(pairs[2 * i]);
        en.values[i] = strdup(pairs[2 * i + 1]);
        if (!en.fields[i] || !en.values[i]) {
            for (int j = 0; j <= i; j++) {
                free(en.fields[j]);
                free(en.values[j]);
            }
            free(en.fields);
            free(en.values);
            return -2;
        }
        en.nfields++;
    }
    if (!e) {
        if (s->count * LOAD_FACTOR_DEN > s->cap * LOAD_FACTOR_NUM)
            store_resize(s);
        e = calloc(1, sizeof(Entry));
        if (!e) {
            stream_entry_free(&en);
            return -2;
        }
        e->key = strdup(key);
        e->stream = stream_new();
        if (!e->key || !e->stream) {
            entry_free(e);
            stream_entry_free(&en);
            return -2;
        }
        e->type = ENTRY_STREAM;
        e->expire_at = -1;
        e->last_access = ++s->clock;
        e->version = ++s->mod_seq;
        size_t idx = hash_key(key) % s->cap;
        e->next = s->buckets[idx];
        s->buckets[idx] = e;
        s->count++;
        st = e->stream;
    }
    if (st->len == st->cap) {
        size_t ncap = st->cap ? st->cap * 2 : 8;
        StreamEntry *nen = realloc(st->entries, ncap * sizeof(*nen));
        if (!nen) {
            stream_entry_free(&en);
            return -2;
        }
        st->entries = nen;
        st->cap = ncap;
    }
    st->entries[st->len++] = en;
    st->last_ms = ms;
    st->last_seq = seq;
    /* MAXLEN trims from the front (the oldest entries go first). A
       trim that empties the stream deletes the key, as in Redis, so
       a snapshot or a full sync never has to carry an empty stream. */
    if (maxlen >= 0) {
        while ((long long)st->len > maxlen) {
            stream_entry_free(&st->entries[0]);
            memmove(&st->entries[0], &st->entries[1],
                    (st->len - 1) * sizeof(*st->entries));
            st->len--;
        }
        if (st->len == 0) {
            snprintf(id_out, 48, "%llu-%llu",
                     (unsigned long long)ms, (unsigned long long)seq);
            store_del(s, key);
            return 0;
        }
    }
    e->last_access = ++s->clock;
    entry_touch(s, e);
    snprintf(id_out, 48, "%llu-%llu",
             (unsigned long long)ms, (unsigned long long)seq);
    return 0;
}

int stream_foreach(const Stream *st, stream_foreach_fn fn, void *arg) {
    if (!st) return -1;
    for (size_t i = 0; i < st->len; i++) {
        const StreamEntry *en = &st->entries[i];
        int rc = fn(en->ms, en->seq, en->fields, en->values, en->nfields,
                    arg);
        if (rc != 0) return rc;
    }
    return 0;
}

long long store_xlen(Store *s, const char *key) {
    Entry *e = store_find(s, key, NULL);
    if (!e) return 0;
    if (e->type != ENTRY_STREAM) return -1;
    e->last_access = ++s->clock;
    return (long long)e->stream->len;
}

int store_stream_top(Store *s, const char *key, uint64_t *ms_out,
                     uint64_t *seq_out) {
    Entry *e = store_find(s, key, NULL);
    if (!e) {
        *ms_out = 0;
        *seq_out = 0;
        return 0;
    }
    if (e->type != ENTRY_STREAM) return -1;
    e->last_access = ++s->clock;
    if (e->stream->len == 0) {
        /* A stream with no entries, which the store never keeps
           around, has top ID 0-0, so every later entry qualifies. */
        *ms_out = 0;
        *seq_out = 0;
        return 0;
    }
    *ms_out = e->stream->last_ms;
    *seq_out = e->stream->last_seq;
    return 0;
}

int stream_parse_bound(const char *s, int is_end, uint64_t *ms_out,
                       uint64_t *seq_out) {
    if (strcmp(s, "-") == 0) {
        *ms_out = 0;
        *seq_out = 0;
        return 0;
    }
    if (strcmp(s, "+") == 0) {
        *ms_out = UINT64_MAX;
        *seq_out = UINT64_MAX;
        return 0;
    }
    const char *dash = strchr(s, '-');
    if (!dash) {
        /* A bare ms: the whole millisecond. A start bound takes the
           first sequence in it, an end bound the last. */
        uint64_t ms;
        if (!stream_parse_u64(s, strlen(s), &ms)) return -1;
        *ms_out = ms;
        *seq_out = is_end ? UINT64_MAX : 0;
        return 0;
    }
    uint64_t ms, seq;
    if (!stream_parse_u64(s, (size_t)(dash - s), &ms)) return -1;
    if (!stream_parse_u64(dash + 1, strlen(dash + 1), &seq)) return -1;
    *ms_out = ms;
    *seq_out = seq;
    return 0;
}

/* One stream entry being collected for an XRANGE reply: a copy of
   the entry, so the reply can be built after the scan. */
struct xrange_collect_arg {
    uint64_t start_ms, start_seq, end_ms, end_seq;
    long long count; /* < 0 means no limit */
    StreamEntry *entries;
    size_t n, cap;
    int oom;
};

static int xrange_collect_cb(uint64_t ms, uint64_t seq, char *const *fields,
                             char *const *values, size_t nfields,
                             void *arg) {
    struct xrange_collect_arg *a = arg;
    if (ms < a->start_ms ||
        (ms == a->start_ms && seq < a->start_seq))
        return 0; /* before the range: skip */
    if (ms > a->end_ms || (ms == a->end_ms && seq > a->end_seq))
        return 1; /* past the range: entries are in ID order, stop */
    if (a->count >= 0 && (long long)a->n >= a->count) return 1;
    if (a->n == a->cap) {
        size_t ncap = a->cap ? a->cap * 2 : 8;
        StreamEntry *nen = realloc(a->entries, ncap * sizeof(*nen));
        if (!nen) {
            a->oom = 1;
            return -1;
        }
        a->entries = nen;
        a->cap = ncap;
    }
    StreamEntry *en = &a->entries[a->n];
    memset(en, 0, sizeof(*en));
    en->ms = ms;
    en->seq = seq;
    en->cap = nfields > 0 ? nfields : 1;
    en->fields = malloc(en->cap * sizeof(char *));
    en->values = malloc(en->cap * sizeof(char *));
    if (!en->fields || !en->values) {
        free(en->fields);
        free(en->values);
        a->oom = 1;
        return -1;
    }
    for (size_t i = 0; i < nfields; i++) {
        en->fields[i] = strdup(fields[i]);
        en->values[i] = strdup(values[i]);
        if (!en->fields[i] || !en->values[i]) {
            for (size_t j = 0; j <= i; j++) {
                free(en->fields[j]);
                free(en->values[j]);
            }
            free(en->fields);
            free(en->values);
            a->oom = 1;
            return -1;
        }
        en->nfields++;
    }
    a->n++;
    return 0;
}

int store_xrange(Store *s, const char *key, uint64_t start_ms,
                 uint64_t start_seq, uint64_t end_ms, uint64_t end_seq,
                 long long count, StreamEntry **entries_out,
                 size_t *n_out) {
    *entries_out = NULL;
    *n_out = 0;
    Entry *e = store_find(s, key, NULL);
    if (!e) return 0;
    if (e->type != ENTRY_STREAM) return -1;
    e->last_access = ++s->clock;
    struct xrange_collect_arg arg;
    memset(&arg, 0, sizeof(arg));
    arg.start_ms = start_ms;
    arg.start_seq = start_seq;
    arg.end_ms = end_ms;
    arg.end_seq = end_seq;
    arg.count = count;
    int rc = stream_foreach(e->stream, xrange_collect_cb, &arg);
    if (arg.oom || rc == -1) {
        stream_entries_free(arg.entries, arg.n);
        return -2;
    }
    *entries_out = arg.entries;
    *n_out = arg.n;
    return 0;
}

void stream_entries_free(StreamEntry *entries, size_t n) {
    if (!entries) return;
    for (size_t i = 0; i < n; i++) stream_entry_free(&entries[i]);
    free(entries);
}
