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
    h->dict = hdict_new();
    if (!h->fields || !h->values || !h->dict) {
        hash_free(h);
        return NULL;
    }
    return h;
}

/* Index of field, or -1 when absent. */
static long long hash_find(Hash *h, const char *field) {
    return hdict_find(h->dict, field);
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
    if (!hdict_set(h->dict, f, h->len - 1)) {
        /* The field is appended but unindexed; roll the append back
           rather than leave a field no lookup can find. */
        h->len--;
        free(h->fields[h->len]);
        free(h->values[h->len]);
        h->fields[h->len] = NULL;
        h->values[h->len] = NULL;
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
    }
    h->len--;
    h->fields[h->len] = NULL;
    h->values[h->len] = NULL;
    /* The fields that moved down one slot need their dict indexes
       moved with them; re-indexing an existing field cannot fail. */
    for (size_t i = (size_t)idx; i < h->len; i++)
        hdict_set(h->dict, h->fields[i], i);
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
           so it does not touch the version; a new field or a changed
           value does. */
        long long idx = hash_find(e->hash, field);
        int changed = idx < 0 || strcmp(e->hash->values[idx], value) != 0;
        int rc = hash_put(e->hash, field, value);
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
    if (hash_put(e->hash, field, vbuf) < 0) return -4;
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
    if (hash_put(e->hash, field, vbuf) < 0) return -4;
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
