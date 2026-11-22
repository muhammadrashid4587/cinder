#define _GNU_SOURCE
#include "rdb.h"

#include <errno.h>
#include <fcntl.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#define RDB_VERSION 1
#define RDB_MAGIC "CINDER"
#define RDB_MAGIC_LEN 6
#define RDB_END 0xFF

/* ---------- writers ---------- */

static int w_u8(FILE *f, unsigned char v) {
    return fwrite(&v, 1, 1, f) == 1 ? 0 : -1;
}

static int w_u32(FILE *f, uint32_t v) {
    unsigned char b[4] = {(unsigned char)(v & 0xff),
                          (unsigned char)((v >> 8) & 0xff),
                          (unsigned char)((v >> 16) & 0xff),
                          (unsigned char)((v >> 24) & 0xff)};
    return fwrite(b, 1, 4, f) == 4 ? 0 : -1;
}

static int w_i64(FILE *f, int64_t v) {
    unsigned char b[8];
    for (int i = 0; i < 8; i++) b[i] = (unsigned char)((v >> (8 * i)) & 0xff);
    return fwrite(b, 1, 8, f) == 8 ? 0 : -1;
}

static int w_str(FILE *f, const char *s) {
    size_t n = strlen(s);
    if (n > UINT32_MAX) return -1;
    if (w_u32(f, (uint32_t)n) < 0) return -1;
    return fwrite(s, 1, n, f) == n ? 0 : -1;
}

/* fsync the directory holding path, so the rename below is durable. */
static void fsync_dir_of(const char *path) {
    char dir[1024];
    const char *slash = strrchr(path, '/');
    if (!slash) {
        snprintf(dir, sizeof(dir), ".");
    } else if (slash == path) {
        snprintf(dir, sizeof(dir), "/");
    } else {
        size_t n = (size_t)(slash - path);
        if (n >= sizeof(dir)) n = sizeof(dir) - 1;
        memcpy(dir, path, n);
        dir[n] = '\0';
    }
    int dfd = open(dir, O_RDONLY);
    if (dfd >= 0) {
        fsync(dfd);
        close(dfd);
    }
}

typedef struct {
    FILE *f;
    int err;
} SaveCtx;

static int save_entry(const Entry *e, void *arg) {
    SaveCtx *ctx = arg;
    if (ctx->err) return 1;
    FILE *f = ctx->f;
    if (w_u8(f, (unsigned char)e->type) < 0 || w_i64(f, e->expire_at) < 0 ||
        w_str(f, e->key) < 0) {
        ctx->err = 1;
        return 1;
    }
    if (e->type == ENTRY_STRING) {
        if (w_str(f, e->value) < 0) { ctx->err = 1; return 1; }
    } else if (e->type == ENTRY_LIST) {
        List *l = e->list;
        if (w_u32(f, (uint32_t)l->len) < 0) { ctx->err = 1; return 1; }
        for (size_t i = 0; i < l->len; i++) {
            if (w_str(f, l->items[(l->head + i) % l->cap]) < 0) {
                ctx->err = 1;
                return 1;
            }
        }
    } else if (e->type == ENTRY_SET) {
        Set *st = e->set;
        if (w_u32(f, (uint32_t)st->len) < 0) { ctx->err = 1; return 1; }
        for (size_t i = 0; i < st->len; i++) {
            if (w_str(f, st->members[i]) < 0) { ctx->err = 1; return 1; }
        }
    } else if (e->type == ENTRY_HASH) {
        Hash *h = e->hash;
        if (w_u32(f, (uint32_t)h->len) < 0) { ctx->err = 1; return 1; }
        for (size_t i = 0; i < h->len; i++) {
            if (w_str(f, h->fields[i]) < 0 || w_str(f, h->values[i]) < 0) {
                ctx->err = 1;
                return 1;
            }
        }
    }
    return 0;
}

int rdb_save(Store *s, const char *path) {
    char *tmp = malloc(strlen(path) + 5);
    if (!tmp) return -1;
    snprintf(tmp, strlen(path) + 5, "%s.tmp", path);
    FILE *f = fopen(tmp, "wb");
    if (!f) {
        int e = errno;
        free(tmp);
        errno = e;
        return -1;
    }
    int rc = -1;
    if (fwrite(RDB_MAGIC, 1, RDB_MAGIC_LEN, f) != RDB_MAGIC_LEN) goto done;
    if (w_u32(f, RDB_VERSION) < 0) goto done;
    SaveCtx ctx = {.f = f, .err = 0};
    store_foreach(s, save_entry, &ctx);
    if (ctx.err) goto done;
    if (w_u8(f, RDB_END) < 0) goto done;
    if (fflush(f) != 0) goto done;
    if (fsync(fileno(f)) != 0) goto done;
    if (fclose(f) != 0) {
        f = NULL; /* already closed (or failed); don't close twice */
        goto done;
    }
    f = NULL;
    if (rename(tmp, path) != 0) goto done;
    fsync_dir_of(path);
    rc = 0;
done:
    if (f) fclose(f);
    if (rc != 0) unlink(tmp);
    free(tmp);
    return rc;
}

/* ---------- readers ---------- */

typedef struct {
    const unsigned char *data;
    size_t len;
    size_t off;
} Reader;

static int r_u8(Reader *r, unsigned char *out) {
    if (r->off + 1 > r->len) return -1;
    *out = r->data[r->off++];
    return 0;
}

static int r_u32(Reader *r, uint32_t *out) {
    if (r->off + 4 > r->len) return -1;
    *out = (uint32_t)r->data[r->off] |
           ((uint32_t)r->data[r->off + 1] << 8) |
           ((uint32_t)r->data[r->off + 2] << 16) |
           ((uint32_t)r->data[r->off + 3] << 24);
    r->off += 4;
    return 0;
}

static int r_i64(Reader *r, int64_t *out) {
    if (r->off + 8 > r->len) return -1;
    uint64_t v = 0;
    for (int i = 0; i < 8; i++) v |= (uint64_t)r->data[r->off + i] << (8 * i);
    r->off += 8;
    *out = (int64_t)v;
    return 0;
}

/* Read a length-prefixed string, NUL-terminated. Caller frees. */
static int r_str(Reader *r, char **out) {
    uint32_t n;
    if (r_u32(r, &n) < 0) return -1;
    if (r->off + n > r->len) return -1;
    char *s = malloc((size_t)n + 1);
    if (!s) return -1;
    memcpy(s, r->data + r->off, n);
    s[n] = '\0';
    r->off += n;
    *out = s;
    return 0;
}

long long rdb_load(Store *s, const char *path) {
    FILE *f = fopen(path, "rb");
    if (!f) return -1; /* errno ENOENT when there is no snapshot yet */
    fseek(f, 0, SEEK_END);
    long size = ftell(f);
    fseek(f, 0, SEEK_SET);
    if (size < 0) {
        fclose(f);
        errno = EINVAL;
        return -1;
    }
    unsigned char *data = malloc((size_t)size ? (size_t)size : 1);
    if (!data) {
        fclose(f);
        return -1;
    }
    if (size > 0 && fread(data, 1, (size_t)size, f) != (size_t)size) {
        free(data);
        fclose(f);
        errno = EINVAL;
        return -1;
    }
    fclose(f);

    Reader r = {.data = data, .len = (size_t)size, .off = 0};
    long long loaded = 0;
    if (r.len < RDB_MAGIC_LEN ||
        memcmp(r.data, RDB_MAGIC, RDB_MAGIC_LEN) != 0) {
        free(data);
        errno = EINVAL;
        return -1;
    }
    r.off = RDB_MAGIC_LEN;
    uint32_t version;
    if (r_u32(&r, &version) < 0 || version != RDB_VERSION) {
        free(data);
        errno = EINVAL;
        return -1;
    }
    for (;;) {
        unsigned char type;
        if (r_u8(&r, &type) < 0) goto malformed;
        if (type == RDB_END) break;
        if (type > ENTRY_HASH) goto malformed;
        int64_t expire_at;
        char *key = NULL;
        if (r_i64(&r, &expire_at) < 0) goto malformed;
        if (r_str(&r, &key) < 0) goto malformed;
        /* A key whose TTL already passed is gone, as if it had been
           lazily expired before the save. */
        int expired = expire_at != -1 && expire_at <= now_ms();
        if (type == ENTRY_STRING) {
            char *val = NULL;
            if (r_str(&r, &val) < 0) { free(key); goto malformed; }
            if (!expired) store_set(s, key, val, expire_at);
            free(val);
        } else if (type == ENTRY_LIST || type == ENTRY_SET) {
            uint32_t n;
            if (r_u32(&r, &n) < 0) { free(key); goto malformed; }
            for (uint32_t i = 0; i < n; i++) {
                char *el = NULL;
                if (r_str(&r, &el) < 0) { free(key); goto malformed; }
                if (!expired) {
                    if (type == ENTRY_LIST) store_rpush(s, key, el);
                    else store_sadd(s, key, el);
                }
                free(el);
            }
            if (!expired && expire_at != -1) store_expire(s, key, expire_at);
        } else { /* ENTRY_HASH */
            uint32_t n;
            if (r_u32(&r, &n) < 0) { free(key); goto malformed; }
            for (uint32_t i = 0; i < n; i++) {
                char *field = NULL, *val = NULL;
                if (r_str(&r, &field) < 0) { free(key); goto malformed; }
                if (r_str(&r, &val) < 0) {
                    free(field);
                    free(key);
                    goto malformed;
                }
                if (!expired) store_hset(s, key, field, val);
                free(field);
                free(val);
            }
            if (!expired && expire_at != -1) store_expire(s, key, expire_at);
        }
        free(key);
        if (!expired) loaded++;
    }
    if (r.off != r.len) goto malformed;
    free(data);
    return loaded;

malformed:
    free(data);
    errno = EINVAL;
    return -1;
}
