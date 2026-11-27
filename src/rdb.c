#define _GNU_SOURCE
#include "rdb.h"

#include <errno.h>
#include <fcntl.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#define RDB_VERSION 2
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

/* Length-aware w_str: writes n bytes verbatim, NUL bytes included,
   so a SETRANGE-padded string value round-trips through a snapshot. */
static int w_bytes(FILE *f, const char *s, size_t n) {
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

/* One member of a sorted set being written to the snapshot: the
   score is a %.17g string, which round-trips a double exactly, so
   the snapshot needs no binary float format and stays portable. */
static int save_zset_member(const char *member, double score, void *arg) {
    SaveCtx *ctx = arg;
    char scorebuf[32];
    snprintf(scorebuf, sizeof(scorebuf), "%.17g", score);
    if (w_str(ctx->f, member) < 0 || w_str(ctx->f, scorebuf) < 0) {
        ctx->err = 1;
        return 1;
    }
    return 0;
}

static int save_entry(const Entry *e, void *arg) {
    SaveCtx *ctx = arg;
    if (ctx->err) return 1;
    FILE *f = ctx->f;
    /* A hash whose fields have all expired saves nothing at all:
       the record would reload as a key that never comes into being,
       and counting it as loaded would be wrong. The expiry instant
       is read once so the live count and the written fields agree. */
    long long now = now_ms();
    size_t hash_alive = 0;
    if (e->type == ENTRY_HASH) {
        Hash *h = e->hash;
        for (size_t i = 0; i < h->len; i++)
            if (h->field_expire[i] == -1 || h->field_expire[i] > now)
                hash_alive++;
        if (hash_alive == 0) return 0;
    }
    if (w_u8(f, (unsigned char)e->type) < 0 || w_i64(f, e->expire_at) < 0 ||
        w_str(f, e->key) < 0) {
        ctx->err = 1;
        return 1;
    }
    if (e->type == ENTRY_STRING) {
        if (w_bytes(f, e->value, e->value_len) < 0) { ctx->err = 1; return 1; }
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
        /* Version 2 writes one absolute expiry per field alongside
           the pair, so a field TTL survives the snapshot; expired
           fields are left out, as with a key whose TTL passed. */
        if (w_u32(f, (uint32_t)hash_alive) < 0) { ctx->err = 1; return 1; }
        for (size_t i = 0; i < h->len; i++) {
            if (h->field_expire[i] != -1 && h->field_expire[i] <= now)
                continue;
            if (w_str(f, h->fields[i]) < 0 || w_str(f, h->values[i]) < 0 ||
                w_i64(f, h->field_expire[i]) < 0) {
                ctx->err = 1;
                return 1;
            }
        }
    } else if (e->type == ENTRY_ZSET) {
        ZSet *z = e->zset;
        if (w_u32(f, (uint32_t)z->len) < 0) { ctx->err = 1; return 1; }
        if (zset_foreach(z, save_zset_member, ctx) != 0) return 1;
    }
    return 0;
}

/* Write the whole snapshot (magic, version, one record per key,
   end marker) to an already-open stream. Shared by rdb_save (which
   writes straight to the temp file) and rdb_serialize (which writes
   to a memory stream, so the event loop can hand the bytes to a
   worker thread instead of doing disk I/O itself). */
static int rdb_write_stream(FILE *f, Store *s) {
    if (fwrite(RDB_MAGIC, 1, RDB_MAGIC_LEN, f) != RDB_MAGIC_LEN) return -1;
    if (w_u32(f, RDB_VERSION) < 0) return -1;
    SaveCtx ctx = {.f = f, .err = 0};
    store_foreach(s, save_entry, &ctx);
    if (ctx.err) return -1;
    if (w_u8(f, RDB_END) < 0) return -1;
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
    if (rdb_write_stream(f, s) < 0) goto done;
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

/* Serialize the store to a freshly malloc'd buffer (*out, *outlen).
   The caller owns the buffer. Returns 0 on success, -1 on error
   (errno set when it comes from the stream). No disk I/O happens
   here: the bytes are meant to be handed to a worker thread, which
   writes them with rdb_write_snapshot. */
int rdb_serialize(Store *s, char **out, size_t *outlen) {
    char *buf = NULL;
    size_t len = 0;
    FILE *f = open_memstream(&buf, &len);
    if (!f) return -1;
    int wrc = rdb_write_stream(f, s);
    int crc = fclose(f);
    /* On a failed fclose the C library releases the buffer, so
       nothing is freed here on either error path; on success buf/len
       are the serialized snapshot. */
    if (wrc < 0 || crc != 0) return -1;
    *out = buf;
    *outlen = len;
    return 0;
}

/* Write already-serialized snapshot bytes to path atomically: to
   path + tmp_suffix, fsync, rename over path, fsync the directory.
   Safe to call from a worker thread: it touches only the bytes it
   was given and the files it creates. The caller picks tmp_suffix so
   a background save (".bgsave.tmp") never clobbers a concurrent
   synchronous SAVE (".tmp"); both snapshots are valid, and the last
   rename wins. Returns 0 on success, -1 on any I/O error. */
int rdb_write_snapshot(const char *path, const char *tmp_suffix,
                       const char *data, size_t len) {
    size_t tmplen = strlen(path) + strlen(tmp_suffix) + 1;
    char *tmp = malloc(tmplen);
    if (!tmp) return -1;
    snprintf(tmp, tmplen, "%s%s", path, tmp_suffix);
    FILE *f = fopen(tmp, "wb");
    if (!f) {
        int e = errno;
        free(tmp);
        errno = e;
        return -1;
    }
    int rc = -1;
    if (fwrite(data, 1, len, f) == len && fflush(f) == 0 &&
        fsync(fileno(f)) == 0 && fclose(f) == 0) {
        f = NULL; /* closed by the fclose above */
        if (rename(tmp, path) == 0) {
            fsync_dir_of(path);
            rc = 0;
        }
    }
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

/* Length-aware string read: *len_out is the stored byte length, NUL
   bytes included, so a SETRANGE-padded value loads back whole.
   The buffer is NUL-terminated at [*len_out]. Caller frees. */
static int r_str_len(Reader *r, char **out, size_t *len_out) {
    uint32_t n;
    if (r_u32(r, &n) < 0) return -1;
    if (r->off + n > r->len) return -1;
    char *s = malloc((size_t)n + 1);
    if (!s) return -1;
    memcpy(s, r->data + r->off, n);
    s[n] = '\0';
    r->off += n;
    *out = s;
    if (len_out) *len_out = n;
    return 0;
}

/* Read a length-prefixed string, NUL-terminated. Caller frees. */
static int r_str(Reader *r, char **out) {
    return r_str_len(r, out, NULL);
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
    /* Version 1 snapshots (no per-field expiry) still load: their
       hash fields simply come back without a TTL. */
    if (r_u32(&r, &version) < 0 || (version != 1 && version != 2)) {
        free(data);
        errno = EINVAL;
        return -1;
    }
    for (;;) {
        unsigned char type;
        if (r_u8(&r, &type) < 0) goto malformed;
        if (type == RDB_END) break;
        if (type > ENTRY_ZSET) goto malformed;
        int64_t expire_at;
        char *key = NULL;
        if (r_i64(&r, &expire_at) < 0) goto malformed;
        if (r_str(&r, &key) < 0) goto malformed;
        /* A key whose TTL already passed is gone, as if it had been
           lazily expired before the save. */
        int expired = expire_at != -1 && expire_at <= now_ms();
        if (type == ENTRY_STRING) {
            char *val = NULL;
            size_t vlen = 0;
            if (r_str_len(&r, &val, &vlen) < 0) { free(key); goto malformed; }
            if (!expired) store_set_len(s, key, val, vlen, expire_at);
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
        } else { /* ENTRY_HASH or ENTRY_ZSET */
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
                /* Version 2 hash records carry one absolute field
                   expiry per pair, after the value. */
                int64_t field_expire = -1;
                if (type == ENTRY_HASH && version == 2) {
                    if (r_i64(&r, &field_expire) < 0) {
                        free(field);
                        free(val);
                        free(key);
                        goto malformed;
                    }
                }
                if (!expired) {
                    if (type == ENTRY_HASH) {
                        store_hset(s, key, field, val);
                        if (field_expire != -1)
                            store_hexpire_field(s, key, field, field_expire,
                                                HFIELD_EXPIRE_NONE);
                    } else {
                        /* The score string is %.17g from the writer,
                           so it parses back to the exact double. */
                        store_zadd(s, key, field, strtod(val, NULL));
                    }
                }
                free(field);
                free(val);
            }
            if (!expired && expire_at != -1) store_expire(s, key, expire_at);
        }
        /* Every field's expiry had already passed while loading, so
           the key never came into being: do not count it. The check
           runs before key is freed. */
        int key_gone = !expired && type == ENTRY_HASH &&
                       store_type(s, key) == 0;
        free(key);
        if (!expired && !key_gone) loaded++;
    }
    if (r.off != r.len) goto malformed;
    free(data);
    return loaded;

malformed:
    free(data);
    errno = EINVAL;
    return -1;
}
