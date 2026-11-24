/* cinder: a Redis-compatible in-memory key-value store in C.
 *
 * Single-threaded, epoll-based event loop for commands, plus a small
 * worker thread pool that runs background saves (BGSAVE) off the
 * loop. String values with TTLs, approximate LRU eviction, an
 * append-only file for durability, and RDB-style snapshots (SAVE)
 * that are reloaded on startup.
 * Speaks RESP so real redis clients (redis-cli, redis-benchmark, and
 * any RESP client library) work against it.
 */
#define _GNU_SOURCE
#include <arpa/inet.h>
#include <ctype.h>
#include <errno.h>
#include <fcntl.h>
#include <getopt.h>
#include <math.h>
#include <netdb.h>
#include <netinet/in.h>
#include <pthread.h>
#include <signal.h>
#include <stdarg.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <sys/epoll.h>
#include <sys/eventfd.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include "aof.h"
#include "rdb.h"
#include "resp.h"
#include "store.h"
#include "threadpool.h"

#define CINDER_VERSION "0.34.0"
#define INBUF_CAP 65536

static Store *g_store;
static char g_rdb_path[1024];
static char g_repl_state_path[1024]; /* data dir + "/replication.state" */
static long long g_start_ms;
static long long g_commands_processed;
static size_t g_max_entries; /* 0 = unlimited */
static int g_replaying;

/* Replication offsets and backlog. The primary counts every byte of
   the live write stream it forwards (g_master_repl_offset) under its
   run id (g_replid, fresh per process); a replica learns the primary's
   run id and offset from the +FULLRESYNC line and counts the stream
   bytes it applies (g_slave_repl_offset). The backlog is a ring buffer
   holding the most recent bytes of that stream (default the last
   1 MiB, --repl-backlog-size), so a PSYNC whose offset is still inside
   it gets a partial resync (+CONTINUE plus only the missed bytes)
   instead of a full sync. */
static char g_replid[41];
static long long g_master_repl_offset;
static char g_primary_replid[41];
static long long g_slave_repl_offset;
/* The backlog records the exact byte stream g_master_repl_offset
   counts, whether or not a replica is currently attached, so a replica
   that drops and reconnects can resume from any offset still inside
   it. g_backlog_start is the stream offset of the oldest byte held
   (g_backlog_head); bytes older than that have dropped off the front
   and can only be had with a full sync. */
static char *g_backlog;
static size_t g_backlog_cap = 1048576; /* 1 MiB default */
static size_t g_backlog_len;
static size_t g_backlog_head;
static long long g_backlog_start;
static char g_master_host[256];
static long long g_master_port;
static volatile sig_atomic_t g_running = 1;

/* ---------- thread pool and background saves ----------
 * The event loop stays single-threaded for commands: the store is
 * never touched off it. The pool exists for work that is safe to
 * move, which today is exactly one job: BGSAVE. The loop serializes
 * the store to memory (a consistent picture, taken while the loop
 * owns the store), and a worker writes those bytes to dump.rdb
 * (temp file, fsync, rename) while the server keeps answering
 * commands. The worker touches only its own buffer and the snapshot
 * files; the only shared state is the small status block below,
 * guarded by g_bgsave_mu. */
static ThreadPool *g_pool;
static int g_nthreads = 1; /* worker threads; --threads, default 1 */
static pthread_mutex_t g_bgsave_mu = PTHREAD_MUTEX_INITIALIZER;
static int g_bgsave_in_progress;
static int g_bgsave_last_status; /* 0 = none yet, 1 = ok, -1 = error */
static long long g_bgsave_last_ms; /* epoch ms of the last finished save, 0 = never */

typedef struct {
    char *data; /* serialized snapshot bytes; the worker frees them */
    size_t len;
    char path[1024]; /* dump.rdb path, copied at submit time */
} BgsaveJob;

static void bgsave_run(void *arg) {
    BgsaveJob *job = arg;
    int rc = rdb_write_snapshot(job->path, ".bgsave.tmp", job->data, job->len);
    free(job->data);
    free(job);
    pthread_mutex_lock(&g_bgsave_mu);
    g_bgsave_in_progress = 0;
    g_bgsave_last_status = rc == 0 ? 1 : -1;
    g_bgsave_last_ms = now_ms();
    pthread_mutex_unlock(&g_bgsave_mu);
    printf("cinder: background save %s\n", rc == 0 ? "finished" : "failed");
    fflush(stdout);
}

/* Copy the background-save status out from under the mutex. */
static void bgsave_info(int *in_progress, int *last_status, long long *last_ms) {
    pthread_mutex_lock(&g_bgsave_mu);
    *in_progress = g_bgsave_in_progress;
    *last_status = g_bgsave_last_status;
    *last_ms = g_bgsave_last_ms;
    pthread_mutex_unlock(&g_bgsave_mu);
}

static void on_signal(int sig) {
    (void)sig;
    g_running = 0;
}

/* ---------- dynamic output buffer ---------- */

typedef struct {
    char *data;
    size_t len;
    size_t cap;
} Dbuf;

static int dbuf_reserve(Dbuf *b, size_t extra) {
    if (b->len + extra <= b->cap) return 0;
    size_t ncap = b->cap ? b->cap : 256;
    while (ncap < b->len + extra) ncap *= 2;
    char *nd = realloc(b->data, ncap);
    if (!nd) return -1;
    b->data = nd;
    b->cap = ncap;
    return 0;
}

static int dbuf_put(Dbuf *b, const void *src, size_t n) {
    if (dbuf_reserve(b, n) < 0) return -1;
    memcpy(b->data + b->len, src, n);
    b->len += n;
    return 0;
}

static int dbuf_putf(Dbuf *b, const char *fmt, ...) {
    char tmp[256];
    va_list ap;
    va_start(ap, fmt);
    int n = vsnprintf(tmp, sizeof(tmp), fmt, ap);
    va_end(ap);
    if (n < 0) return -1;
    if ((size_t)n < sizeof(tmp)) return dbuf_put(b, tmp, (size_t)n);
    char *big = malloc((size_t)n + 1);
    if (!big) return -1;
    va_start(ap, fmt);
    vsnprintf(big, (size_t)n + 1, fmt, ap);
    va_end(ap);
    int rc = dbuf_put(b, big, (size_t)n);
    free(big);
    return rc;
}

static int dbuf_simple(Dbuf *b, const char *msg) { return dbuf_putf(b, "+%s\r\n", msg); }
static int dbuf_error(Dbuf *b, const char *msg) { return dbuf_putf(b, "-ERR %s\r\n", msg); }
static int dbuf_int(Dbuf *b, long long v) { return dbuf_putf(b, ":%lld\r\n", v); }
static int dbuf_null(Dbuf *b) { return dbuf_put(b, "$-1\r\n", 5); }
static int dbuf_null_array(Dbuf *b) { return dbuf_put(b, "*-1\r\n", 5); }
static int dbuf_bulk(Dbuf *b, const char *str, size_t slen) {
    if (dbuf_putf(b, "$%zu\r\n", slen) < 0) return -1;
    if (dbuf_put(b, str, slen) < 0) return -1;
    return dbuf_put(b, "\r\n", 2);
}
static int dbuf_array_header(Dbuf *b, long long n) { return dbuf_putf(b, "*%lld\r\n", n); }

/* ---------- helpers ---------- */

static int parse_ll(const char *s, long long *out) {
    if (!s || !*s) return 0;
    char *end = NULL;
    errno = 0;
    long long v = strtoll(s, &end, 10);
    if (errno != 0 || end == s || *end != '\0') return 0;
    *out = v;
    return 1;
}

/* Strict unsigned parse: digits only, no sign, no overflow. */
static int parse_ull(const char *s, unsigned long long *out) {
    if (!s || !*s) return 0;
    unsigned long long v = 0;
    for (const char *p = s; *p; p++) {
        if (*p < '0' || *p > '9') return 0;
        if (v > (UINT64_MAX - (unsigned long long)(*p - '0')) / 10) return 0;
        v = v * 10 + (unsigned long long)(*p - '0');
    }
    *out = v;
    return 1;
}

/* Parse a BITFIELD encoding: i1..i64 or u1..u63. Redis rejects u64
   because RESP integers are signed 64-bit and a u64 field cannot be
   returned. */
static int parse_bitfield_type(const char *s, int *is_signed, int *bits) {
    if (!s || (s[0] != 'i' && s[0] != 'u' && s[0] != 'I' && s[0] != 'U'))
        return 0;
    long long b;
    if (!parse_ll(s + 1, &b)) return 0;
    if (b < 1 || b > 64) return 0;
    if ((s[0] == 'u' || s[0] == 'U') && b == 64) return 0;
    *is_signed = (s[0] == 'i' || s[0] == 'I');
    *bits = (int)b;
    return 1;
}

/* Parse a BITFIELD offset for a field of `bits` width: a plain number
   is a bit offset; #n is the n-th field of that width, so the bit
   offset is n*bits. The field must end inside a 512 MB string
   (offset + bits <= 2^32), as in Redis. */
static int parse_bitfield_offset(const char *s, int bits,
                                 unsigned long long *out) {
    unsigned long long max_off = 4294967296ULL - (unsigned long long)bits;
    if (s[0] == '#') {
        unsigned long long n;
        if (!parse_ull(s + 1, &n)) return 0;
        if (n > max_off / (unsigned long long)bits) return 0;
        *out = n * (unsigned long long)bits;
        return 1;
    }
    long long off;
    if (!parse_ll(s, &off)) return 0;
    if (off < 0 || (unsigned long long)off > max_off) return 0;
    *out = (unsigned long long)off;
    return 1;
}

/* Strict double parse: the whole string must be one number, and NaN
   is rejected (Redis refuses NaN scores too). Infinities are
   accepted, as in Redis ("inf" / "-inf"). */
static int parse_double(const char *s, double *out) {
    if (!s || !*s) return 0;
    char *end = NULL;
    errno = 0;
    double v = strtod(s, &end);
    if (errno != 0 || end == s || *end != '\0' || isnan(v)) return 0;
    *out = v;
    return 1;
}

/* Parse a score bound for ZRANGEBYSCORE: an optional leading '('
   makes the bound exclusive, the rest is a plain double ("-inf",
   "+inf", "inf" included). NaN is rejected, as in parse_double. */
static int parse_score_bound(const char *s, double *out, int *excl) {
    *excl = 0;
    if (!s || !*s) return 0;
    if (s[0] == '(') {
        *excl = 1;
        s++;
        if (!*s) return 0;
    }
    return parse_double(s, out);
}

/* Format a score the way Redis does: %.17g round-trips a double
   exactly, and prints integral scores without a decimal point
   ("2", not "2.0"). */
static void format_score(char *buf, size_t cap, double score) {
    snprintf(buf, cap, "%.17g", score);
}

/* ---------- command dispatch ----------
 * Returns: 0 = ok, 1 = mutated state (log to AOF), 2 = close connection,
 *          3 = shut the server down. */

/* Replication state lives in the client table (defined below); these
   helpers are declared here so INFO can report the role and the
   replica count. */
typedef struct Client Client;
static Client *g_clients;
static size_t g_nclients;
static size_t g_deferred_count; /* replies parked across all clients;
                                   defined with the deferred-reply
                                   queue below; INFO reads it here */
static int repl_is_replica(void);
static size_t repl_connected_replicas(void);
static void repl_info_replicas(Dbuf *info);

static int dispatch(char **argv, int argc, Dbuf *reply) {
    const char *cmd = argv[0];

    if (strcasecmp(cmd, "PING") == 0) {
        if (argc == 1) return dbuf_simple(reply, "PONG"), 0;
        if (argc == 2) return dbuf_bulk(reply, argv[1], strlen(argv[1])), 0;
        return dbuf_error(reply, "wrong number of arguments for 'ping' command"), 0;
    }
    if (strcasecmp(cmd, "ECHO") == 0) {
        if (argc != 2) return dbuf_error(reply, "wrong number of arguments for 'echo' command"), 0;
        return dbuf_bulk(reply, argv[1], strlen(argv[1])), 0;
    }
    if (strcasecmp(cmd, "QUIT") == 0) {
        dbuf_simple(reply, "OK");
        return 2;
    }
    if (strcasecmp(cmd, "SHUTDOWN") == 0) {
        dbuf_simple(reply, "OK");
        return 3;
    }
    if (strcasecmp(cmd, "SET") == 0) {
        if (argc < 3) return dbuf_error(reply, "wrong number of arguments for 'set' command"), 0;
        long long expire_at = -1;
        int nx = 0, xx = 0;
        for (int i = 3; i < argc; i++) {
            if (strcasecmp(argv[i], "EX") == 0 && i + 1 < argc) {
                long long s;
                if (!parse_ll(argv[++i], &s) || s <= 0)
                    return dbuf_error(reply, "invalid expire time in 'set' command"), 0;
                expire_at = now_ms() + s * 1000;
            } else if (strcasecmp(argv[i], "PX") == 0 && i + 1 < argc) {
                long long ms;
                if (!parse_ll(argv[++i], &ms) || ms <= 0)
                    return dbuf_error(reply, "invalid expire time in 'set' command"), 0;
                expire_at = now_ms() + ms;
            } else if (strcasecmp(argv[i], "NX") == 0) {
                nx = 1;
            } else if (strcasecmp(argv[i], "XX") == 0) {
                xx = 1;
            } else {
                return dbuf_error(reply, "syntax error"), 0;
            }
        }
        int exists = store_exists(g_store, argv[1]);
        if (nx && exists) return dbuf_null(reply), 0;
        if (xx && !exists) return dbuf_null(reply), 0;
        if (!store_set(g_store, argv[1], argv[2], expire_at))
            return dbuf_error(reply, "out of memory"), 0;
        if (g_max_entries) store_evict_lru(g_store, g_max_entries);
        dbuf_simple(reply, "OK");
        return 1;
    }
    if (strcasecmp(cmd, "MSET") == 0) {
        if (argc < 3 || (argc % 2) == 0)
            return dbuf_error(reply, "wrong number of arguments for 'mset' command"), 0;
        /* store_set overwrites any type, as in Redis: an MSET over a
           list/set/hash key replaces it with the string. A duplicate
           key in one call takes the last value. Single-threaded, so
           the whole call is atomic with respect to other clients. */
        for (int i = 1; i < argc; i += 2) {
            if (!store_set(g_store, argv[i], argv[i + 1], -1))
                return dbuf_error(reply, "out of memory"), 0;
        }
        if (g_max_entries) store_evict_lru(g_store, g_max_entries);
        dbuf_simple(reply, "OK");
        return 1;
    }
    if (strcasecmp(cmd, "GET") == 0) {
        if (argc != 2) return dbuf_error(reply, "wrong number of arguments for 'get' command"), 0;
        if (store_type(g_store, argv[1]) > 1)
            return dbuf_error(reply, "WRONGTYPE Operation against a key holding the wrong kind of value"), 0;
        size_t vlen;
        char *v = store_get_len(g_store, argv[1], &vlen);
        if (!v) return dbuf_null(reply), 0;
        dbuf_bulk(reply, v, vlen);
        free(v);
        return 0;
    }
    if (strcasecmp(cmd, "MGET") == 0) {
        if (argc < 2) return dbuf_error(reply, "wrong number of arguments for 'mget' command"), 0;
        /* Unlike GET, a key holding a non-string is nil here, not a
           WRONGTYPE error — that is real Redis behaviour. */
        dbuf_array_header(reply, (long long)(argc - 1));
        for (int i = 1; i < argc; i++) {
            if (store_type(g_store, argv[i]) > 1) {
                dbuf_null(reply);
                continue;
            }
            size_t vlen;
            char *v = store_get_len(g_store, argv[i], &vlen);
            if (!v) {
                dbuf_null(reply);
                continue;
            }
            dbuf_bulk(reply, v, vlen);
            free(v);
        }
        return 0;
    }
    if (strcasecmp(cmd, "GETDEL") == 0) {
        if (argc != 2) return dbuf_error(reply, "wrong number of arguments for 'getdel' command"), 0;
        if (store_type(g_store, argv[1]) > 1)
            return dbuf_error(reply, "WRONGTYPE Operation against a key holding the wrong kind of value"), 0;
        size_t vlen;
        char *v = store_get_len(g_store, argv[1], &vlen);
        if (!v) return dbuf_null(reply), 0;
        dbuf_bulk(reply, v, vlen);
        free(v);
        /* The value is already copied into the reply, so deleting
           after answering cannot lose it. Only a real delete is a
           mutation: a missing key logs and replicates nothing. */
        int deleted = store_del(g_store, argv[1]);
        return deleted ? 1 : 0;
    }
    if (strcasecmp(cmd, "GETEX") == 0) {
        if (argc < 2 || argc > 4)
            return dbuf_error(reply, "wrong number of arguments for 'getex' command"), 0;
        if (store_type(g_store, argv[1]) > 1)
            return dbuf_error(reply, "WRONGTYPE Operation against a key holding the wrong kind of value"), 0;
        /* expire_at == -2 means "leave the TTL alone" (no option
           given, or PERSIST on a key that has none). */
        long long expire_at = -2;
        int persist = 0;
        if (argc == 3) {
            if (strcasecmp(argv[2], "PERSIST") == 0) {
                persist = 1;
            } else {
                return dbuf_error(reply, "syntax error"), 0;
            }
        } else if (argc == 4) {
            long long t;
            if (!parse_ll(argv[3], &t) || t <= 0)
                return dbuf_error(reply, "invalid expire time in 'getex' command"), 0;
            if (strcasecmp(argv[2], "EX") == 0) {
                expire_at = now_ms() + t * 1000;
            } else if (strcasecmp(argv[2], "PX") == 0) {
                expire_at = now_ms() + t;
            } else {
                return dbuf_error(reply, "syntax error"), 0;
            }
        }
        char *v = NULL;
        size_t vlen = 0;
        v = store_get_len(g_store, argv[1], &vlen);
        if (!v) return dbuf_null(reply), 0;
        dbuf_bulk(reply, v, vlen);
        free(v);
        if (persist) {
            /* PERSIST is only a mutation when a TTL actually exists
               to remove; on a key with no expiry it changes nothing
               and must not be logged or replicated. */
            if (store_ttl_ms(g_store, argv[1]) == -1) return 0;
            store_expire(g_store, argv[1], -1);
            return 1;
        }
        if (expire_at != -2) {
            store_expire(g_store, argv[1], expire_at);
            return 1;
        }
        return 0;
    }
    if (strcasecmp(cmd, "SETNX") == 0) {
        if (argc != 3) return dbuf_error(reply, "wrong number of arguments for 'setnx' command"), 0;
        /* SETNX returns 0 when the key already exists, whatever it
           holds — that is real Redis behaviour, not a WRONGTYPE
           error — and only a real set is logged or replicated. */
        if (store_exists(g_store, argv[1])) return dbuf_int(reply, 0), 0;
        if (!store_set(g_store, argv[1], argv[2], -1))
            return dbuf_error(reply, "out of memory"), 0;
        if (g_max_entries) store_evict_lru(g_store, g_max_entries);
        dbuf_int(reply, 1);
        return 1;
    }
    if (strcasecmp(cmd, "SETEX") == 0) {
        if (argc != 4) return dbuf_error(reply, "wrong number of arguments for 'setex' command"), 0;
        long long s;
        if (!parse_ll(argv[2], &s) || s <= 0)
            return dbuf_error(reply, "invalid expire time in 'setex' command"), 0;
        /* SETEX overwrites any type, like SET. */
        if (!store_set(g_store, argv[1], argv[3], now_ms() + s * 1000))
            return dbuf_error(reply, "out of memory"), 0;
        if (g_max_entries) store_evict_lru(g_store, g_max_entries);
        dbuf_simple(reply, "OK");
        return 1;
    }
    if (strcasecmp(cmd, "GETSET") == 0) {
        if (argc != 3) return dbuf_error(reply, "wrong number of arguments for 'getset' command"), 0;
        if (store_type(g_store, argv[1]) > 1)
            return dbuf_error(reply, "WRONGTYPE Operation against a key holding the wrong kind of value"), 0;
        size_t oldlen;
        char *old = store_get_len(g_store, argv[1], &oldlen);
        /* GETSET clears the TTL, like a plain SET. */
        if (!store_set(g_store, argv[1], argv[2], -1)) {
            free(old);
            return dbuf_error(reply, "out of memory"), 0;
        }
        if (g_max_entries) store_evict_lru(g_store, g_max_entries);
        if (!old) return dbuf_null(reply), 1;
        dbuf_bulk(reply, old, oldlen);
        free(old);
        return 1;
    }
    if (strcasecmp(cmd, "APPEND") == 0) {
        if (argc != 3) return dbuf_error(reply, "wrong number of arguments for 'append' command"), 0;
        long long n = store_append(g_store, argv[1], argv[2]);
        if (n == -1)
            return dbuf_error(reply, "WRONGTYPE Operation against a key holding the wrong kind of value"), 0;
        if (n == -2)
            return dbuf_error(reply, "out of memory"), 0;
        if (g_max_entries) store_evict_lru(g_store, g_max_entries);
        dbuf_int(reply, n);
        return 1;
    }
    if (strcasecmp(cmd, "STRLEN") == 0) {
        if (argc != 2) return dbuf_error(reply, "wrong number of arguments for 'strlen' command"), 0;
        if (store_type(g_store, argv[1]) > 1)
            return dbuf_error(reply, "WRONGTYPE Operation against a key holding the wrong kind of value"), 0;
        size_t vlen;
        char *v = store_get_len(g_store, argv[1], &vlen);
        if (!v) return dbuf_int(reply, 0), 0;
        dbuf_int(reply, (long long)vlen);
        free(v);
        return 0;
    }
    if (strcasecmp(cmd, "GETRANGE") == 0) {
        if (argc != 4) return dbuf_error(reply, "wrong number of arguments for 'getrange' command"), 0;
        long long start, stop;
        if (!parse_ll(argv[2], &start) || !parse_ll(argv[3], &stop))
            return dbuf_error(reply, "value is not an integer or out of range"), 0;
        if (store_type(g_store, argv[1]) > 1)
            return dbuf_error(reply, "WRONGTYPE Operation against a key holding the wrong kind of value"), 0;
        char *out = NULL;
        size_t n = 0;
        int rc = store_getrange(g_store, argv[1], start, stop, &out, &n);
        if (rc == -2) return dbuf_error(reply, "out of memory"), 0;
        /* The slice is length-delimited, so zero bytes from a
           SETRANGE pad come back intact, as in Redis. */
        dbuf_bulk(reply, out ? out : "", n);
        free(out);
        return 0;
    }
    if (strcasecmp(cmd, "SETRANGE") == 0) {
        if (argc != 4) return dbuf_error(reply, "wrong number of arguments for 'setrange' command"), 0;
        long long offset;
        if (!parse_ll(argv[2], &offset))
            return dbuf_error(reply, "value is not an integer or out of range"), 0;
        if (offset < 0)
            return dbuf_error(reply, "offset is out of range"), 0;
        size_t vlen = strlen(argv[3]);
        long long n = store_setrange(g_store, argv[1], offset, argv[3], vlen);
        if (n == -1)
            return dbuf_error(reply, "WRONGTYPE Operation against a key holding the wrong kind of value"), 0;
        if (n == -2)
            return dbuf_error(reply, "out of memory"), 0;
        dbuf_int(reply, n);
        /* An empty value changes nothing and logs nothing, as in
           Redis; any real write is logged and replicated. */
        if (vlen == 0) return 0;
        if (g_max_entries) store_evict_lru(g_store, g_max_entries);
        return 1;
    }
    if (strcasecmp(cmd, "SETBIT") == 0) {
        if (argc != 4) return dbuf_error(reply, "wrong number of arguments for 'setbit' command"), 0;
        long long offset;
        if (!parse_ll(argv[2], &offset) || offset < 0 || offset > 4294967295LL)
            return dbuf_error(reply, "bit offset is not an integer or out of range"), 0;
        long long bit;
        if (!parse_ll(argv[3], &bit) || (bit != 0 && bit != 1))
            return dbuf_error(reply, "bit is not an integer or out of range"), 0;
        int old = 0, changed = 0;
        int rc = store_setbit(g_store, argv[1], offset, (int)bit, &old, &changed);
        if (rc == -1)
            return dbuf_error(reply, "WRONGTYPE Operation against a key holding the wrong kind of value"), 0;
        if (rc == -2)
            return dbuf_error(reply, "out of memory"), 0;
        dbuf_int(reply, old);
        /* Only a real change is logged and replicated: setting a bit
           that already holds the requested value changes nothing. */
        if (changed) {
            if (g_max_entries) store_evict_lru(g_store, g_max_entries);
            return 1;
        }
        return 0;
    }
    if (strcasecmp(cmd, "GETBIT") == 0) {
        if (argc != 3) return dbuf_error(reply, "wrong number of arguments for 'getbit' command"), 0;
        long long offset;
        if (!parse_ll(argv[2], &offset) || offset < 0 || offset > 4294967295LL)
            return dbuf_error(reply, "bit offset is not an integer or out of range"), 0;
        int bit = 0;
        int rc = store_getbit(g_store, argv[1], offset, &bit);
        if (rc == -1)
            return dbuf_error(reply, "WRONGTYPE Operation against a key holding the wrong kind of value"), 0;
        dbuf_int(reply, bit);
        return 0;
    }
    if (strcasecmp(cmd, "BITCOUNT") == 0) {
        if (argc != 2 && argc != 4)
            return dbuf_error(reply, "wrong number of arguments for 'bitcount' command"), 0;
        long long start = 0, end = 0;
        if (argc == 4) {
            if (!parse_ll(argv[2], &start) || !parse_ll(argv[3], &end))
                return dbuf_error(reply, "value is not an integer or out of range"), 0;
        }
        long long n = 0;
        int rc = store_bitcount(g_store, argv[1], start, end, argc == 4, &n);
        if (rc == -1)
            return dbuf_error(reply, "WRONGTYPE Operation against a key holding the wrong kind of value"), 0;
        dbuf_int(reply, n);
        return 0;
    }
    if (strcasecmp(cmd, "BITOP") == 0) {
        if (argc < 4) return dbuf_error(reply, "wrong number of arguments for 'bitop' command"), 0;
        int op;
        if (strcasecmp(argv[1], "AND") == 0) op = BITOP_AND;
        else if (strcasecmp(argv[1], "OR") == 0) op = BITOP_OR;
        else if (strcasecmp(argv[1], "XOR") == 0) op = BITOP_XOR;
        else if (strcasecmp(argv[1], "NOT") == 0) op = BITOP_NOT;
        else return dbuf_error(reply, "syntax error"), 0;
        if (op == BITOP_NOT) {
            if (argc != 4)
                return dbuf_error(reply, "BITOP NOT must be called with a single source key."), 0;
        } else if (argc < 5) {
            return dbuf_error(reply, "wrong number of arguments for 'bitop' command"), 0;
        }
        long long n = 0;
        int changed = 0;
        int rc = store_bitop(g_store, op, argv[2], (const char **)(argv + 3),
                             argc - 3, &n, &changed);
        if (rc == -1)
            return dbuf_error(reply, "WRONGTYPE Operation against a key holding the wrong kind of value"), 0;
        if (rc == -2)
            return dbuf_error(reply, "out of memory"), 0;
        dbuf_int(reply, n);
        /* Only a real change is logged and replicated: a BITOP
           whose result matches the destination, or one over
           missing keys with no destination to delete, changes
           nothing and does not invalidate a WATCH. */
        if (changed) {
            if (g_max_entries) store_evict_lru(g_store, g_max_entries);
            return 1;
        }
        return 0;
    }
    if (strcasecmp(cmd, "BITFIELD") == 0) {
        if (argc < 2) return dbuf_error(reply, "wrong number of arguments for 'bitfield' command"), 0;
        /* Parse every subcommand first: a syntax error anywhere means
           nothing executes, as in Redis. OVERFLOW sets the policy for
           the SET / INCRBY operations that follow it in this call. */
        BitfieldOp *ops = calloc((size_t)(argc > 2 ? argc - 2 : 1), sizeof(BitfieldOp));
        BitfieldResult *results = calloc((size_t)(argc > 2 ? argc - 2 : 1), sizeof(BitfieldResult));
        if (!ops || !results) {
            free(ops);
            free(results);
            return dbuf_error(reply, "out of memory"), 0;
        }
        int nops = 0;
        int overflow = BITFIELD_OVERFLOW_WRAP;
        int bad = 0;
        const char *err = NULL;
        for (int i = 2; i < argc && !bad;) {
            if (strcasecmp(argv[i], "GET") == 0) {
                if (i + 2 >= argc) { err = "syntax error"; bad = 1; break; }
                int is_signed, bits;
                unsigned long long off;
                if (!parse_bitfield_type(argv[i + 1], &is_signed, &bits)) {
                    err = "Invalid bitfield type. Use something like i16 u8. Note that u64 is the only unsigned 64 bit type.";
                    bad = 1; break;
                }
                if (!parse_bitfield_offset(argv[i + 2], bits, &off)) {
                    err = "bit offset is not an integer or out of range";
                    bad = 1; break;
                }
                ops[nops].kind = BITFIELD_GET;
                ops[nops].is_signed = is_signed;
                ops[nops].bits = bits;
                ops[nops].offset = off;
                nops++;
                i += 3;
            } else if (strcasecmp(argv[i], "SET") == 0 ||
                       strcasecmp(argv[i], "INCRBY") == 0) {
                int is_set = strcasecmp(argv[i], "SET") == 0;
                if (i + 3 >= argc) { err = "syntax error"; bad = 1; break; }
                int is_signed, bits;
                unsigned long long off;
                long long v;
                if (!parse_bitfield_type(argv[i + 1], &is_signed, &bits)) {
                    err = "Invalid bitfield type. Use something like i16 u8. Note that u64 is the only unsigned 64 bit type.";
                    bad = 1; break;
                }
                if (!parse_bitfield_offset(argv[i + 2], bits, &off)) {
                    err = "bit offset is not an integer or out of range";
                    bad = 1; break;
                }
                if (!parse_ll(argv[i + 3], &v)) {
                    err = "value is not an integer or out of range";
                    bad = 1; break;
                }
                ops[nops].kind = is_set ? BITFIELD_SET : BITFIELD_INCRBY;
                ops[nops].is_signed = is_signed;
                ops[nops].bits = bits;
                ops[nops].offset = off;
                ops[nops].value = v;
                ops[nops].overflow = overflow;
                nops++;
                i += 4;
            } else if (strcasecmp(argv[i], "OVERFLOW") == 0) {
                if (i + 1 >= argc) { err = "syntax error"; bad = 1; break; }
                if (strcasecmp(argv[i + 1], "WRAP") == 0) overflow = BITFIELD_OVERFLOW_WRAP;
                else if (strcasecmp(argv[i + 1], "SAT") == 0) overflow = BITFIELD_OVERFLOW_SAT;
                else if (strcasecmp(argv[i + 1], "FAIL") == 0) overflow = BITFIELD_OVERFLOW_FAIL;
                else { err = "Invalid OVERFLOW type"; bad = 1; break; }
                i += 2;
            } else {
                err = "syntax error";
                bad = 1;
                break;
            }
        }
        if (bad) {
            free(ops);
            free(results);
            return dbuf_error(reply, err), 0;
        }
        int changed = 0;
        int rc = store_bitfield(g_store, argv[1], ops, nops, results, &changed);
        if (rc == -1) {
            free(ops);
            free(results);
            return dbuf_error(reply, "WRONGTYPE Operation against a key holding the wrong kind of value"), 0;
        }
        if (rc == -2) {
            free(ops);
            free(results);
            return dbuf_error(reply, "out of memory"), 0;
        }
        if (rc == -3) {
            free(ops);
            free(results);
            return dbuf_error(reply, "bit offset is not an integer or out of range"), 0;
        }
        /* One reply element per GET / SET / INCRBY, in order; an
           INCRBY or SET that overflowed under OVERFLOW FAIL answers
           nil. Unsigned fields are at most 63 bits, so every value
           fits a signed 64-bit reply, as in Redis. */
        dbuf_array_header(reply, (long long)nops);
        for (int i = 0; i < nops; i++) {
            if (results[i].is_nil) {
                dbuf_null(reply);
                continue;
            }
            long long v = ops[i].is_signed
                ? store_bitfield_to_signed(results[i].raw, ops[i].bits)
                : (long long)results[i].raw;
            dbuf_int(reply, v);
        }
        free(ops);
        free(results);
        /* Only a real change is logged and replicated: a GET-only
           call, or writes that leave every byte identical, change
           nothing and do not invalidate a WATCH. */
        if (changed) {
            if (g_max_entries) store_evict_lru(g_store, g_max_entries);
            return 1;
        }
        return 0;
    }
    if (strcasecmp(cmd, "DEL") == 0) {
        if (argc < 2) return dbuf_error(reply, "wrong number of arguments for 'del' command"), 0;
        long long n = 0;
        for (int i = 1; i < argc; i++) n += store_del(g_store, argv[i]);
        dbuf_int(reply, n);
        return n > 0 ? 1 : 0;
    }
    if (strcasecmp(cmd, "EXISTS") == 0) {
        if (argc < 2) return dbuf_error(reply, "wrong number of arguments for 'exists' command"), 0;
        long long n = 0;
        for (int i = 1; i < argc; i++) n += store_exists(g_store, argv[i]);
        dbuf_int(reply, n);
        return 0;
    }
    if (strcasecmp(cmd, "EXPIRE") == 0 || strcasecmp(cmd, "PEXPIRE") == 0) {
        if (argc != 3) return dbuf_error(reply, "wrong number of arguments for 'expire' command"), 0;
        long long t;
        if (!parse_ll(argv[2], &t) || t < 0)
            return dbuf_error(reply, "invalid expire time in 'expire' command"), 0;
        long long at = strcasecmp(cmd, "EXPIRE") == 0 ? now_ms() + t * 1000 : now_ms() + t;
        int ok = store_expire(g_store, argv[1], at);
        dbuf_int(reply, ok);
        return ok ? 1 : 0;
    }
    if (strcasecmp(cmd, "TTL") == 0 || strcasecmp(cmd, "PTTL") == 0) {
        if (argc != 2) return dbuf_error(reply, "wrong number of arguments for 'ttl' command"), 0;
        long long ms = store_ttl_ms(g_store, argv[1]);
        if (ms == -2) return dbuf_int(reply, -2), 0;
        if (ms == -1) return dbuf_int(reply, -1), 0;
        dbuf_int(reply, strcasecmp(cmd, "TTL") == 0 ? ms / 1000 : ms);
        return 0;
    }
    if (strcasecmp(cmd, "INCR") == 0 || strcasecmp(cmd, "DECR") == 0 ||
        strcasecmp(cmd, "INCRBY") == 0 || strcasecmp(cmd, "DECRBY") == 0) {
        long long delta = strcasecmp(cmd, "INCR") == 0 ? 1 : strcasecmp(cmd, "DECR") == 0 ? -1 : 0;
        if (delta == 0) {
            if (argc != 3) return dbuf_error(reply, "wrong number of arguments"), 0;
            if (!parse_ll(argv[2], &delta))
                return dbuf_error(reply, "value is not an integer or out of range"), 0;
            if (strcasecmp(cmd, "DECRBY") == 0) delta = -delta;
        } else if (argc != 2) {
            return dbuf_error(reply, "wrong number of arguments"), 0;
        }
        long long v;
        int rc = store_incr(g_store, argv[1], delta, &v);
        if (rc == -1)
            return dbuf_error(reply, "WRONGTYPE Operation against a key holding the wrong kind of value"), 0;
        if (rc == -2)
            return dbuf_error(reply, "value is not an integer or out of range"), 0;
        if (rc == -3)
            return dbuf_error(reply, "out of memory"), 0;
        if (g_max_entries) store_evict_lru(g_store, g_max_entries);
        dbuf_int(reply, v);
        return 1;
    }
    if (strcasecmp(cmd, "KEYS") == 0) {
        if (argc != 2) return dbuf_error(reply, "wrong number of arguments for 'keys' command"), 0;
        char **keys = NULL;
        size_t n = store_keys_matching(g_store, argv[1], &keys);
        dbuf_array_header(reply, (long long)n);
        for (size_t i = 0; i < n; i++) {
            dbuf_bulk(reply, keys[i], strlen(keys[i]));
            free(keys[i]);
        }
        free(keys);
        return 0;
    }
    if (strcasecmp(cmd, "SCAN") == 0) {
        if (argc < 2)
            return dbuf_error(reply, "wrong number of arguments for 'scan' command"), 0;
        /* The cursor is an unsigned decimal integer; anything else
           is the same "invalid cursor" error Redis returns. A
           cursor past the end of the table is still valid — the
           store masks it — so no range check is needed. */
        unsigned long long cursor = 0;
        if (!*argv[1])
            return dbuf_error(reply, "invalid cursor"), 0;
        for (const char *p = argv[1]; *p; p++) {
            if (*p < '0' || *p > '9')
                return dbuf_error(reply, "invalid cursor"), 0;
            cursor = cursor * 10 + (unsigned long long)(*p - '0');
        }
        const char *pattern = "*";
        long long count = 10; /* Redis's default COUNT */
        int seen_match = 0, seen_count = 0;
        for (int i = 2; i < argc; i++) {
            if (strcasecmp(argv[i], "MATCH") == 0) {
                if (seen_match || i + 1 >= argc)
                    return dbuf_error(reply, "syntax error"), 0;
                seen_match = 1;
                pattern = argv[++i];
            } else if (strcasecmp(argv[i], "COUNT") == 0) {
                if (seen_count || i + 1 >= argc)
                    return dbuf_error(reply, "syntax error"), 0;
                seen_count = 1;
                long long n;
                if (!parse_ll(argv[++i], &n))
                    return dbuf_error(reply, "value is not an integer or out of range"), 0;
                if (n < 0)
                    return dbuf_error(reply, "syntax error"), 0;
                count = n;
            } else {
                return dbuf_error(reply, "syntax error"), 0;
            }
        }
        char **keys = NULL;
        size_t n = 0;
        unsigned long long next = store_scan(g_store, cursor, pattern,
                                             (size_t)count, &keys, &n);
        dbuf_array_header(reply, 2);
        char curbuf[32];
        snprintf(curbuf, sizeof(curbuf), "%llu", next);
        dbuf_bulk(reply, curbuf, strlen(curbuf));
        dbuf_array_header(reply, (long long)n);
        for (size_t i = 0; i < n; i++) {
            dbuf_bulk(reply, keys[i], strlen(keys[i]));
            free(keys[i]);
        }
        free(keys);
        return 0;
    }
    if (strcasecmp(cmd, "LPUSH") == 0 || strcasecmp(cmd, "RPUSH") == 0) {
        if (argc < 3) return dbuf_error(reply, "wrong number of arguments for 'lpush' command"), 0;
        int at_head = strcasecmp(cmd, "LPUSH") == 0;
        long long len = 0;
        for (int i = 2; i < argc; i++) {
            len = at_head ? store_lpush(g_store, argv[1], argv[i])
                          : store_rpush(g_store, argv[1], argv[i]);
            if (len == -1)
                return dbuf_error(reply, "WRONGTYPE Operation against a key holding the wrong kind of value"), 0;
            if (len == -2)
                return dbuf_error(reply, "out of memory"), 0;
        }
        if (g_max_entries) store_evict_lru(g_store, g_max_entries);
        dbuf_int(reply, len);
        return 1;
    }
    if (strcasecmp(cmd, "LPOP") == 0 || strcasecmp(cmd, "RPOP") == 0) {
        if (argc != 2) return dbuf_error(reply, "wrong number of arguments for 'lpop' command"), 0;
        int wrongtype = 0;
        char *el = strcasecmp(cmd, "LPOP") == 0
                       ? store_lpop(g_store, argv[1], &wrongtype)
                       : store_rpop(g_store, argv[1], &wrongtype);
        if (wrongtype)
            return dbuf_error(reply, "WRONGTYPE Operation against a key holding the wrong kind of value"), 0;
        if (!el) return dbuf_null(reply), 0;
        dbuf_bulk(reply, el, strlen(el));
        free(el);
        return 1;
    }
    if (strcasecmp(cmd, "LLEN") == 0) {
        if (argc != 2) return dbuf_error(reply, "wrong number of arguments for 'llen' command"), 0;
        long long n = store_llen(g_store, argv[1]);
        if (n == -1)
            return dbuf_error(reply, "WRONGTYPE Operation against a key holding the wrong kind of value"), 0;
        dbuf_int(reply, n);
        return 0;
    }
    if (strcasecmp(cmd, "LRANGE") == 0) {
        if (argc != 4) return dbuf_error(reply, "wrong number of arguments for 'lrange' command"), 0;
        long long start, stop;
        if (!parse_ll(argv[2], &start) || !parse_ll(argv[3], &stop))
            return dbuf_error(reply, "value is not an integer or out of range"), 0;
        char **elems = NULL;
        size_t n = 0;
        int rc = store_lrange(g_store, argv[1], start, stop, &elems, &n);
        if (rc == -1)
            return dbuf_error(reply, "WRONGTYPE Operation against a key holding the wrong kind of value"), 0;
        if (rc == -2)
            return dbuf_error(reply, "out of memory"), 0;
        dbuf_array_header(reply, (long long)n);
        for (size_t i = 0; i < n; i++) {
            dbuf_bulk(reply, elems[i], strlen(elems[i]));
            free(elems[i]);
        }
        free(elems);
        return 0;
    }
    if (strcasecmp(cmd, "LINDEX") == 0) {
        if (argc != 3) return dbuf_error(reply, "wrong number of arguments for 'lindex' command"), 0;
        long long index;
        if (!parse_ll(argv[2], &index))
            return dbuf_error(reply, "value is not an integer or out of range"), 0;
        char *el = NULL;
        int rc = store_lindex(g_store, argv[1], index, &el);
        if (rc == -1)
            return dbuf_error(reply, "WRONGTYPE Operation against a key holding the wrong kind of value"), 0;
        if (rc == -2)
            return dbuf_error(reply, "out of memory"), 0;
        if (!el) return dbuf_null(reply), 0;
        dbuf_bulk(reply, el, strlen(el));
        free(el);
        return 0;
    }
    if (strcasecmp(cmd, "LSET") == 0) {
        if (argc != 4) return dbuf_error(reply, "wrong number of arguments for 'lset' command"), 0;
        long long index;
        if (!parse_ll(argv[2], &index))
            return dbuf_error(reply, "value is not an integer or out of range"), 0;
        int rc = store_lset(g_store, argv[1], index, argv[3]);
        if (rc == -1)
            return dbuf_error(reply, "WRONGTYPE Operation against a key holding the wrong kind of value"), 0;
        if (rc == -2)
            return dbuf_error(reply, "out of memory"), 0;
        if (rc == -3)
            return dbuf_error(reply, "no such key"), 0;
        if (rc == -4)
            return dbuf_error(reply, "index out of range"), 0;
        dbuf_simple(reply, "OK");
        /* A no-op set (the element already held that value) changes
           nothing, so it is not logged or replicated. */
        return rc == 0 ? 1 : 0;
    }
    if (strcasecmp(cmd, "LINSERT") == 0) {
        if (argc != 5) return dbuf_error(reply, "wrong number of arguments for 'linsert' command"), 0;
        int before;
        if (strcasecmp(argv[2], "BEFORE") == 0) before = 1;
        else if (strcasecmp(argv[2], "AFTER") == 0) before = 0;
        else return dbuf_error(reply, "syntax error"), 0;
        long long n = store_linsert(g_store, argv[1], before, argv[3], argv[4]);
        if (n == -2)
            return dbuf_error(reply, "WRONGTYPE Operation against a key holding the wrong kind of value"), 0;
        if (n == -3)
            return dbuf_error(reply, "out of memory"), 0;
        dbuf_int(reply, n);
        /* -1 (no such key) and 0 (pivot not found) change nothing,
           so they are not logged or replicated. */
        return n > 0 ? 1 : 0;
    }
    if (strcasecmp(cmd, "LPUSHX") == 0 || strcasecmp(cmd, "RPUSHX") == 0) {
        if (argc < 3) return dbuf_error(reply, "wrong number of arguments for 'lpushx' command"), 0;
        int at_head = strcasecmp(cmd, "LPUSHX") == 0;
        long long len = 0;
        for (int i = 2; i < argc; i++) {
            len = at_head ? store_lpushx(g_store, argv[1], argv[i])
                          : store_rpushx(g_store, argv[1], argv[i]);
            if (len == -1)
                return dbuf_error(reply, "WRONGTYPE Operation against a key holding the wrong kind of value"), 0;
            if (len == -2)
                return dbuf_error(reply, "out of memory"), 0;
        }
        if (g_max_entries) store_evict_lru(g_store, g_max_entries);
        dbuf_int(reply, len);
        /* 0 (no such key) changes nothing, so it is not logged or
           replicated. */
        return len > 0 ? 1 : 0;
    }
    if (strcasecmp(cmd, "LTRIM") == 0) {
        if (argc != 4) return dbuf_error(reply, "wrong number of arguments for 'ltrim' command"), 0;
        long long start, stop;
        if (!parse_ll(argv[2], &start) || !parse_ll(argv[3], &stop))
            return dbuf_error(reply, "value is not an integer or out of range"), 0;
        int rc = store_ltrim(g_store, argv[1], start, stop);
        if (rc == -1)
            return dbuf_error(reply, "WRONGTYPE Operation against a key holding the wrong kind of value"), 0;
        if (rc == -2)
            return dbuf_error(reply, "out of memory"), 0;
        dbuf_simple(reply, "OK");
        /* A missing key changes nothing, so it is not logged or
           replicated. */
        return rc == 0 ? 1 : 0;
    }
    if (strcasecmp(cmd, "LMOVE") == 0) {
        if (argc != 5) return dbuf_error(reply, "wrong number of arguments for 'lmove' command"), 0;
        int from_head, to_head;
        if (strcasecmp(argv[3], "LEFT") == 0) from_head = 1;
        else if (strcasecmp(argv[3], "RIGHT") == 0) from_head = 0;
        else return dbuf_error(reply, "syntax error"), 0;
        if (strcasecmp(argv[4], "LEFT") == 0) to_head = 1;
        else if (strcasecmp(argv[4], "RIGHT") == 0) to_head = 0;
        else return dbuf_error(reply, "syntax error"), 0;
        char *el = NULL;
        int rc = store_lmove(g_store, argv[1], argv[2], from_head, to_head, &el);
        if (rc == -1)
            return dbuf_error(reply, "WRONGTYPE Operation against a key holding the wrong kind of value"), 0;
        if (rc == -2)
            return dbuf_error(reply, "out of memory"), 0;
        if (!el) return dbuf_null(reply), 0;
        dbuf_bulk(reply, el, strlen(el));
        free(el);
        if (g_max_entries) store_evict_lru(g_store, g_max_entries);
        return 1;
    }
    if (strcasecmp(cmd, "SADD") == 0) {
        if (argc < 3) return dbuf_error(reply, "wrong number of arguments for 'sadd' command"), 0;
        long long added = 0;
        for (int i = 2; i < argc; i++) {
            long long rc = store_sadd(g_store, argv[1], argv[i]);
            if (rc == -1)
                return dbuf_error(reply, "WRONGTYPE Operation against a key holding the wrong kind of value"), 0;
            if (rc == -2)
                return dbuf_error(reply, "out of memory"), 0;
            added += rc;
        }
        if (g_max_entries) store_evict_lru(g_store, g_max_entries);
        dbuf_int(reply, added);
        return added > 0 ? 1 : 0;
    }
    if (strcasecmp(cmd, "SREM") == 0) {
        if (argc < 3) return dbuf_error(reply, "wrong number of arguments for 'srem' command"), 0;
        long long removed = 0;
        for (int i = 2; i < argc; i++) {
            long long rc = store_srem(g_store, argv[1], argv[i]);
            if (rc == -1)
                return dbuf_error(reply, "WRONGTYPE Operation against a key holding the wrong kind of value"), 0;
            removed += rc;
        }
        dbuf_int(reply, removed);
        return removed > 0 ? 1 : 0;
    }
    if (strcasecmp(cmd, "SMEMBERS") == 0) {
        if (argc != 2) return dbuf_error(reply, "wrong number of arguments for 'smembers' command"), 0;
        char **members = NULL;
        size_t n = 0;
        int rc = store_smembers(g_store, argv[1], &members, &n);
        if (rc == -1)
            return dbuf_error(reply, "WRONGTYPE Operation against a key holding the wrong kind of value"), 0;
        if (rc == -2)
            return dbuf_error(reply, "out of memory"), 0;
        dbuf_array_header(reply, (long long)n);
        for (size_t i = 0; i < n; i++) {
            dbuf_bulk(reply, members[i], strlen(members[i]));
            free(members[i]);
        }
        free(members);
        return 0;
    }
    if (strcasecmp(cmd, "SISMEMBER") == 0) {
        if (argc != 3) return dbuf_error(reply, "wrong number of arguments for 'sismember' command"), 0;
        int rc = store_sismember(g_store, argv[1], argv[2]);
        if (rc == -1)
            return dbuf_error(reply, "WRONGTYPE Operation against a key holding the wrong kind of value"), 0;
        dbuf_int(reply, rc);
        return 0;
    }
    if (strcasecmp(cmd, "SCARD") == 0) {
        if (argc != 2) return dbuf_error(reply, "wrong number of arguments for 'scard' command"), 0;
        long long n = store_scard(g_store, argv[1]);
        if (n == -1)
            return dbuf_error(reply, "WRONGTYPE Operation against a key holding the wrong kind of value"), 0;
        dbuf_int(reply, n);
        return 0;
    }
    if (strcasecmp(cmd, "HSET") == 0) {
        if (argc < 4 || (argc % 2) != 0)
            return dbuf_error(reply, "wrong number of arguments for 'hset' command"), 0;
        long long added = 0;
        for (int i = 2; i < argc; i += 2) {
            long long rc = store_hset(g_store, argv[1], argv[i], argv[i + 1]);
            if (rc == -1)
                return dbuf_error(reply, "WRONGTYPE Operation against a key holding the wrong kind of value"), 0;
            if (rc == -2)
                return dbuf_error(reply, "out of memory"), 0;
            added += rc;
        }
        if (g_max_entries) store_evict_lru(g_store, g_max_entries);
        dbuf_int(reply, added);
        /* Log even when every field already existed: an update still
           changes state and must reach the AOF. */
        return 1;
    }
    if (strcasecmp(cmd, "HGET") == 0) {
        if (argc != 3) return dbuf_error(reply, "wrong number of arguments for 'hget' command"), 0;
        int t = store_type(g_store, argv[1]);
        if (t != 0 && t != 4)
            return dbuf_error(reply, "WRONGTYPE Operation against a key holding the wrong kind of value"), 0;
        char *v = store_hget(g_store, argv[1], argv[2]);
        if (!v) return dbuf_null(reply), 0;
        dbuf_bulk(reply, v, strlen(v));
        free(v);
        return 0;
    }
    if (strcasecmp(cmd, "HDEL") == 0) {
        if (argc < 3) return dbuf_error(reply, "wrong number of arguments for 'hdel' command"), 0;
        long long removed = 0;
        for (int i = 2; i < argc; i++) {
            long long rc = store_hdel(g_store, argv[1], argv[i]);
            if (rc == -1)
                return dbuf_error(reply, "WRONGTYPE Operation against a key holding the wrong kind of value"), 0;
            removed += rc;
        }
        dbuf_int(reply, removed);
        return removed > 0 ? 1 : 0;
    }
    if (strcasecmp(cmd, "HEXISTS") == 0) {
        if (argc != 3) return dbuf_error(reply, "wrong number of arguments for 'hexists' command"), 0;
        int rc = store_hexists(g_store, argv[1], argv[2]);
        if (rc == -1)
            return dbuf_error(reply, "WRONGTYPE Operation against a key holding the wrong kind of value"), 0;
        dbuf_int(reply, rc);
        return 0;
    }
    if (strcasecmp(cmd, "HLEN") == 0) {
        if (argc != 2) return dbuf_error(reply, "wrong number of arguments for 'hlen' command"), 0;
        long long n = store_hlen(g_store, argv[1]);
        if (n == -1)
            return dbuf_error(reply, "WRONGTYPE Operation against a key holding the wrong kind of value"), 0;
        dbuf_int(reply, n);
        return 0;
    }
    if (strcasecmp(cmd, "HGETALL") == 0) {
        if (argc != 2) return dbuf_error(reply, "wrong number of arguments for 'hgetall' command"), 0;
        char **pairs = NULL;
        size_t n = 0;
        int rc = store_hgetall(g_store, argv[1], &pairs, &n);
        if (rc == -1)
            return dbuf_error(reply, "WRONGTYPE Operation against a key holding the wrong kind of value"), 0;
        if (rc == -2)
            return dbuf_error(reply, "out of memory"), 0;
        dbuf_array_header(reply, (long long)n);
        for (size_t i = 0; i < n; i++) {
            dbuf_bulk(reply, pairs[i], strlen(pairs[i]));
            free(pairs[i]);
        }
        free(pairs);
        return 0;
    }
    if (strcasecmp(cmd, "ZADD") == 0) {
        if (argc < 4)
            return dbuf_error(reply, "wrong number of arguments for 'zadd' command"), 0;
        /* Options come after the key and before the first score:
           NX | XX | GT | LT | CH | INCR. Anything after the first
           score is a score/member pair, never an option. */
        int nx = 0, xx = 0, gt = 0, lt = 0, ch = 0, incr = 0;
        int i = 2;
        for (; i < argc; i++) {
            if (strcasecmp(argv[i], "NX") == 0) nx = 1;
            else if (strcasecmp(argv[i], "XX") == 0) xx = 1;
            else if (strcasecmp(argv[i], "GT") == 0) gt = 1;
            else if (strcasecmp(argv[i], "LT") == 0) lt = 1;
            else if (strcasecmp(argv[i], "CH") == 0) ch = 1;
            else if (strcasecmp(argv[i], "INCR") == 0) incr = 1;
            else break;
        }
        /* Conflicting options are errors, as in Redis: NX clashes
           with XX and with GT/LT, and GT clashes with LT. XX can
           combine with GT or LT. */
        if (nx && xx)
            return dbuf_error(reply, "XX and NX options at the same time are not compatible"), 0;
        if ((gt && lt) || (gt && nx) || (lt && nx))
            return dbuf_error(reply, "GT, LT, and/or NX options at the same time are not compatible"), 0;
        int nrest = argc - i;
        if (nrest < 2 || (nrest % 2) != 0)
            return dbuf_error(reply, "wrong number of arguments for 'zadd' command"), 0;
        size_t npairs = (size_t)nrest / 2;
        if (incr && npairs != 1)
            return dbuf_error(reply, "INCR option supports a single increment-element pair"), 0;
        /* Parse every score before applying any pair, so a bad score
           errors without a partial apply. */
        double *scores = malloc(npairs * sizeof(double));
        if (!scores)
            return dbuf_error(reply, "out of memory"), 0;
        for (size_t k = 0; k < npairs; k++) {
            if (!parse_double(argv[i + 2 * k], &scores[k])) {
                free(scores);
                return dbuf_error(reply, "value is not a valid float"), 0;
            }
        }
        /* Wrong type is an error before any pair is considered: even
           a pair the options would skip still errors on a key
           holding another type, as in Redis. */
        int zt = store_type(g_store, argv[1]);
        if (zt != 0 && zt != 5) {
            free(scores);
            return dbuf_error(reply, "WRONGTYPE Operation against a key holding the wrong kind of value"), 0;
        }
        if (incr) {
            /* INCR acts like ZINCRBY for one member: the score is
               an increment, and GT/LT compare the resulting score
               against the current one. A condition that rejects the
               update answers nil, as in Redis; CH does not affect
               the reply. */
            const char *member = argv[i + 1];
            double cur = 0;
            int exists = store_zscore(g_store, argv[1], member, &cur) == 1;
            if ((nx && exists) || (xx && !exists)) {
                free(scores);
                return dbuf_null(reply), 0;
            }
            double next = cur + scores[0];
            if (isnan(next)) {
                free(scores);
                return dbuf_error(reply, "resulting score is not a number (NaN)"), 0;
            }
            if (exists && ((gt && next <= cur) || (lt && next >= cur))) {
                free(scores);
                return dbuf_null(reply), 0;
            }
            double out;
            int rc = store_zincrby(g_store, argv[1], member, scores[0], &out);
            free(scores);
            if (rc == -2)
                return dbuf_error(reply, "out of memory"), 0;
            char buf[32];
            format_score(buf, sizeof(buf), out);
            dbuf_bulk(reply, buf, strlen(buf));
            if (g_max_entries) store_evict_lru(g_store, g_max_entries);
            /* A 0 increment on an existing member changes nothing,
               so like the ZADD no-op it is neither logged nor a
               WATCH invalidation. */
            return rc == 2 ? 0 : 1;
        }
        long long added = 0, updated = 0;
        int changed = 0;
        for (size_t k = 0; k < npairs; k++) {
            const char *member = argv[i + 1 + 2 * k];
            double score = scores[k];
            /* The conditions only gate updates of members already
               there: a new member is always added (GT/LT do not
               block new members), so read the current score first.
               Re-reading each pair also makes a duplicate member in
               one call see the score the earlier pair just set. */
            double cur = 0;
            int exists = store_zscore(g_store, argv[1], member, &cur) == 1;
            if ((nx && exists) || (xx && !exists)) continue;
            if (exists && ((gt && score <= cur) || (lt && score >= cur)))
                continue;
            long long rc = store_zadd(g_store, argv[1], member, score);
            if (rc == -2) {
                free(scores);
                return dbuf_error(reply, "out of memory"), 0;
            }
            if (rc == 1) added++;
            if (rc == 0) updated++;
            if (rc != 2) changed = 1;
        }
        free(scores);
        if (g_max_entries) store_evict_lru(g_store, g_max_entries);
        dbuf_int(reply, ch ? added + updated : added);
        /* Log only when something actually changed: re-adding a
           member with its current score, or a pair the options
           skipped, is a no-op, like SADD of a member already
           present. */
        return changed ? 1 : 0;
    }
    if (strcasecmp(cmd, "ZSCORE") == 0) {
        if (argc != 3) return dbuf_error(reply, "wrong number of arguments for 'zscore' command"), 0;
        int t = store_type(g_store, argv[1]);
        if (t != 0 && t != 5)
            return dbuf_error(reply, "WRONGTYPE Operation against a key holding the wrong kind of value"), 0;
        double score;
        int rc = store_zscore(g_store, argv[1], argv[2], &score);
        if (rc == -1)
            return dbuf_error(reply, "WRONGTYPE Operation against a key holding the wrong kind of value"), 0;
        if (rc == 0) return dbuf_null(reply), 0;
        char buf[32];
        format_score(buf, sizeof(buf), score);
        dbuf_bulk(reply, buf, strlen(buf));
        return 0;
    }
    if (strcasecmp(cmd, "ZCARD") == 0) {
        if (argc != 2) return dbuf_error(reply, "wrong number of arguments for 'zcard' command"), 0;
        long long n = store_zcard(g_store, argv[1]);
        if (n == -1)
            return dbuf_error(reply, "WRONGTYPE Operation against a key holding the wrong kind of value"), 0;
        dbuf_int(reply, n);
        return 0;
    }
    if (strcasecmp(cmd, "ZRANGE") == 0 || strcasecmp(cmd, "ZREVRANGE") == 0) {
        int reverse = strcasecmp(cmd, "ZREVRANGE") == 0;
        if (argc != 4 && argc != 5)
            return dbuf_error(reply, reverse ? "wrong number of arguments for 'zrevrange' command" : "wrong number of arguments for 'zrange' command"), 0;
        long long start, stop;
        if (!parse_ll(argv[2], &start) || !parse_ll(argv[3], &stop))
            return dbuf_error(reply, "value is not an integer or out of range"), 0;
        int withscores = 0;
        if (argc == 5) {
            if (strcasecmp(argv[4], "WITHSCORES") != 0)
                return dbuf_error(reply, "syntax error"), 0;
            withscores = 1;
        }
        char **members = NULL;
        double *scores = NULL;
        size_t n = 0;
        int rc = store_zrange(g_store, argv[1], start, stop, reverse,
                              &members, &scores, &n);
        if (rc == -1)
            return dbuf_error(reply, "WRONGTYPE Operation against a key holding the wrong kind of value"), 0;
        if (rc == -2)
            return dbuf_error(reply, "out of memory"), 0;
        dbuf_array_header(reply, (long long)(withscores ? n * 2 : n));
        for (size_t i = 0; i < n; i++) {
            dbuf_bulk(reply, members[i], strlen(members[i]));
            if (withscores) {
                char buf[32];
                format_score(buf, sizeof(buf), scores[i]);
                dbuf_bulk(reply, buf, strlen(buf));
            }
            free(members[i]);
        }
        free(members);
        free(scores);
        return 0;
    }
    if (strcasecmp(cmd, "ZRANGEBYSCORE") == 0 ||
        strcasecmp(cmd, "ZREVRANGEBYSCORE") == 0) {
        /* ZREVRANGEBYSCORE takes its bounds max-first, as in Redis,
           and returns the matches highest score first; the bound
           parsing, WITHSCORES and LIMIT handling are otherwise the
           same as ZRANGEBYSCORE. */
        int reverse = strcasecmp(cmd, "ZREVRANGEBYSCORE") == 0;
        if (argc < 4)
            return dbuf_error(reply, reverse ? "wrong number of arguments for 'zrevrangebyscore' command" : "wrong number of arguments for 'zrangebyscore' command"), 0;
        double min, max;
        int min_excl, max_excl;
        if (reverse) {
            if (!parse_score_bound(argv[2], &max, &max_excl) ||
                !parse_score_bound(argv[3], &min, &min_excl))
                return dbuf_error(reply, "value is not a valid float"), 0;
        } else if (!parse_score_bound(argv[2], &min, &min_excl) ||
                   !parse_score_bound(argv[3], &max, &max_excl)) {
            return dbuf_error(reply, "value is not a valid float"), 0;
        }
        int withscores = 0;
        int have_limit = 0;
        long long offset = 0, count = -1;
        for (int i = 4; i < argc; i++) {
            if (strcasecmp(argv[i], "WITHSCORES") == 0) {
                withscores = 1;
            } else if (strcasecmp(argv[i], "LIMIT") == 0) {
                if (i + 2 >= argc || have_limit)
                    return dbuf_error(reply, "syntax error"), 0;
                if (!parse_ll(argv[i + 1], &offset) ||
                    !parse_ll(argv[i + 2], &count) || offset < 0)
                    return dbuf_error(reply, "syntax error"), 0;
                have_limit = 1;
                i += 2;
            } else {
                return dbuf_error(reply, "syntax error"), 0;
            }
        }
        char **members = NULL;
        double *scores = NULL;
        size_t n = 0;
        int rc = store_zrangebyscore(g_store, argv[1], min, min_excl,
                                     max, max_excl, offset, count, reverse,
                                     &members, &scores, &n);
        if (rc == -1)
            return dbuf_error(reply, "WRONGTYPE Operation against a key holding the wrong kind of value"), 0;
        if (rc == -2)
            return dbuf_error(reply, "out of memory"), 0;
        dbuf_array_header(reply, (long long)(withscores ? n * 2 : n));
        for (size_t i = 0; i < n; i++) {
            dbuf_bulk(reply, members[i], strlen(members[i]));
            if (withscores) {
                char buf[32];
                format_score(buf, sizeof(buf), scores[i]);
                dbuf_bulk(reply, buf, strlen(buf));
            }
            free(members[i]);
        }
        free(members);
        free(scores);
        return 0;
    }
    if (strcasecmp(cmd, "ZREM") == 0) {
        if (argc < 3) return dbuf_error(reply, "wrong number of arguments for 'zrem' command"), 0;
        long long removed = 0;
        for (int i = 2; i < argc; i++) {
            long long rc = store_zrem(g_store, argv[1], argv[i]);
            if (rc == -1)
                return dbuf_error(reply, "WRONGTYPE Operation against a key holding the wrong kind of value"), 0;
            removed += rc;
        }
        dbuf_int(reply, removed);
        return removed > 0 ? 1 : 0;
    }
    if (strcasecmp(cmd, "ZRANK") == 0) {
        if (argc != 3) return dbuf_error(reply, "wrong number of arguments for 'zrank' command"), 0;
        long long rank;
        int rc = store_zrank(g_store, argv[1], argv[2], 0, &rank);
        if (rc == -1)
            return dbuf_error(reply, "WRONGTYPE Operation against a key holding the wrong kind of value"), 0;
        if (rc == 0) return dbuf_null(reply), 0;
        dbuf_int(reply, rank);
        return 0;
    }
    if (strcasecmp(cmd, "ZINCRBY") == 0) {
        if (argc != 4) return dbuf_error(reply, "wrong number of arguments for 'zincrby' command"), 0;
        double incr;
        if (!parse_double(argv[2], &incr))
            return dbuf_error(reply, "value is not a valid float"), 0;
        double next;
        int rc = store_zincrby(g_store, argv[1], argv[3], incr, &next);
        if (rc == -1)
            return dbuf_error(reply, "WRONGTYPE Operation against a key holding the wrong kind of value"), 0;
        if (rc == -2)
            return dbuf_error(reply, "out of memory"), 0;
        char buf[32];
        format_score(buf, sizeof(buf), next);
        dbuf_bulk(reply, buf, strlen(buf));
        /* A 0 increment on an existing member changes nothing, so
           like the ZADD no-op it is neither logged nor a WATCH
           invalidation. */
        return rc == 2 ? 0 : 1;
    }
    if (strcasecmp(cmd, "ZPOPMIN") == 0 || strcasecmp(cmd, "ZPOPMAX") == 0) {
        int from_max = strcasecmp(cmd, "ZPOPMAX") == 0;
        if (argc != 2 && argc != 3)
            return dbuf_error(reply, from_max ? "wrong number of arguments for 'zpopmax' command" : "wrong number of arguments for 'zpopmin' command"), 0;
        long long count = 1;
        if (argc == 3) {
            if (!parse_ll(argv[2], &count))
                return dbuf_error(reply, "value is not an integer or out of range"), 0;
            if (count < 1)
                return dbuf_error(reply, "value is out of range, must be positive"), 0;
        }
        char **members = NULL;
        double *scores = NULL;
        size_t n = 0;
        int rc = store_zpop(g_store, argv[1], count, from_max,
                            &members, &scores, &n);
        if (rc == -1)
            return dbuf_error(reply, "WRONGTYPE Operation against a key holding the wrong kind of value"), 0;
        if (rc == -2)
            return dbuf_error(reply, "out of memory"), 0;
        dbuf_array_header(reply, (long long)(n * 2));
        for (size_t i = 0; i < n; i++) {
            dbuf_bulk(reply, members[i], strlen(members[i]));
            char buf[32];
            format_score(buf, sizeof(buf), scores[i]);
            dbuf_bulk(reply, buf, strlen(buf));
            free(members[i]);
        }
        free(members);
        free(scores);
        /* A pop that removed nothing (a missing key) logs nothing;
           a real pop is logged and replicated as the raw command,
           which replays identically — the popped members are
           determined by the sorted order, so a replica pops the
           same ones. */
        return n > 0 ? 1 : 0;
    }
    if (strcasecmp(cmd, "ZRANDMEMBER") == 0) {
        if (argc != 2 && argc != 3 && argc != 4)
            return dbuf_error(reply, "wrong number of arguments for 'zrandmember' command"), 0;
        long long count = 0;
        int have_count = argc >= 3;
        int withscores = 0;
        if (have_count) {
            if (!parse_ll(argv[2], &count))
                return dbuf_error(reply, "value is not an integer or out of range"), 0;
        }
        if (argc == 4) {
            if (strcasecmp(argv[3], "WITHSCORES") != 0)
                return dbuf_error(reply, "syntax error"), 0;
            withscores = 1;
        }
        char **members = NULL;
        double *scores = NULL;
        size_t n = 0;
        /* A bare ZRANDMEMBER is one member, or nil when the key is
           missing; it reuses the count path with count 1. */
        int rc = store_zrandmember(g_store, argv[1], have_count ? count : 1,
                                   &members, &scores, &n);
        if (rc == -1)
            return dbuf_error(reply, "WRONGTYPE Operation against a key holding the wrong kind of value"), 0;
        if (rc == -2)
            return dbuf_error(reply, "out of memory"), 0;
        if (!have_count) {
            if (n == 0) {
                free(members);
                free(scores);
                return dbuf_null(reply), 0;
            }
            dbuf_bulk(reply, members[0], strlen(members[0]));
            free(members[0]);
            free(members);
            free(scores);
            return 0;
        }
        dbuf_array_header(reply, (long long)(withscores ? n * 2 : n));
        for (size_t i = 0; i < n; i++) {
            dbuf_bulk(reply, members[i], strlen(members[i]));
            if (withscores) {
                char buf[32];
                format_score(buf, sizeof(buf), scores[i]);
                dbuf_bulk(reply, buf, strlen(buf));
            }
            free(members[i]);
        }
        free(members);
        free(scores);
        return 0;
    }
    if (strcasecmp(cmd, "FLUSHALL") == 0) {
        store_flush(g_store);
        dbuf_simple(reply, "OK");
        return 1;
    }
    if (strcasecmp(cmd, "DBSIZE") == 0) {
        dbuf_int(reply, (long long)store_size(g_store));
        return 0;
    }
    if (strcasecmp(cmd, "INFO") == 0) {
        Dbuf info = {0};
        dbuf_putf(&info, "# Server\r\n");
        dbuf_putf(&info, "cinder_version:%s\r\n", CINDER_VERSION);
        dbuf_putf(&info, "uptime_in_seconds:%lld\r\n", (now_ms() - g_start_ms) / 1000);
        dbuf_putf(&info, "total_commands_processed:%lld\r\n", g_commands_processed);
        dbuf_putf(&info, "threads:%d\r\n", g_nthreads);
        dbuf_putf(&info, "threadpool_jobs_completed:%lld\r\n", tp_jobs_completed(g_pool));
        {
            int in_progress = 0, last_status = 0;
            long long last_ms = 0;
            bgsave_info(&in_progress, &last_status, &last_ms);
            dbuf_putf(&info, "bgsave_in_progress:%d\r\n", in_progress);
            dbuf_putf(&info, "last_bgsave_status:%s\r\n",
                      last_status == 0 ? "none" : last_status == 1 ? "ok" : "error");
            dbuf_putf(&info, "last_bgsave_time_ms:%lld\r\n", last_ms);
        }
        dbuf_putf(&info, "# Keyspace\r\n");
        dbuf_putf(&info, "keys:%zu\r\n", store_size(g_store));
        dbuf_putf(&info, "max_entries:%zu\r\n", g_max_entries);
        dbuf_putf(&info, "aof_fsync_policy:%s\r\n", aof_policy_name());
        {
            AofFlushInfo fi;
            aof_flush_info(&fi);
            dbuf_putf(&info, "aof_flush_thread:%d\r\n", fi.thread_running);
            dbuf_putf(&info, "aof_flush_in_progress:%d\r\n", fi.flush_in_progress);
            dbuf_putf(&info, "aof_flushes_completed:%lld\r\n", fi.flushes_completed);
            dbuf_putf(&info, "aof_dirty:%d\r\n", fi.dirty);
            dbuf_putf(&info, "aof_last_fsync_seconds_ago:%lld\r\n", fi.seconds_since_fsync);
            dbuf_putf(&info, "aof_pending_appends:%lld\r\n", fi.pending_appends);
            dbuf_putf(&info, "aof_deferred_replies:%zu\r\n", g_deferred_count);
        }
        dbuf_putf(&info, "# Replication\r\n");
        dbuf_putf(&info, "role:%s\r\n", repl_is_replica() ? "replica" : "master");
        if (repl_is_replica()) {
            dbuf_putf(&info, "master_host:%s\r\n", g_master_host);
            dbuf_putf(&info, "master_port:%lld\r\n", g_master_port);
            dbuf_putf(&info, "master_link_status:up\r\n");
            dbuf_putf(&info, "master_replid:%s\r\n",
                      g_primary_replid[0] ? g_primary_replid : "?");
            dbuf_putf(&info, "master_repl_offset:%lld\r\n", g_slave_repl_offset);
            dbuf_putf(&info, "slave_repl_offset:%lld\r\n", g_slave_repl_offset);
        } else {
            dbuf_putf(&info, "master_replid:%s\r\n", g_replid);
            dbuf_putf(&info, "master_repl_offset:%lld\r\n", g_master_repl_offset);
        }
        dbuf_putf(&info, "connected_replicas:%zu\r\n", repl_connected_replicas());
        dbuf_putf(&info, "repl_backlog_active:%d\r\n", g_backlog_cap > 0 ? 1 : 0);
        dbuf_putf(&info, "repl_backlog_size:%zu\r\n", g_backlog_cap);
        dbuf_putf(&info, "repl_backlog_first_byte_offset:%lld\r\n", g_backlog_start);
        dbuf_putf(&info, "repl_backlog_histlen:%zu\r\n", g_backlog_len);
        repl_info_replicas(&info);
        dbuf_bulk(reply, info.data ? info.data : "", info.len);
        free(info.data);
        return 0;
    }
    if (strcasecmp(cmd, "COMMAND") == 0) {
        static const char *names[] = {"PING", "ECHO", "QUIT", "SHUTDOWN", "SET", "GET",
            "MSET", "MGET", "GETDEL", "GETEX",
            "SETNX", "SETEX", "GETSET", "APPEND", "STRLEN",
            "GETRANGE", "SETRANGE", "SETBIT", "GETBIT", "BITCOUNT", "BITOP", "BITFIELD",
            "DEL", "EXISTS", "EXPIRE", "PEXPIRE", "TTL", "PTTL", "INCR", "DECR",
            "INCRBY", "DECRBY", "LPUSH", "RPUSH", "LPOP", "RPOP", "LLEN", "LRANGE",
            "LINDEX", "LSET", "LINSERT", "LPUSHX", "RPUSHX", "LTRIM",
            "LMOVE",
            "SADD", "SREM", "SMEMBERS", "SISMEMBER", "SCARD",
            "HSET", "HGET", "HDEL", "HEXISTS", "HLEN", "HGETALL",
            "ZADD", "ZSCORE", "ZCARD", "ZRANGE", "ZREVRANGE", "ZRANGEBYSCORE",
            "ZREVRANGEBYSCORE", "ZREM",
            "ZRANK", "ZINCRBY", "ZPOPMIN", "ZPOPMAX", "ZRANDMEMBER",
            "KEYS", "SCAN", "FLUSHALL", "DBSIZE", "INFO", "COMMAND", "SAVE", "BGSAVE",
            "MULTI", "EXEC", "DISCARD", "WATCH", "UNWATCH",
            "SYNC", "PSYNC", "REPLICAOF"};
        dbuf_array_header(reply, (long long)(sizeof(names) / sizeof(names[0])));
        for (size_t i = 0; i < sizeof(names) / sizeof(names[0]); i++)
            dbuf_bulk(reply, names[i], strlen(names[i]));
        return 0;
    }
    if (strcasecmp(cmd, "SAVE") == 0) {
        if (argc != 1)
            return dbuf_error(reply, "wrong number of arguments for 'save' command"), 0;
        /* Snapshot the whole dataset to dump.rdb, then force the AOF
           to disk too, as SAVE did before snapshots existed. */
        if (rdb_save(g_store, g_rdb_path) < 0)
            return dbuf_error(reply, "ERR failed to write snapshot"), 0;
        aof_fsync();
        dbuf_simple(reply, "OK");
        return 0;
    }
    if (strcasecmp(cmd, "BGSAVE") == 0) {
        if (argc != 1)
            return dbuf_error(reply, "wrong number of arguments for 'bgsave' command"), 0;
        /* One background save at a time: a second BGSAVE while one is
           in progress is rejected rather than queued behind it, so a
           client never waits on a stale snapshot. */
        int in_progress = 0, last_status = 0;
        long long last_ms = 0;
        bgsave_info(&in_progress, &last_status, &last_ms);
        if (in_progress)
            return dbuf_error(reply, "background save already in progress"), 0;
        /* Serialize on the event-loop thread: the snapshot is a
           consistent picture of the store, and the worker only ever
           touches the bytes, never the store. */
        char *data = NULL;
        size_t len = 0;
        if (rdb_serialize(g_store, &data, &len) < 0)
            return dbuf_error(reply, "failed to serialize snapshot"), 0;
        BgsaveJob *job = malloc(sizeof(*job));
        if (!job) {
            free(data);
            return dbuf_error(reply, "ERR out of memory"), 0;
        }
        job->data = data;
        job->len = len;
        snprintf(job->path, sizeof(job->path), "%s", g_rdb_path);
        pthread_mutex_lock(&g_bgsave_mu);
        g_bgsave_in_progress = 1;
        pthread_mutex_unlock(&g_bgsave_mu);
        if (tp_submit(g_pool, bgsave_run, job) < 0) {
            pthread_mutex_lock(&g_bgsave_mu);
            g_bgsave_in_progress = 0;
            pthread_mutex_unlock(&g_bgsave_mu);
            free(job->data);
            free(job);
            return dbuf_error(reply, "background save could not start"), 0;
        }
        dbuf_simple(reply, "Background saving started");
        return 0;
    }
    dbuf_error(reply, "unknown command");
    return 0;
}

/* ---------- clients ---------- */

/* One queued command inside a MULTI/EXEC transaction: a deep copy of
   the argv the client sent, owned by the queue. */
typedef struct {
    char **argv;
    int argc;
} QueuedCmd;

/* One key a client WATCHed: the key, the version it had when watched
   (0 = it did not exist), and the store's flush generation then, so
   a FLUSHALL invalidates even a watch on a key that did not exist. */
typedef struct {
    char *key;
    unsigned long long version;
    unsigned long long flush_seq;
} WatchedKey;

typedef struct Client {
    int fd;
    unsigned long long gen; /* generation: distinguishes this client
                               from a later one that reuses its fd, so
                               a deferred reply is never delivered to
                               the wrong connection */
    char inbuf[INBUF_CAP];
    size_t inlen;
    Dbuf out;
    int dead;
    int in_multi;       /* inside a MULTI/EXEC transaction: commands
                           queue instead of executing */
    QueuedCmd *queue;   /* queued commands, in the order received */
    size_t queue_len;
    size_t queue_cap;
    WatchedKey *watch;  /* keys this client WATCHed, with the version
                           each had when watched */
    size_t watch_len;
    size_t watch_cap;
    unsigned long long deferred_last_target; /* append seq of this
        client's most recent deferred reply (0 = none pending) */
    size_t deferred_count; /* replies of this client still queued */
    int is_replica;     /* primary side: this client is a replica (SYNC'd);
                           only PING is accepted from it, writes are
                           streamed to it */
    int is_repl_link;   /* replica side: this socket is the outbound link
                           to our primary; commands on it are applied */
    int repl_connecting; /* outbound link still completing its connect */
    long long repl_offset; /* primary side: stream offset of the last
                              byte queued to this replica */
    int repl_syncing;    /* replica side: still applying the initial
                            full-sync dump; dump bytes are the snapshot,
                            not the live stream, so they do not advance
                            the slave offset */
    uint32_t epoll_events; /* interest currently registered with epoll
                              for this fd (0 until client_add registers
                              it); client_sync_interest keeps it in step
                              with out.len / repl_connecting */
} Client;

static int g_epollfd = -1; /* created once in main, before the loop */
static int g_aof_wakeup_fd = -1; /* eventfd the AOF flush thread writes
                                    to after every completed sync */

static size_t g_clients_cap;
static unsigned long long g_client_gen; /* bumped per client_add */

static void client_add(int fd) {
    if (g_nclients >= g_clients_cap) {
        size_t ncap = g_clients_cap ? g_clients_cap * 2 : 64;
        Client *nc = realloc(g_clients, ncap * sizeof(Client));
        if (!nc) { close(fd); return; }
        g_clients = nc;
        g_clients_cap = ncap;
    }
    Client *c = &g_clients[g_nclients++];
    memset(c, 0, sizeof(*c));
    c->fd = fd;
    c->gen = ++g_client_gen;
    /* Register with epoll for reads; EPOLLOUT is added by
       client_sync_interest when the client has output pending or its
       outbound replication connect is still completing. */
    if (g_epollfd >= 0) {
        struct epoll_event ev;
        memset(&ev, 0, sizeof(ev));
        ev.events = EPOLLIN;
        ev.data.fd = fd;
        if (epoll_ctl(g_epollfd, EPOLL_CTL_ADD, fd, &ev) == 0)
            c->epoll_events = EPOLLIN;
        else
            perror("epoll_ctl ADD");
    }
}

/* Drop a client's whole MULTI/EXEC queue, freeing the deep-copied
   argv strings. A no-op on a client that never queued anything. */
static void multi_clear(Client *c) {
    for (size_t i = 0; i < c->queue_len; i++) {
        for (int j = 0; j < c->queue[i].argc; j++)
            free(c->queue[i].argv[j]);
        free(c->queue[i].argv);
    }
    free(c->queue);
    c->queue = NULL;
    c->queue_len = 0;
    c->queue_cap = 0;
}

/* Queue one command for a client inside MULTI: deep-copy argv, so
   the queue survives the event loop reusing the input buffer.
   Returns 0 on success, -1 on allocation failure. */
static int multi_queue(Client *c, char **argv, int argc) {
    if (c->queue_len == c->queue_cap) {
        size_t ncap = c->queue_cap ? c->queue_cap * 2 : 8;
        QueuedCmd *nq = realloc(c->queue, ncap * sizeof(QueuedCmd));
        if (!nq) return -1;
        c->queue = nq;
        c->queue_cap = ncap;
    }
    QueuedCmd *q = &c->queue[c->queue_len];
    q->argv = malloc((argc > 0 ? argc : 1) * sizeof(char *));
    if (!q->argv) return -1;
    q->argc = argc;
    for (int i = 0; i < argc; i++) {
        q->argv[i] = strdup(argv[i]);
        if (!q->argv[i]) {
            for (int j = 0; j < i; j++) free(q->argv[j]);
            free(q->argv);
            return -1;
        }
    }
    c->queue_len++;
    return 0;
}

/* Drop a client's whole WATCH set, freeing the key copies. A no-op
   on a client that never watched anything. */
static void watch_clear(Client *c) {
    for (size_t i = 0; i < c->watch_len; i++)
        free(c->watch[i].key);
    free(c->watch);
    c->watch = NULL;
    c->watch_len = 0;
    c->watch_cap = 0;
}

/* Record a key in a client's WATCH set with the version it has right
   now (0 when it does not exist) and the store's flush generation.
   Re-watching a key refreshes its recorded version, as in Redis.
   Returns 0 on success, -1 on allocation failure. */
static int watch_add(Client *c, const char *key) {
    unsigned long long ver = store_key_version(g_store, key);
    unsigned long long fs = store_flush_seq(g_store);
    for (size_t i = 0; i < c->watch_len; i++) {
        if (strcmp(c->watch[i].key, key) == 0) {
            c->watch[i].version = ver;
            c->watch[i].flush_seq = fs;
            return 0;
        }
    }
    if (c->watch_len == c->watch_cap) {
        size_t ncap = c->watch_cap ? c->watch_cap * 2 : 8;
        WatchedKey *nw = realloc(c->watch, ncap * sizeof(WatchedKey));
        if (!nw) return -1;
        c->watch = nw;
        c->watch_cap = ncap;
    }
    c->watch[c->watch_len].key = strdup(key);
    if (!c->watch[c->watch_len].key) return -1;
    c->watch[c->watch_len].version = ver;
    c->watch[c->watch_len].flush_seq = fs;
    c->watch_len++;
    return 0;
}

/* Has any key this client WATCHed changed since it was watched: a
   different version now (created, modified, deleted, or expired —
   expiry deletes the entry, so its version reads 0), or a FLUSHALL
   since, which bumps the flush generation even for keys that never
   existed. 1 = dirty, EXEC must abort. */
static int watch_dirty(Client *c) {
    unsigned long long fs = store_flush_seq(g_store);
    for (size_t i = 0; i < c->watch_len; i++) {
        if (c->watch[i].flush_seq != fs) return 1;
        if (store_key_version(g_store, c->watch[i].key) !=
            c->watch[i].version)
            return 1;
    }
    return 0;
}

/* Recompute a client's epoll interest from its current state and
   update the registration only when it changed. Called once per loop
   iteration for every client, before epoll_wait: the poll() loop this
   replaces recomputed interest every iteration for free, and with
   epoll the same recompute is what catches output queued for a client
   by another client's command (forward_to_replicas) or by a full sync
   (send_full_sync / backlog_send). */
static void client_sync_interest(Client *c) {
    if (g_epollfd < 0 || c->epoll_events == 0) return;
    uint32_t want = EPOLLIN;
    if (c->out.len > 0 || c->repl_connecting) want |= EPOLLOUT;
    if (want == c->epoll_events) return;
    struct epoll_event ev;
    memset(&ev, 0, sizeof(ev));
    ev.events = want;
    ev.data.fd = c->fd;
    if (epoll_ctl(g_epollfd, EPOLL_CTL_MOD, c->fd, &ev) == 0)
        c->epoll_events = want;
    else
        perror("epoll_ctl MOD");
}

static void client_remove(size_t idx) {
    if (g_epollfd >= 0)
        epoll_ctl(g_epollfd, EPOLL_CTL_DEL, g_clients[idx].fd, NULL);
    close(g_clients[idx].fd);
    free(g_clients[idx].out.data);
    /* A client that disconnects mid-transaction drops its queue with
       it: nothing queued ever executes. */
    multi_clear(&g_clients[idx]);
    watch_clear(&g_clients[idx]);
    g_clients[idx] = g_clients[--g_nclients];
}

static int set_nonblocking(int fd) {
    int flags = fcntl(fd, F_GETFL, 0);
    if (flags < 0) return -1;
    return fcntl(fd, F_SETFL, flags | O_NONBLOCK);
}

/* ---------- deferred replies (the `always` fsync slice) ----------
 * Under `always` a mutating command's reply must wait for the fsync
 * covering its AOF append, but the fdatasync runs on the flush
 * thread, not on the event loop. The reply is therefore parked in a
 * FIFO queue keyed by the append sequence number it waits for, and
 * released in order once aof_synced_through() reaches it. The queue
 * is global (one ordering across all clients, matching command
 * execution order) and entries carry the client's fd plus its
 * generation, because the client array compacts on removal and an fd
 * can be reused: an entry whose client is gone, or whose generation
 * no longer matches, is dropped at drain time.
 *
 * Per-client order is the subtle part: while a client has a reply
 * parked, every later reply to that client — even a read that needs
 * no sync — is parked behind it too, keyed by the client's most
 * recent parked target. Otherwise a GET sent after a SET could
 * overtake the SET's +OK on the wire. client_reply() is the single
 * choke point that enforces this; every command path routes its
 * reply through it. */

typedef struct DeferredReply {
    int fd;
    unsigned long long gen;
    unsigned long long target; /* append seq the reply waits for */
    char *data;                /* reply bytes; owned by this entry */
    size_t len;
    struct DeferredReply *next;
} DeferredReply;

static DeferredReply *g_deferred_head;
static DeferredReply *g_deferred_tail;

/* Park a reply in the queue; takes ownership of reply->data. */
static void defer_reply(Client *c, Dbuf *reply, unsigned long long target) {
    DeferredReply *e = malloc(sizeof(*e));
    if (!e) {
        /* Out of memory: deliver immediately rather than drop the
           reply. The durability guarantee is weakened for this one
           command, but the client is never left hanging. */
        dbuf_put(&c->out, reply->data, reply->len);
        return;
    }
    e->fd = c->fd;
    e->gen = c->gen;
    e->target = target;
    e->data = reply->data;
    e->len = reply->len;
    e->next = NULL;
    reply->data = NULL;
    reply->len = 0;
    if (g_deferred_tail) g_deferred_tail->next = e;
    else g_deferred_head = e;
    g_deferred_tail = e;
    g_deferred_count++;
    c->deferred_count++;
    c->deferred_last_target = target;
}

/* Deliver a reply to a client, deferring it when the `always` policy
   requires a sync that has not landed yet, or when the client already
   has replies parked (later replies must not overtake them). `target`
   is the append sequence number the reply waits for, or 0 when the
   reply needs no sync. Takes ownership of reply->data only when it
   parks the reply; otherwise the caller still owns it. */
static void client_reply(Client *c, Dbuf *reply, unsigned long long target) {
    if (reply->len == 0) return;
    if (c->deferred_count > 0) {
        defer_reply(c, reply, target ? target : c->deferred_last_target);
        return;
    }
    if (target > 0 && aof_always_policy() &&
        aof_synced_through() < target) {
        defer_reply(c, reply, target);
        return;
    }
    dbuf_put(&c->out, reply->data, reply->len);
}

/* Release every parked reply whose sync has landed, in queue order.
   Targets are non-decreasing along the queue (appends are sequential
   and a sync covers a prefix of them), so checking the head is
   enough: once the head is covered, so is everything before it. */
static void deferred_drain(void) {
    while (g_deferred_head) {
        DeferredReply *e = g_deferred_head;
        if (aof_synced_through() < e->target) break;
        g_deferred_head = e->next;
        if (!g_deferred_head) g_deferred_tail = NULL;
        g_deferred_count--;
        Client *c = NULL;
        for (size_t i = 0; i < g_nclients; i++)
            if (g_clients[i].fd == e->fd && g_clients[i].gen == e->gen) {
                c = &g_clients[i];
                break;
            }
        if (c) {
            dbuf_put(&c->out, e->data, e->len);
            if (c->deferred_count > 0) c->deferred_count--;
            if (c->deferred_count == 0) c->deferred_last_target = 0;
        }
        /* No matching client: it disconnected while the reply was
           parked; the reply is dropped with it. */
        free(e->data);
        free(e);
    }
}

/* Wait for the syncs covering a client's parked replies, deliver
   them, and flush the client's socket before it closes (used for
   QUIT: without this, a +OK parked behind a write would never reach
   the client). */
static void deferred_flush_client(Client *c) {
    if (c->deferred_count == 0) return;
    aof_wait_for(c->deferred_last_target);
    deferred_drain();
    if (c->out.len > 0) {
        int flags = fcntl(c->fd, F_GETFL, 0);
        if (flags >= 0) fcntl(c->fd, F_SETFL, flags & ~O_NONBLOCK);
        size_t off = 0;
        while (off < c->out.len) {
            ssize_t w = write(c->fd, c->out.data + off, c->out.len - off);
            if (w <= 0) break;
            off += (size_t)w;
        }
    }
}

/* ---------- replication ----------
 * Primary side: a client sends SYNC, is marked a replica, and gets a
 * full sync (FLUSHALL plus one rebuild command per key, as RESP
 * arrays) followed by a +FULLSYNC marker. After that, every mutating
 * command from any other client is forwarded to it verbatim.
 * A client may instead send PSYNC <replid> <offset> to resume from a
 * stream offset. If it names this server's run id and the offset is
 * still inside the replication backlog, the answer is
 * +CONTINUE <replid> <offset> followed by only the missed bytes;
 * otherwise it is +FULLRESYNC <replid> <offset> followed by a full
 * sync. The backlog holds the most recent bytes of the live write
 * stream, so a short disconnect resumes instead of re-syncing.
 * Offsets: the primary counts every byte of the live write stream it
 * forwards (master_repl_offset) and tracks, per replica, the offset
 * of the last byte queued to it. A replica learns the primary's run
 * id and offset from +FULLRESYNC and counts the stream bytes it
 * applies (slave_repl_offset); dump bytes do not count, so a caught-up
 * replica's slave offset equals the primary's master offset.
 * Replica side: REPLICAOF host port opens a non-blocking connection
 * to the primary, sends PSYNC, and applies every command arriving on
 * that link through the normal dispatcher (and its own AOF, so the
 * replicated data is durable). REPLICAOF NO ONE is promotion: the
 * link drops, the replica state (primary run id and stream offset)
 * is forgotten, and the node is a primary again, keeping the data it
 * replicated; a later REPLICAOF to the old primary full-syncs. The
 * replica state is also written to replication.state in the data dir
 * on a clean shutdown and reloaded (then consumed) on startup, so a
 * replica that restarts cleanly resumes from its offset with a
 * partial resync instead of full-syncing; an unclean stop leaves no
 * state file, so the next REPLICAOF full-syncs rather than risk
 * replaying commands from a stale offset.
 * A replica can itself accept SYNC/PSYNC and forward what it applies,
 * so replicas chain; a command is never sent back down the link it
 * arrived on, so there is no loop. */

static Client *find_repl_link(void) {
    for (size_t i = 0; i < g_nclients; i++)
        if (g_clients[i].is_repl_link) return &g_clients[i];
    return NULL;
}

static int repl_is_replica(void) { return find_repl_link() != NULL; }

static size_t repl_connected_replicas(void) {
    size_t n = 0;
    for (size_t i = 0; i < g_nclients; i++)
        if (g_clients[i].is_replica) n++;
    return n;
}

/* Mark the outbound replication link dead; the sweep at the end of
   the loop iteration closes it. */
static void repl_link_close(void) {
    Client *link = find_repl_link();
    if (link) link->dead = 1;
}

/* Drop any persisted replication state from the data dir. */
static void repl_state_discard(void) {
    if (g_repl_state_path[0]) unlink(g_repl_state_path);
}

/* Forget which primary this node replicates from: the stream position
   (primary run id and slave offset) and the master's address. Used by
   promotion (REPLICAOF NO ONE) and when attaching to a different
   primary, so a later REPLICAOF starts from a full sync. The dataset
   is untouched. */
static void repl_forget_master(void) {
    g_master_host[0] = '\0';
    g_master_port = 0;
    g_primary_replid[0] = '\0';
    g_slave_repl_offset = 0;
    repl_state_discard();
}

/* Persist the replication stream position to the data dir. Only
   called on a clean shutdown, when the offset is exact: every applied
   command is already in the AOF, and no more will be applied after
   this, so reloading it resumes precisely where this process left
   off. Best effort: a failure to write just means the next REPLICAOF
   full-syncs. */
static void repl_state_save(void) {
    if (!g_repl_state_path[0] || !g_primary_replid[0]) return;
    FILE *f = fopen(g_repl_state_path, "w");
    if (!f) return;
    fprintf(f, "cinder-repl-state-v1\n%s\n%lld\n%s\n%lld\n",
            g_primary_replid, g_slave_repl_offset,
            g_master_host, g_master_port);
    fclose(f);
}

/* Reload a persisted stream position, written by repl_state_save on
   a clean shutdown. The file is consumed (deleted) as soon as it is
   read, so a stale position can never be replayed after an unclean
   stop: a process that dies without a clean shutdown leaves no file,
   and the next REPLICAOF full-syncs. A corrupt file is ignored. */
static void repl_state_load(void) {
    if (!g_repl_state_path[0]) return;
    FILE *f = fopen(g_repl_state_path, "r");
    if (!f) return;
    char magic[64] = "", rid[64] = "", host[256] = "";
    char offbuf[64] = "", portbuf[64] = "";
    int ok = 0;
    if (fgets(magic, sizeof(magic), f) && fgets(rid, sizeof(rid), f) &&
        fgets(offbuf, sizeof(offbuf), f) && fgets(host, sizeof(host), f) &&
        fgets(portbuf, sizeof(portbuf), f)) {
        /* Strip trailing newlines. */
        magic[strcspn(magic, "\r\n")] = '\0';
        rid[strcspn(rid, "\r\n")] = '\0';
        offbuf[strcspn(offbuf, "\r\n")] = '\0';
        host[strcspn(host, "\r\n")] = '\0';
        portbuf[strcspn(portbuf, "\r\n")] = '\0';
        long long off = -1, port = 0;
        int hex = strlen(rid) == 40;
        for (int i = 0; hex && i < 40; i++)
            if (!isxdigit((unsigned char)rid[i])) hex = 0;
        if (strcmp(magic, "cinder-repl-state-v1") == 0 && hex &&
            parse_ll(offbuf, &off) && off >= 0 && host[0] &&
            parse_ll(portbuf, &port) && port > 0 && port <= 65535) {
            snprintf(g_primary_replid, sizeof(g_primary_replid), "%s", rid);
            g_slave_repl_offset = off;
            snprintf(g_master_host, sizeof(g_master_host), "%s", host);
            g_master_port = port;
            ok = 1;
        }
    }
    fclose(f);
    unlink(g_repl_state_path);
    if (ok)
        printf("cinder: resumed replication state: primary %s:%lld, replid %s, offset %lld\n",
               g_master_host, g_master_port, g_primary_replid, g_slave_repl_offset);
}

/* Record raw stream bytes in the backlog. The bytes cover offsets
   [g_master_repl_offset, g_master_repl_offset + n); the caller bumps
   the offset afterwards. When the buffer is full the oldest bytes drop
   off the front and g_backlog_start advances past them. */
static void backlog_append(const char *data, size_t n) {
    if (g_backlog_cap == 0 || !g_backlog || n == 0) return;
    if (g_backlog_len == 0)
        g_backlog_start = g_master_repl_offset;
    if (n >= g_backlog_cap) {
        /* Only the tail of this one command fits; everything older
           is gone. */
        memcpy(g_backlog, data + (n - g_backlog_cap), g_backlog_cap);
        g_backlog_head = 0;
        g_backlog_len = g_backlog_cap;
        g_backlog_start = g_master_repl_offset + (long long)(n - g_backlog_cap);
        return;
    }
    if (g_backlog_len + n > g_backlog_cap) {
        size_t drop = g_backlog_len + n - g_backlog_cap;
        g_backlog_head = (g_backlog_head + drop) % g_backlog_cap;
        g_backlog_len -= drop;
        g_backlog_start += (long long)drop;
    }
    size_t tail = (g_backlog_head + g_backlog_len) % g_backlog_cap;
    size_t first = g_backlog_cap - tail;
    if (first >= n) {
        memcpy(g_backlog + tail, data, n);
    } else {
        memcpy(g_backlog + tail, data, first);
        memcpy(g_backlog, data + first, n - first);
    }
    g_backlog_len += n;
}

/* True when stream offset `off` is still in the backlog: the bytes
   from `off` to the current master offset are all held. An offset of
   exactly the master offset is in the backlog too (nothing missed). */
static int backlog_contains(long long off) {
    return g_backlog_cap > 0 && g_backlog != NULL &&
           off >= g_backlog_start && off <= g_master_repl_offset;
}

/* Queue the backlog bytes from stream offset `from` (inclusive) to
   the current master offset into a replica's output buffer. The
   caller checks backlog_contains(from) first, and sends the +CONTINUE
   line before calling this, so the line precedes the stream bytes. */
static void backlog_send(Client *c, long long from) {
    size_t n = (size_t)(g_master_repl_offset - from);
    if (n == 0) return;
    size_t pos = (g_backlog_head + (size_t)(from - g_backlog_start)) % g_backlog_cap;
    size_t first = g_backlog_cap - pos;
    if (first >= n) {
        dbuf_put(&c->out, g_backlog + pos, n);
    } else {
        dbuf_put(&c->out, g_backlog + pos, first);
        dbuf_put(&c->out, g_backlog, n - first);
    }
}

/* Forward one raw command (as received, RESP array or inline) to
   every connected replica. The offset counts stream bytes whether or
   not a replica is currently attached, like Redis's
   master_repl_offset; each replica records the offset of the last
   byte queued to it. The backlog records the same bytes, so a replica
   that drops can resume from any offset still inside it. */
static void forward_to_replicas(const char *raw, size_t rawlen) {
    backlog_append(raw, rawlen);
    g_master_repl_offset += (long long)rawlen;
    for (size_t i = 0; i < g_nclients; i++) {
        if (g_clients[i].is_replica) {
            dbuf_put(&g_clients[i].out, raw, rawlen);
            g_clients[i].repl_offset = g_master_repl_offset;
        }
    }
}

/* One `replicaN:ip=...,port=...,offset=...` line per connected replica
   for INFO. */
static void repl_info_replicas(Dbuf *info) {
    size_t n = 0;
    for (size_t i = 0; i < g_nclients; i++) {
        if (!g_clients[i].is_replica) continue;
        struct sockaddr_in peer;
        socklen_t plen = sizeof(peer);
        char ip[64] = "?";
        int port = 0;
        if (getpeername(g_clients[i].fd, (struct sockaddr *)&peer, &plen) == 0) {
            inet_ntop(AF_INET, &peer.sin_addr, ip, sizeof(ip));
            port = ntohs(peer.sin_port);
        }
        dbuf_putf(info, "replica%zu:ip=%s,port=%d,offset=%lld\r\n",
                  n++, ip, port, g_clients[i].repl_offset);
    }
}

/* Encode one command as a RESP array into out. argv strings are
   borrowed; the caller keeps them alive for the call. */
static void emit_resp_cmd(Dbuf *out, char **argv, int argc) {
    dbuf_array_header(out, argc);
    for (int i = 0; i < argc; i++) dbuf_bulk(out, argv[i], strlen(argv[i]));
}

/* One entry of the full sync: the commands that rebuild this key on
   the replica. Strings carry their remaining TTL as PX; other types
   get a PEXPIRE after the rebuild. */
static int sync_emit_entry(const Entry *e, void *arg) {
    Dbuf *out = arg;
    long long ttl = e->expire_at > 0 ? e->expire_at - now_ms() : -1;
    if (e->expire_at > 0 && ttl <= 0) return 0; /* expired since the scan */
    char ttlbuf[32];
    if (e->type == ENTRY_STRING) {
        /* A value holding zero bytes (SETRANGE padding) cannot ride
           a single SET: the replica's dispatcher reads argv as C
           strings, so the value would truncate at the first NUL.
           Rebuild it instead as a SET of the leading non-zero run
           (carrying the PX when the key has a TTL) plus one SETRANGE
           per later non-zero run; SETRANGE zero-pads the gaps, so
           the replica ends up byte-identical. A value whose last
           byte is a zero byte (SETBIT can produce one) loses its
           tail in that rebuild, since a NUL cannot ride an argv
           string either, so a trailing SETBIT of the last bit
           extends the value back to its full length. */
        size_t i = 0, len = e->value_len;
        int first = 1;
        while (i <= len) {
            size_t j = i;
            while (j < len && e->value[j] != '\0') j++;
            if (first) {
                dbuf_array_header(out, ttl > 0 ? 5 : 3);
                dbuf_bulk(out, "SET", 3);
                dbuf_bulk(out, e->key, strlen(e->key));
                dbuf_bulk(out, e->value + i, j - i);
                if (ttl > 0) {
                    snprintf(ttlbuf, sizeof(ttlbuf), "%lld", ttl);
                    dbuf_bulk(out, "PX", 2);
                    dbuf_bulk(out, ttlbuf, strlen(ttlbuf));
                }
                first = 0;
            } else if (j > i) {
                char offbuf[32];
                snprintf(offbuf, sizeof(offbuf), "%zu", i);
                dbuf_array_header(out, 4);
                dbuf_bulk(out, "SETRANGE", 8);
                dbuf_bulk(out, e->key, strlen(e->key));
                dbuf_bulk(out, offbuf, strlen(offbuf));
                dbuf_bulk(out, e->value + i, j - i);
            }
            i = j + 1; /* skip the NUL (or step past the end) */
        }
        if (len > 0 && e->value[len - 1] == '\0') {
            /* SETBIT of the last bit (to 0) zero-pads the value out
               to this byte, restoring the trailing zero bytes the
               SET/SETRANGE rebuild above could not carry. */
            char offbuf[32];
            snprintf(offbuf, sizeof(offbuf), "%zu", (len - 1) * 8);
            dbuf_array_header(out, 4);
            dbuf_bulk(out, "SETBIT", 6);
            dbuf_bulk(out, e->key, strlen(e->key));
            dbuf_bulk(out, offbuf, strlen(offbuf));
            dbuf_bulk(out, "0", 1);
        }
        return 0;
    }
    if (e->type == ENTRY_LIST) {
        List *l = e->list;
        if (l->len == 0) return 0;
        char **av = malloc((2 + l->len) * sizeof(char *));
        if (!av) return 0;
        av[0] = "RPUSH";
        av[1] = (char *)e->key;
        for (size_t i = 0; i < l->len; i++)
            av[2 + i] = l->items[(l->head + i) % l->cap];
        emit_resp_cmd(out, av, 2 + (int)l->len);
        free(av);
    } else if (e->type == ENTRY_SET) {
        Set *st = e->set;
        if (st->len == 0) return 0;
        char **av = malloc((2 + st->len) * sizeof(char *));
        if (!av) return 0;
        av[0] = "SADD";
        av[1] = (char *)e->key;
        for (size_t i = 0; i < st->len; i++) av[2 + i] = st->members[i];
        emit_resp_cmd(out, av, 2 + (int)st->len);
        free(av);
    } else if (e->type == ENTRY_HASH) {
        Hash *h = e->hash;
        if (h->len == 0) return 0;
        char **av = malloc((2 + 2 * h->len) * sizeof(char *));
        if (!av) return 0;
        av[0] = "HSET";
        av[1] = (char *)e->key;
        for (size_t i = 0; i < h->len; i++) {
            av[2 + 2 * i] = h->fields[i];
            av[3 + 2 * i] = h->values[i];
        }
        emit_resp_cmd(out, av, 2 + (int)(2 * h->len));
        free(av);
    } else if (e->type == ENTRY_ZSET) {
        ZSet *z = e->zset;
        if (z->len == 0) return 0;
        /* One ZADD carrying every member; scores are %.17g strings,
           which parse back to the exact same doubles. The array is
           already sorted by (score, member), but order does not
           matter for the rebuild — ZADD re-sorts on the replica. */
        char **av = malloc((2 + 2 * z->len) * sizeof(char *));
        char (*scores)[32] = malloc(z->len * sizeof(*scores));
        if (!av || !scores) {
            free(av);
            free(scores);
            return 0;
        }
        av[0] = "ZADD";
        av[1] = (char *)e->key;
        for (size_t i = 0; i < z->len; i++) {
            format_score(scores[i], sizeof(scores[i]), z->scores[i]);
            av[2 + 2 * i] = scores[i];
            av[3 + 2 * i] = z->members[i];
        }
        emit_resp_cmd(out, av, 2 + (int)(2 * z->len));
        free(av);
        free(scores);
    }
    if (ttl > 0) {
        snprintf(ttlbuf, sizeof(ttlbuf), "%lld", ttl);
        char *av[] = {"PEXPIRE", (char *)e->key, ttlbuf};
        emit_resp_cmd(out, av, 3);
    }
    return 0;
}

/* Queue the full sync into a replica client's output buffer. FLUSHALL
   first: a full sync replaces the replica's dataset, it does not merge
   into it. The +FULLSYNC marker tells the replica the dump is done. */
static void send_full_sync(Client *c) {
    char *flush[] = {"FLUSHALL"};
    emit_resp_cmd(&c->out, flush, 1);
    store_foreach(g_store, sync_emit_entry, &c->out);
    dbuf_simple(&c->out, "FULLSYNC OK");
    printf("cinder: full sync queued for replica (fd %d, %zu keys)\n",
           c->fd, store_size(g_store));
    fflush(stdout);
}

/* Ask the primary for a sync, resuming from the offset we already
   have when we know this primary's run id. The primary answers
   +CONTINUE and streams only the missed bytes when the offset is
   still in its backlog, +FULLRESYNC and a full sync otherwise. */
static void send_psync_request(Client *link) {
    if (g_primary_replid[0])
        dbuf_putf(&link->out, "PSYNC %s %lld\r\n",
                  g_primary_replid, g_slave_repl_offset);
    else
        dbuf_put(&link->out, "PSYNC ? -1\r\n", 12);
}

/* Open the outbound link to a primary. On success the socket is a
   client with is_repl_link set; PSYNC is sent as soon as the connect
   completes (immediately for localhost, on EPOLLOUT otherwise). */
static int replicaof_connect(const char *host, int port, Dbuf *reply) {
    char portstr[16];
    snprintf(portstr, sizeof(portstr), "%d", port);
    struct addrinfo hints, *res = NULL;
    memset(&hints, 0, sizeof(hints));
    hints.ai_family = AF_INET;
    hints.ai_socktype = SOCK_STREAM;
    if (getaddrinfo(host, portstr, &hints, &res) != 0 || !res) {
        dbuf_error(reply, "could not resolve replication host");
        if (res) freeaddrinfo(res);
        return -1;
    }
    int fd = socket(res->ai_family, res->ai_socktype, res->ai_protocol);
    if (fd < 0) {
        freeaddrinfo(res);
        dbuf_error(reply, "could not create replication socket");
        return -1;
    }
    set_nonblocking(fd);
    int rc = connect(fd, res->ai_addr, res->ai_addrlen);
    freeaddrinfo(res);
    if (rc < 0 && errno != EINPROGRESS) {
        close(fd);
        dbuf_error(reply, "could not connect to primary");
        return -1;
    }
    client_add(fd);
    /* client_add may have moved the array; find the new client by fd. */
    Client *link = NULL;
    for (size_t i = 0; i < g_nclients; i++)
        if (g_clients[i].fd == fd) { link = &g_clients[i]; break; }
    if (!link) {
        close(fd);
        dbuf_error(reply, "out of memory");
        return -1;
    }
    link->is_repl_link = 1;
    link->repl_syncing = 1; /* the dump that follows is the snapshot */
    if (rc == 0) {
        link->repl_connecting = 0;
        send_psync_request(link);
    } else {
        link->repl_connecting = 1;
    }
    return 0;
}

/* REPLICAOF host port | REPLICAOF NO ONE. Runs in the main loop (not
   dispatch): it creates a socket, which dispatch must never do, and
   it is never AOF-logged. NO ONE is promotion: the replica detaches
   from its primary, forgets the replication stream, and becomes a
   primary itself, keeping the data it replicated. */
static void handle_replicaof(char **argv, int argc, Dbuf *reply) {
    if (argc == 3 && strcasecmp(argv[1], "NO") == 0 &&
        strcasecmp(argv[2], "ONE") == 0) {
        repl_link_close();
        repl_forget_master();
        dbuf_simple(reply, "OK");
        return;
    }
    if (argc != 3) {
        dbuf_error(reply, "wrong number of arguments for 'replicaof' command");
        return;
    }
    long long port;
    if (!parse_ll(argv[2], &port) || port <= 0 || port > 65535) {
        dbuf_error(reply, "invalid port");
        return;
    }
    /* A new primary means the stored run id and offset are for a
       different replication stream; forget them. Reconnecting to the
       same primary keeps them, so PSYNC can ask to resume. */
    if (strcasecmp(argv[1], g_master_host) != 0 || port != g_master_port)
        repl_forget_master();
    repl_link_close(); /* drop any previous link first */
    if (replicaof_connect(argv[1], (int)port, reply) < 0) return;
    snprintf(g_master_host, sizeof(g_master_host), "%s", argv[1]);
    g_master_port = port;
    dbuf_simple(reply, "OK");
}

/* ---------- AOF replay ---------- */

static void replay_aof(const char *path) {
    FILE *f = fopen(path, "rb");
    if (!f) return;
    fseek(f, 0, SEEK_END);
    long size = ftell(f);
    fseek(f, 0, SEEK_SET);
    if (size <= 0) { fclose(f); return; }
    char *data = malloc((size_t)size);
    if (!data) { fclose(f); return; }
    if (fread(data, 1, (size_t)size, f) != (size_t)size) { free(data); fclose(f); return; }
    fclose(f);
    g_replaying = 1;
    size_t off = 0;
    long long loaded = 0;
    while (off < (size_t)size) {
        char **argv = NULL;
        int argc = 0;
        size_t consumed = 0;
        int rc = resp_parse_command(data + off, (size_t)size - off, &argv, &argc, &consumed);
        if (rc != 1) break;
        Dbuf reply = {0};
        dispatch(argv, argc, &reply);
        free(reply.data);
        for (int i = 0; i < argc; i++) free(argv[i]);
        free(argv);
        off += consumed;
        loaded++;
    }
    g_replaying = 0;
    free(data);
    printf("cinder: replayed %lld commands from AOF, %zu keys loaded\n",
           loaded, store_size(g_store));
}

/* 40 hex chars, like a Redis run id; fresh every process start, so a
   restarted server never claims to be the same replication run. */
static void gen_replid(char out[41]) {
    static const char hex[] = "0123456789abcdef";
    unsigned char buf[20];
    FILE *f = fopen("/dev/urandom", "rb");
    if (f) {
        size_t got = fread(buf, 1, sizeof(buf), f);
        fclose(f);
        if (got == sizeof(buf)) {
            for (int i = 0; i < 20; i++) {
                out[2 * i] = hex[buf[i] >> 4];
                out[2 * i + 1] = hex[buf[i] & 0xf];
            }
            out[40] = '\0';
            return;
        }
    }
    for (int i = 0; i < 20; i++) {
        unsigned v = (unsigned)rand();
        out[2 * i] = hex[(v >> 4) & 0xf];
        out[2 * i + 1] = hex[v & 0xf];
    }
    out[40] = '\0';
}

/* Execute a queued MULTI/EXEC transaction: run every queued command
   in order through the normal dispatcher, collecting each reply into
   one array. A mutating command is AOF-logged and forwarded to
   replicas exactly as if the client had sent it outside a
   transaction, so the AOF and the replication stream hold the
   individual commands, never MULTI/EXEC themselves. An error inside
   the transaction does not abort it: the error reply takes its place
   in the array and the rest still run, like real Redis. The whole
   array waits for the sync covering the last append it made, like
   any mutating reply. A QUIT queued in the transaction closes the
   client once the array is sent; a queued SHUTDOWN stops the server
   (via *shutdown_out). WATCH is optimistic locking: if any key the
   client watched changed since it was watched — created, modified,
   deleted, expired, or flushed away — EXEC aborts, answers a nil
   array, and the queue never runs. The watch set is cleared after
   EXEC either way, and after DISCARD, as in Redis. A queued UNWATCH
   runs at EXEC and clears the set, which is a no-op by then: the
   dirty check already ran. */
static void handle_exec(Client *c, int *shutdown_out) {
    Dbuf reply = {0};
    if (watch_dirty(c)) {
        /* Optimistic lock lost: the queue never runs, the client gets
           a nil multi-bulk reply, and the watch set is gone. */
        dbuf_null_array(&reply);
        multi_clear(c);
        watch_clear(c);
        c->in_multi = 0;
        client_reply(c, &reply, 0);
        free(reply.data);
        return;
    }
    dbuf_array_header(&reply, (long long)c->queue_len);
    unsigned long long last_seq = 0;
    int quit = 0;
    for (size_t i = 0; i < c->queue_len; i++) {
        QueuedCmd *q = &c->queue[i];
        Dbuf sub = {0};
        if (strcasecmp(q->argv[0], "UNWATCH") == 0) {
            /* UNWATCH queues like any other command inside MULTI (as
               in real Redis) and runs at EXEC: it clears the watch
               set and answers +OK, and is never AOF-logged. */
            watch_clear(c);
            dbuf_simple(&sub, "OK");
            dbuf_put(&reply, sub.data, sub.len);
            free(sub.data);
            continue;
        }
        int drc = dispatch(q->argv, q->argc, &sub);
        if (drc == 1 && !g_replaying) {
            last_seq = aof_append(q->argv, q->argc);
            Dbuf raw = {0};
            emit_resp_cmd(&raw, q->argv, q->argc);
            forward_to_replicas(raw.data, raw.len);
            free(raw.data);
        }
        dbuf_put(&reply, sub.data, sub.len);
        free(sub.data);
        if (drc == 2) quit = 1;
        if (drc == 3) *shutdown_out = 1;
    }
    multi_clear(c);
    watch_clear(c);
    c->in_multi = 0;
    client_reply(c, &reply, last_seq);
    free(reply.data);
    if (quit) {
        /* QUIT behind parked replies: wait for the syncs and flush
           the socket, or the client never sees the +OKs. */
        deferred_flush_client(c);
        c->dead = 1;
    }
}

/* ---------- main ---------- */

static void usage(const char *prog) {
    fprintf(stderr,
        "Usage: %s [--port N] [--data-dir DIR] [--max-entries N] [--aof-fsync POLICY]\n"
        "       [--repl-backlog-size N] [--threads N]\n"
        "  --port        TCP port to listen on (default 6379)\n"
        "  --data-dir    directory for the append-only log (default ./data)\n"
        "  --max-entries evict approximately-LRU keys past this many (default 0 = unlimited)\n"
        "  --aof-fsync   AOF fsync policy: always (default) or everysec\n"
        "  --repl-backlog-size bytes of recent replication stream kept for\n"
        "                partial resync (default 1048576, 0 disables it)\n"
        "  --threads     worker threads for background saves (default 1, 1..32)\n",
        prog);
}

int main(int argc, char **argv) {
    int port = 6379;
    const char *data_dir = "./data";
    static struct option long_opts[] = {
        {"port", required_argument, NULL, 'p'},
        {"data-dir", required_argument, NULL, 'd'},
        {"max-entries", required_argument, NULL, 'm'},
        {"aof-fsync", required_argument, NULL, 'f'},
        {"repl-backlog-size", required_argument, NULL, 'b'},
        {"threads", required_argument, NULL, 't'},
        {"help", no_argument, NULL, 'h'},
        {NULL, 0, NULL, 0},
    };
    int opt;
    while ((opt = getopt_long(argc, argv, "p:d:m:f:b:t:h", long_opts, NULL)) != -1) {
        switch (opt) {
        case 'p': port = atoi(optarg); break;
        case 'd': data_dir = optarg; break;
        case 'm': g_max_entries = (size_t)atoll(optarg); break;
        case 'f':
            if (aof_set_policy(optarg) < 0) {
                fprintf(stderr, "cinder: unknown AOF fsync policy %s (want always or everysec)\n",
                        optarg);
                return 1;
            }
            break;
        case 'b': {
            long long n = atoll(optarg);
            if (n < 0) {
                fprintf(stderr, "cinder: invalid replication backlog size %s\n", optarg);
                return 1;
            }
            g_backlog_cap = (size_t)n;
            break;
        }
        case 't': {
            long long n = atoll(optarg);
            if (n < 1 || n > 32) {
                fprintf(stderr, "cinder: invalid thread count %s (want 1..32)\n", optarg);
                return 1;
            }
            g_nthreads = (int)n;
            break;
        }
        case 'h': usage(argv[0]); return 0;
        default: usage(argv[0]); return 1;
        }
    }
    if (port <= 0 || port > 65535) {
        fprintf(stderr, "cinder: invalid port %d\n", port);
        return 1;
    }

    srand((unsigned)(now_ms() ^ (long long)getpid()));
    gen_replid(g_replid);
    /* The replication backlog is allocated once, up front; a failed
       allocation disables partial resync rather than failing the start. */
    if (g_backlog_cap > 0) {
        g_backlog = malloc(g_backlog_cap);
        if (!g_backlog) {
            fprintf(stderr,
                    "cinder: warning: cannot allocate %zu-byte replication backlog; partial resync disabled\n",
                    g_backlog_cap);
            g_backlog_cap = 0;
        }
    }
    g_store = store_new();
    if (!g_store) {
        fprintf(stderr, "cinder: out of memory\n");
        return 1;
    }
    g_start_ms = now_ms();

    /* The worker pool starts before the AOF replay: a background save
       is only ever requested by a live client, but the pool is part
       of the server's steady state, so it is up before any command
       runs. */
    g_pool = tp_create(g_nthreads);
    if (!g_pool) {
        fprintf(stderr, "cinder: cannot start thread pool (%d threads)\n", g_nthreads);
        return 1;
    }

    if (mkdir(data_dir, 0755) < 0 && errno != EEXIST) {
        fprintf(stderr, "cinder: cannot create data dir %s: %s\n", data_dir, strerror(errno));
        return 1;
    }
    char aof_path[1024];
    snprintf(aof_path, sizeof(aof_path), "%s/appendonly.aof", data_dir);
    snprintf(g_rdb_path, sizeof(g_rdb_path), "%s/dump.rdb", data_dir);
    snprintf(g_repl_state_path, sizeof(g_repl_state_path),
             "%s/replication.state", data_dir);
    if (aof_open(aof_path) < 0) {
        fprintf(stderr, "cinder: cannot open AOF %s: %s\n", aof_path, strerror(errno));
        return 1;
    }
    /* Load the snapshot first, then replay the AOF over it. The AOF
       holds the complete history, so replaying it after the snapshot
       reproduces the exact final state; a missing snapshot is normal
       (nothing saved yet), a corrupt one is only a warning. */
    errno = 0;
    long long rdb_keys = rdb_load(g_store, g_rdb_path);
    if (rdb_keys >= 0)
        printf("cinder: loaded %lld keys from snapshot %s\n", rdb_keys, g_rdb_path);
    else if (errno != ENOENT)
        fprintf(stderr, "cinder: warning: snapshot %s is corrupt (%s); continuing with AOF replay\n",
                g_rdb_path, strerror(errno));
    replay_aof(aof_path);
    /* A clean-shutdown replication state file resumes the stream
       position a replica had when it last stopped; see
       repl_state_load. */
    repl_state_load();

    signal(SIGINT, on_signal);
    signal(SIGTERM, on_signal);
    signal(SIGPIPE, SIG_IGN);

    int listener = socket(AF_INET, SOCK_STREAM, 0);
    if (listener < 0) {
        perror("socket");
        return 1;
    }
    int one = 1;
    setsockopt(listener, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));
    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_ANY);
    addr.sin_port = htons((uint16_t)port);
    if (bind(listener, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        perror("bind");
        return 1;
    }
    if (listen(listener, 128) < 0) {
        perror("listen");
        return 1;
    }
    set_nonblocking(listener);
    printf("cinder %s listening on port %d (data dir %s, max entries %zu, aof fsync %s, repl backlog %zu bytes, threads %d)\n",
           CINDER_VERSION, port, data_dir, g_max_entries, aof_policy_name(), g_backlog_cap, g_nthreads);
    fflush(stdout);

    /* The event loop is epoll-based: the listener and every client fd
       are registered once (client_add / client_remove), and each
       iteration syncs each client's interest (EPOLLOUT while it has
       output pending or a replication connect is completing) before
       waiting. Level-triggered, like the poll() loop it replaces, so a
       ready fd is reported on every wait until its condition clears.
       Unlike that loop, the ready set is fixed when epoll_wait returns,
       so a client added mid-iteration (REPLICAOF) cannot run past the
       end of the event array. */
    g_epollfd = epoll_create1(0);
    if (g_epollfd < 0) {
        perror("epoll_create1");
        return 1;
    }
    struct epoll_event lev;
    memset(&lev, 0, sizeof(lev));
    lev.events = EPOLLIN;
    lev.data.fd = listener;
    if (epoll_ctl(g_epollfd, EPOLL_CTL_ADD, listener, &lev) < 0) {
        perror("epoll_ctl listener");
        return 1;
    }
    /* The AOF flush thread wakes the loop through this fd after every
       completed sync, so a reply deferred on that sync goes out at
       once instead of waiting for the next epoll_wait timeout. */
    g_aof_wakeup_fd = aof_wakeup_fd();
    if (g_aof_wakeup_fd >= 0) {
        struct epoll_event wev;
        memset(&wev, 0, sizeof(wev));
        wev.events = EPOLLIN;
        wev.data.fd = g_aof_wakeup_fd;
        if (epoll_ctl(g_epollfd, EPOLL_CTL_ADD, g_aof_wakeup_fd, &wev) < 0)
            perror("epoll_ctl aof wakeup");
    }

    long long loop = 0;
    while (g_running) {
        /* Release replies whose sync landed (the backstop for the
           wakeup fd, which usually drains them as soon as the sync
           completes). This runs before the interest sync, so a reply
           released here is registered for EPOLLOUT in the same
           iteration and written without waiting out a timeout. */
        deferred_drain();
        for (size_t i = 0; i < g_nclients; i++)
            client_sync_interest(&g_clients[i]);
        struct epoll_event events[128];
        int nev = epoll_wait(g_epollfd, events, 128, 100);
        if (nev < 0) {
            if (errno == EINTR) continue;
            perror("epoll_wait");
            break;
        }
        for (int e = 0; e < nev; e++) {
            uint32_t re = events[e].events;
            int fd = events[e].data.fd;
            if (fd == listener) {
                if (re & EPOLLIN) {
                    for (;;) {
                        int cfd = accept(listener, NULL, NULL);
                        if (cfd < 0) break;
                        set_nonblocking(cfd);
                        client_add(cfd);
                    }
                }
                continue;
            }
            if (fd == g_aof_wakeup_fd) {
                /* A flush-thread sync landed: drain the wakeup pipe
                   and release the replies it covers. */
                uint64_t v;
                while (read(fd, &v, sizeof(v)) > 0) { }
                deferred_drain();
                continue;
            }
            /* Find the client by fd: the client array compacts on
               removal, so an index is never stable across the loop. */
            size_t idx = g_nclients; /* sentinel: already gone */
            for (size_t i = 0; i < g_nclients; i++)
                if (g_clients[i].fd == fd) { idx = i; break; }
            if (idx == g_nclients) continue;
            Client *c = &g_clients[idx];
            if (re & (EPOLLERR | EPOLLHUP)) { c->dead = 1; continue; }
            if (c->is_repl_link && c->repl_connecting && (re & EPOLLOUT)) {
                /* Non-blocking connect finished: check for an error,
                   then ask the primary for a full sync. */
                int err = 0;
                socklen_t elen = sizeof(err);
                if (getsockopt(c->fd, SOL_SOCKET, SO_ERROR, &err, &elen) < 0 ||
                    err != 0) {
                    fprintf(stderr, "cinder: replication link failed: %s\n",
                            strerror(err ? err : ECONNREFUSED));
                    c->dead = 1;
                    continue;
                }
                c->repl_connecting = 0;
                send_psync_request(c);
            }
            if (re & EPOLLOUT && c->out.len > 0) {
                ssize_t w = write(c->fd, c->out.data, c->out.len);
                if (w > 0) {
                    memmove(c->out.data, c->out.data + w, c->out.len - (size_t)w);
                    c->out.len -= (size_t)w;
                } else if (w < 0 && errno != EAGAIN && errno != EWOULDBLOCK) {
                    c->dead = 1;
                    continue;
                }
            }
            if (re & EPOLLIN) {
                ssize_t r = read(c->fd, c->inbuf + c->inlen, sizeof(c->inbuf) - c->inlen);
                if (r == 0) { c->dead = 1; continue; }
                if (r < 0) {
                    if (errno != EAGAIN && errno != EWOULDBLOCK) c->dead = 1;
                    continue;
                }
                c->inlen += (size_t)r;
                /* Execute every complete command in the buffer. */
                size_t off = 0;
                int shutdown = 0;
                while (off < c->inlen) {
                    /* The primary ends a full sync with a +FULLSYNC
                       marker line, which is not a command; skip it
                       (and any -ERR line) on the replication link.
                       Learn the primary's run id and stream offset
                       from +FULLRESYNC, and mark the end of the dump
                       with +FULLSYNC: dump bytes are the snapshot,
                       not the live stream, so they do not advance the
                       slave offset. */
                    if (c->is_repl_link &&
                        (c->inbuf[off] == '+' || c->inbuf[off] == '-')) {
                        size_t j = off;
                        int found = 0;
                        while (j + 1 < c->inlen) {
                            if (c->inbuf[j] == '\r' && c->inbuf[j + 1] == '\n') {
                                found = 1;
                                break;
                            }
                            j++;
                        }
                        if (!found) break; /* need more data */
                        if (c->inbuf[off] == '+') {
                            size_t linelen = j - off;
                            if (linelen >= 12 &&
                                memcmp(c->inbuf + off, "+FULLRESYNC ", 12) == 0 &&
                                linelen < 100) {
                                char line[100];
                                char rid[41];
                                long long o;
                                memcpy(line, c->inbuf + off, linelen);
                                line[linelen] = '\0';
                                if (sscanf(line + 12, "%40s %lld", rid, &o) == 2) {
                                    snprintf(g_primary_replid,
                                             sizeof(g_primary_replid), "%s", rid);
                                    g_slave_repl_offset = o;
                                    c->repl_syncing = 1;
                                }
                            } else if (linelen >= 10 &&
                                       memcmp(c->inbuf + off, "+CONTINUE ", 10) == 0 &&
                                       linelen < 100) {
                                /* Partial resync: the stream resumes
                                   at the offset we asked for, so the
                                   bytes that follow are live stream,
                                   not a dump. Take the offset from
                                   the line and stay out of syncing
                                   mode, so each applied command
                                   advances the slave offset. */
                                char line[100];
                                char rid[41];
                                long long o;
                                memcpy(line, c->inbuf + off, linelen);
                                line[linelen] = '\0';
                                if (sscanf(line + 10, "%40s %lld", rid, &o) == 2) {
                                    snprintf(g_primary_replid,
                                             sizeof(g_primary_replid), "%s", rid);
                                    g_slave_repl_offset = o;
                                }
                                c->repl_syncing = 0;
                            } else if (linelen >= 9 &&
                                       memcmp(c->inbuf + off, "+FULLSYNC", 9) == 0) {
                                c->repl_syncing = 0;
                            }
                        }
                        off = j + 2;
                        continue;
                    }
                    char **cargv = NULL;
                    int cargc = 0;
                    size_t consumed = 0;
                    int prc = resp_parse_command(c->inbuf + off, c->inlen - off,
                                                 &cargv, &cargc, &consumed);
                    if (prc == 0) break; /* need more data */
                    if (prc < 0) {
                        dbuf_error(&c->out, "Protocol error");
                        c->dead = 1;
                        break;
                    }
                    g_commands_processed++;
                    const char *raw = c->inbuf + off;
                    size_t rawlen = consumed;
                    if (c->is_repl_link) {
                        /* A command streamed from the primary: apply
                           it, log it to our own AOF, and forward it
                           to our own replicas. The reply is discarded;
                           the primary never reads it. QUIT/SHUTDOWN
                           from a primary are ignored. The slave offset
                           is a stream position, so it advances for
                           every byte received on the link, even a
                           command that turns out to be a no-op locally
                           (a DEL of an already-expired key, say):
                           otherwise the offset would lag the stream
                           and a later PSYNC would replay commands
                           already applied. Dump bytes are the
                           exception: while repl_syncing they are the
                           snapshot, not the stream. */
                        Dbuf reply = {0};
                        int drc = dispatch(cargv, cargc, &reply);
                        free(reply.data);
                        if (!c->repl_syncing)
                            g_slave_repl_offset += (long long)rawlen;
                        if (drc == 1) {
                            aof_append(cargv, cargc);
                            forward_to_replicas(raw, rawlen);
                        }
                    } else if (c->in_multi &&
                               strcasecmp(cargv[0], "MULTI") != 0 &&
                               strcasecmp(cargv[0], "EXEC") != 0 &&
                               strcasecmp(cargv[0], "DISCARD") != 0 &&
                               strcasecmp(cargv[0], "WATCH") != 0) {
                        /* Inside MULTI, every other command queues
                           instead of executing: the client gets
                           +QUEUED and the command runs at EXEC, in
                           order, atomically with the rest. WATCH is
                           not queued: it runs at once and errors
                           inside MULTI, as in Redis. UNWATCH does
                           queue, as in Redis, and runs at EXEC. */
                        Dbuf reply = {0};
                        if (multi_queue(c, cargv, cargc) < 0)
                            dbuf_error(&reply, "ERR out of memory");
                        else
                            dbuf_simple(&reply, "QUEUED");
                        client_reply(c, &reply, 0);
                        free(reply.data);
                    } else if (strcasecmp(cargv[0], "SYNC") == 0 && cargc == 1) {
                        /* A replica asking for a full sync. */
                        c->is_replica = 1;
                        send_full_sync(c);
                    } else if (strcasecmp(cargv[0], "PSYNC") == 0 && cargc == 3) {
                        /* A replica asking to resume from an offset.
                           Same run id and an offset still in the
                           backlog: partial resync, stream only the
                           missed bytes. Anything else: full resync. */
                        long long want = -1;
                        parse_ll(cargv[2], &want);
                        c->is_replica = 1;
                        if (strcasecmp(cargv[1], g_replid) == 0 &&
                            backlog_contains(want)) {
                            dbuf_putf(&c->out, "+CONTINUE %s %lld\r\n",
                                      g_replid, want);
                            backlog_send(c, want);
                        } else {
                            dbuf_putf(&c->out, "+FULLRESYNC %s %lld\r\n",
                                      g_replid, g_master_repl_offset);
                            send_full_sync(c);
                        }
                    } else if (strcasecmp(cargv[0], "REPLICAOF") == 0) {
                        Dbuf reply = {0};
                        handle_replicaof(cargv, cargc, &reply);
                        client_reply(c, &reply, 0);
                        free(reply.data);
                        /* handle_replicaof may add a client, which
                           can move the client array. */
                        c = &g_clients[idx];
                    } else if (c->is_replica) {
                        /* A replica connection is read-only; it only
                           ever sends PING to check the link. */
                        if (strcasecmp(cargv[0], "PING") == 0) {
                            Dbuf reply = {0};
                            dispatch(cargv, cargc, &reply);
                            client_reply(c, &reply, 0);
                            free(reply.data);
                        } else {
                            Dbuf reply = {0};
                            dbuf_error(&reply, "replica connection is read-only");
                            client_reply(c, &reply, 0);
                            free(reply.data);
                        }
                    } else {
                        /* MULTI/EXEC/DISCARD/WATCH/UNWATCH are transaction
                           control, handled here rather than in dispatch:
                           they are per-client state, not store
                           operations, and they are never AOF-logged or
                           replicated — only the commands EXEC runs are. */
                        if (strcasecmp(cargv[0], "MULTI") == 0) {
                            Dbuf reply = {0};
                            if (cargc != 1)
                                dbuf_error(&reply, "wrong number of arguments for 'multi' command");
                            else if (c->in_multi)
                                dbuf_error(&reply, "MULTI calls can not be nested");
                            else {
                                c->in_multi = 1;
                                dbuf_simple(&reply, "OK");
                            }
                            client_reply(c, &reply, 0);
                            free(reply.data);
                        } else if (strcasecmp(cargv[0], "EXEC") == 0) {
                            if (cargc != 1) {
                                Dbuf reply = {0};
                                dbuf_error(&reply, "wrong number of arguments for 'exec' command");
                                client_reply(c, &reply, 0);
                                free(reply.data);
                            } else if (!c->in_multi) {
                                Dbuf reply = {0};
                                dbuf_error(&reply, "EXEC without MULTI");
                                client_reply(c, &reply, 0);
                                free(reply.data);
                            } else {
                                handle_exec(c, &shutdown);
                            }
                        } else if (strcasecmp(cargv[0], "DISCARD") == 0) {
                            Dbuf reply = {0};
                            if (cargc != 1)
                                dbuf_error(&reply, "wrong number of arguments for 'discard' command");
                            else if (!c->in_multi)
                                dbuf_error(&reply, "DISCARD without MULTI");
                            else {
                                multi_clear(c);
                                watch_clear(c);
                                c->in_multi = 0;
                                dbuf_simple(&reply, "OK");
                            }
                            client_reply(c, &reply, 0);
                            free(reply.data);
                        } else if (strcasecmp(cargv[0], "WATCH") == 0) {
                            Dbuf reply = {0};
                            if (cargc < 2)
                                dbuf_error(&reply, "wrong number of arguments for 'watch' command");
                            else if (c->in_multi)
                                dbuf_error(&reply, "WATCH inside MULTI is not allowed");
                            else {
                                int bad = 0;
                                for (int k = 1; k < cargc; k++) {
                                    if (watch_add(c, cargv[k]) < 0) {
                                        bad = 1;
                                        break;
                                    }
                                }
                                if (bad)
                                    dbuf_error(&reply, "ERR out of memory");
                                else
                                    dbuf_simple(&reply, "OK");
                            }
                            client_reply(c, &reply, 0);
                            free(reply.data);
                        } else if (strcasecmp(cargv[0], "UNWATCH") == 0) {
                            Dbuf reply = {0};
                            if (cargc != 1)
                                dbuf_error(&reply, "wrong number of arguments for 'unwatch' command");
                            else {
                                watch_clear(c);
                                dbuf_simple(&reply, "OK");
                            }
                            client_reply(c, &reply, 0);
                            free(reply.data);
                        } else {
                            Dbuf reply = {0};
                            int drc = dispatch(cargv, cargc, &reply);
                            unsigned long long aof_seq = 0;
                            if (drc == 1 && !g_replaying) {
                                aof_seq = aof_append(cargv, cargc);
                                forward_to_replicas(raw, rawlen);
                            }
                            /* Under `always` a mutating reply parks until
                               the flush thread's sync covers its append;
                               client_reply also parks later replies behind
                               it, so per-client order never breaks. */
                            client_reply(c, &reply, aof_seq);
                            free(reply.data);
                            if (drc == 2) {
                                /* QUIT behind parked replies: wait for the
                                   syncs and flush the socket, or the
                                   client never sees the +OKs. */
                                deferred_flush_client(c);
                                c->dead = 1;
                            }
                            if (drc == 3) shutdown = 1;
                        }
                    }
                    for (int k = 0; k < cargc; k++) free(cargv[k]);
                    free(cargv);
                    off += consumed;
                    if (c->dead || shutdown) break;
                }
                if (off > 0) {
                    memmove(c->inbuf, c->inbuf + off, c->inlen - off);
                    c->inlen -= off;
                }
                if (shutdown) g_running = 0;
            }
        }
        /* Sweep dead clients. */
        for (size_t i = 0; i < g_nclients;) {
            if (g_clients[i].dead) client_remove(i);
            else i++;
        }
        /* everysec AOF policy: flush pending writes once a second. */
        aof_tick();
        /* `always` AOF policy: wake the flush thread for the appends
           made this iteration, now that the burst is fully appended. */
        aof_kick();
        if ((++loop % 100) == 0) store_expire_scan(g_store, 20);
    }

    printf("\ncinder: shutting down, %zu keys in memory\n", store_size(g_store));
    /* Wait for the pool to drain before the store is freed and the
       process exits, so a background save that already started
       always lands its snapshot. */
    tp_shutdown(g_pool);
    g_pool = NULL;
    /* Persist the replication stream position, if this node is a
       replica, so a clean restart can resume from it. */
    repl_state_save();
    /* Deferred `always`-policy replies: wait for the syncs covering
       them and deliver them into the clients' out buffers before
       flushing the sockets below, or a SHUTDOWN +OK parked behind a
       write would never reach its client. */
    aof_fsync();
    deferred_drain();
    /* Flush any pending replies (notably the SHUTDOWN +OK) before
       closing client sockets: switch them back to blocking briefly. */
    for (size_t i = 0; i < g_nclients; i++) {
        Client *c = &g_clients[i];
        if (c->out.len > 0) {
            int flags = fcntl(c->fd, F_GETFL, 0);
            if (flags >= 0) fcntl(c->fd, F_SETFL, flags & ~O_NONBLOCK);
            size_t off = 0;
            while (off < c->out.len) {
                ssize_t w = write(c->fd, c->out.data + off, c->out.len - off);
                if (w <= 0) break;
                off += (size_t)w;
            }
        }
    }
    for (size_t i = 0; i < g_nclients; i++) {
        close(g_clients[i].fd);
        free(g_clients[i].out.data);
    }
    free(g_clients);
    free(g_backlog);
    close(g_epollfd);
    close(listener);
    aof_close();
    store_free(g_store);
    return 0;
}
