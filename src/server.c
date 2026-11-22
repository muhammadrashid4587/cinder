/* cinder: a Redis-compatible in-memory key-value store in C.
 *
 * Single-threaded, poll()-based event loop. String values with TTLs,
 * approximate LRU eviction, an append-only file for durability, and
 * RDB-style snapshots (SAVE) that are reloaded on startup.
 * Speaks RESP so real redis clients (redis-cli, redis-benchmark, and
 * any RESP client library) work against it.
 */
#define _GNU_SOURCE
#include <arpa/inet.h>
#include <errno.h>
#include <fcntl.h>
#include <getopt.h>
#include <netdb.h>
#include <netinet/in.h>
#include <poll.h>
#include <signal.h>
#include <stdarg.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include "aof.h"
#include "rdb.h"
#include "resp.h"
#include "store.h"

#define CINDER_VERSION "0.7.0"
#define INBUF_CAP 65536

static Store *g_store;
static char g_rdb_path[1024];
static long long g_start_ms;
static long long g_commands_processed;
static size_t g_max_entries; /* 0 = unlimited */
static int g_replaying;
static volatile sig_atomic_t g_running = 1;

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

/* ---------- command dispatch ----------
 * Returns: 0 = ok, 1 = mutated state (log to AOF), 2 = close connection,
 *          3 = shut the server down. */

/* Replication state lives in the client table (defined below); these
   helpers are declared here so INFO can report the role and the
   replica count. */
typedef struct Client Client;
static Client *g_clients;
static size_t g_nclients;
static int repl_is_replica(void);
static size_t repl_connected_replicas(void);

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
    if (strcasecmp(cmd, "GET") == 0) {
        if (argc != 2) return dbuf_error(reply, "wrong number of arguments for 'get' command"), 0;
        if (store_type(g_store, argv[1]) > 1)
            return dbuf_error(reply, "WRONGTYPE Operation against a key holding the wrong kind of value"), 0;
        char *v = store_get(g_store, argv[1]);
        if (!v) return dbuf_null(reply), 0;
        dbuf_bulk(reply, v, strlen(v));
        free(v);
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
        if (store_type(g_store, argv[1]) > 1)
            return dbuf_error(reply, "WRONGTYPE Operation against a key holding the wrong kind of value"), 0;
        char *cur = store_get(g_store, argv[1]);
        long long v = 0;
        if (cur) {
            if (!parse_ll(cur, &v)) {
                free(cur);
                return dbuf_error(reply, "value is not an integer or out of range"), 0;
            }
            free(cur);
        }
        v += delta;
        char vbuf[32];
        snprintf(vbuf, sizeof(vbuf), "%lld", v);
        long long exp = -1;
        /* INCR preserves the key's TTL. */
        {
            long long ttl = store_ttl_ms(g_store, argv[1]);
            if (ttl > 0) exp = now_ms() + ttl;
        }
        if (!store_set(g_store, argv[1], vbuf, exp))
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
    if (strcasecmp(cmd, "LPUSH") == 0 || strcasecmp(cmd, "RPUSH") == 0) {
        if (argc < 3) return dbuf_error(reply, "wrong number of arguments for 'lpush' command"), 0;
        int at_head = strcasecmp(cmd, "LPUSH") == 0;
        int t = store_type(g_store, argv[1]);
        if (t != 0 && t != 2)
            return dbuf_error(reply, "WRONGTYPE Operation against a key holding the wrong kind of value"), 0;
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
        dbuf_putf(&info, "# Keyspace\r\n");
        dbuf_putf(&info, "keys:%zu\r\n", store_size(g_store));
        dbuf_putf(&info, "max_entries:%zu\r\n", g_max_entries);
        dbuf_putf(&info, "aof_fsync_policy:%s\r\n", aof_policy_name());
        dbuf_putf(&info, "# Replication\r\n");
        dbuf_putf(&info, "role:%s\r\n", repl_is_replica() ? "replica" : "primary");
        dbuf_putf(&info, "connected_replicas:%zu\r\n", repl_connected_replicas());
        dbuf_bulk(reply, info.data ? info.data : "", info.len);
        free(info.data);
        return 0;
    }
    if (strcasecmp(cmd, "COMMAND") == 0) {
        static const char *names[] = {"PING", "ECHO", "QUIT", "SHUTDOWN", "SET", "GET",
            "DEL", "EXISTS", "EXPIRE", "PEXPIRE", "TTL", "PTTL", "INCR", "DECR",
            "INCRBY", "DECRBY", "LPUSH", "RPUSH", "LPOP", "RPOP", "LLEN", "LRANGE",
            "SADD", "SREM", "SMEMBERS", "SISMEMBER", "SCARD",
            "HSET", "HGET", "HDEL", "HEXISTS", "HLEN", "HGETALL",
            "KEYS", "FLUSHALL", "DBSIZE", "INFO", "COMMAND", "SAVE",
            "SYNC", "REPLICAOF"};
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
    dbuf_error(reply, "unknown command");
    return 0;
}

/* ---------- clients ---------- */

typedef struct Client {
    int fd;
    char inbuf[INBUF_CAP];
    size_t inlen;
    Dbuf out;
    int dead;
    int is_replica;     /* primary side: this client is a replica (SYNC'd);
                           only PING is accepted from it, writes are
                           streamed to it */
    int is_repl_link;   /* replica side: this socket is the outbound link
                           to our primary; commands on it are applied */
    int repl_connecting; /* outbound link still completing its connect */
} Client;

static size_t g_clients_cap;

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
}

static void client_remove(size_t idx) {
    close(g_clients[idx].fd);
    free(g_clients[idx].out.data);
    g_clients[idx] = g_clients[--g_nclients];
}

static int set_nonblocking(int fd) {
    int flags = fcntl(fd, F_GETFL, 0);
    if (flags < 0) return -1;
    return fcntl(fd, F_SETFL, flags | O_NONBLOCK);
}

/* ---------- replication ----------
 * Primary side: a client sends SYNC, is marked a replica, and gets a
 * full sync (FLUSHALL plus one rebuild command per key, as RESP
 * arrays) followed by a +FULLSYNC marker. After that, every mutating
 * command from any other client is forwarded to it verbatim.
 * Replica side: REPLICAOF host port opens a non-blocking connection
 * to the primary, sends SYNC, and applies every command arriving on
 * that link through the normal dispatcher (and its own AOF, so the
 * replicated data is durable). REPLICAOF NO ONE drops the link.
 * A replica can itself accept SYNC and forward what it applies, so
 * replicas chain; a command is never sent back down the link it
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

/* Forward one raw command (as received, RESP array or inline) to
   every connected replica. */
static void forward_to_replicas(const char *raw, size_t rawlen) {
    for (size_t i = 0; i < g_nclients; i++)
        if (g_clients[i].is_replica) dbuf_put(&g_clients[i].out, raw, rawlen);
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
        if (ttl > 0) {
            snprintf(ttlbuf, sizeof(ttlbuf), "%lld", ttl);
            char *av[] = {"SET", (char *)e->key, (char *)e->value, "PX", ttlbuf};
            emit_resp_cmd(out, av, 5);
        } else {
            char *av[] = {"SET", (char *)e->key, (char *)e->value};
            emit_resp_cmd(out, av, 3);
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

/* Open the outbound link to a primary. On success the socket is a
   client with is_repl_link set; SYNC is sent as soon as the connect
   completes (immediately for localhost, on POLLOUT otherwise). */
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
    if (rc == 0) {
        link->repl_connecting = 0;
        dbuf_put(&link->out, "SYNC\r\n", 6);
    } else {
        link->repl_connecting = 1;
    }
    return 0;
}

/* REPLICAOF host port | REPLICAOF NO ONE. Runs in the main loop (not
   dispatch): it creates a socket, which dispatch must never do, and
   it is never AOF-logged. */
static void handle_replicaof(char **argv, int argc, Dbuf *reply) {
    if (argc == 3 && strcasecmp(argv[1], "NO") == 0 &&
        strcasecmp(argv[2], "ONE") == 0) {
        repl_link_close();
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
    repl_link_close(); /* drop any previous link first */
    if (replicaof_connect(argv[1], (int)port, reply) < 0) return;
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

/* ---------- main ---------- */

static void usage(const char *prog) {
    fprintf(stderr,
        "Usage: %s [--port N] [--data-dir DIR] [--max-entries N] [--aof-fsync POLICY]\n"
        "  --port        TCP port to listen on (default 6379)\n"
        "  --data-dir    directory for the append-only log (default ./data)\n"
        "  --max-entries evict approximately-LRU keys past this many (default 0 = unlimited)\n"
        "  --aof-fsync   AOF fsync policy: always (default) or everysec\n",
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
        {"help", no_argument, NULL, 'h'},
        {NULL, 0, NULL, 0},
    };
    int opt;
    while ((opt = getopt_long(argc, argv, "p:d:m:f:h", long_opts, NULL)) != -1) {
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
        case 'h': usage(argv[0]); return 0;
        default: usage(argv[0]); return 1;
        }
    }
    if (port <= 0 || port > 65535) {
        fprintf(stderr, "cinder: invalid port %d\n", port);
        return 1;
    }

    srand((unsigned)(now_ms() ^ (long long)getpid()));
    g_store = store_new();
    if (!g_store) {
        fprintf(stderr, "cinder: out of memory\n");
        return 1;
    }
    g_start_ms = now_ms();

    if (mkdir(data_dir, 0755) < 0 && errno != EEXIST) {
        fprintf(stderr, "cinder: cannot create data dir %s: %s\n", data_dir, strerror(errno));
        return 1;
    }
    char aof_path[1024];
    snprintf(aof_path, sizeof(aof_path), "%s/appendonly.aof", data_dir);
    snprintf(g_rdb_path, sizeof(g_rdb_path), "%s/dump.rdb", data_dir);
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
    printf("cinder %s listening on port %d (data dir %s, max entries %zu, aof fsync %s)\n",
           CINDER_VERSION, port, data_dir, g_max_entries, aof_policy_name());
    fflush(stdout);

    long long loop = 0;
    while (g_running) {
        size_t nfds = g_nclients + 1;
        struct pollfd *pfds = malloc(nfds * sizeof(struct pollfd));
        if (!pfds) break;
        pfds[0].fd = listener;
        pfds[0].events = POLLIN;
        pfds[0].revents = 0;
        for (size_t i = 0; i < g_nclients; i++) {
            pfds[i + 1].fd = g_clients[i].fd;
            pfds[i + 1].events = POLLIN;
            /* A link still completing its connect needs POLLOUT so
               the completion is reported. */
            if (g_clients[i].out.len > 0 || g_clients[i].repl_connecting)
                pfds[i + 1].events |= POLLOUT;
            pfds[i + 1].revents = 0;
        }
        int rc = poll(pfds, (nfds_t)nfds, 100);
        if (rc < 0) {
            if (errno == EINTR) { free(pfds); continue; }
            perror("poll");
            free(pfds);
            break;
        }
        if (pfds[0].revents & POLLIN) {
            for (;;) {
                int cfd = accept(listener, NULL, NULL);
                if (cfd < 0) break;
                set_nonblocking(cfd);
                client_add(cfd);
            }
        }
        for (size_t i = 0; i < g_nclients; i++) {
            Client *c = &g_clients[i];
            short re = pfds[i + 1].revents;
            if (re & (POLLERR | POLLHUP | POLLNVAL)) { c->dead = 1; continue; }
            if (c->is_repl_link && c->repl_connecting && (re & POLLOUT)) {
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
                dbuf_put(&c->out, "SYNC\r\n", 6);
            }
            if (re & POLLOUT && c->out.len > 0) {
                ssize_t w = write(c->fd, c->out.data, c->out.len);
                if (w > 0) {
                    memmove(c->out.data, c->out.data + w, c->out.len - (size_t)w);
                    c->out.len -= (size_t)w;
                } else if (w < 0 && errno != EAGAIN && errno != EWOULDBLOCK) {
                    c->dead = 1;
                    continue;
                }
            }
            if (re & POLLIN) {
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
                       (and any -ERR line) on the replication link. */
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
                           from a primary are ignored. */
                        Dbuf reply = {0};
                        int drc = dispatch(cargv, cargc, &reply);
                        free(reply.data);
                        if (drc == 1) {
                            aof_append(cargv, cargc);
                            forward_to_replicas(raw, rawlen);
                        }
                    } else if (strcasecmp(cargv[0], "SYNC") == 0 && cargc == 1) {
                        /* A replica asking for a full sync. */
                        c->is_replica = 1;
                        send_full_sync(c);
                    } else if (strcasecmp(cargv[0], "REPLICAOF") == 0) {
                        Dbuf reply = {0};
                        handle_replicaof(cargv, cargc, &reply);
                        if (reply.len > 0) dbuf_put(&c->out, reply.data, reply.len);
                        free(reply.data);
                        /* handle_replicaof may add a client, which
                           can move the client array. */
                        c = &g_clients[i];
                    } else if (c->is_replica) {
                        /* A replica connection is read-only; it only
                           ever sends PING to check the link. */
                        if (strcasecmp(cargv[0], "PING") == 0) {
                            Dbuf reply = {0};
                            dispatch(cargv, cargc, &reply);
                            if (reply.len > 0) dbuf_put(&c->out, reply.data, reply.len);
                            free(reply.data);
                        } else {
                            dbuf_error(&c->out, "replica connection is read-only");
                        }
                    } else {
                        Dbuf reply = {0};
                        int drc = dispatch(cargv, cargc, &reply);
                        if (reply.len > 0) dbuf_put(&c->out, reply.data, reply.len);
                        free(reply.data);
                        if (drc == 1 && !g_replaying) {
                            aof_append(cargv, cargc);
                            forward_to_replicas(raw, rawlen);
                        }
                        if (drc == 2) c->dead = 1;
                        if (drc == 3) shutdown = 1;
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
        free(pfds);
        /* everysec AOF policy: flush pending writes once a second. */
        aof_tick();
        if ((++loop % 100) == 0) store_expire_scan(g_store, 20);
    }

    printf("\ncinder: shutting down, %zu keys in memory\n", store_size(g_store));
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
    close(listener);
    aof_close();
    store_free(g_store);
    return 0;
}
