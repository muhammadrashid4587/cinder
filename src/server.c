/* cinder: a Redis-compatible in-memory key-value store in C.
 *
 * Single-threaded, poll()-based event loop. String values with TTLs,
 * approximate LRU eviction, and an append-only file for durability.
 * Speaks RESP so real redis clients (redis-cli, redis-benchmark, and
 * any RESP client library) work against it.
 */
#define _GNU_SOURCE
#include <arpa/inet.h>
#include <errno.h>
#include <fcntl.h>
#include <getopt.h>
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
#include "resp.h"
#include "store.h"

#define CINDER_VERSION "0.1.0"
#define INBUF_CAP 65536

static Store *g_store;
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
        dbuf_bulk(reply, info.data ? info.data : "", info.len);
        free(info.data);
        return 0;
    }
    if (strcasecmp(cmd, "COMMAND") == 0) {
        static const char *names[] = {"PING", "ECHO", "QUIT", "SHUTDOWN", "SET", "GET",
            "DEL", "EXISTS", "EXPIRE", "PEXPIRE", "TTL", "PTTL", "INCR", "DECR",
            "INCRBY", "DECRBY", "KEYS", "FLUSHALL", "DBSIZE", "INFO", "COMMAND", "SAVE"};
        dbuf_array_header(reply, (long long)(sizeof(names) / sizeof(names[0])));
        for (size_t i = 0; i < sizeof(names) / sizeof(names[0]); i++)
            dbuf_bulk(reply, names[i], strlen(names[i]));
        return 0;
    }
    if (strcasecmp(cmd, "SAVE") == 0) {
        /* AOF is always on, so SAVE just forces the log to disk. */
        dbuf_simple(reply, "OK");
        return 0;
    }
    dbuf_error(reply, "unknown command");
    return 0;
}

/* ---------- clients ---------- */

typedef struct {
    int fd;
    char inbuf[INBUF_CAP];
    size_t inlen;
    Dbuf out;
    int dead;
} Client;

static Client *g_clients;
static size_t g_nclients, g_clients_cap;

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
        "Usage: %s [--port N] [--data-dir DIR] [--max-entries N]\n"
        "  --port        TCP port to listen on (default 6379)\n"
        "  --data-dir    directory for the append-only log (default ./data)\n"
        "  --max-entries evict approximately-LRU keys past this many (default 0 = unlimited)\n",
        prog);
}

int main(int argc, char **argv) {
    int port = 6379;
    const char *data_dir = "./data";
    static struct option long_opts[] = {
        {"port", required_argument, NULL, 'p'},
        {"data-dir", required_argument, NULL, 'd'},
        {"max-entries", required_argument, NULL, 'm'},
        {"help", no_argument, NULL, 'h'},
        {NULL, 0, NULL, 0},
    };
    int opt;
    while ((opt = getopt_long(argc, argv, "p:d:m:h", long_opts, NULL)) != -1) {
        switch (opt) {
        case 'p': port = atoi(optarg); break;
        case 'd': data_dir = optarg; break;
        case 'm': g_max_entries = (size_t)atoll(optarg); break;
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
    if (aof_open(aof_path) < 0) {
        fprintf(stderr, "cinder: cannot open AOF %s: %s\n", aof_path, strerror(errno));
        return 1;
    }
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
    printf("cinder %s listening on port %d (data dir %s, max entries %zu)\n",
           CINDER_VERSION, port, data_dir, g_max_entries);
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
            if (g_clients[i].out.len > 0) pfds[i + 1].events |= POLLOUT;
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
                    Dbuf reply = {0};
                    int drc = dispatch(cargv, cargc, &reply);
                    if (reply.len > 0) dbuf_put(&c->out, reply.data, reply.len);
                    free(reply.data);
                    if (drc == 1 && !g_replaying) aof_append(cargv, cargc);
                    if (drc == 2) c->dead = 1;
                    if (drc == 3) shutdown = 1;
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
