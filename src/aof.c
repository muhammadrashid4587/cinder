#define _GNU_SOURCE
#include "aof.h"
#include "resp.h"

#include <fcntl.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

static int aof_fd = -1;
static AofFsyncPolicy aof_policy = AOF_FSYNC_ALWAYS;
/* Monotonic ms of the last fdatasync; 0 = none yet. */
static long long aof_last_fsync_ms;
static int aof_dirty; /* writes since the last fdatasync */

static long long mono_ms(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (long long)ts.tv_sec * 1000 + ts.tv_nsec / 1000000;
}

int aof_set_policy(const char *name) {
    if (strcmp(name, "always") == 0) {
        aof_policy = AOF_FSYNC_ALWAYS;
        return 0;
    }
    if (strcmp(name, "everysec") == 0) {
        aof_policy = AOF_FSYNC_EVERYSEC;
        return 0;
    }
    return -1;
}

const char *aof_policy_name(void) {
    return aof_policy == AOF_FSYNC_ALWAYS ? "always" : "everysec";
}

int aof_open(const char *path) {
    aof_fd = open(path, O_WRONLY | O_CREAT | O_APPEND, 0644);
    return aof_fd < 0 ? -1 : 0;
}

void aof_append(char **argv, int argc) {
    if (aof_fd < 0 || argc <= 0) return;
    /* Size the buffer exactly: "*N\r\n" plus, per arg, "$len\r\n" + arg + "\r\n". */
    size_t total = 32;
    for (int i = 0; i < argc; i++)
        total += 32 + strlen(argv[i]) + 2;
    char *buf = malloc(total);
    if (!buf) return;
    size_t len = 0;
    if (resp_put_array_header(buf, total, &len, argc) < 0) { free(buf); return; }
    for (int i = 0; i < argc; i++) {
        if (resp_put_bulk(buf, total, &len, argv[i], strlen(argv[i])) < 0) {
            free(buf);
            return;
        }
    }
    size_t off = 0;
    while (off < len) {
        ssize_t w = write(aof_fd, buf + off, len - off);
        if (w <= 0) break;
        off += (size_t)w;
    }
    free(buf);
    if (aof_policy == AOF_FSYNC_ALWAYS) {
        fdatasync(aof_fd);
        aof_last_fsync_ms = mono_ms();
        aof_dirty = 0;
    } else {
        /* everysec: sync now if a second has passed since the last one,
           otherwise just remember there is unsynced data; aof_tick()
           flushes it when the second is up. */
        long long now = mono_ms();
        if (now - aof_last_fsync_ms >= 1000) {
            fdatasync(aof_fd);
            aof_last_fsync_ms = now;
            aof_dirty = 0;
        } else {
            aof_dirty = 1;
        }
    }
}

void aof_fsync(void) {
    if (aof_fd < 0) return;
    fdatasync(aof_fd);
    aof_last_fsync_ms = mono_ms();
    aof_dirty = 0;
}

void aof_tick(void) {
    if (aof_policy != AOF_FSYNC_EVERYSEC || !aof_dirty) return;
    long long now = mono_ms();
    if (now - aof_last_fsync_ms >= 1000) {
        fdatasync(aof_fd);
        aof_last_fsync_ms = now;
        aof_dirty = 0;
    }
}

void aof_close(void) {
    if (aof_fd >= 0) {
        /* Flush pending everysec data so a clean shutdown loses nothing. */
        aof_fsync();
        close(aof_fd);
        aof_fd = -1;
    }
}
