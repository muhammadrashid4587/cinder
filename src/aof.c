#define _GNU_SOURCE
#include "aof.h"
#include "resp.h"

#include <fcntl.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

static int aof_fd = -1;

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
    /* fsync policy: always. One fdatasync per mutating command is the
       honest durability story for v1; an everysec policy is a roadmap item. */
    fdatasync(aof_fd);
    free(buf);
}

void aof_close(void) {
    if (aof_fd >= 0) {
        close(aof_fd);
        aof_fd = -1;
    }
}
