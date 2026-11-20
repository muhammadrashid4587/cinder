#include "resp.h"

#include <ctype.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static int put_bytes(char *out, size_t cap, size_t *len, const char *src, size_t n) {
    if (*len + n > cap) return -1;
    memcpy(out + *len, src, n);
    *len += n;
    return 0;
}

static int put_str(char *out, size_t cap, size_t *len, const char *s) {
    return put_bytes(out, cap, len, s, strlen(s));
}

int resp_put_simple(char *out, size_t cap, size_t *len, const char *msg) {
    if (put_str(out, cap, len, "+") < 0) return -1;
    if (put_str(out, cap, len, msg) < 0) return -1;
    return put_str(out, cap, len, "\r\n");
}

int resp_put_error(char *out, size_t cap, size_t *len, const char *msg) {
    if (put_str(out, cap, len, "-ERR ") < 0) return -1;
    if (put_str(out, cap, len, msg) < 0) return -1;
    return put_str(out, cap, len, "\r\n");
}

int resp_put_int(char *out, size_t cap, size_t *len, long long v) {
    char tmp[32];
    int n = snprintf(tmp, sizeof(tmp), ":%lld\r\n", v);
    if (n < 0) return -1;
    return put_bytes(out, cap, len, tmp, (size_t)n);
}

int resp_put_bulk(char *out, size_t cap, size_t *len, const char *str, size_t slen) {
    char tmp[32];
    int n = snprintf(tmp, sizeof(tmp), "$%zu\r\n", slen);
    if (n < 0) return -1;
    if (put_bytes(out, cap, len, tmp, (size_t)n) < 0) return -1;
    if (put_bytes(out, cap, len, str, slen) < 0) return -1;
    return put_str(out, cap, len, "\r\n");
}

int resp_put_null(char *out, size_t cap, size_t *len) {
    return put_str(out, cap, len, "$-1\r\n");
}

int resp_put_array_header(char *out, size_t cap, size_t *len, long long n) {
    char tmp[32];
    int nn = snprintf(tmp, sizeof(tmp), "*%lld\r\n", n);
    if (nn < 0) return -1;
    return put_bytes(out, cap, len, tmp, (size_t)nn);
}

/* --- parsing --- */

/* Find CRLF in buf[off..len). Returns index of '\r' or -1. */
static long find_crlf(const char *buf, size_t off, size_t len) {
    for (size_t i = off; i + 1 < len; i++)
        if (buf[i] == '\r' && buf[i + 1] == '\n')
            return (long)i;
    return -1;
}

/* Parse a decimal integer from buf[off..end). Returns 1 on success. */
static int parse_long(const char *buf, size_t off, size_t end, long long *out) {
    if (off >= end) return 0;
    long long v = 0;
    int neg = 0;
    size_t i = off;
    if (buf[i] == '-') {
        neg = 1;
        i++;
        if (i >= end) return 0;
    }
    if (i >= end) return 0;
    for (; i < end; i++) {
        if (buf[i] < '0' || buf[i] > '9') return 0;
        v = v * 10 + (buf[i] - '0');
    }
    *out = neg ? -v : v;
    return 1;
}

static void free_argv(char **argv, int argc) {
    if (!argv) return;
    for (int i = 0; i < argc; i++) free(argv[i]);
    free(argv);
}

int resp_parse_command(const char *buf, size_t len, char ***argv_out,
                       int *argc_out, size_t *consumed) {
    *argv_out = NULL;
    *argc_out = 0;
    *consumed = 0;
    if (len == 0) return 0;

    if (buf[0] == '*') {
        /* RESP array of bulk strings. */
        long line_end = find_crlf(buf, 1, len);
        if (line_end < 0) return 0; /* need more data */
        long long count = 0;
        if (!parse_long(buf, 1, (size_t)line_end, &count) || count <= 0 || count > 1024)
            return -1;
        char **argv = calloc((size_t)count, sizeof(char *));
        if (!argv) return -1;
        size_t off = (size_t)line_end + 2;
        int argc = 0;
        for (long long i = 0; i < count; i++) {
            if (off >= len) { free_argv(argv, argc); return 0; }
            if (buf[off] != '$') { free_argv(argv, argc); return -1; }
            long sl_end = find_crlf(buf, off + 1, len);
            if (sl_end < 0) { free_argv(argv, argc); return 0; }
            long long slen = 0;
            if (!parse_long(buf, off + 1, (size_t)sl_end, &slen) || slen < 0 || slen > (1 << 26)) {
                free_argv(argv, argc);
                return -1;
            }
            size_t data_off = (size_t)sl_end + 2;
            if (data_off + (size_t)slen + 2 > len) { free_argv(argv, argc); return 0; }
            if (buf[data_off + (size_t)slen] != '\r' || buf[data_off + (size_t)slen + 1] != '\n') {
                free_argv(argv, argc);
                return -1;
            }
            argv[argc] = malloc((size_t)slen + 1);
            if (!argv[argc]) { free_argv(argv, argc); return -1; }
            memcpy(argv[argc], buf + data_off, (size_t)slen);
            argv[argc][slen] = '\0';
            argc++;
            off = data_off + (size_t)slen + 2;
        }
        *argv_out = argv;
        *argc_out = argc;
        *consumed = off;
        return 1;
    }

    /* Inline command: one line, space separated (telnet-style). */
    long line_end = find_crlf(buf, 0, len);
    if (line_end < 0) return 0;
    if (line_end == 0) return -1; /* empty line */
    char **argv = NULL;
    int argc = 0, argv_cap = 0;
    size_t i = 0;
    while (i < (size_t)line_end) {
        while (i < (size_t)line_end && isspace((unsigned char)buf[i])) i++;
        if (i >= (size_t)line_end) break;
        size_t start = i;
        int quoted = 0;
        if (buf[i] == '"') { quoted = 1; i++; start = i; }
        if (quoted) {
            while (i < (size_t)line_end && buf[i] != '"') i++;
        } else {
            while (i < (size_t)line_end && !isspace((unsigned char)buf[i])) i++;
        }
        size_t tok_len = i - start;
        if (quoted && i < (size_t)line_end) i++; /* closing quote */
        if (argc >= argv_cap) {
            argv_cap = argv_cap ? argv_cap * 2 : 8;
            char **na = realloc(argv, (size_t)argv_cap * sizeof(char *));
            if (!na) { free_argv(argv, argc); return -1; }
            argv = na;
        }
        argv[argc] = malloc(tok_len + 1);
        if (!argv[argc]) { free_argv(argv, argc); return -1; }
        memcpy(argv[argc], buf + start, tok_len);
        argv[argc][tok_len] = '\0';
        argc++;
    }
    if (argc == 0) { free(argv); return -1; }
    *argv_out = argv;
    *argc_out = argc;
    *consumed = (size_t)line_end + 2;
    return 1;
}
