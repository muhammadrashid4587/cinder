#ifndef CINDER_RESP_H
#define CINDER_RESP_H

#include <stddef.h>

/* Parse one RESP command (array of bulk strings, or an inline command)
   from buf[0..len).
   Returns 1 = complete command parsed (argv malloc'd, *argc_out set,
             *consumed = bytes used from buf)
           0 = need more data
          -1 = protocol error
   Caller frees each argv string and the argv array. */
int resp_parse_command(const char *buf, size_t len, char ***argv_out,
                       int *argc_out, size_t *consumed);

/* Encoding helpers. Each appends to out (cap bytes total, *len = current
   length) and returns 0 on success, -1 if out would overflow. */
int resp_put_simple(char *out, size_t cap, size_t *len, const char *msg);
int resp_put_error(char *out, size_t cap, size_t *len, const char *msg);
int resp_put_int(char *out, size_t cap, size_t *len, long long v);
int resp_put_bulk(char *out, size_t cap, size_t *len, const char *str, size_t slen);
int resp_put_null(char *out, size_t cap, size_t *len);
int resp_put_array_header(char *out, size_t cap, size_t *len, long long n);

#endif
