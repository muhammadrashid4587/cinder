#ifndef CINDER_RDB_H
#define CINDER_RDB_H

#include "store.h"

/* RDB-style snapshot: a compact binary dump of the whole dataset,
 * written alongside the AOF.
 *
 * Format (all integers little-endian):
 *   "CINDER" magic (6 bytes), uint32 version (= 4), then one record
 *   per non-expired key:
 *     uint8  type      (0 = string, 1 = list, 2 = set, 3 = hash,
 *                        4 = sorted set, 5 = stream)
 *     int64  expire_at (ms since epoch, -1 = no expiry)
 *     string key       (uint32 length + bytes)
 *     string value              for strings
 *     uint32 count, then count strings      for lists and sets
 *     uint32 count, then count field+value string pairs for hashes
 *     uint32 count, then count stream entries (each an "ms-seq"
 *       string, a field count, then the pairs), then — version 4
 *       only — uint32 group count and one record per consumer
 *       group: name string, last-delivered ID "ms-seq" string,
 *       uint32 consumer count then the consumer names, uint32
 *       pending count then one pending entry each (ID string,
 *       consumer string, int64 delivery time, int64 delivery
 *       count), for streams
 *   and a final uint8 0xFF end marker.
 *
 * Values are C strings, like everywhere else in cinder: embedded NUL
 * bytes are not supported (the RESP layer and the AOF treat values
 * the same way).
 *
 * The snapshot is written to path.tmp, fsynced, and renamed over
 * path, so a crash mid-save never leaves a torn snapshot behind.
 * Loading skips entries whose TTL already passed. Loading happens
 * on startup before the AOF is replayed; the AOF holds the complete
 * history, so replaying it after the snapshot reproduces the exact
 * final state either way, and a corrupt snapshot is harmless. */

/* Write a snapshot of the store to path. Returns 0 on success,
   -1 on any I/O error (errno set). */
int rdb_save(Store *s, const char *path);

/* Serialize a snapshot of the store to a freshly malloc'd buffer.
   No disk I/O: the bytes are for a worker thread to write with
   rdb_write_snapshot. Returns 0 on success, -1 on error. */
int rdb_serialize(Store *s, char **out, size_t *outlen);

/* Write already-serialized snapshot bytes to path atomically
   (path + tmp_suffix, fsync, rename, fsync the directory). Safe
   from a worker thread. Returns 0 on success, -1 on error. */
int rdb_write_snapshot(const char *path, const char *tmp_suffix,
                       const char *data, size_t len);

/* Load a snapshot into the store. Meant for a fresh (empty) store at
   startup. Returns the number of keys loaded, or -1 when the file is
   missing (errno ENOENT) or malformed (errno EINVAL). Keys parsed
   before a malformed record stay loaded; the AOF replay that follows
   corrects any gap. */
long long rdb_load(Store *s, const char *path);

#endif
