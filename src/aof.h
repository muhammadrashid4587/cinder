#ifndef CINDER_AOF_H
#define CINDER_AOF_H

/* Append-only log. Every mutating command is recorded as a RESP array so
   the dataset can be rebuilt by replaying the file through the same
   command dispatcher used for live clients. */

/* Fsync policy, Redis-style:
 *   always    - fdatasync after every mutating command (default).
 *               Slowest, but no acknowledged write is lost on a crash.
 *   everysec  - write immediately, fdatasync at most once per second.
 *               Up to a second of acknowledged writes can be lost on a
 *               machine crash, but throughput is far higher. */
typedef enum { AOF_FSYNC_ALWAYS, AOF_FSYNC_EVERYSEC } AofFsyncPolicy;

/* Select the policy by name ("always" or "everysec"). Returns 0 on
   success, -1 for an unknown name. */
int aof_set_policy(const char *name);
const char *aof_policy_name(void);

/* Open (creating if needed) the AOF file. Returns 0 on success. */
int aof_open(const char *path);
/* Append one command, then sync according to the policy. */
void aof_append(char **argv, int argc);
/* Force any unsynced data to disk now (used by SAVE and on shutdown). */
void aof_fsync(void);
/* Call regularly from the event loop: fsyncs if the everysec policy is
   due. Cheap no-op otherwise. */
void aof_tick(void);

/* Snapshot of the background flush thread's state, for INFO. */
typedef struct {
    int thread_running;        /* flush thread alive (0 = synchronous fallback) */
    int flush_in_progress;     /* thread is inside fdatasync right now */
    long long flushes_completed; /* syncs the thread has finished */
    int dirty;                 /* writes not yet covered by a completed sync */
    int last_flush_error;      /* errno of the last failed sync, 0 = none */
    long long seconds_since_fsync; /* -1 if no sync has completed yet */
} AofFlushInfo;
void aof_flush_info(AofFlushInfo *out);

void aof_close(void);

#endif
