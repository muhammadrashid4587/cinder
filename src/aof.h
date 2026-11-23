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
/* True when the policy is `always`: under it a mutating command's
   reply is deferred until the fsync covering its append completes. */
int aof_always_policy(void);

/* Open (creating if needed) the AOF file. Returns 0 on success. */
int aof_open(const char *path);
/* Append one command, then sync according to the policy. Returns the
   append's sequence number (1-based count of appends so far), or 0
   when nothing was appended. Under `always` the sync runs on the
   background flush thread: the caller must not acknowledge the write
   until aof_synced_through() has reached the returned number. Under
   `everysec` the caller need not wait. When the flush thread is not
   running, `always` still syncs inline on the calling thread, so the
   returned number is already covered. */
unsigned long long aof_append(char **argv, int argc);
/* Append count covered by the last completed fdatasync (a prefix of
   the appends: a sync always covers whole appends, in order). */
unsigned long long aof_synced_through(void);
/* Block until a sync has completed that covers append `target`
   (or every append so far, for aof_fsync). Asks the flush thread for
   a sync when one is running; syncs inline otherwise. A failed sync
   still advances the covered count, so a broken disk unblocks the
   waiter instead of hanging it forever. */
void aof_wait_for(unsigned long long target);
/* Force any unsynced data to disk now (used by SAVE and on shutdown). */
void aof_fsync(void);
/* Call regularly from the event loop: fsyncs if the everysec policy is
   due. Cheap no-op otherwise. */
void aof_tick(void);
/* Call once per event-loop iteration, after the read batch is
   processed: wakes the flush thread when a sync was requested, so a
   burst of appends coalesces into one sync. Cheap no-op otherwise. */
void aof_kick(void);

/* Read end of the wakeup pipe the flush thread writes to after every
   completed sync (-1 if it could not be created). The event loop
   watches it so a deferred reply is released as soon as the sync
   covering it lands, instead of waiting for the next loop timeout. */
int aof_wakeup_fd(void);

/* Snapshot of the background flush thread's state, for INFO. */
typedef struct {
    int thread_running;        /* flush thread alive (0 = synchronous fallback) */
    int flush_in_progress;     /* thread is inside fdatasync right now */
    long long flushes_completed; /* syncs the thread has finished */
    int dirty;                 /* writes not yet covered by a completed sync */
    int last_flush_error;      /* errno of the last failed sync, 0 = none */
    long long seconds_since_fsync; /* -1 if no sync has completed yet */
    long long pending_appends; /* appends made but not yet covered by a sync */
} AofFlushInfo;
void aof_flush_info(AofFlushInfo *out);

void aof_close(void);

#endif
