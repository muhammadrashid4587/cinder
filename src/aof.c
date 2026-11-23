#define _GNU_SOURCE
#include "aof.h"
#include "resp.h"

#include <errno.h>
#include <fcntl.h>
#include <pthread.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

static int aof_fd = -1;
static AofFsyncPolicy aof_policy = AOF_FSYNC_ALWAYS;
/* Monotonic ms of the last completed fdatasync; 0 = none yet. */
static long long aof_last_fsync_ms;
static int aof_dirty; /* writes since the last fdatasync */

/* Background flush thread (the everysec slice of the background-AOF
   work): the once-a-second fdatasync runs here instead of on the event
   loop, so a slow disk never stalls command handling. All shared state
   below is guarded by aof_mu; the thread is woken on aof_cv whenever
   a sync is due or forced, and wakes itself every 100 ms as a
   backstop. If the thread cannot be started, the code falls back to
   synchronous syncs on the calling thread, as before. */
static pthread_t aof_thread;
static int aof_thread_running;
static pthread_mutex_t aof_mu = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t aof_cv = PTHREAD_COND_INITIALIZER;
static unsigned long long aof_append_count;   /* total aof_append calls */
static unsigned long long aof_synced_through; /* appends covered by the last completed sync */
static long long aof_flush_count;             /* syncs completed by the thread */
static int aof_flush_requested;               /* aof_fsync() wants a sync now */
static int aof_flush_in_progress;             /* thread is inside fdatasync */
static int aof_last_flush_error;              /* errno of the last failed sync, 0 = none */
static int aof_stop;                          /* thread should exit after a final sync */

static long long mono_ms(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (long long)ts.tv_sec * 1000 + ts.tv_nsec / 1000000;
}

static void *aof_flush_thread(void *arg) {
    (void)arg;
    for (;;) {
        pthread_mutex_lock(&aof_mu);
        for (;;) {
            if (aof_stop && !aof_dirty && !aof_flush_requested) {
                pthread_mutex_unlock(&aof_mu);
                return NULL;
            }
            long long now = mono_ms();
            int due = aof_policy == AOF_FSYNC_EVERYSEC && aof_dirty &&
                      now - aof_last_fsync_ms >= 1000;
            if (aof_flush_requested || due) break;
            if (aof_stop) break; /* stopping with pending data: one final sync */
            /* Wake periodically so a due flush is not missed when
               nothing pokes the thread. */
            struct timespec ts;
            clock_gettime(CLOCK_REALTIME, &ts);
            ts.tv_nsec += 100 * 1000000;
            if (ts.tv_nsec >= 1000000000L) {
                ts.tv_sec++;
                ts.tv_nsec -= 1000000000L;
            }
            pthread_cond_timedwait(&aof_cv, &aof_mu, &ts);
        }
        /* Sync: snapshot the append count first, so a write landing
           mid-sync is not mistaken for synced data. A waiter in
           aof_fsync() watches aof_synced_through against the count at
           its call time; if appends arrived during this sync, the
           request stays set and the next pass covers them. */
        unsigned long long covered = aof_append_count;
        aof_flush_requested = 0;
        aof_flush_in_progress = 1;
        pthread_mutex_unlock(&aof_mu);
        int rc = fdatasync(aof_fd);
        int saved_errno = errno;
        pthread_mutex_lock(&aof_mu);
        aof_flush_in_progress = 0;
        if (rc == 0) {
            aof_last_fsync_ms = mono_ms();
            aof_synced_through = covered;
            if (aof_append_count == covered) aof_dirty = 0;
            aof_flush_count++;
            aof_last_flush_error = 0;
        } else {
            /* Failed sync: record it, unblock any waiter (the disk is
               broken; hanging SAVE forever helps nobody), and keep the
               data marked dirty so the next due pass retries. */
            aof_last_flush_error = saved_errno;
            aof_synced_through = covered;
        }
        if (aof_stop) {
            /* Final sync attempted: exit even if it failed or new
               writes landed mid-sync; the process is going away. */
            aof_dirty = 0;
            aof_flush_requested = 0;
        }
        pthread_cond_broadcast(&aof_cv);
        pthread_mutex_unlock(&aof_mu);
    }
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
    if (aof_fd < 0) return -1;
    if (pthread_create(&aof_thread, NULL, aof_flush_thread, NULL) == 0)
        aof_thread_running = 1;
    /* If the thread fails to start, aof_append()/aof_fsync() fall back
       to synchronous syncs on the calling thread. */
    return 0;
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

    pthread_mutex_lock(&aof_mu);
    aof_append_count++;
    aof_dirty = 1;
    long long now = mono_ms();
    int due = now - aof_last_fsync_ms >= 1000;
    pthread_mutex_unlock(&aof_mu);

    if (aof_policy == AOF_FSYNC_ALWAYS) {
        /* Unchanged: under always the reply must wait for the fsync,
           so it still happens inline on the event loop. Moving this
           off-loop needs deferred replies and is the next slice. */
        fdatasync(aof_fd);
        pthread_mutex_lock(&aof_mu);
        aof_last_fsync_ms = mono_ms();
        aof_synced_through = aof_append_count;
        aof_dirty = 0;
        pthread_mutex_unlock(&aof_mu);
    } else if (aof_thread_running) {
        /* everysec: the write is already in the file; only the sync
           is deferred, and the flush thread owns it now. Poke it when
           a second has passed so it does not wait for its next
           100 ms wakeup. */
        if (due) {
            pthread_mutex_lock(&aof_mu);
            pthread_cond_signal(&aof_cv);
            pthread_mutex_unlock(&aof_mu);
        }
    } else {
        /* No flush thread: fall back to the old in-loop sync. */
        if (due) {
            fdatasync(aof_fd);
            pthread_mutex_lock(&aof_mu);
            aof_last_fsync_ms = mono_ms();
            aof_synced_through = aof_append_count;
            aof_dirty = 0;
            pthread_mutex_unlock(&aof_mu);
        }
    }
}

void aof_fsync(void) {
    if (aof_fd < 0) return;
    if (!aof_thread_running) {
        if (fdatasync(aof_fd) == 0) {
            pthread_mutex_lock(&aof_mu);
            aof_last_fsync_ms = mono_ms();
            aof_synced_through = aof_append_count;
            aof_dirty = 0;
            pthread_mutex_unlock(&aof_mu);
        }
        return;
    }
    /* Ask the flush thread for a sync and wait until a sync has
       completed that covers every append made before this call, so
       SAVE (and a clean shutdown) still forces the data to disk. */
    pthread_mutex_lock(&aof_mu);
    unsigned long long target = aof_append_count;
    if (aof_synced_through < target) {
        aof_flush_requested = 1;
        pthread_cond_signal(&aof_cv);
        while (aof_synced_through < target)
            pthread_cond_wait(&aof_cv, &aof_mu);
    }
    pthread_mutex_unlock(&aof_mu);
}

void aof_tick(void) {
    if (aof_policy != AOF_FSYNC_EVERYSEC || !aof_thread_running) return;
    /* Cheap poke: the thread wakes itself every 100 ms anyway, but
       the tick fires the due flush without waiting for that. */
    pthread_mutex_lock(&aof_mu);
    if (aof_dirty && mono_ms() - aof_last_fsync_ms >= 1000)
        pthread_cond_signal(&aof_cv);
    pthread_mutex_unlock(&aof_mu);
}

void aof_flush_info(AofFlushInfo *out) {
    pthread_mutex_lock(&aof_mu);
    out->thread_running = aof_thread_running;
    out->flush_in_progress = aof_flush_in_progress;
    out->flushes_completed = aof_flush_count;
    out->dirty = aof_dirty;
    out->last_flush_error = aof_last_flush_error;
    out->seconds_since_fsync =
        aof_last_fsync_ms == 0 ? -1 : (mono_ms() - aof_last_fsync_ms) / 1000;
    pthread_mutex_unlock(&aof_mu);
}

void aof_close(void) {
    if (aof_fd < 0) return;
    if (aof_thread_running) {
        /* Flush pending everysec data so a clean shutdown loses
           nothing, then join the thread before closing the file. */
        pthread_mutex_lock(&aof_mu);
        aof_stop = 1;
        aof_flush_requested = 1;
        pthread_cond_signal(&aof_cv);
        pthread_mutex_unlock(&aof_mu);
        pthread_join(aof_thread, NULL);
        aof_thread_running = 0;
    } else {
        aof_fsync();
    }
    close(aof_fd);
    aof_fd = -1;
}
