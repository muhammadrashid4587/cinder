#ifndef CINDER_THREADPOOL_H
#define CINDER_THREADPOOL_H

/* A small worker thread pool: a FIFO job queue fed by the event-loop
 * thread and drained by N worker threads.
 *
 * The pool owns no cinder state; jobs are plain function pointers,
 * and whatever a job touches must be safe to touch off the event
 * loop. Today that is only the background-save job, which writes
 * snapshot bytes the main thread serialized before submitting — the
 * store itself is never touched by a worker, so the single-threaded
 * command path keeps its correctness guarantees exactly as before.
 * Later jobs (a background AOF flush, say) have to meet the same
 * bar: either they touch no shared state, or they take a lock. */

typedef void (*tp_job_fn)(void *arg);

typedef struct ThreadPool ThreadPool;

/* Create a pool with n worker threads (1..32). Returns NULL when n
   is out of range or a thread cannot be started. */
ThreadPool *tp_create(int n);

/* Queue a job; the pool takes ownership of arg. Returns 0 on
   success, -1 when the pool is shutting down (the caller keeps arg). */
int tp_submit(ThreadPool *tp, tp_job_fn fn, void *arg);

/* Stop accepting jobs, drain the queue, join the workers, free the
   pool. Blocks until every queued job has run. */
void tp_shutdown(ThreadPool *tp);

/* Number of jobs that have finished running. */
long long tp_jobs_completed(ThreadPool *tp);

#endif
