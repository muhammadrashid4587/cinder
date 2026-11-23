#define _GNU_SOURCE
#include "threadpool.h"

#include <pthread.h>
#include <stdlib.h>

typedef struct Job {
    tp_job_fn fn;
    void *arg;
    struct Job *next;
} Job;

struct ThreadPool {
    pthread_t *threads;
    int nthreads;
    pthread_mutex_t mu;
    pthread_cond_t work_avail;
    Job *head;
    Job *tail;
    int stopping; /* set by tp_shutdown: drain, then exit */
    long long completed;
};

static void *worker_main(void *arg) {
    ThreadPool *tp = arg;
    for (;;) {
        pthread_mutex_lock(&tp->mu);
        while (!tp->head && !tp->stopping)
            pthread_cond_wait(&tp->work_avail, &tp->mu);
        if (!tp->head && tp->stopping) {
            pthread_mutex_unlock(&tp->mu);
            return NULL;
        }
        Job *job = tp->head;
        tp->head = job->next;
        if (!tp->head) tp->tail = NULL;
        pthread_mutex_unlock(&tp->mu);

        job->fn(job->arg);
        free(job);

        pthread_mutex_lock(&tp->mu);
        tp->completed++;
        pthread_mutex_unlock(&tp->mu);
    }
}

ThreadPool *tp_create(int n) {
    if (n < 1 || n > 32) return NULL;
    ThreadPool *tp = calloc(1, sizeof(*tp));
    if (!tp) return NULL;
    tp->threads = calloc((size_t)n, sizeof(pthread_t));
    if (!tp->threads) {
        free(tp);
        return NULL;
    }
    tp->nthreads = n;
    pthread_mutex_init(&tp->mu, NULL);
    pthread_cond_init(&tp->work_avail, NULL);
    int started = 0;
    for (int i = 0; i < n; i++) {
        if (pthread_create(&tp->threads[i], NULL, worker_main, tp) != 0)
            break;
        started++;
    }
    if (started == 0) {
        pthread_mutex_destroy(&tp->mu);
        pthread_cond_destroy(&tp->work_avail);
        free(tp->threads);
        free(tp);
        return NULL;
    }
    tp->nthreads = started;
    return tp;
}

int tp_submit(ThreadPool *tp, tp_job_fn fn, void *arg) {
    Job *job = malloc(sizeof(*job));
    if (!job) return -1;
    job->fn = fn;
    job->arg = arg;
    job->next = NULL;
    pthread_mutex_lock(&tp->mu);
    if (tp->stopping) {
        pthread_mutex_unlock(&tp->mu);
        free(job);
        return -1;
    }
    if (tp->tail) tp->tail->next = job;
    else tp->head = job;
    tp->tail = job;
    pthread_cond_signal(&tp->work_avail);
    pthread_mutex_unlock(&tp->mu);
    return 0;
}

void tp_shutdown(ThreadPool *tp) {
    if (!tp) return;
    pthread_mutex_lock(&tp->mu);
    tp->stopping = 1;
    pthread_cond_broadcast(&tp->work_avail);
    pthread_mutex_unlock(&tp->mu);
    /* Workers drain the queue before they exit, so every submitted
       job runs; the join below waits for that. */
    for (int i = 0; i < tp->nthreads; i++)
        pthread_join(tp->threads[i], NULL);
    pthread_mutex_lock(&tp->mu);
    Job *job = tp->head;
    while (job) {
        Job *next = job->next;
        free(job);
        job = next;
    }
    pthread_mutex_unlock(&tp->mu);
    pthread_mutex_destroy(&tp->mu);
    pthread_cond_destroy(&tp->work_avail);
    free(tp->threads);
    free(tp);
}

long long tp_jobs_completed(ThreadPool *tp) {
    if (!tp) return 0;
    pthread_mutex_lock(&tp->mu);
    long long n = tp->completed;
    pthread_mutex_unlock(&tp->mu);
    return n;
}
