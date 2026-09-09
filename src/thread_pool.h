#ifndef _THREAD_POOL_H_
#define _THREAD_POOL_H_

#include <pthread.h>
#define MAX_THREADS 4        /* 线程池里固定的线程数 */



/* ----- 任务节点：一个函数 + 一个参数 ----- */
typedef struct task {
    void (*func)(void *);    /* 要执行的函数 */
    void *arg;               /* 传给函数的参数 */
    struct task *next;       /* 链表指向下一个任务 */
} task_t;

/* ----- 线程池 ----- */
typedef struct {
    task_t *head;            /* 队列头（取任务从这里） */
    task_t *tail;            /* 队列尾（加任务加这里） */
    pthread_mutex_t lock;    /* 保护队列 */
    pthread_cond_t  cond;    /* 通知"有任务了" */
    pthread_t threads[MAX_THREADS];
    int stop;                /* 1 = 通知所有线程退出 */
} threadpool_t;



/* ----- 工作线程的主循环 ----- */
void *worker(void *arg);
/* ----- 初始化线程池 ----- */
threadpool_t *threadpool_create(void);
/* ----- 提交任务：把函数放进队列 ----- */
void threadpool_submit(threadpool_t *pool, void (*func)(void *), void *arg);
/* ----- 销毁线程池：等所有线程退出，清理资源 ----- */
void threadpool_destroy(threadpool_t *pool);







#endif