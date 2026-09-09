#include <stdio.h>
#include <stdlib.h>

#include <unistd.h>
#include "thread_pool.h"
/* ============================================================
 *  一个最小可用的线程池
 *  核心组件：
 *    1. 任务队列（链表）      —— 存待执行的任务
 *    2. 线程数组              —— 固定数量的工作线程
 *    3. 互斥锁 + 条件变量      —— 同步：空队列时线程睡，来任务时唤醒
 * ============================================================ */




/* ----- 工作线程的主循环 ----- */
void *worker(void *arg)
{
    threadpool_t *pool = (threadpool_t *)arg;

    while (1) {
        task_t *t;

        /* ① 加锁，从队列头取任务 */
        pthread_mutex_lock(&pool->lock);

        /* ② 队列空 && 没让退出 → 睡，等 cond 唤醒 */
        while (pool->head == NULL && !pool->stop) {
            pthread_cond_wait(&pool->cond, &pool->lock);
        }

        /* ③ 让退出 && 队列也空了 → 线程结束 */
        if (pool->stop && pool->head == NULL) {
            pthread_mutex_unlock(&pool->lock);
            break;
        }

        /* ④ 摘下队头任务 */
        t = pool->head;
        pool->head = t->next;
        if (pool->head == NULL)
            pool->tail = NULL;

        pthread_mutex_unlock(&pool->lock);

        /* ⑤ 解锁后执行任务（不要在锁里执行，否则串行了） */
        t->func(t->arg);
        free(t);
        // 执行任务
    }
    return NULL;
}


/* ----- 初始化线程池 ----- */
threadpool_t *threadpool_create(void)
{
    threadpool_t *pool = malloc(sizeof(threadpool_t));
    pool->head = pool->tail = NULL;
    pool->stop = 0;
    pthread_mutex_init(&pool->lock, NULL);
    pthread_cond_init(&pool->cond, NULL);

    for (int i = 0; i < MAX_THREADS; i++)
        pthread_create(&pool->threads[i], NULL, worker, pool);

    return pool;
}


/* ----- 提交任务：把函数放进队列 ----- */
void threadpool_submit(threadpool_t *pool, void (*func)(void *), void *arg)
{
    task_t *t = malloc(sizeof(task_t));
    t->func = func;
    t->arg  = arg;
    t->next = NULL;

    pthread_mutex_lock(&pool->lock);

    /* 尾插 */
    if (pool->tail == NULL) {
        pool->head = pool->tail = t;
    } else {
        pool->tail->next = t;
        pool->tail = t;
    }

    /* 通知一个睡着的线程：有活了 */
    pthread_cond_signal(&pool->cond);
    pthread_mutex_unlock(&pool->lock);
}


/* ----- 销毁线程池：等所有线程退出，清理资源 ----- */
void threadpool_destroy(threadpool_t *pool)
{
    pthread_mutex_lock(&pool->lock);
    pool->stop = 1;
    /* 广播唤醒所有睡着的线程，让它们看到 stop 退出 */
    pthread_cond_broadcast(&pool->cond);
    pthread_mutex_unlock(&pool->lock);

    for (int i = 0; i < MAX_THREADS; i++)
        pthread_join(pool->threads[i], NULL);

    pthread_mutex_destroy(&pool->lock);
    pthread_cond_destroy(&pool->cond);
    free(pool);
}


/* ============================================================
 *  测试
 * ============================================================ */
// void my_task(void *arg)
// {
//     int n = *(int *)arg;
//     printf("线程 %lu 正在执行任务 %d\n", pthread_self(), n);
//     sleep(1);                       /* 模拟任务耗时 */
// }
