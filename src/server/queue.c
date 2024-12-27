#include <pthread.h>
#include <stdlib.h>
#include <string.h>
#include <stdio.h>

#define MAX_TASKS 256

typedef struct {
    char message[PIPE_BUF];  // Mensagem do FIFO ou comando a ser processado
} Task;

typedef struct {
    Task tasks[MAX_TASKS];
    size_t front, rear;
    size_t count;
    pthread_mutex_t mutex;
    pthread_cond_t cond_nonempty;
    pthread_cond_t cond_nonfull;
} TaskQueue;

void task_queue_init(TaskQueue* queue) {
    queue->front = 0;
    queue->rear = 0;
    queue->count = 0;
    pthread_mutex_init(&queue->mutex, NULL);
    pthread_cond_init(&queue->cond_nonempty, NULL);
    pthread_cond_init(&queue->cond_nonfull, NULL);
}

void task_queue_destroy(TaskQueue* queue) {
    pthread_mutex_destroy(&queue->mutex);
    pthread_cond_destroy(&queue->cond_nonempty);
    pthread_cond_destroy(&queue->cond_nonfull);
}

void task_queue_push(TaskQueue* queue, const char* message) {
    pthread_mutex_lock(&queue->mutex);
    while (queue->count == MAX_TASKS) {
        pthread_cond_wait(&queue->cond_nonfull, &queue->mutex);
    }

    strncpy(queue->tasks[queue->rear].message, message, PIPE_BUF - 1);
    queue->tasks[queue->rear].message[PIPE_BUF - 1] = '\0';
    queue->rear = (queue->rear + 1) % MAX_TASKS;
    queue->count++;

    pthread_cond_signal(&queue->cond_nonempty);
    pthread_mutex_unlock(&queue->mutex);
}

Task task_queue_pop(TaskQueue* queue) {
    pthread_mutex_lock(&queue->mutex);
    while (queue->count == 0) {
        pthread_cond_wait(&queue->cond_nonempty, &queue->mutex);
    }

    Task task = queue->tasks[queue->front];
    queue->front = (queue->front + 1) % MAX_TASKS;
    queue->count--;

    pthread_cond_signal(&queue->cond_nonfull);
    pthread_mutex_unlock(&queue->mutex);
    return task;
}
