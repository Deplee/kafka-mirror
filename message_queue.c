#include "kafka_mirror.h"
#include <stdlib.h>
#include <string.h>
#include <errno.h>

int message_queue_init(message_queue_t *queue, int capacity) {
    if (!queue || capacity <= 0) {
        return -1;
    }
    
    queue->messages = malloc(capacity * sizeof(work_message_t));
    if (!queue->messages) {
        return -1;
    }
    
    queue->head = 0;
    queue->tail = 0;
    queue->count = 0;
    queue->capacity = capacity;
    queue->shutdown = false;
    
    if (pthread_mutex_init(&queue->mutex, NULL) != 0) {
        free(queue->messages);
        return -1;
    }
    
    if (pthread_cond_init(&queue->not_empty, NULL) != 0) {
        pthread_mutex_destroy(&queue->mutex);
        free(queue->messages);
        return -1;
    }
    
    if (pthread_cond_init(&queue->not_full, NULL) != 0) {
        pthread_cond_destroy(&queue->not_empty);
        pthread_mutex_destroy(&queue->mutex);
        free(queue->messages);
        return -1;
    }
    
    return 0;
}

void message_queue_destroy(message_queue_t *queue) {
    if (!queue) return;
    
    pthread_cond_destroy(&queue->not_full);
    pthread_cond_destroy(&queue->not_empty);
    pthread_mutex_destroy(&queue->mutex);
    
    if (queue->messages) {
        free(queue->messages);
        queue->messages = NULL;
    }
}

int message_queue_put(message_queue_t *queue, work_message_t *msg) {
    if (!queue || !msg) {
        return -1;
    }
    
    pthread_mutex_lock(&queue->mutex);
    
    if (queue->shutdown) {
        pthread_mutex_unlock(&queue->mutex);
        return -1;
    }
    
    while (queue->count >= queue->capacity && !queue->shutdown) {
        pthread_cond_wait(&queue->not_full, &queue->mutex);
    }
    
    if (queue->shutdown) {
        pthread_mutex_unlock(&queue->mutex);
        return -1;
    }
    
    queue->messages[queue->tail] = *msg;
    queue->tail = (queue->tail + 1) % queue->capacity;
    queue->count++;
    
    pthread_cond_signal(&queue->not_empty);
    pthread_mutex_unlock(&queue->mutex);
    
    return 0;
}

int message_queue_get(message_queue_t *queue, work_message_t *msg) {
    if (!queue || !msg) {
        return -1;
    }
    
    pthread_mutex_lock(&queue->mutex);
    
    while (queue->count == 0 && !queue->shutdown) {
        pthread_cond_wait(&queue->not_empty, &queue->mutex);
    }
    
    if (queue->shutdown) {
        pthread_mutex_unlock(&queue->mutex);
        return -1;
    }
    
    *msg = queue->messages[queue->head];
    queue->head = (queue->head + 1) % queue->capacity;
    queue->count--;
    
    pthread_cond_signal(&queue->not_full);
    pthread_mutex_unlock(&queue->mutex);
    
    return 0;
}

bool message_queue_is_empty(message_queue_t *queue) {
    if (!queue) return true;
    
    pthread_mutex_lock(&queue->mutex);
    bool empty = (queue->count == 0);
    pthread_mutex_unlock(&queue->mutex);
    
    return empty;
}

void message_queue_wake_all(message_queue_t *queue) {
    if (!queue) return;
    
    pthread_mutex_lock(&queue->mutex);
    pthread_cond_broadcast(&queue->not_empty);
    pthread_mutex_unlock(&queue->mutex);
}

void message_queue_shutdown(message_queue_t *queue) {
    if (!queue) return;
    
    pthread_mutex_lock(&queue->mutex);
    queue->shutdown = true;
    pthread_cond_broadcast(&queue->not_empty);
    pthread_cond_broadcast(&queue->not_full);
    pthread_mutex_unlock(&queue->mutex);
}

bool message_queue_is_full(message_queue_t *queue) {
    if (!queue) return false;
    
    pthread_mutex_lock(&queue->mutex);
    bool full = (queue->count >= queue->capacity);
    pthread_mutex_unlock(&queue->mutex);
    
    return full;
}
