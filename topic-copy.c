#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <signal.h>
#include <errno.h>
#include <sys/time.h>
#include <pthread.h>
#include <librdkafka/rdkafka.h>

#include "kafka_mirror.h"
#include "config.h"
#include "utils.h"

static kafka_mirror_t *g_mirror = NULL;


void kafka_mirror_signal_handler(int sig) {
    static int signal_count = 0;
    signal_count++;
    
    if (g_mirror) {
        printf("Получен сигнал %d. Завершение работы...\n", sig);
        g_mirror->running = false;
        
        message_queue_shutdown(&g_mirror->message_queue);
        
        if (signal_count == 1) {
            printf("Ожидание завершения обработки текущих сообщений...\n");
            printf("Нажмите Ctrl+C еще раз для принудительного завершения\n");
        } else {
            printf("Принудительное завершение...\n");
            exit(1);
        }
    }
}

void kafka_mirror_setup_signal_handlers(void) {
    signal(SIGINT, kafka_mirror_signal_handler);
    signal(SIGTERM, kafka_mirror_signal_handler);
}

int kafka_mirror_init(kafka_mirror_t *mirror, const kafka_mirror_config_t *config) {
    if (!mirror || !config) {
        return -1;
    }

    memset(mirror, 0, sizeof(kafka_mirror_t));
    mirror->config = *config;
    mirror->running = false;
    mirror->first_run = true;
    mirror->partitions_assigned = false;
    mirror->partitions_ready_for_status = false;
    mirror->stats.start_time = time(NULL);
    mirror->stats.last_message_time = time(NULL);
    mirror->stats.last_status_time = time(NULL);
    mirror->stats.last_status_delivered = 0;
    mirror->stats.total_processed_by_workers = 0;
    mirror->stats.messages_in_queue = 0;
    mirror->stats.messages_in_producer_queue = 0;

    if (pthread_mutex_init(&mirror->stats_mutex, NULL) != 0) {
        fprintf(stderr, "Ошибка инициализации мьютекса статистики\n");
        return -1;
    }

    if (pthread_mutex_init(&mirror->retry_mutex, NULL) != 0) {
        fprintf(stderr, "Ошибка инициализации мьютекса retry очереди\n");
        pthread_mutex_destroy(&mirror->stats_mutex);
        return -1;
    }

    int max_concurrent_producers = config->num_producers * 2;
    if (sem_init(&mirror->producer_semaphore, 0, max_concurrent_producers) != 0) {
        fprintf(stderr, "Ошибка инициализации семафора producer\n");
        pthread_mutex_destroy(&mirror->stats_mutex);
        pthread_mutex_destroy(&mirror->retry_mutex);
        return -1;
    }

    mirror->worker_threads = malloc(config->num_workers * sizeof(pthread_t));
    if (!mirror->worker_threads) {
        fprintf(stderr, "Ошибка выделения памяти для worker потоков\n");
        pthread_mutex_destroy(&mirror->stats_mutex);
        pthread_mutex_destroy(&mirror->retry_mutex);
        return -1;
    }
    
    for (int i = 0; i < config->num_workers; i++) {
        mirror->worker_threads[i] = 0;
    }
    mirror->status_thread = 0;
    mirror->retry_thread = 0;
    mirror->topic_discovery_thread = 0;
    mirror->target_admin_client = NULL;
    mirror->cross_cluster_initialized = false;
    
    int queue_capacity = (config->num_workers > 8) ? config->batch_count * 50 : config->batch_count * 20;
    printf("Инициализация очереди сообщений с размером: %d\n", queue_capacity);
    if (message_queue_init(&mirror->message_queue, queue_capacity) != 0) {
        fprintf(stderr, "Ошибка инициализации очереди сообщений\n");
        free(mirror->worker_threads);
        pthread_mutex_destroy(&mirror->stats_mutex);
        pthread_mutex_destroy(&mirror->retry_mutex);
        return -1;
    }

    snprintf(mirror->state_file, sizeof(mirror->state_file), 
             "mirror_state_%s.json", config->consumer_group);

    return 0;
}

int kafka_mirror_create_consumer(kafka_mirror_t *mirror) {
    char errstr[512];
    rd_kafka_conf_t *conf;
    rd_kafka_topic_conf_t *topic_conf;
    const kafka_mirror_config_t *config = &mirror->config;

    conf = rd_kafka_conf_new();
    if (!conf) {
        fprintf(stderr, "Ошибка создания конфигурации consumer\n");
        return -1;
    }

    char bootstrap_servers[2048] = {0};
    for (int i = 0; i < config->broker_count; i++) {
        if (i > 0) strcat(bootstrap_servers, ",");
        strcat(bootstrap_servers, config->brokers[i]);
    }

    if (rd_kafka_conf_set(conf, "bootstrap.servers", bootstrap_servers, errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK) {
        fprintf(stderr, "Ошибка настройки bootstrap.servers: %s\n", errstr);
        rd_kafka_conf_destroy(conf);
        return -1;
    }

    if (rd_kafka_conf_set(conf, "group.id", config->consumer_group, errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK) {
        fprintf(stderr, "Ошибка настройки group.id: %s\n", errstr);
        rd_kafka_conf_destroy(conf);
        return -1;
    }

    if (rd_kafka_conf_set(conf, "auto.offset.reset", "earliest", errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK) {
        fprintf(stderr, "Ошибка настройки auto.offset.reset: %s\n", errstr);
        rd_kafka_conf_destroy(conf);
        return -1;
    }

    if (rd_kafka_conf_set(conf, "enable.auto.commit", "false", errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK) {
        fprintf(stderr, "Ошибка настройки enable.auto.commit: %s\n", errstr);
        rd_kafka_conf_destroy(conf);
        return -1;
    }

    char max_poll_interval_ms[32];
    snprintf(max_poll_interval_ms, sizeof(max_poll_interval_ms), "%d", config->global_timeout_ms);
    if (rd_kafka_conf_set(conf, "max.poll.interval.ms", max_poll_interval_ms, errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK) {
        fprintf(stderr, "Ошибка настройки max.poll.interval.ms: %s\n", errstr);
        rd_kafka_conf_destroy(conf);
        return -1;
    }


    char fetch_max_bytes[32];
    snprintf(fetch_max_bytes, sizeof(fetch_max_bytes), "%d", 
             config->skip_size_validation ? 1000000000 : config->max_message_size);
    if (rd_kafka_conf_set(conf, "fetch.max.bytes", fetch_max_bytes, errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK) {
        fprintf(stderr, "Ошибка настройки fetch.max.bytes: %s\n", errstr);
        rd_kafka_conf_destroy(conf);
        return -1;
    }

    char session_timeout_ms[32];
    snprintf(session_timeout_ms, sizeof(session_timeout_ms), "%d", 10000);
    if (rd_kafka_conf_set(conf, "session.timeout.ms", session_timeout_ms, errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK) {
        fprintf(stderr, "Ошибка настройки session.timeout.ms: %s\n", errstr);
        rd_kafka_conf_destroy(conf);
        return -1;
    }

    char heartbeat_interval_ms[32];
    snprintf(heartbeat_interval_ms, sizeof(heartbeat_interval_ms), "%d", 3000);
    if (rd_kafka_conf_set(conf, "heartbeat.interval.ms", heartbeat_interval_ms, errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK) {
        fprintf(stderr, "Ошибка настройки heartbeat.interval.ms: %s\n", errstr);
        rd_kafka_conf_destroy(conf);
        return -1;
    }

    char fetch_min_bytes[32];
    snprintf(fetch_min_bytes, sizeof(fetch_min_bytes), "%d", config->batch_size / 1024);
    if (rd_kafka_conf_set(conf, "fetch.min.bytes", fetch_min_bytes, errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK) {
        fprintf(stderr, "Ошибка настройки fetch.min.bytes: %s\n", errstr);
        rd_kafka_conf_destroy(conf);
        return -1;
    }

    char fetch_wait_max_ms[32];
    snprintf(fetch_wait_max_ms, sizeof(fetch_wait_max_ms), "%d", config->linger_ms);
    if (rd_kafka_conf_set(conf, "fetch.wait.max.ms", fetch_wait_max_ms, errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK) {
        fprintf(stderr, "Ошибка настройки fetch.wait.max.ms: %s\n", errstr);
        rd_kafka_conf_destroy(conf);
        return -1;
    }

    if (config->security_protocol == SECURITY_SSL) {
        if (rd_kafka_conf_set(conf, "security.protocol", "ssl", errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK) {
            fprintf(stderr, "Ошибка настройки security.protocol: %s\n", errstr);
            rd_kafka_conf_destroy(conf);
            return -1;
        }

        if (strlen(config->ssl_cafile) > 0) {
            if (rd_kafka_conf_set(conf, "ssl.ca.location", config->ssl_cafile, errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK) {
                fprintf(stderr, "Ошибка настройки ssl.ca.location: %s\n", errstr);
                rd_kafka_conf_destroy(conf);
                return -1;
            }
        }

        if (strlen(config->ssl_certfile) > 0) {
            if (rd_kafka_conf_set(conf, "ssl.certificate.location", config->ssl_certfile, errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK) {
                fprintf(stderr, "Ошибка настройки ssl.certificate.location: %s\n", errstr);
                rd_kafka_conf_destroy(conf);
                return -1;
            }
        }

        if (strlen(config->ssl_keyfile) > 0) {
            if (rd_kafka_conf_set(conf, "ssl.key.location", config->ssl_keyfile, errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK) {
                fprintf(stderr, "Ошибка настройки ssl.key.location: %s\n", errstr);
                rd_kafka_conf_destroy(conf);
                return -1;
            }
        }
    } else if (config->security_protocol == SECURITY_SASL_PLAINTEXT || 
               config->security_protocol == SECURITY_SASL_SSL) {
        
        const char *security_protocol = (config->security_protocol == SECURITY_SASL_SSL) ? "sasl_ssl" : "sasl_plaintext";
        if (rd_kafka_conf_set(conf, "security.protocol", security_protocol, errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK) {
            fprintf(stderr, "Ошибка настройки security.protocol: %s\n", errstr);
            rd_kafka_conf_destroy(conf);
            return -1;
        }

        const char *sasl_mechanism = sasl_mechanism_to_string(config->sasl_mechanism);
        if (rd_kafka_conf_set(conf, "sasl.mechanism", sasl_mechanism, errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK) {
            fprintf(stderr, "Ошибка настройки sasl.mechanism: %s\n", errstr);
            rd_kafka_conf_destroy(conf);
            return -1;
        }

        const char *sasl_username = config->cross_cluster_mode ? config->source_sasl_username : config->sasl_username;
        const char *sasl_password = config->cross_cluster_mode ? config->source_sasl_password : config->sasl_password;
        
        if (rd_kafka_conf_set(conf, "sasl.username", sasl_username, errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK) {
            fprintf(stderr, "Ошибка настройки sasl.username: %s\n", errstr);
            rd_kafka_conf_destroy(conf);
            return -1;
        }
        
        if (rd_kafka_conf_set(conf, "sasl.password", sasl_password, errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK) {
            fprintf(stderr, "Ошибка настройки sasl.password: %s\n", errstr);
            rd_kafka_conf_destroy(conf);
            return -1;
        }
    }

    rd_kafka_conf_set_rebalance_cb(conf, kafka_mirror_rebalance_cb);
    rd_kafka_conf_set_opaque(conf, mirror);

    mirror->consumer = rd_kafka_new(RD_KAFKA_CONSUMER, conf, errstr, sizeof(errstr));
    if (!mirror->consumer) {
        fprintf(stderr, "Ошибка создания consumer: %s\n", errstr);
        return -1;
    }

    topic_conf = rd_kafka_topic_conf_new();
    if (!topic_conf) {
        fprintf(stderr, "Ошибка создания конфигурации топика\n");
        rd_kafka_destroy(mirror->consumer);
        return -1;
    }

    char max_partition_fetch_bytes[32];
    snprintf(max_partition_fetch_bytes, sizeof(max_partition_fetch_bytes), "%d", 
             config->skip_size_validation ? 1000000000 : config->max_message_size);
    if (rd_kafka_conf_set(conf, "max.partition.fetch.bytes", max_partition_fetch_bytes, errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK) {
        fprintf(stderr, "Ошибка настройки max.partition.fetch.bytes: %s\n", errstr);
        rd_kafka_topic_conf_destroy(topic_conf);
        rd_kafka_destroy(mirror->consumer);
        return -1;
    }

    mirror->topic_partition_list = rd_kafka_topic_partition_list_new(config->topic_count);
    if (!mirror->topic_partition_list) {
        fprintf(stderr, "Ошибка создания списка партиций\n");
        rd_kafka_topic_conf_destroy(topic_conf);
        rd_kafka_destroy(mirror->consumer);
        return -1;
    }

    for (int i = 0; i < config->topic_count; i++) {
        rd_kafka_topic_partition_t *tp = rd_kafka_topic_partition_list_add(mirror->topic_partition_list, 
                                                                           config->topics[i], RD_KAFKA_PARTITION_UA);
        if (!tp) {
            fprintf(stderr, "Ошибка добавления топика %s в список\n", config->topics[i]);
            rd_kafka_topic_partition_list_destroy(mirror->topic_partition_list);
            rd_kafka_topic_conf_destroy(topic_conf);
            rd_kafka_destroy(mirror->consumer);
            return -1;
        }
    }

    rd_kafka_resp_err_t err = rd_kafka_subscribe(mirror->consumer, mirror->topic_partition_list);
    if (err != RD_KAFKA_RESP_ERR_NO_ERROR) {
        fprintf(stderr, "Ошибка подписки на топики: %s\n", rd_kafka_err2str(err));
        rd_kafka_topic_partition_list_destroy(mirror->topic_partition_list);
        rd_kafka_topic_conf_destroy(topic_conf);
        rd_kafka_destroy(mirror->consumer);
        return -1;
    }

    printf("Consumer создан успешно, подписан на %d топиков\n", config->topic_count);
    return 0;
}


int kafka_mirror_create_producer_pool(kafka_mirror_t *mirror) {
    const kafka_mirror_config_t *config = &mirror->config;
    
    int optimal_producers = config->num_producers;
    if (optimal_producers < 4) {
        optimal_producers = 4;
    }
    if (optimal_producers > MAX_PRODUCERS) {
        optimal_producers = MAX_PRODUCERS;
    }
    
    mirror->producers = malloc(optimal_producers * sizeof(rd_kafka_t*));
    if (!mirror->producers) {
        fprintf(stderr, "Ошибка выделения памяти для producer pool\n");
        return -1;
    }
    
    mirror->producer_count = optimal_producers;
    mirror->current_producer = 0;
    
    if (pthread_mutex_init(&mirror->producer_mutex, NULL) != 0) {
        fprintf(stderr, "Ошибка инициализации мьютекса producer pool\n");
        free(mirror->producers);
        return -1;
    }
    
    for (int i = 0; i < optimal_producers; i++) {
        if (kafka_mirror_create_single_producer(mirror, i) != 0) {
            fprintf(stderr, "Ошибка создания producer %d\n", i);
            kafka_mirror_destroy_producer_pool(mirror);
            return -1;
        }
    }
    
    printf("Producer pool создан успешно: %d producer'ов (оптимизировано для %d worker'ов)\n", 
           optimal_producers, config->num_workers);
    return 0;
}

int kafka_mirror_create_single_producer(kafka_mirror_t *mirror, int producer_id) {
    char errstr[512];
    rd_kafka_conf_t *conf;
    const kafka_mirror_config_t *config = &mirror->config;

    conf = rd_kafka_conf_new();
    if (!conf) {
        fprintf(stderr, "Ошибка создания конфигурации producer %d\n", producer_id);
        return -1;
    }

    char bootstrap_servers[2048] = {0};
    if (config->cross_cluster_mode) {
        for (int i = 0; i < config->target_broker_count; i++) {
            if (i > 0) strcat(bootstrap_servers, ",");
            strcat(bootstrap_servers, config->target_brokers[i]);
        }
    } else {
        for (int i = 0; i < config->broker_count; i++) {
            if (i > 0) strcat(bootstrap_servers, ",");
            strcat(bootstrap_servers, config->brokers[i]);
        }
    }

    if (rd_kafka_conf_set(conf, "bootstrap.servers", bootstrap_servers, errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK) {
        fprintf(stderr, "Ошибка настройки bootstrap.servers для producer %d: %s\n", producer_id, errstr);
        rd_kafka_conf_destroy(conf);
        return -1;
    }

    char message_max_bytes[32];
    snprintf(message_max_bytes, sizeof(message_max_bytes), "%d", 
             config->skip_size_validation ? 1000000000 : config->max_message_size);
    if (rd_kafka_conf_set(conf, "message.max.bytes", message_max_bytes, errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK) {
        fprintf(stderr, "Ошибка настройки message.max.bytes для producer %d: %s\n", producer_id, errstr);
        rd_kafka_conf_destroy(conf);
        return -1;
    }

    char queue_buffering_max_kbytes[32];
    snprintf(queue_buffering_max_kbytes, sizeof(queue_buffering_max_kbytes), "%lld", config->buffer_memory / 1024);
    if (rd_kafka_conf_set(conf, "queue.buffering.max.kbytes", queue_buffering_max_kbytes, errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK) {
        fprintf(stderr, "Ошибка настройки queue.buffering.max.kbytes для producer %d: %s\n", producer_id, errstr);
        rd_kafka_conf_destroy(conf);
        return -1;
    }

    if (config->enable_compression) {
        if (rd_kafka_conf_set(conf, "compression.type", config->compression_type, errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK) {
            fprintf(stderr, "Ошибка настройки compression.type для producer %d: %s\n", producer_id, errstr);
            rd_kafka_conf_destroy(conf);
            return -1;
        }
    }

    char retries[32];
    snprintf(retries, sizeof(retries), "%d", 5);
    if (rd_kafka_conf_set(conf, "retries", retries, errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK) {
        fprintf(stderr, "Ошибка настройки retries для producer %d: %s\n", producer_id, errstr);
        rd_kafka_conf_destroy(conf);
        return -1;
    }

    char acks[8];
    snprintf(acks, sizeof(acks), "%s", config->disable_idempotence ? "1" : "all");
    if (rd_kafka_conf_set(conf, "acks", acks, errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK) {
        fprintf(stderr, "Ошибка настройки acks для producer %d: %s\n", producer_id, errstr);
        rd_kafka_conf_destroy(conf);
        return -1;
    }

    char request_timeout_ms[32];
    snprintf(request_timeout_ms, sizeof(request_timeout_ms), "%d", config->request_timeout_ms);
    if (rd_kafka_conf_set(conf, "request.timeout.ms", request_timeout_ms, errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK) {
        fprintf(stderr, "Ошибка настройки request.timeout.ms для producer %d: %s\n", producer_id, errstr);
        rd_kafka_conf_destroy(conf);
        return -1;
    }

    char delivery_timeout_ms[32];
    snprintf(delivery_timeout_ms, sizeof(delivery_timeout_ms), "%d", config->delivery_timeout_ms);
    if (rd_kafka_conf_set(conf, "delivery.timeout.ms", delivery_timeout_ms, errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK) {
        fprintf(stderr, "Ошибка настройки delivery.timeout.ms для producer %d: %s\n", producer_id, errstr);
        rd_kafka_conf_destroy(conf);
        return -1;
    }

    char linger_ms[32];
    snprintf(linger_ms, sizeof(linger_ms), "%d", config->linger_ms);
    if (rd_kafka_conf_set(conf, "linger.ms", linger_ms, errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK) {
        fprintf(stderr, "Ошибка настройки linger.ms для producer %d: %s\n", producer_id, errstr);
        rd_kafka_conf_destroy(conf);
        return -1;
    }

    char batch_size[32];
    snprintf(batch_size, sizeof(batch_size), "%d", config->batch_size);
    if (rd_kafka_conf_set(conf, "batch.size", batch_size, errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK) {
        fprintf(stderr, "Ошибка настройки batch.size для producer %d: %s\n", producer_id, errstr);
        rd_kafka_conf_destroy(conf);
        return -1;
    }

    char max_in_flight[32];
    snprintf(max_in_flight, sizeof(max_in_flight), "%d", 
             config->disable_idempotence ? config->max_in_flight : 1);
    if (rd_kafka_conf_set(conf, "max.in.flight.requests.per.connection", max_in_flight, errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK) {
        fprintf(stderr, "Ошибка настройки max.in.flight.requests.per.connection для producer %d: %s\n", producer_id, errstr);
        rd_kafka_conf_destroy(conf);
        return -1;
    }

    char enable_idempotence[8];
    snprintf(enable_idempotence, sizeof(enable_idempotence), "%s", 
             config->disable_idempotence ? "false" : "true");
    if (rd_kafka_conf_set(conf, "enable.idempotence", enable_idempotence, errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK) {
        fprintf(stderr, "Ошибка настройки enable.idempotence для producer %d: %s\n", producer_id, errstr);
        rd_kafka_conf_destroy(conf);
        return -1;
    }

    char queue_buffering_max_ms[32];
    snprintf(queue_buffering_max_ms, sizeof(queue_buffering_max_ms), "%d", config->linger_ms / 2);
    if (rd_kafka_conf_set(conf, "queue.buffering.max.ms", queue_buffering_max_ms, errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK) {
        fprintf(stderr, "Ошибка настройки queue.buffering.max.ms для producer %d: %s\n", producer_id, errstr);
        rd_kafka_conf_destroy(conf);
        return -1;
    }

    char queue_buffering_max_messages[32];
    int max_messages;
    if (config->cross_cluster_mode) {
        max_messages = (config->num_workers > 16) ? config->batch_count * 200 : config->batch_count * 100;
        if (max_messages > 10000000) max_messages = 10000000;
    } else {
        max_messages = (config->num_workers > 16) ? config->batch_count * 500 : config->batch_count * 300;
        if (max_messages > 10000000) max_messages = 10000000;
    }
    snprintf(queue_buffering_max_messages, sizeof(queue_buffering_max_messages), "%d", max_messages);
    if (rd_kafka_conf_set(conf, "queue.buffering.max.messages", queue_buffering_max_messages, errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK) {
        fprintf(stderr, "Ошибка настройки queue.buffering.max.messages для producer %d: %s\n", producer_id, errstr);
        rd_kafka_conf_destroy(conf);
        return -1;
    }

    char batch_num_messages[32];
    int batch_num = (config->num_workers > 16) ? config->batch_count * 12 : config->batch_count * 8;
    if (batch_num > 1000000) batch_num = 1000000;
    snprintf(batch_num_messages, sizeof(batch_num_messages), "%d", batch_num);
    if (rd_kafka_conf_set(conf, "batch.num.messages", batch_num_messages, errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK) {
        fprintf(stderr, "Ошибка настройки batch.num.messages для producer %d: %s\n", producer_id, errstr);
        rd_kafka_conf_destroy(conf);
        return -1;
    }

    char socket_send_buffer_bytes[32];
    int buffer_divisor = (config->num_workers > 16) ? 4 : 8;
    long long socket_buffer_size = config->buffer_memory / buffer_divisor;
    if (socket_buffer_size > 100000000) {
        socket_buffer_size = 100000000;
    }
    snprintf(socket_send_buffer_bytes, sizeof(socket_send_buffer_bytes), "%lld", socket_buffer_size);
    if (rd_kafka_conf_set(conf, "socket.send.buffer.bytes", socket_send_buffer_bytes, errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK) {
        fprintf(stderr, "Ошибка настройки socket.send.buffer.bytes для producer %d: %s\n", producer_id, errstr);
        rd_kafka_conf_destroy(conf);
        return -1;
    }

    char socket_receive_buffer_bytes[32];
    snprintf(socket_receive_buffer_bytes, sizeof(socket_receive_buffer_bytes), "%lld", socket_buffer_size);
    if (rd_kafka_conf_set(conf, "socket.receive.buffer.bytes", socket_receive_buffer_bytes, errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK) {
        fprintf(stderr, "Ошибка настройки socket.receive.buffer.bytes для producer %d: %s\n", producer_id, errstr);
        rd_kafka_conf_destroy(conf);
        return -1;
    }

    security_protocol_t security_protocol = config->cross_cluster_mode ? config->target_security_protocol : config->security_protocol;
    
    if (security_protocol == SECURITY_SSL) {
        if (rd_kafka_conf_set(conf, "security.protocol", "ssl", errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK) {
            fprintf(stderr, "Ошибка настройки security.protocol для producer %d: %s\n", producer_id, errstr);
            rd_kafka_conf_destroy(conf);
            return -1;
        }
        
        const char *ssl_cafile = config->cross_cluster_mode ? config->target_ssl_cafile : config->ssl_cafile;
        const char *ssl_certfile = config->cross_cluster_mode ? config->target_ssl_certfile : config->ssl_certfile;
        const char *ssl_keyfile = config->cross_cluster_mode ? config->target_ssl_keyfile : config->ssl_keyfile;
        
        if (strlen(ssl_cafile) > 0) {
            if (rd_kafka_conf_set(conf, "ssl.ca.location", ssl_cafile, errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK) {
                fprintf(stderr, "Ошибка настройки ssl.ca.location для producer %d: %s\n", producer_id, errstr);
                rd_kafka_conf_destroy(conf);
                return -1;
            }
        }
        
        if (strlen(ssl_certfile) > 0) {
            if (rd_kafka_conf_set(conf, "ssl.certificate.location", ssl_certfile, errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK) {
                fprintf(stderr, "Ошибка настройки ssl.certificate.location для producer %d: %s\n", producer_id, errstr);
                rd_kafka_conf_destroy(conf);
                return -1;
            }
        }
        
        if (strlen(ssl_keyfile) > 0) {
            if (rd_kafka_conf_set(conf, "ssl.key.location", ssl_keyfile, errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK) {
                fprintf(stderr, "Ошибка настройки ssl.key.location для producer %d: %s\n", producer_id, errstr);
                rd_kafka_conf_destroy(conf);
                return -1;
            }
        }
    } else if (security_protocol == SECURITY_SASL_PLAINTEXT || 
               security_protocol == SECURITY_SASL_SSL) {
        
        const char *security_protocol_str = (security_protocol == SECURITY_SASL_SSL) ? "sasl_ssl" : "sasl_plaintext";
        if (rd_kafka_conf_set(conf, "security.protocol", security_protocol_str, errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK) {
            fprintf(stderr, "Ошибка настройки security.protocol для producer %d: %s\n", producer_id, errstr);
            rd_kafka_conf_destroy(conf);
            return -1;
        }
        
        sasl_mechanism_t sasl_mechanism = config->cross_cluster_mode ? config->target_sasl_mechanism : config->sasl_mechanism;
        const char *sasl_mechanism_str = sasl_mechanism_to_string(sasl_mechanism);
        if (rd_kafka_conf_set(conf, "sasl.mechanism", sasl_mechanism_str, errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK) {
            fprintf(stderr, "Ошибка настройки sasl.mechanism для producer %d: %s\n", producer_id, errstr);
            rd_kafka_conf_destroy(conf);
            return -1;
        }
        
        const char *sasl_username = config->cross_cluster_mode ? config->target_sasl_username : config->sasl_username;
        const char *sasl_password = config->cross_cluster_mode ? config->target_sasl_password : config->sasl_password;
        
        if (rd_kafka_conf_set(conf, "sasl.username", sasl_username, errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK) {
            fprintf(stderr, "Ошибка настройки sasl.username для producer %d: %s\n", producer_id, errstr);
            rd_kafka_conf_destroy(conf);
            return -1;
        }
        
        if (rd_kafka_conf_set(conf, "sasl.password", sasl_password, errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK) {
            fprintf(stderr, "Ошибка настройки sasl.password для producer %d: %s\n", producer_id, errstr);
            rd_kafka_conf_destroy(conf);
            return -1;
        }
    }

    if (rd_kafka_conf_set(conf, "partitioner", "consistent", errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK) {
        fprintf(stderr, "Ошибка настройки partitioner для producer %d: %s\n", producer_id, errstr);
        rd_kafka_conf_destroy(conf);
        return -1;
    }


    mirror->producers[producer_id] = rd_kafka_new(RD_KAFKA_PRODUCER, conf, errstr, sizeof(errstr));
    if (!mirror->producers[producer_id]) {
        fprintf(stderr, "Ошибка создания producer %d: %s\n", producer_id, errstr);
        return -1;
    }

    printf("Producer %d создан успешно\n", producer_id);
    
    if (config->cross_cluster_mode) {
        security_protocol_t security_protocol = config->target_security_protocol;
        const char *security_protocol_str = (security_protocol == SECURITY_SASL_SSL) ? "sasl_ssl" : "sasl_plaintext";
    }
    return 0;
}

void kafka_mirror_destroy_producer_pool(kafka_mirror_t *mirror) {
    if (!mirror || !mirror->producers) return;
    
    for (int i = 0; i < mirror->producer_count; i++) {
        if (mirror->producers[i]) {
            rd_kafka_destroy(mirror->producers[i]);
            mirror->producers[i] = NULL;
        }
    }
    
    free(mirror->producers);
    mirror->producers = NULL;
    mirror->producer_count = 0;
    
    pthread_mutex_destroy(&mirror->producer_mutex);
}

rd_kafka_t* kafka_mirror_get_producer(kafka_mirror_t *mirror, int worker_id) {
    if (!mirror || !mirror->producers || mirror->producer_count == 0) {
        return NULL;
    }
    
    return mirror->producers[worker_id % mirror->producer_count];
}

const char* security_protocol_to_string(security_protocol_t protocol) {
    switch (protocol) {
        case SECURITY_PLAINTEXT: return "plaintext";
        case SECURITY_SSL: return "ssl";
        case SECURITY_SASL_PLAINTEXT: return "sasl_plaintext";
        case SECURITY_SASL_SSL: return "sasl_ssl";
        default: return "unknown";
    }
}

const char* sasl_mechanism_to_string(sasl_mechanism_t mechanism) {
    switch (mechanism) {
        case SASL_MECHANISM_PLAIN: return "PLAIN";
        case SASL_MECHANISM_SCRAM_SHA_256: return "SCRAM-SHA-256";
        case SASL_MECHANISM_SCRAM_SHA_512: return "SCRAM-SHA-512";
        default: return "PLAIN";
    }
}

security_protocol_t string_to_security_protocol(const char *str) {
    if (!str) return SECURITY_PLAINTEXT;
    
    if (strcmp(str, "PLAINTEXT") == 0) return SECURITY_PLAINTEXT;
    if (strcmp(str, "SSL") == 0) return SECURITY_SSL;
    if (strcmp(str, "SASL_PLAINTEXT") == 0) return SECURITY_SASL_PLAINTEXT;
    if (strcmp(str, "SASL_SSL") == 0) return SECURITY_SASL_SSL;
    
    return SECURITY_PLAINTEXT;
}

sasl_mechanism_t string_to_sasl_mechanism(const char *str) {
    if (!str) return SASL_MECHANISM_PLAIN;
    
    if (strcmp(str, "PLAIN") == 0) return SASL_MECHANISM_PLAIN;
    if (strcmp(str, "SCRAM-SHA-256") == 0) return SASL_MECHANISM_SCRAM_SHA_256;
    if (strcmp(str, "SCRAM-SHA-512") == 0) return SASL_MECHANISM_SCRAM_SHA_512;
    
    return SASL_MECHANISM_PLAIN;
}

int main(int argc, char *argv[]) {
    kafka_mirror_config_t config;
    kafka_mirror_t mirror;
    int ret = 0;

    if (parse_arguments(argc, argv, &config) != 0) {
        print_usage(argv[0]);
        return 1;
    }

    print_config(&config);

    if (validate_config(&config) != 0) {
        fprintf(stderr, "Ошибка валидации конфигурации\n");
        return 1;
    }
    
    if (config.cross_cluster_mode && kafka_mirror_validate_cross_cluster_config(&config) != 0) {
        fprintf(stderr, "Ошибка валидации cross-cluster конфигурации\n");
        return 1;
    }

    g_mirror = &mirror;
    kafka_mirror_setup_signal_handlers();

    if (kafka_mirror_init(&mirror, &config) != 0) {
        fprintf(stderr, "Ошибка инициализации mirror\n");
        return 1;
    }

    if (kafka_mirror_check_connection(&config) != 0) {
        fprintf(stderr, "Ошибка проверки соединения\n");
        kafka_mirror_cleanup(&mirror, false);
        return 1;
    }

    if (kafka_mirror_create_consumer(&mirror) != 0) {
        fprintf(stderr, "Ошибка создания consumer\n");
        kafka_mirror_cleanup(&mirror, false);
        return 1;
    }
    
    if (config.cross_cluster_mode) {
        if (kafka_mirror_init_cross_cluster(&mirror) != 0) {
            fprintf(stderr, "Ошибка инициализации cross-cluster режима\n");
            kafka_mirror_cleanup(&mirror, false);
            return 1;
        }
    }

    if (strlen(config.target_topic) > 0) {
        int partition_count = kafka_mirror_get_source_topic_partitions(&config, config.topics[0]);
        if (partition_count <= 0) {
            fprintf(stderr, "Ошибка получения количества партиций исходного топика\n");
            kafka_mirror_cleanup(&mirror, false);
            return 1;
        }
        
        if (kafka_mirror_create_topic_if_not_exists(&config, config.target_topic, partition_count) != 0) {
            fprintf(stderr, "Ошибка создания целевого топика\n");
            kafka_mirror_cleanup(&mirror, false);
            return 1;
        }
    }

    if (kafka_mirror_create_producer_pool(&mirror) != 0) {
        fprintf(stderr, "Ошибка создания producer pool\n");
        kafka_mirror_cleanup(&mirror, false);
        return 1;
    }

    ret = kafka_mirror_run(&mirror);

    bool graceful = (ret == 0);
    kafka_mirror_cleanup(&mirror, graceful);
    
    return ret;
}
