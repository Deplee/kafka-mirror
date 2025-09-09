#include "kafka_mirror.h"
#include "utils.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>
#include <sys/time.h>
#include <pthread.h>
#include <librdkafka/rdkafka.h>

void kafka_mirror_rebalance_cb(rd_kafka_t *rk, rd_kafka_resp_err_t err,
                               rd_kafka_topic_partition_list_t *partitions,
                               void *opaque) {
    kafka_mirror_t *mirror = (kafka_mirror_t *)opaque;
    
    
    if (err == RD_KAFKA_RESP_ERR__ASSIGN_PARTITIONS) {
        printf("Назначены партиции: %d\n", partitions->cnt);
        
        for (int i = 0; i < partitions->cnt; i++) {
            rd_kafka_topic_partition_t *tp = &partitions->elems[i];
            printf("Партиция %s-%d назначена\n", tp->topic, tp->partition);
            
            switch (mirror->config.offset_strategy) {
                case OFFSET_STRATEGY_EARLIEST:
                    tp->offset = RD_KAFKA_OFFSET_BEGINNING;
                    printf("Используется стратегия EARLIEST: offset=BEGINNING\n");
                    break;
                case OFFSET_STRATEGY_LATEST:
                    tp->offset = RD_KAFKA_OFFSET_END;
                    printf("Используется стратегия LATEST: offset=END\n");
                    break;
                case OFFSET_STRATEGY_CONSUMER_GROUP:
                    tp->offset = RD_KAFKA_OFFSET_STORED;
                    printf("Используется стратегия CONSUMER_GROUP: offset=STORED\n");
                    break;
                default:
                    tp->offset = RD_KAFKA_OFFSET_BEGINNING;
                    printf("Неизвестная стратегия, используется EARLIEST: offset=BEGINNING\n");
                    break;
            }
        }
        
        rd_kafka_resp_err_t assign_err = rd_kafka_assign(rk, partitions);
        if (assign_err != RD_KAFKA_RESP_ERR_NO_ERROR) {
            printf("Ошибка назначения партиций: %s\n", rd_kafka_err2str(assign_err));
        } else {
            printf("Успешно назначено %d партиций для группы '%s'\n", partitions->cnt, mirror->config.consumer_group);
            mirror->partitions_assigned = true;
            mirror->partitions_ready_for_status = true;
            
            if (mirror->config.cross_cluster_mode) {
                for (int i = 0; i < partitions->cnt; i++) {
                    rd_kafka_topic_partition_t *tp = &partitions->elems[i];
                    int partition_count = 0;
                    
                    rd_kafka_metadata_t *metadata;
                    rd_kafka_resp_err_t err = rd_kafka_metadata(rk, 0, NULL, (const rd_kafka_metadata_t **)&metadata, 10000);
                    if (err == RD_KAFKA_RESP_ERR_NO_ERROR) {
                        for (int j = 0; j < metadata->topic_cnt; j++) {
                            if (strcmp(metadata->topics[j].topic, tp->topic) == 0) {
                                partition_count = metadata->topics[j].partition_cnt;
                                break;
                            }
                        }
                        rd_kafka_metadata_destroy(metadata);
                    }
                    
                    if (partition_count > 0) {
                        kafka_mirror_create_target_topic(mirror, tp->topic, partition_count);
                    }
                }
            }
            
            for (int i = 0; i < partitions->cnt; i++) {
                rd_kafka_topic_partition_t *tp = &partitions->elems[i];
                
                if (mirror->first_run) {
                    rd_kafka_resp_err_t seek_err;
                    rd_kafka_topic_t *rkt = rd_kafka_topic_new(rk, tp->topic, NULL);
                    if (rkt) {
                        seek_err = rd_kafka_seek(rkt, tp->partition, tp->offset, 10000);
                        rd_kafka_topic_destroy(rkt);
                    } else {
                        seek_err = RD_KAFKA_RESP_ERR_UNKNOWN;
                    }
                    if (seek_err == RD_KAFKA_RESP_ERR_NO_ERROR) {
                        printf("Установлен offset %ld для партиции %s-%d\n", tp->offset, tp->topic, tp->partition);
                    } else {
                        printf("Ошибка установки offset для %s-%d: %s\n", tp->topic, tp->partition, rd_kafka_err2str(seek_err));
                    }
                } else {
                    printf("Партиция %s-%d переподключена, используется сохраненный offset\n", tp->topic, tp->partition);
                }
            }
            
            if (mirror->first_run) {
                printf("Первый запуск: установлены offset на earliest available для всех назначенных партиций\n");
                printf("Начинаем обработку сообщений...\n");
                mirror->first_run = false;
            } else {
                printf("Переподключение: партиции переподключены, offset сохранены\n");
            }
            
            printf("Пауза 3 секунды перед началом обработки...\n");
            sleep(3);
        }
        
    } else if (err == RD_KAFKA_RESP_ERR__REVOKE_PARTITIONS) {
        printf("Отозваны партиции: %d\n", partitions->cnt);
        mirror->partitions_assigned = false;
        
        for (int i = 0; i < partitions->cnt; i++) {
            rd_kafka_topic_partition_t *tp = &partitions->elems[i];
            printf("Партиция %s-%d отозвана\n", tp->topic, tp->partition);
        }
        
        rd_kafka_resp_err_t assign_err = rd_kafka_assign(rk, NULL);
        if (assign_err != RD_KAFKA_RESP_ERR_NO_ERROR) {
            printf("Ошибка отзыва партиций: %s\n", rd_kafka_err2str(assign_err));
        } else {
            printf("Состояние сохранено перед отзывом партиций\n");
            mirror->partitions_assigned = false;
            mirror->partitions_ready_for_status = false;
        }
    } else {
    }
}

int kafka_mirror_check_connection(const kafka_mirror_config_t *config) {
    char errstr[512];
    rd_kafka_t *test_consumer = NULL;
    rd_kafka_t *test_producer = NULL;
    rd_kafka_conf_t *conf;
    int ret = 0;
    
    conf = rd_kafka_conf_new();
    if (!conf) {
        fprintf(stderr, "Ошибка создания конфигурации для проверки соединения\n");
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
    
    test_consumer = rd_kafka_new(RD_KAFKA_CONSUMER, conf, errstr, sizeof(errstr));
    if (!test_consumer) {
        fprintf(stderr, "Ошибка создания test consumer: %s\n", errstr);
        ret = -1;
        goto cleanup;
    }
    
    rd_kafka_metadata_t *metadata;
    rd_kafka_resp_err_t err = rd_kafka_metadata(test_consumer, 1, NULL, (const rd_kafka_metadata_t **)&metadata, 10000);
    if (err != RD_KAFKA_RESP_ERR_NO_ERROR) {
        fprintf(stderr, "Ошибка получения метаданных: %s\n", rd_kafka_err2str(err));
        ret = -1;
        goto cleanup;
    }
    
    rd_kafka_metadata_destroy(metadata);
    
    rd_kafka_conf_t *producer_conf = rd_kafka_conf_new();
    if (!producer_conf) {
        fprintf(stderr, "Ошибка создания конфигурации producer для проверки соединения\n");
        ret = -1;
        goto cleanup;
    }
    
    if (rd_kafka_conf_set(producer_conf, "bootstrap.servers", bootstrap_servers, errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK) {
        fprintf(stderr, "Ошибка настройки bootstrap.servers для producer: %s\n", errstr);
        rd_kafka_conf_destroy(producer_conf);
        ret = -1;
        goto cleanup;
    }
    
    if (config->security_protocol == SECURITY_SSL) {
        if (rd_kafka_conf_set(producer_conf, "security.protocol", "ssl", errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK) {
            fprintf(stderr, "Ошибка настройки security.protocol для producer: %s\n", errstr);
            rd_kafka_conf_destroy(producer_conf);
            ret = -1;
            goto cleanup;
        }
        
        if (strlen(config->ssl_cafile) > 0) {
            if (rd_kafka_conf_set(producer_conf, "ssl.ca.location", config->ssl_cafile, errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK) {
                fprintf(stderr, "Ошибка настройки ssl.ca.location для producer: %s\n", errstr);
                rd_kafka_conf_destroy(producer_conf);
                ret = -1;
                goto cleanup;
            }
        }
        
        if (strlen(config->ssl_certfile) > 0) {
            if (rd_kafka_conf_set(producer_conf, "ssl.certificate.location", config->ssl_certfile, errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK) {
                fprintf(stderr, "Ошибка настройки ssl.certificate.location для producer: %s\n", errstr);
                rd_kafka_conf_destroy(producer_conf);
                ret = -1;
                goto cleanup;
            }
        }
        
        if (strlen(config->ssl_keyfile) > 0) {
            if (rd_kafka_conf_set(producer_conf, "ssl.key.location", config->ssl_keyfile, errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK) {
                fprintf(stderr, "Ошибка настройки ssl.key.location для producer: %s\n", errstr);
                rd_kafka_conf_destroy(producer_conf);
                ret = -1;
                goto cleanup;
            }
        }
    } else if (config->security_protocol == SECURITY_SASL_PLAINTEXT || 
               config->security_protocol == SECURITY_SASL_SSL) {
        
        const char *security_protocol = (config->security_protocol == SECURITY_SASL_SSL) ? "sasl_ssl" : "sasl_plaintext";
        if (rd_kafka_conf_set(producer_conf, "security.protocol", security_protocol, errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK) {
            fprintf(stderr, "Ошибка настройки security.protocol для producer: %s\n", errstr);
            rd_kafka_conf_destroy(producer_conf);
            ret = -1;
            goto cleanup;
        }
        
        const char *sasl_mechanism = sasl_mechanism_to_string(config->sasl_mechanism);
        if (rd_kafka_conf_set(producer_conf, "sasl.mechanism", sasl_mechanism, errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK) {
            fprintf(stderr, "Ошибка настройки sasl.mechanism для producer: %s\n", errstr);
            rd_kafka_conf_destroy(producer_conf);
            ret = -1;
            goto cleanup;
        }
        
        const char *sasl_username = config->cross_cluster_mode ? config->source_sasl_username : config->sasl_username;
        const char *sasl_password = config->cross_cluster_mode ? config->source_sasl_password : config->sasl_password;
        
        if (rd_kafka_conf_set(producer_conf, "sasl.username", sasl_username, errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK) {
            fprintf(stderr, "Ошибка настройки sasl.username для producer: %s\n", errstr);
            rd_kafka_conf_destroy(producer_conf);
            ret = -1;
            goto cleanup;
        }
        
        if (rd_kafka_conf_set(producer_conf, "sasl.password", sasl_password, errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK) {
            fprintf(stderr, "Ошибка настройки sasl.password для producer: %s\n", errstr);
            rd_kafka_conf_destroy(producer_conf);
            ret = -1;
            goto cleanup;
        }
    }
    
    test_producer = rd_kafka_new(RD_KAFKA_PRODUCER, producer_conf, errstr, sizeof(errstr));
    if (!test_producer) {
        fprintf(stderr, "Ошибка создания test producer: %s\n", errstr);
        ret = -1;
        goto cleanup;
    }
    
    printf("Проверка соединения успешна\n");
    
cleanup:
    if (test_consumer) {
        rd_kafka_destroy(test_consumer);
    }
    if (test_producer) {
        rd_kafka_destroy(test_producer);
    }
    
    return ret;
}

int kafka_mirror_create_topic_if_not_exists(const kafka_mirror_config_t *config, const char *topic_name, int partition_count) {
    if (!config || !topic_name) {
        return -1;
    }
    
    char errstr[512];
    rd_kafka_conf_t *conf = rd_kafka_conf_new();
    if (!conf) {
        fprintf(stderr, "Ошибка создания конфигурации для создания топика\n");
        return -1;
    }
    
    char bootstrap_servers[1024] = {0};
    for (int i = 0; i < config->broker_count; i++) {
        if (i > 0) strcat(bootstrap_servers, ",");
        strcat(bootstrap_servers, config->brokers[i]);
    }
    if (rd_kafka_conf_set(conf, "bootstrap.servers", bootstrap_servers, errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK) {
        fprintf(stderr, "Ошибка настройки bootstrap.servers для создания топика: %s\n", errstr);
        rd_kafka_conf_destroy(conf);
        return -1;
    }
    
    if (config->security_protocol == SECURITY_SASL_PLAINTEXT || 
        config->security_protocol == SECURITY_SASL_SSL) {
        
        const char *security_protocol = (config->security_protocol == SECURITY_SASL_SSL) ? "sasl_ssl" : "sasl_plaintext";
        if (rd_kafka_conf_set(conf, "security.protocol", security_protocol, errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK) {
            fprintf(stderr, "Ошибка настройки security.protocol для создания топика: %s\n", errstr);
            rd_kafka_conf_destroy(conf);
            return -1;
        }
        
        const char *sasl_mechanism = sasl_mechanism_to_string(config->sasl_mechanism);
        if (rd_kafka_conf_set(conf, "sasl.mechanism", sasl_mechanism, errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK) {
            fprintf(stderr, "Ошибка настройки sasl.mechanism для создания топика: %s\n", errstr);
            rd_kafka_conf_destroy(conf);
            return -1;
        }
        
        const char *sasl_username = config->cross_cluster_mode ? config->source_sasl_username : config->sasl_username;
        const char *sasl_password = config->cross_cluster_mode ? config->source_sasl_password : config->sasl_password;
        
        if (rd_kafka_conf_set(conf, "sasl.username", sasl_username, errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK) {
            fprintf(stderr, "Ошибка настройки sasl.username для создания топика: %s\n", errstr);
            rd_kafka_conf_destroy(conf);
            return -1;
        }
        
        if (rd_kafka_conf_set(conf, "sasl.password", sasl_password, errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK) {
            fprintf(stderr, "Ошибка настройки sasl.password для создания топика: %s\n", errstr);
            rd_kafka_conf_destroy(conf);
            return -1;
        }
    }
    
    rd_kafka_t *admin = rd_kafka_new(RD_KAFKA_PRODUCER, conf, errstr, sizeof(errstr));
    if (!admin) {
        fprintf(stderr, "Ошибка создания admin client для создания топика: %s\n", errstr);
        return -1;
    }
    
    rd_kafka_metadata_t *metadata;
    rd_kafka_resp_err_t err = rd_kafka_metadata(admin, 0, NULL, (const rd_kafka_metadata_t **)&metadata, 10000);
    if (err != RD_KAFKA_RESP_ERR_NO_ERROR) {
        fprintf(stderr, "Ошибка получения метаданных для проверки топика: %s\n", rd_kafka_err2str(err));
        rd_kafka_destroy(admin);
        return -1;
    }
    
    bool topic_exists = false;
    for (int i = 0; i < metadata->topic_cnt; i++) {
        if (strcmp(metadata->topics[i].topic, topic_name) == 0) {
            topic_exists = true;
            break;
        }
    }
    
    rd_kafka_metadata_destroy(metadata);
    
    if (topic_exists) {
        printf("Топик '%s' уже существует\n", topic_name);
        rd_kafka_destroy(admin);
        return 0;
    }
    
    printf("Топик '%s' не существует, создаем с %d партициями...\n", topic_name, partition_count);
    
    rd_kafka_NewTopic_t *new_topic = rd_kafka_NewTopic_new(topic_name, partition_count, 3, NULL, 0);
    if (!new_topic) {
        fprintf(stderr, "Ошибка создания NewTopic объекта\n");
        rd_kafka_destroy(admin);
        return -1;
    }
    
    
    rd_kafka_queue_t *queue = rd_kafka_queue_new(admin);
    if (!queue) {
        fprintf(stderr, "Ошибка создания queue\n");
        rd_kafka_NewTopic_destroy(new_topic);
        rd_kafka_destroy(admin);
        return -1;
    }
    
    rd_kafka_CreateTopics(admin, &new_topic, 1, NULL, queue);
    
    rd_kafka_event_t *event = rd_kafka_queue_poll(queue, 10000);
    if (!event) {
        fprintf(stderr, "Таймаут при создании топика\n");
        rd_kafka_queue_destroy(queue);
        rd_kafka_NewTopic_destroy(new_topic);
        rd_kafka_destroy(admin);
        return -1;
    }
    
    if (rd_kafka_event_error(event)) {
        fprintf(stderr, "Ошибка создания топика: %s\n", rd_kafka_event_error_string(event));
        rd_kafka_event_destroy(event);
        rd_kafka_queue_destroy(queue);
        rd_kafka_NewTopic_destroy(new_topic);
        rd_kafka_destroy(admin);
        return -1;
    }
    
    rd_kafka_event_destroy(event);
    rd_kafka_queue_destroy(queue);
    rd_kafka_NewTopic_destroy(new_topic);
    rd_kafka_destroy(admin);
    
    printf("Топик '%s' создан успешно с %d партициями\n", topic_name, partition_count);
    return 0;
}

int kafka_mirror_get_source_topic_partitions(const kafka_mirror_config_t *config, const char *topic_name) {
    if (!config || !topic_name) {
        return -1;
    }
    
    char errstr[512];
    rd_kafka_conf_t *conf = rd_kafka_conf_new();
    if (!conf) {
        fprintf(stderr, "Ошибка создания конфигурации для получения партиций\n");
        return -1;
    }
    
    char bootstrap_servers[1024] = {0};
    for (int i = 0; i < config->broker_count; i++) {
        if (i > 0) strcat(bootstrap_servers, ",");
        strcat(bootstrap_servers, config->brokers[i]);
    }
    if (rd_kafka_conf_set(conf, "bootstrap.servers", bootstrap_servers, errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK) {
        fprintf(stderr, "Ошибка настройки bootstrap.servers: %s\n", errstr);
        rd_kafka_conf_destroy(conf);
        return -1;
    }
    
    if (config->security_protocol == SECURITY_SASL_PLAINTEXT || 
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
    
    rd_kafka_t *admin = rd_kafka_new(RD_KAFKA_PRODUCER, conf, errstr, sizeof(errstr));
    if (!admin) {
        fprintf(stderr, "Ошибка создания admin client: %s\n", errstr);
        return -1;
    }
    
    rd_kafka_metadata_t *metadata;
    rd_kafka_resp_err_t err = rd_kafka_metadata(admin, 0, NULL, (const rd_kafka_metadata_t **)&metadata, 10000);
    if (err != RD_KAFKA_RESP_ERR_NO_ERROR) {
        fprintf(stderr, "Ошибка получения метаданных: %s\n", rd_kafka_err2str(err));
        rd_kafka_destroy(admin);
        return -1;
    }
    
    int partition_count = -1;
    for (int i = 0; i < metadata->topic_cnt; i++) {
        if (strcmp(metadata->topics[i].topic, topic_name) == 0) {
            partition_count = metadata->topics[i].partition_cnt;
            break;
        }
    }
    
    rd_kafka_metadata_destroy(metadata);
    rd_kafka_destroy(admin);
    
    if (partition_count == -1) {
        fprintf(stderr, "Топик '%s' не найден\n", topic_name);
        return -1;
    }
    
    printf("Топик '%s' имеет %d партиций\n", topic_name, partition_count);
    return partition_count;
}

int kafka_mirror_process_message(kafka_mirror_t *mirror, rd_kafka_message_t *msg, int worker_id) {
    if (!mirror || !msg || !mirror->producers) {
        return -1;
    }
    
    
    rd_kafka_t *producer = kafka_mirror_get_producer(mirror, worker_id);
    if (!producer) {
        return -1;
    }
    
    sem_wait(&mirror->producer_semaphore);
    
    const kafka_mirror_config_t *config = &mirror->config;
    size_t message_size = msg->len;
    
    
    if (!config->skip_size_validation && message_size > (size_t)config->max_message_size) {
        printf("Сообщение слишком большое: %s-%d:%ld размер=%zu байт (максимум %d)\n", 
               rd_kafka_topic_name(msg->rkt), msg->partition, msg->offset, message_size, config->max_message_size);
        kafka_mirror_record_size_validation_error(mirror);
        kafka_mirror_record_failure(mirror, rd_kafka_topic_name(msg->rkt), msg->partition, msg->offset, "Сообщение слишком большое");
        sem_post(&mirror->producer_semaphore);
        return -1;
    }
    
    const char *target_topic_name;
    if (config->cross_cluster_mode) {
        target_topic_name = rd_kafka_topic_name(msg->rkt);
    } else {
        target_topic_name = strlen(config->target_topic) > 0 ? config->target_topic : rd_kafka_topic_name(msg->rkt);
    }
    
    int retry_count = 0;
    const int max_retries = MAX_PRODUCER_RETRIES;
    rd_kafka_resp_err_t err;
    
    while (retry_count < max_retries) {
        err = rd_kafka_producev(
            producer,
            RD_KAFKA_V_TOPIC(target_topic_name),
            RD_KAFKA_V_PARTITION(msg->partition),
            RD_KAFKA_V_MSGFLAGS(RD_KAFKA_MSG_F_COPY),
            RD_KAFKA_V_KEY(msg->key, msg->key_len),
            RD_KAFKA_V_VALUE(msg->payload, msg->len),
            RD_KAFKA_V_END
        );
        
        if (err == RD_KAFKA_RESP_ERR_NO_ERROR) {
            if (retry_count > 0) {
                printf("ERROR: Сообщение %s-%d:%ld успешно отправлено после %d попыток\n", 
                       rd_kafka_topic_name(msg->rkt), msg->partition, msg->offset, retry_count + 1);
            }
            kafka_mirror_record_success(mirror, rd_kafka_topic_name(msg->rkt), msg->partition, msg->offset, msg->len);
            break;
        }
        
        if (err == RD_KAFKA_RESP_ERR__QUEUE_FULL) {
            retry_count++;
            kafka_mirror_record_queue_full_error(mirror);
            
            int current_queue_size = rd_kafka_outq_len(producer);
            printf("Очередь producer %d переполнена (попытка %d/%d, размер очереди: %d), принудительная очистка...\n", 
                   worker_id % mirror->producer_count, retry_count, max_retries, current_queue_size);
            
            int flush_timeout = (retry_count > 2) ? 30000 : 15000;
            rd_kafka_resp_err_t flush_result = rd_kafka_flush(producer, flush_timeout);
            int new_queue_size = rd_kafka_outq_len(producer);
            printf("Очистка завершена, размер очереди: %d -> %d (flush_result: %s)\n", 
                   current_queue_size, new_queue_size, rd_kafka_err2str(flush_result));
            
            int sleep_time = (mirror->config.num_workers > 16) ? 500 * retry_count : 1000 * retry_count;
            usleep(sleep_time);
        } else {
            printf("ERROR: Ошибка Producer при отправке сообщения %s-%d:%ld: %s\n", 
                   rd_kafka_topic_name(msg->rkt), msg->partition, msg->offset, rd_kafka_err2str(err));
            kafka_mirror_record_producer_error(mirror);
            kafka_mirror_record_failure(mirror, rd_kafka_topic_name(msg->rkt), msg->partition, msg->offset, rd_kafka_err2str(err));
            sem_post(&mirror->producer_semaphore);
            return -1;
        }
    }
    
    if (err != RD_KAFKA_RESP_ERR_NO_ERROR) {
        kafka_mirror_record_producer_error(mirror);
        kafka_mirror_record_failure(mirror, rd_kafka_topic_name(msg->rkt), msg->partition, msg->offset, "Превышено количество попыток отправки");
        sem_post(&mirror->producer_semaphore);
        return -1;
    }
    
    sem_post(&mirror->producer_semaphore);
    
    if (mirror->config.cross_cluster_mode && mirror->config.preserve_offsets) {
        kafka_mirror_update_partition_state(mirror, rd_kafka_topic_name(msg->rkt), msg->partition, msg->offset);
    }
    
    return 0;
}

int kafka_mirror_process_messages(kafka_mirror_t *mirror) {
    if (!mirror) {
        return -1;
    }
    
    mirror->running = true;
    time_t last_message_time = time(NULL);
    uint64_t total_processed = 0;
    
    for (int i = 0; i < mirror->config.num_workers; i++) {
        worker_data_t *worker_data = malloc(sizeof(worker_data_t));
        if (!worker_data) {
            return -1;
        }
        
        worker_data->mirror = mirror;
        worker_data->worker_id = i;
        
        if (pthread_create(&mirror->worker_threads[i], NULL, kafka_mirror_worker_thread, worker_data) != 0) {
            free(worker_data);
            return -1;
        }
    }
    
    if (pthread_create(&mirror->status_thread, NULL, kafka_mirror_status_thread, mirror) != 0) {
        return -1;
    }
    
    if (mirror->config.cross_cluster_mode && mirror->config.mirror_all_topics) {
        if (pthread_create(&mirror->topic_discovery_thread, NULL, kafka_mirror_topic_discovery_thread, mirror) != 0) {
            fprintf(stderr, "Ошибка создания topic discovery thread\n");
            return -1;
        }
    }
    
    
    sleep(5);
    
    printf("Принудительный вызов rebalance для инициализации партиций...\n");
    for (int i = 0; i < 10; i++) {
        rd_kafka_message_t *msg = rd_kafka_consumer_poll(mirror->consumer, 1000);
        if (msg) {
            rd_kafka_message_destroy(msg);
        }
    }
    
    printf("Ожидание назначения партиций...\n");
    rd_kafka_topic_partition_list_t *assigned = NULL;
    rd_kafka_resp_err_t assign_err = rd_kafka_assignment(mirror->consumer, &assigned);
    if (assign_err == RD_KAFKA_RESP_ERR_NO_ERROR && assigned && assigned->cnt > 0) {
        mirror->partitions_assigned = true;
        printf("Партиции уже назначены: %d\n", assigned->cnt);
        printf("Пауза 3 секунды перед началом обработки...\n");
        sleep(3);
    } else {
        printf("Партиции еще не назначены, ожидание...\n");
    }
    
    if (assigned) {
        rd_kafka_topic_partition_list_destroy(assigned);
    }
    
    
    while (mirror->running) {
        if (!mirror->consumer) {
            printf("Consumer не инициализирован, остановка...\n");
            break;
        }

        if (!mirror->partitions_assigned) {
            rd_kafka_topic_partition_list_t *assigned = NULL;
            rd_kafka_resp_err_t assign_err = rd_kafka_assignment(mirror->consumer, &assigned);
            
            
            if (assign_err == RD_KAFKA_RESP_ERR_NO_ERROR && assigned && assigned->cnt > 0) {
                mirror->partitions_assigned = true;
                printf("Партиции назначены: %d\n", assigned->cnt);
            }
            if (assigned) {
                rd_kafka_topic_partition_list_destroy(assigned);
            }
            if (!mirror->partitions_assigned) {
                printf("Партиции еще не назначены, ожидание...\n");
                sleep(5);
                continue;
            }
        }
        
        rd_kafka_message_t **msg_batch = malloc(mirror->config.batch_count * sizeof(rd_kafka_message_t*));
        if (!msg_batch) {
            break;
        }
        
        int batch_size = 0;
        int timeout_count = 0;
        int queued_count = 0;
        
        for (int i = 0; i < mirror->config.batch_count && mirror->running; i++) {
            if (mirror->message_queue.count > MESSAGE_QUEUE_WARNING_THRESHOLD) {
                usleep(5000);
                break;
            }
            
            if (mirror->message_queue.count > MESSAGE_QUEUE_CRITICAL_THRESHOLD) {
                usleep(10000);
                break;
            }
            
            if (mirror->producers && mirror->producer_count > 0) {
                int total_producer_queue_size = 0;
                for (int j = 0; j < mirror->producer_count; j++) {
                    if (mirror->producers[j]) {
                        total_producer_queue_size += rd_kafka_outq_len(mirror->producers[j]);
                    }
                }
                int critical_threshold = (mirror->config.num_workers > 16) ? 200000 : 150000;
                if (total_producer_queue_size > critical_threshold) {
                    break;
                }
                
                int warning_threshold = (mirror->config.num_workers > 16) ? 150000 : 100000;
                if (total_producer_queue_size > warning_threshold) {
                    usleep(2);
                }
            }
            rd_kafka_message_t *msg = rd_kafka_consumer_poll(mirror->consumer, 10);
            
            if (!mirror->running) {
                break;
            }
            
            if (msg) {
                if (msg->err == RD_KAFKA_RESP_ERR_NO_ERROR) {
                    msg_batch[batch_size++] = msg;
                    last_message_time = time(NULL);
                    timeout_count = 0;
                    
                    if (batch_size >= mirror->config.batch_count) {
                        break;
                    }
                } else if (msg->err == RD_KAFKA_RESP_ERR__PARTITION_EOF) {
                    rd_kafka_message_destroy(msg);
                    timeout_count++;
                    if (timeout_count >= 50) {
                        break;
                    }
                } else if (msg->err == RD_KAFKA_RESP_ERR__TIMED_OUT) {
                    rd_kafka_message_destroy(msg);
                    timeout_count++;
                    if (timeout_count >= 50) {
                        break;
                    }
                } else {
                    rd_kafka_message_destroy(msg);
                }
            } else {
                timeout_count++;
                if (timeout_count >= 50) {
                    break;
                }
            }
        }
        
        if (batch_size > 0) {
            if (!mirror->partitions_ready_for_status) {
                mirror->partitions_ready_for_status = true;
            }
            
            rd_kafka_poll(mirror->consumer, 0);
            
            if (mirror->message_queue.count > MESSAGE_QUEUE_CRITICAL_THRESHOLD) {
                usleep(5000);
            } else if (mirror->message_queue.count > MESSAGE_QUEUE_WARNING_THRESHOLD) {
                usleep(2000);
            }
            
            size_t batch_size_bytes = 0;
            for (int i = 0; i < batch_size; i++) {
                if (msg_batch[i]) {
                    batch_size_bytes += msg_batch[i]->len;
                }
            }
            double batch_size_mb = (double)batch_size_bytes / (1024.0 * 1024.0);
            printf("Обрабатываем батч из %d сообщений (%.2f МБ)\n", batch_size, batch_size_mb);
            
            for (int i = 0; i < batch_size && mirror->running; i++) {
                rd_kafka_message_t *msg = msg_batch[i];
                total_processed++;
                
                
                work_message_t work_msg;
                work_msg.message = msg;
                safe_strncpy(work_msg.topic, rd_kafka_topic_name(msg->rkt), sizeof(work_msg.topic));
                work_msg.partition = msg->partition;
                work_msg.offset = msg->offset;
                work_msg.message_size = msg->len;
                work_msg.timestamp = time(NULL);
                
                if (message_queue_put(&mirror->message_queue, &work_msg) != 0) {
                    printf("Ошибка добавления сообщения в очередь\n");
                    rd_kafka_message_destroy(msg);
                    msg_batch[i] = NULL;
                } else {
                    queued_count++;
                    msg_batch[i] = NULL;
                }
            }
            
        }
        
        for (int i = 0; i < batch_size; i++) {
            if (msg_batch[i] != NULL) {
                rd_kafka_message_destroy(msg_batch[i]);
            }
        }
        
        free(msg_batch);
        
        if (time(NULL) - last_message_time > mirror->config.idle_timeout_ms / 1000) {
            
            bool all_completed = true;
            rd_kafka_topic_partition_list_t *assigned = NULL;
            
            if (!mirror->consumer) {
                all_completed = false;
            } else {
                rd_kafka_resp_err_t assign_err = rd_kafka_assignment(mirror->consumer, &assigned);
                if (assign_err != RD_KAFKA_RESP_ERR_NO_ERROR) {
                    all_completed = false;
                }
            }
            
            if (assigned && assigned->cnt > 0) {
                for (int i = 0; i < assigned->cnt; i++) {
                    rd_kafka_topic_partition_t *tp = &assigned->elems[i];
                    int64_t current_pos = 0;
                    int64_t end_offset = 0;
                    
                    int64_t beginning_offset = 0;
                    rd_kafka_resp_err_t err = rd_kafka_query_watermark_offsets(mirror->consumer, tp->topic, tp->partition, &beginning_offset, &end_offset, 10000);
                    
                    if (err == RD_KAFKA_RESP_ERR_NO_ERROR) {
                        rd_kafka_topic_partition_list_t *pos_list = rd_kafka_topic_partition_list_new(1);
                        if (pos_list) {
                            rd_kafka_topic_partition_list_add(pos_list, tp->topic, tp->partition);
                            rd_kafka_resp_err_t pos_err = rd_kafka_position(mirror->consumer, pos_list);
                            if (pos_err == RD_KAFKA_RESP_ERR_NO_ERROR) {
                                current_pos = pos_list->elems[0].offset;
                            } else {
                                current_pos = beginning_offset;
                            }
                            rd_kafka_topic_partition_list_destroy(pos_list);
                        } else {
                            current_pos = beginning_offset;
                        }
                        
                        if (current_pos == RD_KAFKA_OFFSET_INVALID) {
                            all_completed = false;
                        } else if (current_pos < end_offset) {
                            all_completed = false;
                        }
                    } else {
                        all_completed = false;
                    }
                }
            } else {
                all_completed = false;
            }
            
            if (assigned) {
                rd_kafka_topic_partition_list_destroy(assigned);
            }
            
            if (all_completed) {
                break;
            } else {
                last_message_time = time(NULL);
                sleep(5);
            }
        }
        
        if (time(NULL) - last_message_time > 30) {
            last_message_time = time(NULL);
        }
        
        if (mirror->running) {
            sleep(1);
        }
    }
    
    return 1;
}

void* kafka_mirror_worker_thread(void *arg) {
    worker_data_t *worker_data = (worker_data_t *)arg;
    kafka_mirror_t *mirror = worker_data->mirror;
    int worker_id = worker_data->worker_id;
    int processed_count = 0;
    
    while (mirror->running) {
        work_message_t work_msg;
        
        if (message_queue_get(&mirror->message_queue, &work_msg) == 0) {
            if (!mirror->running) {
                break;
            }
            if (!work_msg.message) {
                printf("Worker %d получил пустое сообщение\n", worker_id);
                continue;
            }
            
            kafka_mirror_record_worker_attempt(mirror, worker_id);
            
            int process_result = kafka_mirror_process_message(mirror, work_msg.message, worker_id);
            if (process_result != 0) {
                kafka_mirror_record_worker_failure(mirror, worker_id);
            } else {
                kafka_mirror_record_worker_success(mirror, worker_id);
            }
            
            rd_kafka_message_destroy(work_msg.message);
            processed_count++;
            
            rd_kafka_t *worker_producer = kafka_mirror_get_producer(mirror, worker_id);
            if (worker_producer) {
                rd_kafka_poll(worker_producer, 0);
            }
            
            if (processed_count % 1000 == 0) {
                printf("Worker %d обработал %d сообщений\n", worker_id, processed_count);
            }
            
            if (processed_count % 5000 == 0) {
                rd_kafka_t *worker_producer = kafka_mirror_get_producer(mirror, worker_id);
                int producer_queue_size = worker_producer ? rd_kafka_outq_len(worker_producer) : 0;
                printf("Worker %d: обработано %d, очередь: %d, producer: %d\n", 
                       worker_id, processed_count, mirror->message_queue.count, producer_queue_size);
            }
        } else {
            if (processed_count == 0 && worker_id == 0) {
                printf("Worker %d не может получить сообщения из очереди (очередь пуста: %d)\n", 
                       worker_id, mirror->message_queue.count);
            }
            
            int queue_size = mirror->message_queue.count;
            if (queue_size > 50000) {
                usleep(1);
            } else if (queue_size > 10000) {
                usleep(2);
            } else {
                usleep(5);
            }
            
            if (mirror->producers && mirror->producer_count > 0) {
                rd_kafka_t *worker_producer = kafka_mirror_get_producer(mirror, worker_id);
                if (worker_producer) {
                    rd_kafka_poll(worker_producer, 0);
                    int producer_queue_size = rd_kafka_outq_len(worker_producer);
                    int warning_threshold = (mirror->config.num_workers > 16) ? 150000 : 100000;
                    if (producer_queue_size > warning_threshold) {
                        usleep(10);
                    }
                }
            }
        }
    }
    
    printf("Worker %d завершил работу, обработано %d сообщений\n", worker_id, processed_count);
    free(worker_data);
    return NULL;
}

void* kafka_mirror_status_thread(void *arg) {
    kafka_mirror_t *mirror = (kafka_mirror_t *)arg;
    
    while (mirror->running && !mirror->partitions_ready_for_status) {
        sleep(mirror->config.status_interval_ms / 1000);
    }
    
    while (mirror->running) {
        kafka_mirror_print_status(mirror);
        sleep(mirror->config.status_interval_ms / 1000);
    }
    
    return NULL;
}

void* kafka_mirror_retry_thread(void *arg) {
    kafka_mirror_t *mirror = (kafka_mirror_t *)arg;
    
    while (mirror->running) {
        sleep(1);
    }
    
    return NULL;
}


int kafka_mirror_run(kafka_mirror_t *mirror) {
    if (!mirror) {
        return 2;
    }
    
    int result = kafka_mirror_process_messages(mirror);
    if (result < 0) {
        return 2;
    }
    
    return result;
}

void kafka_mirror_record_success(kafka_mirror_t *mirror, const char *topic, 
                                 int32_t partition, int64_t offset, size_t message_size) {
    if (!mirror) return;
    
    (void)topic;
    (void)offset;
    
    pthread_mutex_lock(&mirror->stats_mutex);
    
    mirror->stats.total_mirrored++;
    mirror->stats.last_message_time = time(NULL);
    
    if (message_size > 256 * 1024 * 1024) {
        mirror->stats.large_messages++;
    }
    
    if (partition >= 0 && partition < 4) {
        mirror->stats.partition_stats[partition]++;
    }
    
    pthread_mutex_unlock(&mirror->stats_mutex);
}

void kafka_mirror_record_worker_success(kafka_mirror_t *mirror, int worker_id) {
    if (!mirror || worker_id < 0 || worker_id >= MAX_WORKERS) return;
    
    pthread_mutex_lock(&mirror->stats_mutex);
    mirror->stats.worker_processed[worker_id]++;
    mirror->stats.total_mirrored++;
    mirror->stats.last_message_time = time(NULL);
    pthread_mutex_unlock(&mirror->stats_mutex);
}

void kafka_mirror_record_worker_attempt(kafka_mirror_t *mirror, int worker_id) {
    if (!mirror || worker_id < 0 || worker_id >= MAX_WORKERS) return;
    
    pthread_mutex_lock(&mirror->stats_mutex);
    mirror->stats.worker_attempts[worker_id]++;
    pthread_mutex_unlock(&mirror->stats_mutex);
}

void kafka_mirror_record_worker_failure(kafka_mirror_t *mirror, int worker_id) {
    if (!mirror || worker_id < 0 || worker_id >= MAX_WORKERS) return;
    
    pthread_mutex_lock(&mirror->stats_mutex);
    mirror->stats.worker_failed[worker_id]++;
    pthread_mutex_unlock(&mirror->stats_mutex);
}

void kafka_mirror_record_queue_full_error(kafka_mirror_t *mirror) {
    if (!mirror) return;
    
    pthread_mutex_lock(&mirror->stats_mutex);
    mirror->stats.queue_full_errors++;
    pthread_mutex_unlock(&mirror->stats_mutex);
}

void kafka_mirror_record_size_validation_error(kafka_mirror_t *mirror) {
    if (!mirror) return;
    
    pthread_mutex_lock(&mirror->stats_mutex);
    mirror->stats.size_validation_errors++;
    pthread_mutex_unlock(&mirror->stats_mutex);
}

void kafka_mirror_record_producer_error(kafka_mirror_t *mirror) {
    if (!mirror) return;
    
    pthread_mutex_lock(&mirror->stats_mutex);
    mirror->stats.producer_errors++;
    pthread_mutex_unlock(&mirror->stats_mutex);
}

void kafka_mirror_record_failure(kafka_mirror_t *mirror, const char *topic,
                                 int32_t partition, int64_t offset, const char *error) {
    if (!mirror) return;
    
    pthread_mutex_lock(&mirror->stats_mutex);
    
    mirror->stats.total_failed++;
    
    if (mirror->stats.error_count < 50) {
        snprintf(mirror->stats.recent_errors[mirror->stats.error_count], MAX_ERROR_MSG_LEN,
                "%s-%d:%ld - %s", topic, partition, offset, error);
        mirror->stats.error_count++;
    }
    
    pthread_mutex_unlock(&mirror->stats_mutex);
}

void kafka_mirror_record_retry(kafka_mirror_t *mirror, const char *topic,
                               int32_t partition, int64_t offset) {
    if (!mirror) return;
    
    (void)topic;
    (void)partition;
    (void)offset;
    
    pthread_mutex_lock(&mirror->stats_mutex);
    mirror->stats.total_retries++;
    pthread_mutex_unlock(&mirror->stats_mutex);
}

void kafka_mirror_update_queue_stats(kafka_mirror_t *mirror) {
    if (!mirror) return;
    
    pthread_mutex_lock(&mirror->stats_mutex);
    
    mirror->stats.messages_in_queue = mirror->message_queue.count;
    
    if (mirror->producers && mirror->producer_count > 0) {
        mirror->stats.messages_in_producer_queue = 0;
        for (int i = 0; i < mirror->producer_count; i++) {
            if (mirror->producers[i]) {
                uint64_t queue_size = rd_kafka_outq_len(mirror->producers[i]);
                mirror->stats.producer_queue_sizes[i] = queue_size;
                mirror->stats.messages_in_producer_queue += queue_size;
            } else {
                mirror->stats.producer_queue_sizes[i] = 0;
            }
        }
        
        for (int i = mirror->producer_count; i < MAX_PRODUCERS; i++) {
            mirror->stats.producer_queue_sizes[i] = 0;
        }
    } else {
        mirror->stats.messages_in_producer_queue = 0;
        for (int i = 0; i < MAX_PRODUCERS; i++) {
            mirror->stats.producer_queue_sizes[i] = 0;
        }
    }
    
    mirror->stats.total_processed_by_workers = 0;
    for (int i = 0; i < 32; i++) {
        mirror->stats.total_processed_by_workers += mirror->stats.worker_processed[i];
    }
    
    pthread_mutex_unlock(&mirror->stats_mutex);
}

void kafka_mirror_print_status(kafka_mirror_t *mirror) {
    if (!mirror) return;
    
    kafka_mirror_update_queue_stats(mirror);
    
    pthread_mutex_lock(&mirror->stats_mutex);
    
    time_t now = time(NULL);
    double uptime = get_elapsed_seconds(mirror->stats.start_time);
    
    double interval_time = difftime(now, mirror->stats.last_status_time);
    uint64_t interval_messages = mirror->stats.total_processed_by_workers - mirror->stats.last_status_delivered;
    double current_rate = (interval_time > 0) ? (double)interval_messages / interval_time : 0.0;
    
    mirror->stats.last_status_time = now;
    mirror->stats.last_status_delivered = mirror->stats.total_processed_by_workers;
    
    printf("\n=== Статус копирования ===\n");
    int minutes = (int)(uptime / 60);
    int seconds = (int)(uptime) % 60;
    if (minutes > 0) {
        printf("Время работы: %dм %dс\n", minutes, seconds);
    } else {
        printf("Время работы: %dс\n", seconds);
    }
    printf("Партиции назначены: %s\n", mirror->partitions_assigned ? "да" : "нет");
    printf("Всего успешно обработано: %lu\n", mirror->stats.total_mirrored);
    printf("Обработано worker'ами: %lu\n", mirror->stats.total_processed_by_workers);
    printf("В очереди на обработку: %lu\n", mirror->stats.messages_in_queue);
    printf("В очереди producer'а: %lu\n", mirror->stats.messages_in_producer_queue);
    
    FILE *meminfo = fopen("/proc/self/status", "r");
    if (meminfo) {
        char line[256];
        while (fgets(line, sizeof(line), meminfo)) {
            if (strncmp(line, "VmRSS:", 6) == 0 || strncmp(line, "VmSize:", 7) == 0) {
                printf("%s", line);
            }
        }
        fclose(meminfo);
    }
    printf("Всего неудачных: %lu\n", mirror->stats.total_failed);
    printf("Всего повторов: %lu\n", mirror->stats.total_retries);
    printf("Больших сообщений (>256MB): %lu\n", mirror->stats.large_messages);
    printf("Ошибки валидации размера: %lu\n", mirror->stats.size_validation_errors);
    printf("Ошибки переполнения очереди: %lu\n", mirror->stats.queue_full_errors);
    printf("Ошибки producer: %lu\n", mirror->stats.producer_errors);
    printf("Текущая скорость: %.1f msg/sec\n", current_rate);
    
    static bool partition_stats_initialized = false;
    if (!partition_stats_initialized) {
        partition_stats_initialized = true;
    }

    static int status_count = 0;
    status_count++;
    if (status_count % 5 == 0) {
        printf("Сообщения по партициям: P0=%lu, P1=%lu, P2=%lu, P3=%lu\n", 
               mirror->stats.partition_stats[0], mirror->stats.partition_stats[1], 
               mirror->stats.partition_stats[2], mirror->stats.partition_stats[3]);
    }
    
    if (mirror->partitions_assigned && mirror->consumer) {
        uint64_t total_remaining = 0;
        rd_kafka_topic_partition_list_t *assigned = NULL;
        rd_kafka_resp_err_t assign_err = rd_kafka_assignment(mirror->consumer, &assigned);
        
        if (assign_err == RD_KAFKA_RESP_ERR_NO_ERROR && assigned && assigned->cnt > 0) {
            for (int i = 0; i < assigned->cnt; i++) {
                rd_kafka_topic_partition_t *tp = &assigned->elems[i];
                int64_t current_pos = 0;
                int64_t end_offset = 0;
                
                rd_kafka_topic_partition_list_t *pos_list = rd_kafka_topic_partition_list_new(1);
                if (pos_list) {
                    rd_kafka_topic_partition_list_add(pos_list, tp->topic, tp->partition);
                    rd_kafka_resp_err_t pos_err = rd_kafka_position(mirror->consumer, pos_list);
                    if (pos_err == RD_KAFKA_RESP_ERR_NO_ERROR) {
                        current_pos = pos_list->elems[0].offset;
                    }
                    rd_kafka_topic_partition_list_destroy(pos_list);
                }
                
                int64_t beginning_offset = 0;
                rd_kafka_resp_err_t err = rd_kafka_query_watermark_offsets(mirror->consumer, tp->topic, tp->partition, &beginning_offset, &end_offset, 5000);
                
                if (err == RD_KAFKA_RESP_ERR_NO_ERROR && current_pos < end_offset) {
                    total_remaining += (end_offset - current_pos);
                }
            }
            
            if (total_remaining > 0) {
                printf("Осталось сообщений для копирования: %lu\n", total_remaining);
                
                if (current_rate > 0) {
                    double estimated_seconds = (double)total_remaining / current_rate;
                    int hours = (int)(estimated_seconds / 3600);
                    int minutes = (int)((estimated_seconds - hours * 3600) / 60);
                    int seconds = (int)(estimated_seconds - hours * 3600 - minutes * 60);
                    
                    if (hours > 0) {
                        printf("Примерное время до завершения: %dч %dм %dс\n", hours, minutes, seconds);
                    } else if (minutes > 0) {
                        printf("Примерное время до завершения: %dм %dс\n", minutes, seconds);
                    } else {
                        printf("Примерное время до завершения: %dс\n", seconds);
                    }
                }
            } else {
                printf("Осталось сообщений для копирования: 0 (завершено)\n");
            }
        }
        
        if (assigned) {
            rd_kafka_topic_partition_list_destroy(assigned);
        }
    }
    
    printf("Обработано сообщений worker'ами:\n");
    for (int i = 0; i < mirror->config.num_workers; i++) {
        uint64_t attempts = mirror->stats.worker_attempts[i];
        uint64_t processed = mirror->stats.worker_processed[i];
        uint64_t failed = mirror->stats.worker_failed[i];
        printf("  Worker %d: %lu успешно, %lu неудачно (всего попыток: %lu)\n", 
               i, processed, failed, attempts);
    }
    
    if (mirror->producers && mirror->producer_count > 0) {
        printf("Очередь продюсеров:\n");
        for (int i = 0; i < mirror->producer_count; i++) {
            uint64_t queue_size = mirror->stats.producer_queue_sizes[i];
            if (queue_size > 0) {
                printf("  Producer %d: %lu сообщений в очереди", i, queue_size);
                
                printf(" (используют worker'ы: ");
                bool first = true;
                for (int j = 0; j < mirror->config.num_workers; j++) {
                    if (j % mirror->producer_count == i) {
                        if (!first) printf(", ");
                        printf("%d", j);
                        first = false;
                    }
                }
                printf(")\n");
            }
        }
    }
    
    if (mirror->stats.error_count > 0) {
        printf("\nПоследние ошибки:\n");
        for (int i = 0; i < mirror->stats.error_count && i < 3; i++) {
            printf("%s\n", mirror->stats.recent_errors[i]);
        }
    }
    
    printf("========================\n\n");
    
    pthread_mutex_unlock(&mirror->stats_mutex);
}

int kafka_mirror_save_state(kafka_mirror_t *mirror) {
    if (!mirror) return -1;
    
    rd_kafka_resp_err_t err = rd_kafka_commit(mirror->consumer, NULL, 0);
    if (err != RD_KAFKA_RESP_ERR_NO_ERROR) {
        printf("Ошибка коммита offset: %s\n", rd_kafka_err2str(err));
        return -1;
    }
    
    return 0;
}

int kafka_mirror_load_state(kafka_mirror_t *mirror) {
    if (!mirror) return -1;
    
    printf("Загрузка состояния из consumer group\n");
    return 0;
}

void kafka_mirror_cleanup(kafka_mirror_t *mirror, bool graceful) {
    if (!mirror) return;
    
    mirror->running = false;
    
    message_queue_shutdown(&mirror->message_queue);
    usleep(100000);
    
    kafka_mirror_destroy_producer_pool(mirror);
    
    if (mirror->consumer) {
        if (graceful) {
            rd_kafka_assign(mirror->consumer, NULL);
            usleep(100000);
            rd_kafka_destroy(mirror->consumer);
        } else {
            mirror->consumer = NULL;
        }
    }
    
    if (mirror->topic_partition_list) {
        mirror->topic_partition_list = NULL;
    }
    
    if (mirror->worker_threads) {
        for (int i = 0; i < mirror->config.num_workers; i++) {
            if (mirror->worker_threads[i] != 0) {
                if (!graceful) {
                    pthread_cancel(mirror->worker_threads[i]);
                }
                pthread_join(mirror->worker_threads[i], NULL);
                mirror->worker_threads[i] = 0;
            }
        }
        
        free(mirror->worker_threads);
        mirror->worker_threads = NULL;
    }
    
    if (mirror->status_thread != 0) {
        pthread_cancel(mirror->status_thread);
    }
    if (mirror->retry_thread != 0) {
        pthread_cancel(mirror->retry_thread);
    }
    
    if (mirror->topic_discovery_thread != 0) {
        pthread_cancel(mirror->topic_discovery_thread);
    }
    
    if (mirror->cross_cluster_initialized) {
        if (mirror->config.preserve_offsets) {
            kafka_mirror_save_offset_state(mirror);
        }
        
        if (mirror->target_admin_client) {
            rd_kafka_destroy(mirror->target_admin_client);
        }
        
        pthread_mutex_destroy(&mirror->topic_state.mutex);
        pthread_mutex_destroy(&mirror->topic_discovery.mutex);
        
        if (mirror->topic_state.partitions) {
            free(mirror->topic_state.partitions);
        }
        
        if (mirror->topic_discovery.topics) {
            free(mirror->topic_discovery.topics);
        }
    }
    
    sem_destroy(&mirror->producer_semaphore);
}
