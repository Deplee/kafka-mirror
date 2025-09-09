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

int kafka_mirror_init_cross_cluster(kafka_mirror_t *mirror) {
    if (!mirror || !mirror->config.cross_cluster_mode) {
        return -1;
    }
    
    printf("Инициализация cross-cluster режима...\n");
    
    if (kafka_mirror_create_target_admin_client(mirror) != 0) {
        fprintf(stderr, "Ошибка создания admin client для целевого кластера\n");
        return -1;
    }
    
    if (pthread_mutex_init(&mirror->topic_state.mutex, NULL) != 0) {
        fprintf(stderr, "Ошибка инициализации мьютекса topic_state\n");
        return -1;
    }
    
    if (pthread_mutex_init(&mirror->topic_discovery.mutex, NULL) != 0) {
        fprintf(stderr, "Ошибка инициализации мьютекса topic_discovery\n");
        pthread_mutex_destroy(&mirror->topic_state.mutex);
        return -1;
    }
    
    mirror->topic_state.partitions = malloc(1000 * sizeof(topic_partition_state_t));
    if (!mirror->topic_state.partitions) {
        fprintf(stderr, "Ошибка выделения памяти для topic_state\n");
        pthread_mutex_destroy(&mirror->topic_state.mutex);
        pthread_mutex_destroy(&mirror->topic_discovery.mutex);
        return -1;
    }
    mirror->topic_state.count = 0;
    mirror->topic_state.capacity = 1000;
    
    mirror->topic_discovery.topics = malloc(1000 * sizeof(discovered_topic_t));
    if (!mirror->topic_discovery.topics) {
        fprintf(stderr, "Ошибка выделения памяти для topic_discovery\n");
        free(mirror->topic_state.partitions);
        pthread_mutex_destroy(&mirror->topic_state.mutex);
        pthread_mutex_destroy(&mirror->topic_discovery.mutex);
        return -1;
    }
    mirror->topic_discovery.count = 0;
    mirror->topic_discovery.capacity = 1000;
    mirror->topic_discovery.last_discovery_time = 0;
    
    if (kafka_mirror_load_offset_state(mirror) != 0) {
        printf("Предупреждение: не удалось загрузить состояние оффсетов, начинаем с начала\n");
    }
    
    mirror->cross_cluster_initialized = true;
    printf("Cross-cluster режим инициализирован успешно\n");
    
    return 0;
}

int kafka_mirror_create_target_admin_client(kafka_mirror_t *mirror) {
    if (!mirror) {
        return -1;
    }
    
    char errstr[512];
    rd_kafka_conf_t *conf = rd_kafka_conf_new();
    if (!conf) {
        fprintf(stderr, "Ошибка создания конфигурации для target admin client\n");
        return -1;
    }
    
    const kafka_mirror_config_t *config = &mirror->config;
    
    char bootstrap_servers[2048] = {0};
    for (int i = 0; i < config->target_broker_count; i++) {
        if (i > 0) strcat(bootstrap_servers, ",");
        strcat(bootstrap_servers, config->target_brokers[i]);
    }
    
    if (rd_kafka_conf_set(conf, "bootstrap.servers", bootstrap_servers, errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK) {
        fprintf(stderr, "Ошибка настройки bootstrap.servers для target admin: %s\n", errstr);
        rd_kafka_conf_destroy(conf);
        return -1;
    }
    
    if (config->target_security_protocol == SECURITY_SSL) {
        if (rd_kafka_conf_set(conf, "security.protocol", "ssl", errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK) {
            fprintf(stderr, "Ошибка настройки security.protocol для target admin: %s\n", errstr);
            rd_kafka_conf_destroy(conf);
            return -1;
        }
        
        if (strlen(config->target_ssl_cafile) > 0) {
            if (rd_kafka_conf_set(conf, "ssl.ca.location", config->target_ssl_cafile, errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK) {
                fprintf(stderr, "Ошибка настройки ssl.ca.location для target admin: %s\n", errstr);
                rd_kafka_conf_destroy(conf);
                return -1;
            }
        }
        
        if (strlen(config->target_ssl_certfile) > 0) {
            if (rd_kafka_conf_set(conf, "ssl.certificate.location", config->target_ssl_certfile, errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK) {
                fprintf(stderr, "Ошибка настройки ssl.certificate.location для target admin: %s\n", errstr);
                rd_kafka_conf_destroy(conf);
                return -1;
            }
        }
        
        if (strlen(config->target_ssl_keyfile) > 0) {
            if (rd_kafka_conf_set(conf, "ssl.key.location", config->target_ssl_keyfile, errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK) {
                fprintf(stderr, "Ошибка настройки ssl.key.location для target admin: %s\n", errstr);
                rd_kafka_conf_destroy(conf);
                return -1;
            }
        }
    } else if (config->target_security_protocol == SECURITY_SASL_PLAINTEXT || 
               config->target_security_protocol == SECURITY_SASL_SSL) {
        
        const char *security_protocol = (config->target_security_protocol == SECURITY_SASL_SSL) ? "sasl_ssl" : "sasl_plaintext";
        if (rd_kafka_conf_set(conf, "security.protocol", security_protocol, errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK) {
            fprintf(stderr, "Ошибка настройки security.protocol для target admin: %s\n", errstr);
            rd_kafka_conf_destroy(conf);
            return -1;
        }
        
        const char *sasl_mechanism = sasl_mechanism_to_string(config->target_sasl_mechanism);
        if (rd_kafka_conf_set(conf, "sasl.mechanism", sasl_mechanism, errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK) {
            fprintf(stderr, "Ошибка настройки sasl.mechanism для target admin: %s\n", errstr);
            rd_kafka_conf_destroy(conf);
            return -1;
        }
        
        if (rd_kafka_conf_set(conf, "sasl.username", config->target_sasl_username, errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK) {
            fprintf(stderr, "Ошибка настройки sasl.username для target admin: %s\n", errstr);
            rd_kafka_conf_destroy(conf);
            return -1;
        }
        
        if (rd_kafka_conf_set(conf, "sasl.password", config->target_sasl_password, errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK) {
            fprintf(stderr, "Ошибка настройки sasl.password для target admin: %s\n", errstr);
            rd_kafka_conf_destroy(conf);
            return -1;
        }
    }
    
    mirror->target_admin_client = rd_kafka_new(RD_KAFKA_PRODUCER, conf, errstr, sizeof(errstr));
    if (!mirror->target_admin_client) {
        fprintf(stderr, "Ошибка создания target admin client: %s\n", errstr);
        return -1;
    }
    
    printf("Target admin client создан успешно\n");
    return 0;
}

int kafka_mirror_discover_topics(kafka_mirror_t *mirror) {
    if (!mirror || !mirror->consumer) {
        return -1;
    }
    
    printf("Обнаружение топиков в исходном кластере...\n");
    
    rd_kafka_metadata_t *metadata;
    rd_kafka_resp_err_t err = rd_kafka_metadata(mirror->consumer, 1, NULL, (const rd_kafka_metadata_t **)&metadata, 10000);
    if (err != RD_KAFKA_RESP_ERR_NO_ERROR) {
        fprintf(stderr, "Ошибка получения метаданных: %s\n", rd_kafka_err2str(err));
        return -1;
    }
    
    pthread_mutex_lock(&mirror->topic_discovery.mutex);
    
    int discovered_count = 0;
    for (int i = 0; i < metadata->topic_cnt; i++) {
        const rd_kafka_metadata_topic_t *topic = &metadata->topics[i];
        
        if (kafka_mirror_is_system_topic(topic->topic)) {
            continue;
        }
        
        bool already_discovered = false;
        for (int j = 0; j < mirror->topic_discovery.count; j++) {
            if (strcmp(mirror->topic_discovery.topics[j].topic, topic->topic) == 0) {
                already_discovered = true;
                mirror->topic_discovery.topics[j].partition_count = topic->partition_cnt;
                mirror->topic_discovery.topics[j].last_discovery_time = time(NULL);
                break;
            }
        }
        
        if (!already_discovered && mirror->topic_discovery.count < mirror->topic_discovery.capacity) {
            discovered_topic_t *new_topic = &mirror->topic_discovery.topics[mirror->topic_discovery.count];
            safe_strncpy(new_topic->topic, topic->topic, MAX_TOPIC_NAME_LEN);
            new_topic->partition_count = topic->partition_cnt;
            new_topic->is_system_topic = false;
            new_topic->is_mirrored = false;
            new_topic->last_discovery_time = time(NULL);
            mirror->topic_discovery.count++;
            discovered_count++;
            
            printf("Обнаружен новый топик: %s (%d партиций)\n", topic->topic, topic->partition_cnt);
        }
    }
    
    mirror->topic_discovery.last_discovery_time = time(NULL);
    pthread_mutex_unlock(&mirror->topic_discovery.mutex);
    
    rd_kafka_metadata_destroy(metadata);
    
    printf("Обнаружено %d новых топиков, всего: %d\n", discovered_count, mirror->topic_discovery.count);
    return 0;
}

void* kafka_mirror_topic_discovery_thread(void *arg) {
    kafka_mirror_t *mirror = (kafka_mirror_t *)arg;
    
    while (mirror->running) {
        if (mirror->config.mirror_all_topics) {
            kafka_mirror_discover_topics(mirror);
        }
        
        sleep(mirror->config.topic_discovery_interval_ms / 1000);
    }
    
    return NULL;
}

int kafka_mirror_save_offset_state(kafka_mirror_t *mirror) {
    if (!mirror || !mirror->config.preserve_offsets) {
        return 0;
    }
    
    switch (mirror->config.offset_storage_type) {
        case OFFSET_STORAGE_CONSUMER_GROUP:
            return kafka_mirror_save_offset_state_consumer_group(mirror);
        case OFFSET_STORAGE_BINARY_FILE:
            return kafka_mirror_save_offset_state_binary(mirror);
        case OFFSET_STORAGE_CSV_FILE:
            return kafka_mirror_save_offset_state_csv(mirror);
        case OFFSET_STORAGE_MEMORY_ONLY:
            return 0;
        default:
            return -1;
    }
}

int kafka_mirror_save_offset_state_consumer_group(kafka_mirror_t *mirror) {
    if (!mirror || !mirror->consumer) {
        return -1;
    }
    
    rd_kafka_resp_err_t err = rd_kafka_commit(mirror->consumer, NULL, 0);
    if (err != RD_KAFKA_RESP_ERR_NO_ERROR) {
        printf("Предупреждение при коммите оффсетов: %s\n", rd_kafka_err2str(err));
        return -1;
    }
    
    printf("Оффсеты сохранены в consumer group: %s\n", mirror->config.consumer_group);
    return 0;
}

int kafka_mirror_load_offset_state(kafka_mirror_t *mirror) {
    if (!mirror || !mirror->config.preserve_offsets) {
        return 0;
    }
    
    switch (mirror->config.offset_storage_type) {
        case OFFSET_STORAGE_CONSUMER_GROUP:
            return kafka_mirror_load_offset_state_consumer_group(mirror);
        case OFFSET_STORAGE_BINARY_FILE:
            return kafka_mirror_load_offset_state_binary(mirror);
        case OFFSET_STORAGE_CSV_FILE:
            return kafka_mirror_load_offset_state_csv(mirror);
        case OFFSET_STORAGE_MEMORY_ONLY:
            return 0;
        default:
            return -1;
    }
}

int kafka_mirror_load_offset_state_consumer_group(kafka_mirror_t *mirror) {
    if (!mirror || !mirror->consumer) {
        return -1;
    }
    
    printf("Загрузка оффсетов из consumer group: %s\n", mirror->config.consumer_group);
    return 0;
}

int kafka_mirror_save_offset_state_binary(kafka_mirror_t *mirror) {
    if (!mirror) {
        return -1;
    }
    
    FILE *file = fopen(mirror->config.offset_state_file, "wb");
    if (!file) {
        fprintf(stderr, "Ошибка открытия файла состояния: %s\n", mirror->config.offset_state_file);
        return -1;
    }
    
    pthread_mutex_lock(&mirror->topic_state.mutex);
    
    uint32_t version = 1;
    uint32_t count = mirror->topic_state.count;
    uint64_t timestamp = time(NULL);
    
    fwrite(&version, sizeof(version), 1, file);
    fwrite(&count, sizeof(count), 1, file);
    fwrite(&timestamp, sizeof(timestamp), 1, file);
    
    for (int i = 0; i < mirror->topic_state.count; i++) {
        topic_partition_state_t *state = &mirror->topic_state.partitions[i];
        
        uint32_t topic_len = strlen(state->topic);
        fwrite(&topic_len, sizeof(topic_len), 1, file);
        fwrite(state->topic, 1, topic_len, file);
        fwrite(&state->partition, sizeof(state->partition), 1, file);
        fwrite(&state->last_processed_offset, sizeof(state->last_processed_offset), 1, file);
        fwrite(&state->last_committed_offset, sizeof(state->last_committed_offset), 1, file);
        fwrite(&state->last_update_time, sizeof(state->last_update_time), 1, file);
        fwrite(&state->is_active, sizeof(state->is_active), 1, file);
        fwrite(&state->message_count, sizeof(state->message_count), 1, file);
        fwrite(&state->bytes_processed, sizeof(state->bytes_processed), 1, file);
    }
    
    pthread_mutex_unlock(&mirror->topic_state.mutex);
    
    fclose(file);
    printf("Состояние оффсетов сохранено в бинарном формате: %s\n", mirror->config.offset_state_file);
    return 0;
}

int kafka_mirror_load_offset_state_binary(kafka_mirror_t *mirror) {
    if (!mirror) {
        return -1;
    }
    
    FILE *file = fopen(mirror->config.offset_state_file, "rb");
    if (!file) {
        printf("Файл состояния не найден, начинаем с начала\n");
        return 0;
    }
    
    uint32_t version, count;
    uint64_t timestamp;
    
    if (fread(&version, sizeof(version), 1, file) != 1 ||
        fread(&count, sizeof(count), 1, file) != 1 ||
        fread(&timestamp, sizeof(timestamp), 1, file) != 1) {
        fprintf(stderr, "Ошибка чтения заголовка файла состояния\n");
        fclose(file);
        return -1;
    }
    
    if (version != 1) {
        fprintf(stderr, "Неподдерживаемая версия файла состояния: %u\n", version);
        fclose(file);
        return -1;
    }
    
    pthread_mutex_lock(&mirror->topic_state.mutex);
    
    for (uint32_t i = 0; i < count && i < (uint32_t)mirror->topic_state.capacity; i++) {
        uint32_t topic_len;
        if (fread(&topic_len, sizeof(topic_len), 1, file) != 1) {
            break;
        }
        
        if (topic_len >= MAX_TOPIC_NAME_LEN) {
            fprintf(stderr, "Слишком длинное имя топика: %u\n", topic_len);
            break;
        }
        
        topic_partition_state_t *state = &mirror->topic_state.partitions[mirror->topic_state.count];
        
        if (fread(state->topic, 1, topic_len, file) != topic_len) {
            break;
        }
        state->topic[topic_len] = '\0';
        
        if (fread(&state->partition, sizeof(state->partition), 1, file) != 1 ||
            fread(&state->last_processed_offset, sizeof(state->last_processed_offset), 1, file) != 1 ||
            fread(&state->last_committed_offset, sizeof(state->last_committed_offset), 1, file) != 1 ||
            fread(&state->last_update_time, sizeof(state->last_update_time), 1, file) != 1 ||
            fread(&state->is_active, sizeof(state->is_active), 1, file) != 1 ||
            fread(&state->message_count, sizeof(state->message_count), 1, file) != 1 ||
            fread(&state->bytes_processed, sizeof(state->bytes_processed), 1, file) != 1) {
            break;
        }
        
        mirror->topic_state.count++;
    }
    
    pthread_mutex_unlock(&mirror->topic_state.mutex);
    
    fclose(file);
    printf("Загружено состояние для %d партиций из бинарного файла\n", mirror->topic_state.count);
    return 0;
}

int kafka_mirror_save_offset_state_csv(kafka_mirror_t *mirror) {
    if (!mirror) {
        return -1;
    }
    
    FILE *file = fopen(mirror->config.offset_state_file, "w");
    if (!file) {
        fprintf(stderr, "Ошибка открытия файла состояния: %s\n", mirror->config.offset_state_file);
        return -1;
    }
    
    pthread_mutex_lock(&mirror->topic_state.mutex);
    
    fprintf(file, "# Kafka Mirror Offset State\n");
    fprintf(file, "# Format: topic,partition,last_processed_offset,last_committed_offset,last_update_time,is_active,message_count,bytes_processed\n");
    fprintf(file, "# Timestamp: %ld\n", time(NULL));
    
    for (int i = 0; i < mirror->topic_state.count; i++) {
        topic_partition_state_t *state = &mirror->topic_state.partitions[i];
        fprintf(file, "%s,%d,%ld,%ld,%ld,%s,%lu,%lu\n",
                state->topic,
                state->partition,
                state->last_processed_offset,
                state->last_committed_offset,
                state->last_update_time,
                state->is_active ? "true" : "false",
                state->message_count,
                state->bytes_processed);
    }
    
    pthread_mutex_unlock(&mirror->topic_state.mutex);
    
    fclose(file);
    printf("Состояние оффсетов сохранено в CSV формате: %s\n", mirror->config.offset_state_file);
    return 0;
}

int kafka_mirror_load_offset_state_csv(kafka_mirror_t *mirror) {
    if (!mirror) {
        return -1;
    }
    
    FILE *file = fopen(mirror->config.offset_state_file, "r");
    if (!file) {
        printf("Файл состояния не найден, начинаем с начала\n");
        return 0;
    }
    
    char line[1024];
    int loaded_count = 0;
    
    pthread_mutex_lock(&mirror->topic_state.mutex);
    
    while (fgets(line, sizeof(line), file) && mirror->topic_state.count < mirror->topic_state.capacity) {
        if (line[0] == '#' || line[0] == '\n') {
            continue;
        }
        
        char topic[MAX_TOPIC_NAME_LEN];
        int32_t partition;
        int64_t last_processed_offset, last_committed_offset, last_update_time;
        char is_active_str[10];
        uint64_t message_count, bytes_processed;
        
        if (sscanf(line, "%255[^,],%d,%ld,%ld,%ld,%9[^,],%lu,%lu",
                   topic, &partition, &last_processed_offset, &last_committed_offset,
                   &last_update_time, is_active_str, &message_count, &bytes_processed) == 8) {
            
            topic_partition_state_t *state = &mirror->topic_state.partitions[mirror->topic_state.count];
            safe_strncpy(state->topic, topic, MAX_TOPIC_NAME_LEN);
            state->partition = partition;
            state->last_processed_offset = last_processed_offset;
            state->last_committed_offset = last_committed_offset;
            state->last_update_time = last_update_time;
            state->is_active = (strcmp(is_active_str, "true") == 0);
            state->message_count = message_count;
            state->bytes_processed = bytes_processed;
            
            mirror->topic_state.count++;
            loaded_count++;
        }
    }
    
    pthread_mutex_unlock(&mirror->topic_state.mutex);
    
    fclose(file);
    printf("Загружено состояние для %d партиций из CSV файла\n", loaded_count);
    return 0;
}

int kafka_mirror_update_partition_state(kafka_mirror_t *mirror, const char *topic, int32_t partition, int64_t offset) {
    if (!mirror || !topic) {
        return -1;
    }
    
    pthread_mutex_lock(&mirror->topic_state.mutex);
    
    for (int i = 0; i < mirror->topic_state.count; i++) {
        topic_partition_state_t *state = &mirror->topic_state.partitions[i];
        if (strcmp(state->topic, topic) == 0 && state->partition == partition) {
            state->last_processed_offset = offset;
            state->last_update_time = time(NULL);
            state->is_active = true;
            state->message_count++;
            pthread_mutex_unlock(&mirror->topic_state.mutex);
            return 0;
        }
    }
    
    if (mirror->topic_state.count < mirror->topic_state.capacity) {
        topic_partition_state_t *new_state = &mirror->topic_state.partitions[mirror->topic_state.count];
        safe_strncpy(new_state->topic, topic, MAX_TOPIC_NAME_LEN);
        new_state->partition = partition;
        new_state->last_processed_offset = offset;
        new_state->last_committed_offset = offset;
        new_state->last_update_time = time(NULL);
        new_state->is_active = true;
        new_state->message_count = 1;
        new_state->bytes_processed = 0;
        
        mirror->topic_state.count++;
    }
    
    pthread_mutex_unlock(&mirror->topic_state.mutex);
    return 0;
}

int kafka_mirror_get_partition_state(kafka_mirror_t *mirror, const char *topic, int32_t partition, int64_t *last_offset) {
    if (!mirror || !topic || !last_offset) {
        return -1;
    }
    
    pthread_mutex_lock(&mirror->topic_state.mutex);
    
    for (int i = 0; i < mirror->topic_state.count; i++) {
        topic_partition_state_t *state = &mirror->topic_state.partitions[i];
        if (strcmp(state->topic, topic) == 0 && state->partition == partition) {
            *last_offset = state->last_processed_offset;
            pthread_mutex_unlock(&mirror->topic_state.mutex);
            return 0;
        }
    }
    
    pthread_mutex_unlock(&mirror->topic_state.mutex);
    *last_offset = RD_KAFKA_OFFSET_BEGINNING;
    return 0;
}

int kafka_mirror_create_target_topic(kafka_mirror_t *mirror, const char *topic_name, int partition_count) {
    if (!mirror || !topic_name || !mirror->target_admin_client) {
        return -1;
    }
    
    if (!mirror->config.auto_create_topics) {
        return 0;
    }
    
    printf("Создание топика %s в целевом кластере с %d партициями...\n", topic_name, partition_count);
    
    rd_kafka_metadata_t *metadata;
    rd_kafka_resp_err_t err = rd_kafka_metadata(mirror->target_admin_client, 0, NULL, (const rd_kafka_metadata_t **)&metadata, 10000);
    if (err != RD_KAFKA_RESP_ERR_NO_ERROR) {
        fprintf(stderr, "Ошибка получения метаданных целевого кластера: %s\n", rd_kafka_err2str(err));
        return -1;
    }
    
    bool topic_exists = false;
    int existing_partitions = 0;
    for (int i = 0; i < metadata->topic_cnt; i++) {
        if (strcmp(metadata->topics[i].topic, topic_name) == 0) {
            topic_exists = true;
            existing_partitions = metadata->topics[i].partition_cnt;
            break;
        }
    }
    
    rd_kafka_metadata_destroy(metadata);
    
    if (topic_exists) {
        printf("Топик %s уже существует в целевом кластере с %d партициями\n", topic_name, existing_partitions);
        if (existing_partitions != partition_count) {
            printf("Предупреждение: количество партиций не совпадает (источник: %d, целевой: %d)\n", partition_count, existing_partitions);
        }
        return 0;
    }
    
    rd_kafka_NewTopic_t *new_topic = rd_kafka_NewTopic_new(topic_name, partition_count, 3, NULL, 0);
    if (!new_topic) {
        fprintf(stderr, "Ошибка создания NewTopic объекта\n");
        return -1;
    }
    
    
    rd_kafka_queue_t *queue = rd_kafka_queue_new(mirror->target_admin_client);
    if (!queue) {
        fprintf(stderr, "Ошибка создания queue\n");
        rd_kafka_NewTopic_destroy(new_topic);
        return -1;
    }
    
    rd_kafka_CreateTopics(mirror->target_admin_client, &new_topic, 1, NULL, queue);
    
    rd_kafka_event_t *event = rd_kafka_queue_poll(queue, 10000);
    if (!event) {
        fprintf(stderr, "Таймаут при создании топика\n");
        rd_kafka_queue_destroy(queue);
        rd_kafka_NewTopic_destroy(new_topic);
        return -1;
    }
    
    if (rd_kafka_event_error(event)) {
        fprintf(stderr, "Ошибка создания топика: %s\n", rd_kafka_event_error_string(event));
        rd_kafka_event_destroy(event);
        rd_kafka_queue_destroy(queue);
        rd_kafka_NewTopic_destroy(new_topic);
        return -1;
    }
    
    rd_kafka_event_destroy(event);
    rd_kafka_queue_destroy(queue);
    rd_kafka_NewTopic_destroy(new_topic);
    
    printf("Топик %s создан успешно в целевом кластере с %d партициями\n", topic_name, partition_count);
    return 0;
}

bool kafka_mirror_is_system_topic(const char *topic_name) {
    if (!topic_name) {
        return true;
    }
    
    if (strncmp(topic_name, "__", 2) == 0) {
        return true;
    }
    
    if (strcmp(topic_name, "_schemas") == 0) {
        return true;
    }
    
    return false;
}

int kafka_mirror_validate_cross_cluster_config(const kafka_mirror_config_t *config) {
    if (!config) {
        return -1;
    }
    
    if (!config->cross_cluster_mode) {
        return 0;
    }
    
    if (config->target_broker_count == 0) {
        fprintf(stderr, "Ошибка: не указаны целевые брокеры для cross-cluster режима\n");
        return -1;
    }
    
    if (config->target_security_protocol == SECURITY_SSL) {
        if (strlen(config->target_ssl_cafile) == 0 || 
            strlen(config->target_ssl_certfile) == 0 || 
            strlen(config->target_ssl_keyfile) == 0) {
            fprintf(stderr, "Ошибка: SSL протокол целевого кластера требует все SSL файлы\n");
            return -1;
        }
    }
    
    if (config->target_security_protocol == SECURITY_SASL_PLAINTEXT || 
        config->target_security_protocol == SECURITY_SASL_SSL) {
        if (strlen(config->target_sasl_username) == 0 || 
            strlen(config->target_sasl_password) == 0) {
            fprintf(stderr, "Ошибка: SASL протокол целевого кластера требует username и password\n");
            return -1;
        }
    }
    
    return 0;
}
