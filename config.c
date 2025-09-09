#include "config.h"
#include "utils.h"
#include "kafka_mirror.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <getopt.h>

void print_usage(const char *program_name) {
    printf("Использование: %s [ОПЦИИ]\n", program_name);
    printf("\nОбязательные опции:\n");
    printf("  --source-brokers BROKERS    Брокеры исходного кластера (через запятую)\n");
    printf("  --topics TOPICS            Топики для копирования (через запятую или .json файл)\n");
    printf("\nОпциональные опции:\n");
    printf("  --target-brokers BROKERS    Брокеры целевого кластера (через запятую)\n");
    printf("  --consumer-group GROUP      ID группы потребителей (по умолчанию: mirrormaker)\n");
    printf("  --security-protocol PROTO   Протокол безопасности (PLAINTEXT, SSL, SASL_PLAINTEXT, SASL_SSL)\n");
    printf("  --ssl-cafile FILE          Файл CA сертификата для SSL\n");
    printf("  --ssl-certfile FILE        Файл клиентского сертификата для SSL\n");
    printf("  --ssl-keyfile FILE         Файл клиентского ключа для SSL\n");
    printf("  --sasl-mechanism MECH      SASL механизм (PLAIN, SCRAM-SHA-256, SCRAM-SHA-512)\n");
    printf("  --sasl-username USER       Имя пользователя SASL (для same-cluster режима)\n");
    printf("  --sasl-password PASS       Пароль SASL (для same-cluster режима)\n");
    printf("  --source-sasl-username USER Имя пользователя SASL для исходного кластера\n");
    printf("  --source-sasl-password PASS Пароль SASL для исходного кластера\n");
    printf("  --target-sasl-username USER Имя пользователя SASL для целевого кластера\n");
    printf("  --target-sasl-password PASS Пароль SASL для целевого кластера\n");
    printf("  --target-security-protocol PROTO Протокол безопасности для целевого кластера\n");
    printf("  --target-ssl-cafile FILE   Файл CA сертификата для SSL целевого кластера\n");
    printf("  --target-ssl-certfile FILE Файл клиентского сертификата для SSL целевого кластера\n");
    printf("  --target-ssl-keyfile FILE  Файл клиентского ключа для SSL целевого кластера\n");
    printf("  --target-sasl-mechanism MECH SASL механизм для целевого кластера\n");
    printf("  --cross-cluster           Включить режим мироринга между кластерами\n");
    printf("  --preserve-offsets        Сохранять оффсеты для идемпотентности\n");
    printf("  --offset-storage TYPE     Тип хранения оффсетов (consumer-group, binary, csv, memory)\n");
    printf("  --offset-state-file FILE  Файл для сохранения состояния оффсетов\n");
    printf("  --topic-discovery-interval N Интервал обнаружения новых топиков в секундах\n");
    printf("  --auto-create-topics      Автоматически создавать топики в целевом кластере\n");
    printf("  --max-topics-per-worker N Максимальное количество топиков на worker\n");
    printf("  --enable-compression      Включить сжатие сообщений\n");
    printf("  --compression-type TYPE   Тип сжатия (lz4, gzip, snappy)\n");
    printf("  --mirror-all-topics       Копировать все несистемные топики\n");
    printf("  --max-retries N            Максимальное количество повторов для неудачных сообщений\n");
    printf("  --status-interval N        Интервал вывода статуса в секундах\n");
    printf("  --max-message-size N       Максимальный размер сообщения в байтах\n");
    printf("  --idle-timeout N           Выход через N секунд без сообщений\n");
    printf("  --target-topic TOPIC       Имя целевого топика (для копирования в том же кластере)\n");
    printf("  --same-cluster             Копировать сообщения в том же кластере\n");
    printf("  --skip-size-validation     Пропустить проверку размера сообщения\n");
    printf("  --num-workers N            Количество worker потоков для параллельной обработки\n");
    printf("  --num-producers N          Количество producer'ов для параллельной отправки\n");
    printf("  --batch-size N             Размер батча producer в байтах\n");
    printf("  --batch-count N            Количество сообщений в батче для обработки\n");
    printf("  --linger-ms N              Время ожидания producer в миллисекундах\n");
    printf("  --buffer-memory N          Буферная память producer в байтах\n");
    printf("  --max-in-flight N          Максимальное количество in-flight запросов на соединение\n");
    printf("  --disable-idempotence      Отключить идемпотентный producer для лучшей производительности\n");
    printf("  --request-timeout-ms N     Таймаут запроса в миллисекундах\n");
    printf("  --delivery-timeout-ms N    Таймаут доставки в миллисекундах\n");
    printf("  --max-block-ms N           Максимальное время блокировки в миллисекундах\n");
    printf("  --global-timeout N         Глобальный таймаут для всех операций в секундах\n");
    printf("  --offset-strategy STRATEGY Стратегия оффсетов (earliest, latest, consumer-group)\n");
    printf("  --help                     Показать эту справку\n");
    printf("\nПримеры:\n");
    printf("  %s --source-brokers localhost:9092 --target-brokers localhost:9093 --topics my-topic\n", program_name);
    printf("  %s --source-brokers localhost:9092 --same-cluster --target-topic backup-topic --topics my-topic\n", program_name);
    printf("  %s --source-brokers localhost:9092 --mirror-all-topics --security-protocol SASL_PLAINTEXT --source-sasl-username user --source-sasl-password pass\n", program_name);
}

void print_config(const kafka_mirror_config_t *config) {
    printf("\n=== Конфигурация Kafka Mirror ===\n");
    printf("Исходные брокеры: ");
    for (int i = 0; i < config->broker_count; i++) {
        printf("%s", config->brokers[i]);
        if (i < config->broker_count - 1) printf(", ");
    }
    printf("\n");
    
    printf("Группа потребителей: %s\n", config->consumer_group);
    printf("Топики (%d): ", config->topic_count);
    for (int i = 0; i < config->topic_count; i++) {
        printf("%s", config->topics[i]);
        if (i < config->topic_count - 1) printf(", ");
    }
    printf("\n");
    
    printf("Протокол безопасности: %s\n", security_protocol_to_string(config->security_protocol));
    printf("Максимальные повторы: %d\n", config->max_retries);
    printf("Интервал статуса: %d сек\n", config->status_interval_ms / 1000);
    printf("Максимальный размер сообщения: %d байт\n", config->max_message_size);
    printf("Таймаут простоя: %d сек\n", config->idle_timeout_ms / 1000);
    printf("Количество worker потоков: %d\n", config->num_workers);
    printf("Количество producer'ов: %d\n", config->num_producers);
    printf("Размер батча: %d байт\n", config->batch_size);
    printf("Количество сообщений в батче: %d\n", config->batch_count);
    printf("Время ожидания: %d мс\n", config->linger_ms);
    printf("Буферная память: %lld байт\n", config->buffer_memory);
    printf("Максимальные in-flight запросы: %d\n", config->max_in_flight);
    printf("Идемпотентность: %s\n", config->disable_idempotence ? "отключена" : "включена");
    printf("Таймаут запроса: %d мс\n", config->request_timeout_ms);
    printf("Таймаут доставки: %d мс\n", config->delivery_timeout_ms);
    printf("Максимальное время блокировки: %d мс\n", config->max_block_ms);
    printf("Глобальный таймаут: %d сек\n", config->global_timeout_ms / 1000);
    
    if (config->cross_cluster_mode) {
        printf("Режим: мироринг между кластерами\n");
        printf("Целевые брокеры: ");
        for (int i = 0; i < config->target_broker_count; i++) {
            printf("%s", config->target_brokers[i]);
            if (i < config->target_broker_count - 1) printf(", ");
        }
        printf("\n");
        printf("Протокол безопасности целевого кластера: %s\n", security_protocol_to_string(config->target_security_protocol));
        printf("Сохранение оффсетов: %s\n", config->preserve_offsets ? "включено" : "отключено");
        if (config->preserve_offsets) {
            const char *storage_type_str;
            switch (config->offset_storage_type) {
                case OFFSET_STORAGE_CONSUMER_GROUP: storage_type_str = "consumer-group"; break;
                case OFFSET_STORAGE_BINARY_FILE: storage_type_str = "binary"; break;
                case OFFSET_STORAGE_CSV_FILE: storage_type_str = "csv"; break;
                case OFFSET_STORAGE_MEMORY_ONLY: storage_type_str = "memory"; break;
                default: storage_type_str = "unknown"; break;
            }
            printf("Тип хранения оффсетов: %s\n", storage_type_str);
            if (config->offset_storage_type != OFFSET_STORAGE_CONSUMER_GROUP && 
                config->offset_storage_type != OFFSET_STORAGE_MEMORY_ONLY) {
                printf("Файл состояния оффсетов: %s\n", config->offset_state_file);
            }
        }
        printf("Интервал обнаружения топиков: %d сек\n", config->topic_discovery_interval_ms / 1000);
        printf("Автосоздание топиков: %s\n", config->auto_create_topics ? "включено" : "отключено");
        printf("Максимум топиков на worker: %d\n", config->max_topics_per_worker);
        printf("Сжатие: %s (%s)\n", config->enable_compression ? "включено" : "отключено", config->compression_type);
    }
    
    if (strlen(config->target_topic) > 0) {
        printf("Целевой топик: %s\n", config->target_topic);
    }
    
    if (config->same_cluster) {
        printf("Режим: копирование в том же кластере\n");
    }
    
    if (config->skip_size_validation) {
        printf("Проверка размера: отключена\n");
    }
    
    printf("Стратегия оффсетов: %s\n", offset_strategy_to_string(config->offset_strategy));
    printf("================================\n\n");
}

int validate_config(const kafka_mirror_config_t *config) {
    if (config->broker_count == 0) {
        fprintf(stderr, "Ошибка: не указаны брокеры\n");
        return -1;
    }
    
    if (config->topic_count == 0) {
        fprintf(stderr, "Ошибка: не указаны топики\n");
        return -1;
    }
    
    if (strlen(config->consumer_group) == 0) {
        fprintf(stderr, "Ошибка: не указана группа потребителей\n");
        return -1;
    }
    
    if (config->same_cluster && strlen(config->target_topic) == 0) {
        fprintf(stderr, "Ошибка: при использовании --same-cluster необходимо указать --target-topic\n");
        return -1;
    }
    
    if (config->same_cluster && config->topic_count > 1) {
        fprintf(stderr, "Ошибка: при использовании --same-cluster можно указать только один исходный топик\n");
        return -1;
    }
    
    if (config->cross_cluster_mode) {
        if (config->target_broker_count == 0) {
            fprintf(stderr, "Ошибка: в режиме cross-cluster необходимо указать --target-brokers\n");
            return -1;
        }
        
        if (config->preserve_offsets && config->offset_storage_type == OFFSET_STORAGE_CONSUMER_GROUP) {
            if (strlen(config->consumer_group) == 0) {
                fprintf(stderr, "Ошибка: для consumer-group режима необходимо указать --consumer-group\n");
                return -1;
            }
        }
    } else if (!config->same_cluster && config->broker_count == 0) {
        fprintf(stderr, "Ошибка: необходимо указать --target-brokers или использовать --same-cluster\n");
        return -1;
    }
    
    if (config->security_protocol == SECURITY_SSL) {
        if (strlen(config->ssl_cafile) == 0 || strlen(config->ssl_certfile) == 0 || strlen(config->ssl_keyfile) == 0) {
            fprintf(stderr, "Ошибка: SSL протокол требует --ssl-cafile, --ssl-certfile и --ssl-keyfile\n");
            return -1;
        }
    }
    
    if (config->security_protocol == SECURITY_SASL_PLAINTEXT || config->security_protocol == SECURITY_SASL_SSL) {
        if (config->cross_cluster_mode) {
            if (strlen(config->source_sasl_username) == 0 || strlen(config->source_sasl_password) == 0) {
                fprintf(stderr, "Ошибка: SASL протокол требует --source-sasl-username и --source-sasl-password\n");
                return -1;
            }
        } else {
            if (strlen(config->sasl_username) == 0 || strlen(config->sasl_password) == 0) {
                fprintf(stderr, "Ошибка: SASL протокол требует --sasl-username и --sasl-password\n");
                return -1;
            }
        }
    }
    
    if (config->cross_cluster_mode) {
        if (config->target_security_protocol == SECURITY_SSL) {
            if (strlen(config->target_ssl_cafile) == 0 || strlen(config->target_ssl_certfile) == 0 || strlen(config->target_ssl_keyfile) == 0) {
                fprintf(stderr, "Ошибка: SSL протокол целевого кластера требует --target-ssl-cafile, --target-ssl-certfile и --target-ssl-keyfile\n");
                return -1;
            }
        }
        
        if (config->target_security_protocol == SECURITY_SASL_PLAINTEXT || config->target_security_protocol == SECURITY_SASL_SSL) {
            if (strlen(config->target_sasl_username) == 0 || strlen(config->target_sasl_password) == 0) {
                fprintf(stderr, "Ошибка: SASL протокол целевого кластера требует --target-sasl-username и --target-sasl-password\n");
                return -1;
            }
        }
    }
    
    if (config->num_workers <= 0) {
        fprintf(stderr, "Ошибка: --num-workers должно быть больше 0\n");
        return -1;
    }
    
    if (config->num_producers <= 0 || config->num_producers > MAX_PRODUCERS) {
        fprintf(stderr, "Ошибка: --num-producers должно быть от 1 до %d\n", MAX_PRODUCERS);
        return -1;
    }
    
    if (config->batch_size <= 0) {
        fprintf(stderr, "Ошибка: --batch-size должно быть больше 0\n");
        return -1;
    }
    
    if (config->batch_count <= 0) {
        fprintf(stderr, "Ошибка: --batch-count должно быть больше 0\n");
        return -1;
    }
    
    if (config->linger_ms < 0) {
        fprintf(stderr, "Ошибка: --linger-ms должно быть неотрицательным\n");
        return -1;
    }
    
    if (config->buffer_memory <= 0) {
        fprintf(stderr, "Ошибка: --buffer-memory должно быть больше 0\n");
        return -1;
    }
    
    if (config->max_in_flight <= 0) {
        fprintf(stderr, "Ошибка: --max-in-flight должно быть больше 0\n");
        return -1;
    }
    
    if (config->request_timeout_ms <= 0) {
        fprintf(stderr, "Ошибка: --request-timeout-ms должно быть больше 0\n");
        return -1;
    }
    
    if (config->delivery_timeout_ms <= 0) {
        fprintf(stderr, "Ошибка: --delivery-timeout-ms должно быть больше 0\n");
        return -1;
    }
    
    if (config->max_block_ms <= 0) {
        fprintf(stderr, "Ошибка: --max-block-ms должно быть больше 0\n");
        return -1;
    }
    
    return 0;
}

int load_topics_from_file(const char *filename, char topics[][MAX_TOPIC_NAME_LEN], int *topic_count) {
    FILE *file = fopen(filename, "r");
    if (!file) {
        fprintf(stderr, "Ошибка открытия файла %s\n", filename);
        return -1;
    }
    
    *topic_count = 0;
    char line[1024];
    
    while (fgets(line, sizeof(line), file) && *topic_count < MAX_TOPICS) {
        trim_whitespace(line);
        if (strlen(line) > 0 && line[0] != '#') {
            if (is_valid_topic_name(line)) {
                safe_strncpy(topics[*topic_count], line, MAX_TOPIC_NAME_LEN);
                (*topic_count)++;
            }
        }
    }
    
    fclose(file);
    return 0;
}

int parse_arguments(int argc, char *argv[], kafka_mirror_config_t *config) {
    if (!config) {
        return -1;
    }
    
    memset(config, 0, sizeof(kafka_mirror_config_t));
    
    strcpy(config->consumer_group, "mirrormaker");
    config->security_protocol = SECURITY_PLAINTEXT;
    config->sasl_mechanism = SASL_MECHANISM_PLAIN;
    config->max_retries = MAX_RETRIES;
    config->status_interval_ms = DEFAULT_STATUS_INTERVAL_MS;
    config->max_message_size = 1048576;
    config->idle_timeout_ms = DEFAULT_IDLE_TIMEOUT_MS;
    config->same_cluster = false;
    config->skip_size_validation = false;
    config->num_workers = DEFAULT_NUM_WORKERS;
    config->num_producers = DEFAULT_NUM_PRODUCERS;
    config->batch_size = DEFAULT_BATCH_SIZE;
    config->batch_count = DEFAULT_BATCH_COUNT;
    config->linger_ms = DEFAULT_LINGER_MS;
    config->buffer_memory = (long long)DEFAULT_BUFFER_MEMORY;
    config->max_in_flight = DEFAULT_MAX_IN_FLIGHT;
    config->disable_idempotence = false;
    config->request_timeout_ms = DEFAULT_REQUEST_TIMEOUT_MS;
    config->delivery_timeout_ms = DEFAULT_DELIVERY_TIMEOUT_MS;
    config->max_block_ms = DEFAULT_MAX_BLOCK_MS;
    config->global_timeout_ms = DEFAULT_GLOBAL_TIMEOUT_MS;
    config->target_broker_count = 0;
    config->target_security_protocol = SECURITY_PLAINTEXT;
    config->target_sasl_mechanism = SASL_MECHANISM_PLAIN;
    config->cross_cluster_mode = false;
    config->preserve_offsets = true;
    config->offset_storage_type = OFFSET_STORAGE_CONSUMER_GROUP;
    strcpy(config->offset_state_file, "kafka_mirror_offsets.bin");
    config->topic_discovery_interval_ms = 60000;
    config->auto_create_topics = true;
    config->max_topics_per_worker = 10;
    config->enable_compression = true;
    strcpy(config->compression_type, "lz4");
    config->offset_strategy = OFFSET_STRATEGY_EARLIEST;
    
    static struct option long_options[] = {
        {"source-brokers", required_argument, 0, 's'},
        {"target-brokers", required_argument, 0, 't'},
        {"consumer-group", required_argument, 0, 'g'},
        {"topics", required_argument, 0, 'T'},
        {"security-protocol", required_argument, 0, 'p'},
        {"ssl-cafile", required_argument, 0, 'c'},
        {"ssl-certfile", required_argument, 0, 'C'},
        {"ssl-keyfile", required_argument, 0, 'k'},
        {"sasl-mechanism", required_argument, 0, 'm'},
        {"sasl-username", required_argument, 0, 'u'},
        {"sasl-password", required_argument, 0, 'P'},
        {"source-sasl-username", required_argument, 0, 'j'},
        {"source-sasl-password", required_argument, 0, 'q'},
        {"target-sasl-username", required_argument, 0, 'U'},
        {"target-sasl-password", required_argument, 0, 'W'},
        {"target-security-protocol", required_argument, 0, 'Q'},
        {"target-ssl-cafile", required_argument, 0, 'E'},
        {"target-ssl-certfile", required_argument, 0, 'F'},
        {"target-ssl-keyfile", required_argument, 0, 'H'},
        {"target-sasl-mechanism", required_argument, 0, 'J'},
        {"cross-cluster", no_argument, 0, 'K'},
        {"preserve-offsets", no_argument, 0, 'L'},
        {"offset-storage", required_argument, 0, '4'},
        {"offset-state-file", required_argument, 0, 'O'},
        {"topic-discovery-interval", required_argument, 0, 'Y'},
        {"auto-create-topics", no_argument, 0, 'Z'},
        {"max-topics-per-worker", required_argument, 0, '1'},
        {"enable-compression", no_argument, 0, '2'},
        {"compression-type", required_argument, 0, '3'},
        {"mirror-all-topics", no_argument, 0, 'a'},
        {"max-retries", required_argument, 0, 'r'},
        {"status-interval", required_argument, 0, 'i'},
        {"max-message-size", required_argument, 0, 'M'},
        {"idle-timeout", required_argument, 0, 'I'},
        {"target-topic", required_argument, 0, 'o'},
        {"same-cluster", no_argument, 0, 'S'},
        {"skip-size-validation", no_argument, 0, 'V'},
        {"num-workers", required_argument, 0, 'w'},
        {"batch-size", required_argument, 0, 'b'},
        {"batch-count", required_argument, 0, 'n'},
        {"linger-ms", required_argument, 0, 'l'},
        {"buffer-memory", required_argument, 0, 'B'},
        {"max-in-flight", required_argument, 0, 'f'},
        {"disable-idempotence", no_argument, 0, 'd'},
        {"request-timeout-ms", required_argument, 0, 'R'},
        {"delivery-timeout-ms", required_argument, 0, 'D'},
        {"max-block-ms", required_argument, 0, 'X'},
        {"global-timeout", required_argument, 0, 'G'},
        {"num-producers", required_argument, 0, 'N'},
        {"offset-strategy", required_argument, 0, '5'},
        {"help", no_argument, 0, 'h'},
        {0, 0, 0, 0}
    };
    
    int option_index = 0;
    int c;
    
    while ((c = getopt_long(argc, argv, "s:t:g:T:p:c:C:k:m:u:P:U:W:Q:E:F:H:J:KLO:Z:12345:ar:i:M:I:o:SVw:b:n:l:B:f:dR:D:G:N:h:j:q:", long_options, &option_index)) != -1) {
        switch (c) {
            case 's':
                if (parse_broker_list(optarg, config->brokers, &config->broker_count) != 0) {
                    fprintf(stderr, "Ошибка парсинга списка брокеров: %s\n", optarg);
                    return -1;
                }
                break;
            case 't':
                if (parse_broker_list(optarg, config->target_brokers, &config->target_broker_count) != 0) {
                    fprintf(stderr, "Ошибка парсинга списка целевых брокеров: %s\n", optarg);
                    return -1;
                }
                break;
            case 'g':
                safe_strncpy(config->consumer_group, optarg, sizeof(config->consumer_group));
                break;
            case 'T':
                if (strstr(optarg, ".json") != NULL) {
                    if (load_topics_from_file(optarg, config->topics, &config->topic_count) != 0) {
                        fprintf(stderr, "Ошибка загрузки топиков из файла: %s\n", optarg);
                        return -1;
                    }
                } else {
                    if (parse_topic_list(optarg, config->topics, &config->topic_count) != 0) {
                        fprintf(stderr, "Ошибка парсинга списка топиков: %s\n", optarg);
                        return -1;
                    }
                }
                break;
            case 'p':
                config->security_protocol = string_to_security_protocol(optarg);
                break;
            case 'c':
                safe_strncpy(config->ssl_cafile, optarg, sizeof(config->ssl_cafile));
                break;
            case 'C':
                safe_strncpy(config->ssl_certfile, optarg, sizeof(config->ssl_certfile));
                break;
            case 'k':
                safe_strncpy(config->ssl_keyfile, optarg, sizeof(config->ssl_keyfile));
                break;
            case 'm':
                config->sasl_mechanism = string_to_sasl_mechanism(optarg);
                break;
            case 'u':
                safe_strncpy(config->sasl_username, optarg, sizeof(config->sasl_username));
                break;
            case 'P':
                safe_strncpy(config->sasl_password, optarg, sizeof(config->sasl_password));
                break;
            case 'j':
                safe_strncpy(config->source_sasl_username, optarg, sizeof(config->source_sasl_username));
                break;
            case 'q':
                safe_strncpy(config->source_sasl_password, optarg, sizeof(config->source_sasl_password));
                break;
            case 'U':
                safe_strncpy(config->target_sasl_username, optarg, sizeof(config->target_sasl_username));
                break;
            case 'W':
                safe_strncpy(config->target_sasl_password, optarg, sizeof(config->target_sasl_password));
                break;
            case 'Q':
                config->target_security_protocol = string_to_security_protocol(optarg);
                break;
            case 'E':
                safe_strncpy(config->target_ssl_cafile, optarg, sizeof(config->target_ssl_cafile));
                break;
            case 'F':
                safe_strncpy(config->target_ssl_certfile, optarg, sizeof(config->target_ssl_certfile));
                break;
            case 'H':
                safe_strncpy(config->target_ssl_keyfile, optarg, sizeof(config->target_ssl_keyfile));
                break;
            case 'J':
                config->target_sasl_mechanism = string_to_sasl_mechanism(optarg);
                break;
            case 'K':
                config->cross_cluster_mode = true;
                break;
            case 'L':
                config->preserve_offsets = true;
                break;
            case 'O':
                safe_strncpy(config->offset_state_file, optarg, sizeof(config->offset_state_file));
                break;
            case 'Y':
                config->topic_discovery_interval_ms = atoi(optarg) * 1000;
                break;
            case 'Z':
                config->auto_create_topics = true;
                break;
            case '1':
                config->max_topics_per_worker = atoi(optarg);
                break;
            case '2':
                config->enable_compression = true;
                break;
            case '3':
                safe_strncpy(config->compression_type, optarg, sizeof(config->compression_type));
                break;
            case '4':
                if (strcmp(optarg, "consumer-group") == 0) {
                    config->offset_storage_type = OFFSET_STORAGE_CONSUMER_GROUP;
                } else if (strcmp(optarg, "binary") == 0) {
                    config->offset_storage_type = OFFSET_STORAGE_BINARY_FILE;
                } else if (strcmp(optarg, "csv") == 0) {
                    config->offset_storage_type = OFFSET_STORAGE_CSV_FILE;
                } else if (strcmp(optarg, "memory") == 0) {
                    config->offset_storage_type = OFFSET_STORAGE_MEMORY_ONLY;
                } else {
                    fprintf(stderr, "Неизвестный тип хранения оффсетов: %s\n", optarg);
                    return -1;
                }
                break;
            case 'a':
                config->mirror_all_topics = true;
                break;
            case 'r':
                config->max_retries = atoi(optarg);
                break;
            case 'i':
                config->status_interval_ms = atoi(optarg) * 1000;
                break;
            case 'M':
                config->max_message_size = atoi(optarg);
                break;
            case 'I':
                config->idle_timeout_ms = atoi(optarg) * 1000;
                break;
            case 'o':
                safe_strncpy(config->target_topic, optarg, sizeof(config->target_topic));
                break;
            case 'S':
                config->same_cluster = true;
                break;
            case 'V':
                config->skip_size_validation = true;
                break;
            case 'w':
                config->num_workers = atoi(optarg);
                break;
            case 'b':
                config->batch_size = atoi(optarg);
                break;
            case 'n':
                config->batch_count = atoi(optarg);
                break;
            case 'l':
                config->linger_ms = atoi(optarg);
                break;
            case 'B':
                config->buffer_memory = strtoll(optarg, NULL, 10);
                break;
            case 'f':
                config->max_in_flight = atoi(optarg);
                break;
            case 'd':
                config->disable_idempotence = true;
                break;
            case 'R':
                config->request_timeout_ms = atoi(optarg);
                break;
            case 'D':
                config->delivery_timeout_ms = atoi(optarg);
                break;
            case 'X':
                config->max_block_ms = atoi(optarg);
                break;
            case 'G':
                config->global_timeout_ms = atoi(optarg) * 1000;
                break;
            case 'N':
                config->num_producers = atoi(optarg);
                break;
            case '5':
                config->offset_strategy = string_to_offset_strategy(optarg);
                if (config->offset_strategy == (offset_strategy_t)-1) {
                    fprintf(stderr, "Неверная стратегия оффсетов: %s. Доступные: earliest, latest, consumer-group\n", optarg);
                    return -1;
                }
                break;
            case 'h':
                print_usage(argv[0]);
                exit(0);
            case '?':
                return -1;
            default:
                return -1;
        }
    }
    
    if (optind < argc) {
        fprintf(stderr, "Неизвестные аргументы: ");
        while (optind < argc) {
            fprintf(stderr, "%s ", argv[optind++]);
        }
        fprintf(stderr, "\n");
        return -1;
    }
    
    return 0;
}
