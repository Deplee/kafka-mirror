#ifndef KAFKA_MIRROR_H
#define KAFKA_MIRROR_H

#include <librdkafka/rdkafka.h>
#include <pthread.h>
#include <stdbool.h>
#include <stdint.h>
#include <time.h>
#include <semaphore.h>

#define MAX_TOPICS 1000
#define MAX_BROKERS 10
#define MAX_TOPIC_NAME_LEN 255
#define MAX_BROKER_LEN 255
#define MAX_ERROR_MSG_LEN 512
#define MAX_RETRIES 5
#define DEFAULT_BATCH_SIZE 16384
#define DEFAULT_BATCH_COUNT 5000
#define DEFAULT_LINGER_MS 100
#define DEFAULT_BUFFER_MEMORY 67108864
#define DEFAULT_MAX_IN_FLIGHT 5
#define DEFAULT_REQUEST_TIMEOUT_MS 30000
#define DEFAULT_DELIVERY_TIMEOUT_MS 60000
#define DEFAULT_MAX_BLOCK_MS 60000
#define DEFAULT_GLOBAL_TIMEOUT_MS 300000
#define DEFAULT_IDLE_TIMEOUT_MS 30000
#define DEFAULT_STATUS_INTERVAL_MS 10000
#define DEFAULT_NUM_WORKERS 4
#define DEFAULT_NUM_PRODUCERS 4
#define PRODUCER_QUEUE_WARNING_THRESHOLD 10000
#define PRODUCER_QUEUE_CRITICAL_THRESHOLD 20000
#define MESSAGE_QUEUE_WARNING_THRESHOLD 100000
#define MESSAGE_QUEUE_CRITICAL_THRESHOLD 200000
#define MAX_PRODUCER_RETRIES 5
#define MAX_PRODUCERS 32
#define MAX_WORKERS 64

typedef enum {
    SECURITY_PLAINTEXT,
    SECURITY_SSL,
    SECURITY_SASL_PLAINTEXT,
    SECURITY_SASL_SSL
} security_protocol_t;

typedef enum {
    SASL_MECHANISM_PLAIN,
    SASL_MECHANISM_SCRAM_SHA_256,
    SASL_MECHANISM_SCRAM_SHA_512
} sasl_mechanism_t;

typedef enum {
    OFFSET_STORAGE_NONE,
    OFFSET_STORAGE_CONSUMER_GROUP,
    OFFSET_STORAGE_BINARY_FILE,
    OFFSET_STORAGE_CSV_FILE,
    OFFSET_STORAGE_MEMORY_ONLY
} offset_storage_type_t;

typedef enum {
    OFFSET_STRATEGY_EARLIEST,
    OFFSET_STRATEGY_LATEST,
    OFFSET_STRATEGY_CONSUMER_GROUP
} offset_strategy_t;

typedef struct {
    char brokers[MAX_BROKERS][MAX_BROKER_LEN];
    int broker_count;
    char consumer_group[256];
    char topics[MAX_TOPICS][MAX_TOPIC_NAME_LEN];
    int topic_count;
    security_protocol_t security_protocol;
    sasl_mechanism_t sasl_mechanism;
    char sasl_username[256];
    char sasl_password[256];
    char ssl_cafile[512];
    char ssl_certfile[512];
    char ssl_keyfile[512];
    int max_retries;
    int status_interval_ms;
    int max_message_size;
    int idle_timeout_ms;
    char target_topic[MAX_TOPIC_NAME_LEN];
    bool same_cluster;
    bool skip_size_validation;
    bool mirror_all_topics;
    int num_workers;
    int batch_size;
    int batch_count;
    int linger_ms;
    long long buffer_memory;
    int max_in_flight;
    bool disable_idempotence;
    int request_timeout_ms;
    int delivery_timeout_ms;
    int max_block_ms;
    int global_timeout_ms;
    int num_producers;
    char source_sasl_username[256];
    char source_sasl_password[256];
    char target_brokers[MAX_BROKERS][MAX_BROKER_LEN];
    int target_broker_count;
    security_protocol_t target_security_protocol;
    sasl_mechanism_t target_sasl_mechanism;
    char target_sasl_username[256];
    char target_sasl_password[256];
    char target_ssl_cafile[512];
    char target_ssl_certfile[512];
    char target_ssl_keyfile[512];
    bool cross_cluster_mode;
    bool preserve_offsets;
    offset_storage_type_t offset_storage_type;
    char offset_state_file[512];
    int topic_discovery_interval_ms;
    bool auto_create_topics;
    int max_topics_per_worker;
    bool enable_compression;
    char compression_type[32];
    offset_strategy_t offset_strategy;
} kafka_mirror_config_t;

typedef struct {
    uint64_t total_mirrored;
    uint64_t total_delivered;
    uint64_t total_failed;
    uint64_t total_retries;
    uint64_t large_messages;
    uint64_t queue_full_errors;
    uint64_t size_validation_errors;
    uint64_t producer_errors;
    time_t start_time;
    time_t last_message_time;
    time_t last_status_time;
    uint64_t last_status_delivered;
    double current_rate;
    char recent_errors[50][MAX_ERROR_MSG_LEN];
    int error_count;
    uint64_t worker_processed[MAX_WORKERS];
    uint64_t worker_attempts[MAX_WORKERS];
    uint64_t worker_failed[MAX_WORKERS];
    uint64_t total_processed_by_workers;
    uint64_t messages_in_queue;
    uint64_t messages_in_producer_queue;
    uint64_t producer_queue_sizes[MAX_PRODUCERS];
    uint64_t partition_stats[4];
} mirror_stats_t;

typedef struct {
    rd_kafka_message_t *message;
    char topic[MAX_TOPIC_NAME_LEN];
    int32_t partition;
    int64_t offset;
    size_t message_size;
    time_t timestamp;
} work_message_t;

typedef struct {
    work_message_t *messages;
    int head;
    int tail;
    int count;
    int capacity;
    bool shutdown;
    pthread_mutex_t mutex;
    pthread_cond_t not_empty;
    pthread_cond_t not_full;
} message_queue_t;

typedef struct {
    char topic[MAX_TOPIC_NAME_LEN];
    int32_t partition;
    int64_t last_processed_offset;
    int64_t last_committed_offset;
    time_t last_update_time;
    bool is_active;
    uint64_t message_count;
    uint64_t bytes_processed;
} topic_partition_state_t;

typedef struct {
    topic_partition_state_t *partitions;
    int count;
    int capacity;
    pthread_mutex_t mutex;
} topic_state_t;

typedef struct {
    char topic[MAX_TOPIC_NAME_LEN];
    int partition_count;
    bool is_system_topic;
    bool is_mirrored;
    time_t last_discovery_time;
} discovered_topic_t;

typedef struct {
    discovered_topic_t *topics;
    int count;
    int capacity;
    pthread_mutex_t mutex;
    time_t last_discovery_time;
} topic_discovery_t;

typedef struct {
    rd_kafka_t *consumer;
    rd_kafka_t **producers;
    int producer_count;
    int current_producer;
    pthread_mutex_t producer_mutex;
    rd_kafka_topic_partition_list_t *topic_partition_list;
    kafka_mirror_config_t config;
    mirror_stats_t stats;
    pthread_mutex_t stats_mutex;
    pthread_mutex_t retry_mutex;
    bool running;
    bool first_run;
    bool partitions_assigned;
    bool partitions_ready_for_status;
    char state_file[512];
    pthread_t *worker_threads;
    pthread_t status_thread;
    pthread_t retry_thread;
    pthread_t topic_discovery_thread;
    message_queue_t message_queue;
    sem_t producer_semaphore;
    topic_state_t topic_state;
    topic_discovery_t topic_discovery;
    rd_kafka_t *target_admin_client;
    bool cross_cluster_initialized;
} kafka_mirror_t;

typedef struct {
    kafka_mirror_t *mirror;
    int worker_id;
} worker_data_t;

typedef struct {
    rd_kafka_message_t *message;
    char topic[MAX_TOPIC_NAME_LEN];
    int32_t partition;
    int64_t offset;
    int retry_count;
    time_t timestamp;
} retry_message_t;

typedef struct {
    retry_message_t *messages;
    int count;
    int capacity;
} retry_queue_t;

int kafka_mirror_init(kafka_mirror_t *mirror, const kafka_mirror_config_t *config);
int kafka_mirror_run(kafka_mirror_t *mirror);
void kafka_mirror_cleanup(kafka_mirror_t *mirror, bool graceful);
int kafka_mirror_check_connection(const kafka_mirror_config_t *config);
int kafka_mirror_create_consumer(kafka_mirror_t *mirror);
int kafka_mirror_create_producer_pool(kafka_mirror_t *mirror);
int kafka_mirror_create_single_producer(kafka_mirror_t *mirror, int producer_id);
void kafka_mirror_destroy_producer_pool(kafka_mirror_t *mirror);
rd_kafka_t* kafka_mirror_get_producer(kafka_mirror_t *mirror, int worker_id);
int kafka_mirror_create_topic_if_not_exists(const kafka_mirror_config_t *config, const char *topic_name, int partition_count);
int kafka_mirror_get_source_topic_partitions(const kafka_mirror_config_t *config, const char *topic_name);
void kafka_mirror_rebalance_cb(rd_kafka_t *rk, rd_kafka_resp_err_t err,
                               rd_kafka_topic_partition_list_t *partitions,
                               void *opaque);
int kafka_mirror_process_messages(kafka_mirror_t *mirror);
void* kafka_mirror_worker_thread(void *arg);
void* kafka_mirror_status_thread(void *arg);
void* kafka_mirror_retry_thread(void *arg);
int kafka_mirror_process_message(kafka_mirror_t *mirror, rd_kafka_message_t *msg, int worker_id);
int kafka_mirror_retry_message(kafka_mirror_t *mirror, retry_message_t *retry_msg);
void kafka_mirror_record_success(kafka_mirror_t *mirror, const char *topic, 
                                 int32_t partition, int64_t offset, size_t message_size);
void kafka_mirror_record_worker_success(kafka_mirror_t *mirror, int worker_id);
void kafka_mirror_record_worker_attempt(kafka_mirror_t *mirror, int worker_id);
void kafka_mirror_record_worker_failure(kafka_mirror_t *mirror, int worker_id);
void kafka_mirror_record_failure(kafka_mirror_t *mirror, const char *topic,
                                 int32_t partition, int64_t offset, const char *error);
void kafka_mirror_record_queue_full_error(kafka_mirror_t *mirror);
void kafka_mirror_record_size_validation_error(kafka_mirror_t *mirror);
void kafka_mirror_record_producer_error(kafka_mirror_t *mirror);
void kafka_mirror_record_retry(kafka_mirror_t *mirror, const char *topic,
                               int32_t partition, int64_t offset);
void kafka_mirror_update_queue_stats(kafka_mirror_t *mirror);
void kafka_mirror_print_status(kafka_mirror_t *mirror);
int kafka_mirror_save_state(kafka_mirror_t *mirror);
int kafka_mirror_load_state(kafka_mirror_t *mirror);
void kafka_mirror_signal_handler(int sig);
void kafka_mirror_setup_signal_handlers(void);

const char* security_protocol_to_string(security_protocol_t protocol);
const char* sasl_mechanism_to_string(sasl_mechanism_t mechanism);
const char* offset_strategy_to_string(offset_strategy_t strategy);
security_protocol_t string_to_security_protocol(const char *str);
sasl_mechanism_t string_to_sasl_mechanism(const char *str);
offset_strategy_t string_to_offset_strategy(const char *str);

int kafka_mirror_init_cross_cluster(kafka_mirror_t *mirror);
int kafka_mirror_create_target_admin_client(kafka_mirror_t *mirror);
int kafka_mirror_discover_topics(kafka_mirror_t *mirror);
void* kafka_mirror_topic_discovery_thread(void *arg);
int kafka_mirror_save_offset_state(kafka_mirror_t *mirror);
int kafka_mirror_load_offset_state(kafka_mirror_t *mirror);
int kafka_mirror_save_offset_state_consumer_group(kafka_mirror_t *mirror);
int kafka_mirror_load_offset_state_consumer_group(kafka_mirror_t *mirror);
int kafka_mirror_save_offset_state_binary(kafka_mirror_t *mirror);
int kafka_mirror_load_offset_state_binary(kafka_mirror_t *mirror);
int kafka_mirror_save_offset_state_csv(kafka_mirror_t *mirror);
int kafka_mirror_load_offset_state_csv(kafka_mirror_t *mirror);
int kafka_mirror_update_partition_state(kafka_mirror_t *mirror, const char *topic, int32_t partition, int64_t offset);
int kafka_mirror_get_partition_state(kafka_mirror_t *mirror, const char *topic, int32_t partition, int64_t *last_offset);
int kafka_mirror_create_target_topic(kafka_mirror_t *mirror, const char *topic_name, int partition_count);
bool kafka_mirror_is_system_topic(const char *topic_name);
int kafka_mirror_validate_cross_cluster_config(const kafka_mirror_config_t *config);

int message_queue_init(message_queue_t *queue, int capacity);
void message_queue_destroy(message_queue_t *queue);
int message_queue_put(message_queue_t *queue, work_message_t *msg);
int message_queue_get(message_queue_t *queue, work_message_t *msg);
bool message_queue_is_empty(message_queue_t *queue);
bool message_queue_is_full(message_queue_t *queue);
void message_queue_wake_all(message_queue_t *queue);
void message_queue_shutdown(message_queue_t *queue);

#endif
