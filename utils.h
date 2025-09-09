#ifndef UTILS_H
#define UTILS_H

#include <time.h>
#include <stdint.h>
#include <stdbool.h>

#define MAX_TOPICS 1000
#define MAX_BROKERS 10
#define MAX_TOPIC_NAME_LEN 255
#define MAX_BROKER_LEN 255

void get_current_time_string(char *buffer, size_t buffer_size);
double get_elapsed_seconds(time_t start_time);
void format_bytes(char *buffer, size_t buffer_size, uint64_t bytes);
void format_rate(char *buffer, size_t buffer_size, double rate);
int safe_strncpy(char *dest, const char *src, size_t dest_size);
int safe_strncat(char *dest, const char *src, size_t dest_size);
void trim_whitespace(char *str);
bool is_valid_topic_name(const char *topic);
bool is_valid_broker_address(const char *broker);
int parse_broker_list(const char *broker_list, char brokers[][MAX_BROKER_LEN], int *broker_count);
int parse_topic_list(const char *topic_list, char topics[][MAX_TOPIC_NAME_LEN], int *topic_count);

#endif
