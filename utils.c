#include "utils.h"
#include "kafka_mirror.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <ctype.h>
#include <regex.h>

void get_current_time_string(char *buffer, size_t buffer_size) {
    time_t now = time(NULL);
    struct tm *tm_info = localtime(&now);
    strftime(buffer, buffer_size, "%Y-%m-%d %H:%M:%S", tm_info);
}

double get_elapsed_seconds(time_t start_time) {
    time_t now = time(NULL);
    return difftime(now, start_time);
}

void format_bytes(char *buffer, size_t buffer_size, uint64_t bytes) {
    const char *units[] = {"B", "KB", "MB", "GB", "TB", "PB"};
    int unit_index = 0;
    double size = (double)bytes;

    while (size >= 1024.0 && unit_index < 5) {
        size /= 1024.0;
        unit_index++;
    }

    if (unit_index == 0) {
        snprintf(buffer, buffer_size, "%lu %s", bytes, units[unit_index]);
    } else {
        snprintf(buffer, buffer_size, "%.2f %s", size, units[unit_index]);
    }
}

void format_rate(char *buffer, size_t buffer_size, double rate) {
    if (rate < 1000) {
        snprintf(buffer, buffer_size, "%.1f msg/sec", rate);
    } else if (rate < 1000000) {
        snprintf(buffer, buffer_size, "%.1fK msg/sec", rate / 1000.0);
    } else {
        snprintf(buffer, buffer_size, "%.1fM msg/sec", rate / 1000000.0);
    }
}

int safe_strncpy(char *dest, const char *src, size_t dest_size) {
    if (!dest || !src || dest_size == 0) {
        return -1;
    }

    strncpy(dest, src, dest_size - 1);
    dest[dest_size - 1] = '\0';
    return 0;
}

int safe_strncat(char *dest, const char *src, size_t dest_size) {
    if (!dest || !src || dest_size == 0) {
        return -1;
    }

    size_t dest_len = strnlen(dest, dest_size);
    if (dest_len >= dest_size - 1) {
        return -1;
    }

    strncat(dest, src, dest_size - dest_len - 1);
    return 0;
}

void trim_whitespace(char *str) {
    if (!str) return;

    char *start = str;
    char *end = start + strlen(str) - 1;

    while (isspace((unsigned char)*start)) start++;
    while (end > start && isspace((unsigned char)*end)) end--;

    end[1] = '\0';

    if (start != str) {
        memmove(str, start, end - start + 2);
    }
}

bool is_valid_topic_name(const char *topic) {
    if (!topic || strlen(topic) == 0) {
        return false;
    }

    if (strlen(topic) > 249) {
        return false;
    }

    if (topic[0] == '.' || topic[0] == '_') {
        return false;
    }

    for (const char *p = topic; *p; p++) {
        if (!isalnum((unsigned char)*p) && *p != '.' && *p != '_' && *p != '-') {
            return false;
        }
    }

    return true;
}

bool is_valid_broker_address(const char *broker) {
    if (!broker || strlen(broker) == 0) {
        return false;
    }

    regex_t regex;
    int ret = regcomp(&regex, "^[a-zA-Z0-9.-]+:[0-9]+$", REG_EXTENDED);
    if (ret != 0) {
        return false;
    }

    ret = regexec(&regex, broker, 0, NULL, 0);
    regfree(&regex);

    return ret == 0;
}

int parse_broker_list(const char *broker_list, char brokers[][MAX_BROKER_LEN], int *broker_count) {
    if (!broker_list || !brokers || !broker_count) {
        return -1;
    }

    *broker_count = 0;
    char *broker_list_copy = strdup(broker_list);
    if (!broker_list_copy) {
        return -1;
    }

    char *token = strtok(broker_list_copy, ",");
    while (token && *broker_count < MAX_BROKERS) {
        trim_whitespace(token);
        if (is_valid_broker_address(token)) {
            safe_strncpy(brokers[*broker_count], token, MAX_BROKER_LEN);
            (*broker_count)++;
        }
        token = strtok(NULL, ",");
    }

    free(broker_list_copy);
    return 0;
}

int parse_topic_list(const char *topic_list, char topics[][MAX_TOPIC_NAME_LEN], int *topic_count) {
    if (!topic_list || !topics || !topic_count) {
        return -1;
    }

    *topic_count = 0;
    char *topic_list_copy = strdup(topic_list);
    if (!topic_list_copy) {
        return -1;
    }

    char *token = strtok(topic_list_copy, ",");
    while (token && *topic_count < MAX_TOPICS) {
        trim_whitespace(token);
        if (is_valid_topic_name(token)) {
            safe_strncpy(topics[*topic_count], token, MAX_TOPIC_NAME_LEN);
            (*topic_count)++;
        }
        token = strtok(NULL, ",");
    }

    free(topic_list_copy);
    return 0;
}

const char* offset_strategy_to_string(offset_strategy_t strategy) {
    switch (strategy) {
        case OFFSET_STRATEGY_EARLIEST:
            return "earliest";
        case OFFSET_STRATEGY_LATEST:
            return "latest";
        case OFFSET_STRATEGY_CONSUMER_GROUP:
            return "consumer-group";
        default:
            return "unknown";
    }
}

offset_strategy_t string_to_offset_strategy(const char *str) {
    if (!str) return (offset_strategy_t)-1;
    
    if (strcasecmp(str, "earliest") == 0) {
        return OFFSET_STRATEGY_EARLIEST;
    } else if (strcasecmp(str, "latest") == 0) {
        return OFFSET_STRATEGY_LATEST;
    } else if (strcasecmp(str, "consumer-group") == 0) {
        return OFFSET_STRATEGY_CONSUMER_GROUP;
    }
    
    return (offset_strategy_t)-1;
}
