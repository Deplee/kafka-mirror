#ifndef CONFIG_H
#define CONFIG_H

#include "kafka_mirror.h"

int parse_arguments(int argc, char *argv[], kafka_mirror_config_t *config);
void print_usage(const char *program_name);
void print_config(const kafka_mirror_config_t *config);
int validate_config(const kafka_mirror_config_t *config);
int load_topics_from_file(const char *filename, char topics[][MAX_TOPIC_NAME_LEN], int *topic_count);

#endif
