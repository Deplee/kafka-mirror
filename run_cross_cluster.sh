#!/bin/bash

echo "=== Cross-Cluster Mirroring ==="
echo "Starting mirroring from source to target cluster..."

./topic-copy \
  --source-brokers "ip-addr-1:9092" \
  --target-brokers "ip-addr-2:9092" \
  --cross-cluster \
  --topics "topic-name" \
  --skip-size-validation \
  --preserve-offsets \
  --offset-storage consumer-group \
  --consumer-group "mirrormaker" \
  --auto-create-topics \
  --security-protocol SASL_PLAINTEXT \
  --sasl-mechanism plain \
  --source-sasl-username "usr_name" \
  --source-sasl-password "pwd" \
  --target-security-protocol SASL_PLAINTEXT \
  --target-sasl-mechanism plain \
  --target-sasl-username "usr_name" \
  --target-sasl-password "pwd" \
  --num-workers 64 \
  --num-producers 32 \
  --batch-size 16777216 \
  --batch-count 150000 \
  --buffer-memory 15442450944 \
  --max-in-flight 1 \
  --enable-compression \
  --compression-type lz4 \
  --linger-ms 200 \
  --request-timeout-ms 900000 \
  --delivery-timeout-ms 900000 \
  --max-message-size 1073741824 \
  --status-interval 15 \
  --topic-discovery-interval 30 \
  --offset-strategy earliest

echo ""
