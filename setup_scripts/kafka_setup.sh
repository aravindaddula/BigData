#!/bin/bash

# Create persistent Docker volumes
docker volume create kafka_data
docker volume create zookeeper_data

# Create a custom Docker network
docker network create kafka-net

# Start Zookeeper container
docker run -d \
  --name zookeeper \
  --restart=always \
  --network kafka-net \
  -p 2181:2181 \
  -v zookeeper_data:/bitnami/zookeeper \
  -e ALLOW_ANONYMOUS_LOGIN=yes \
  bitnami/zookeeper:latest

# Start Kafka container with PLAINTEXT allowed
docker run -d \
  --name kafka \
  --restart=always \
  --network kafka-net \
  -p 9092:9092 \
  -v kafka_data:/bitnami/kafka \
  -e KAFKA_BROKER_ID=1 \
  -e KAFKA_CFG_ZOOKEEPER_CONNECT=zookeeper:2181 \
  -e KAFKA_CFG_LISTENERS=PLAINTEXT://:9092 \
  -e KAFKA_CFG_ADVERTISED_LISTENERS=PLAINTEXT://localhost:9092 \
  -e KAFKA_CFG_LISTENER_SECURITY_PROTOCOL_MAP=PLAINTEXT:PLAINTEXT \
  -e ALLOW_PLAINTEXT_LISTENER=yes \
  bitnami/kafka:3.4.0-debian-11-r0

echo ""
echo "✅ Kafka and Zookeeper are up and running in Zookeeper (PLAINTEXT) mode."
echo "➡️  Run the following command to interact with Kafka:"
echo "    docker exec -it kafka bash"
echo ""
echo "💡 Example Kafka topic command:"
echo "    kafka-topics.sh --bootstrap-server localhost:9092 --create --topic test-topic --partitions 1 --replication-factor 1"
