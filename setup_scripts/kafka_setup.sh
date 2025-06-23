#!/bin/bash

# Create persistent Docker volumes
docker volume create kafka_data
docker volume create zookeeper_data

# Create a custom Docker network if it doesn't exist
docker network inspect kafka-net >/dev/null 2>&1 || docker network create kafka-net

echo ""
echo "🚀 Starting Zookeeper..."
docker run -d \
  --name zookeeper \
  --restart=always \
  --network kafka-net \
  -p 2181:2181 \
  -v zookeeper_data:/bitnami/zookeeper \
  -e ALLOW_ANONYMOUS_LOGIN=yes \
  bitnami/zookeeper:latest

echo ""
echo "🚀 Starting Kafka..."
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
  -e KAFKA_CFG_AUTO_CREATE_TOPICS_ENABLE=true \
  -e ALLOW_PLAINTEXT_LISTENER=yes \
  bitnami/kafka:3.4.0-debian-11-r0

echo ""
echo "🔌 Starting Kafka Connect with Debezium (PostgreSQL)..."
docker run -d \
  --name connect \
  --restart=always \
  --network kafka-net \
  -p 8083:8083 \
  -e GROUP_ID=1 \
  -e CONFIG_STORAGE_TOPIC=my_connect_configs \
  -e OFFSET_STORAGE_TOPIC=my_connect_offsets \
  -e STATUS_STORAGE_TOPIC=my_connect_statuses \
  -e BOOTSTRAP_SERVERS=kafka:9092 \
  -e CONNECT_REST_ADVERTISED_HOST_NAME=connect \
  -e CONNECT_KEY_CONVERTER=org.apache.kafka.connect.json.JsonConverter \
  -e CONNECT_VALUE_CONVERTER=org.apache.kafka.connect.json.JsonConverter \
  -e CONNECT_KEY_CONVERTER_SCHEMAS_ENABLE=false \
  -e CONNECT_VALUE_CONVERTER_SCHEMAS_ENABLE=false \
  debezium/connect:2.1.3.Final


echo ""
echo "📺 Starting Kafka UI (provectuslabs)..."
docker run -d \
  --name kafka-ui \
  --restart=always \
  --network kafka-net \
  -p 8080:8080 \
  -e KAFKA_CLUSTERS_0_NAME=local \
  -e KAFKA_CLUSTERS_0_BOOTSTRAPSERVERS=kafka:9092 \
  provectuslabs/kafka-ui:latest

echo ""
echo "⏳ Waiting 20 seconds for services to initialize..."
sleep 20

echo ""
echo "📡 Registering Debezium PostgreSQL connector..."
curl -i -X POST http://localhost:8083/connectors \
  -H "Accept:application/json" \
  -H "Content-Type:application/json" \
  -d '{
    "name": "postgres-connector",
    "config": {
      "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
      "database.hostname": "postgres_container",
      "database.port": "5432",
      "database.user": "jarvis",
      "database.password": "2650",
      "database.dbname": "avengers",
      "database.server.name": "dbserver1",
      "plugin.name": "pgoutput",
      "slot.name": "avengers_slot",
      "publication.name": "avengers_pub",
      "tombstones.on.delete": "false",
      "transforms": "unwrap",
      "transforms.unwrap.type": "io.debezium.transforms.ExtractNewRecordState",
      "key.converter": "org.apache.kafka.connect.storage.StringConverter",
      "value.converter": "org.apache.kafka.connect.json.JsonConverter",
      "value.converter.schemas.enable": "false"
    }
  }'

echo ""
echo "✅ All services are running!"
echo "🌐 Kafka UI available at: http://localhost:8080"
echo "📡 Kafka Connect REST API at: http://localhost:8083"

echo ""
echo "💡 Useful Kafka CLI commands:"
echo "    docker exec -it kafka bash"
echo "    kafka-topics.sh --bootstrap-server localhost:9092 --list"
echo "    kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic <your-topic> --from-beginning"
