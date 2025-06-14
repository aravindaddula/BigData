#!/bin/bash

# Step 1: Create directories for persistence
mkdir -p $HOME/presto/etc
mkdir -p $HOME/presto/data/catalog
mkdir -p $HOME/presto/warehouse

# Step 2: Create Hive catalog file for Delta Lake support
cat <<EOF > $HOME/presto/data/catalog/delta.properties
connector.name=hive
hive.metastore=file
hive.metastore.catalog.dir=/opt/warehouse
hive.allow-drop-table=true
delta.enabled=true
EOF

# Step 3: Create Presto config.properties
cat <<EOF > $HOME/presto/etc/config.properties
coordinator=true
node-scheduler.include-coordinator=true
http-server.http.port=8080
query.max-memory=512MB
query.max-memory-per-node=128MB
discovery-server.enabled=true
discovery.uri=http://localhost:8080
EOF

# Step 4: Create jvm.config
cat <<EOF > $HOME/presto/etc/jvm.config
-server
-Xmx1G
-XX:+UseG1GC
-XX:G1HeapRegionSize=32M
-XX:+UseGCOverheadLimit
-XX:+ExplicitGCInvokesConcurrent
-XX:+HeapDumpOnOutOfMemoryError
-XX:OnOutOfMemoryError=kill -9 %p
EOF

# Step 5: Create node.properties
cat <<EOF > $HOME/presto/etc/node.properties
node.environment=production
node.id=presto-delta
node.data-dir=/opt/presto/data
EOF

# Step 6: Run Presto Docker with Delta Lake JAR and mount volumes
docker run -d \
  --name presto-delta \
  --restart=always \
  -p 8080:8080 \
  -v $HOME/presto/etc:/opt/presto/etc \
  -v $HOME/presto/data/catalog:/opt/presto/etc/catalog \
  -v $HOME/presto/warehouse:/opt/warehouse \
  prestosql/presto:latest