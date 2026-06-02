# 🔌 Kafka Connect Integration

<div align="center">

# 🚀 Apache Kafka Connect Integration 

### Stream Data from Kafka to PostgreSQL & File Systems

![Apache Kafka](https://img.shields.io/badge/Apache-Kafka-black?style=for-the-badge\&logo=apachekafka)
![Kafka Connect](https://img.shields.io/badge/Kafka-Connect-blue?style=for-the-badge)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-Database-blue?style=for-the-badge\&logo=postgresql)
![Java](https://img.shields.io/badge/Java-11+-orange?style=for-the-badge\&logo=openjdk)
![Linux](https://img.shields.io/badge/Linux-Ubuntu-yellow?style=for-the-badge\&logo=linux)
![JDBC](https://img.shields.io/badge/JDBC-Connector-green?style=for-the-badge)

</div>

---

# 📖 Overview

Apache Kafka Connect simplifies integration between Kafka and external systems by providing a scalable, configuration-driven framework for moving data.

In this lab, you will:

✅ Install Kafka Connect

✅ Configure JDBC Sink Connectors

✅ Stream Kafka Data into PostgreSQL

✅ Configure File Sink Connectors

✅ Monitor Connector Health

✅ Verify End-to-End Data Flow

---

# 🎯 Learning Objectives

By the end of this lab, you will be able to:

### 🔌 Kafka Connect

* Install Kafka Connect
* Configure standalone workers
* Deploy sink connectors

### 🗄️ Database Integration

* Connect Kafka with PostgreSQL
* Stream data automatically
* Validate database inserts

### 📁 File Storage

* Configure file sink connectors
* Store streamed events on local storage

### 📊 Monitoring

* Track connector operations
* Monitor offsets and processing

---

# 📚 Prerequisites

Before starting this lab, ensure you have:

* 🐧 Linux command-line knowledge
* 📨 Understanding of Kafka Topics
* ⚙️ Basic JSON configuration experience
* 🗄️ PostgreSQL knowledge
* ☕ Java 11+ installed

---

# 🖥️ Environment Setup

---

# ☕ Step 1: Install Java

Update package manager:

```bash
sudo apt update
```

Install OpenJDK 11:

```bash
sudo apt install -y openjdk-11-jdk
```

Verify installation:

```bash
java -version
```

Expected Output:

```text
openjdk version "11.x.x"
```

---

# 📦 Step 2: Download and Install Kafka

Download Kafka:

```bash
cd ~

wget https://archive.apache.org/dist/kafka/3.5.1/kafka_2.13-3.5.1.tgz
```

Extract archive:

```bash
tar -xzf kafka_2.13-3.5.1.tgz
```

Navigate:

```bash
cd kafka_2.13-3.5.1
```

Set environment variable:

```bash
export KAFKA_HOME=$(pwd)
```

Verify:

```bash
echo $KAFKA_HOME
```

---

# 🐘 Step 3: Install PostgreSQL

Install PostgreSQL:

```bash
sudo apt install -y postgresql postgresql-contrib
```

Start service:

```bash
sudo systemctl start postgresql
```

Enable at boot:

```bash
sudo systemctl enable postgresql
```

Verify:

```bash
sudo systemctl status postgresql
```

---

# 🔗 Step 4: Install PostgreSQL JDBC Driver

Create connectors directory:

```bash
cd $KAFKA_HOME

mkdir -p connectors

cd connectors
```

Download JDBC driver:

```bash
wget https://jdbc.postgresql.org/download/postgresql-42.6.0.jar
```

Verify:

```bash
ls -lh
```

---

# ⚡ Step 5: Install Kafka JDBC Connector Plugin

Download connector:

```bash
wget https://d1i4a15mxbxib1.cloudfront.net/api/plugins/confluentinc/kafka-connect-jdbc/versions/10.7.4/confluentinc-kafka-connect-jdbc-10.7.4.zip
```

Extract plugin:

```bash
unzip confluentinc-kafka-connect-jdbc-10.7.4.zip
```

Verify:

```bash
ls -lh
```

---

# 🏗️ Task 1: Configure PostgreSQL Sink Connector

---

# 🗄️ Step 1: Prepare PostgreSQL Database

Create database and table:

```bash
sudo -u postgres psql << EOF
CREATE DATABASE kafkadb;

\c kafkadb

CREATE TABLE user_events (
    id SERIAL PRIMARY KEY,
    user_id VARCHAR(50),
    event_type VARCHAR(50),
    timestamp BIGINT,
    data TEXT
);

\q
EOF
```

Verify:

```bash
sudo -u postgres psql -d kafkadb -c "\dt"
```

Expected:

```text
user_events
```

---

# 🚀 Step 2: Start Kafka Services

---

## 🦓 Terminal 1 — Start ZooKeeper

```bash
cd $KAFKA_HOME

bin/zookeeper-server-start.sh config/zookeeper.properties
```

---

## 📨 Terminal 2 — Start Kafka Broker

```bash
cd $KAFKA_HOME

bin/kafka-server-start.sh config/server.properties
```

---

# 📂 Step 3: Create Kafka Topic

Create topic:

```bash
cd $KAFKA_HOME

bin/kafka-topics.sh --create \
  --topic user-events \
  --bootstrap-server localhost:9092 \
  --partitions 1 \
  --replication-factor 1
```

Verify:

```bash
bin/kafka-topics.sh --list \
  --bootstrap-server localhost:9092
```

Expected:

```text
user-events
```

---

# ⚙️ Step 4: Configure JDBC Sink Connector

Create configuration:

```bash
cat > config/jdbc-sink-connector.properties << 'EOF'
name=jdbc-sink-connector
connector.class=io.confluent.connect.jdbc.JdbcSinkConnector
tasks.max=1
topics=user-events
connection.url=jdbc:postgresql://localhost:5432/kafkadb
connection.user=postgres
connection.password=
auto.create=false
auto.evolve=false
insert.mode=insert
pk.mode=none
key.converter=org.apache.kafka.connect.storage.StringConverter
value.converter=org.apache.kafka.connect.json.JsonConverter
value.converter.schemas.enable=false
EOF
```

---

# 🛠️ Step 5: Configure Connect Worker

Create worker configuration:

```bash
cat > config/connect-standalone-custom.properties << 'EOF'
bootstrap.servers=localhost:9092
key.converter=org.apache.kafka.connect.storage.StringConverter
value.converter=org.apache.kafka.connect.json.JsonConverter
key.converter.schemas.enable=false
value.converter.schemas.enable=false
offset.storage.file.filename=/tmp/connect.offsets
offset.flush.interval.ms=10000
plugin.path=/home/ubuntu/kafka_2.13-3.5.1/connectors/confluentinc-kafka-connect-jdbc-10.7.4/lib
EOF
```

---

# ▶️ Step 6: Start Kafka Connect

Open Terminal 4:

```bash
cd $KAFKA_HOME

bin/connect-standalone.sh \
  config/connect-standalone-custom.properties \
  config/jdbc-sink-connector.properties
```

Expected Log:

```text
Connector started
```

---

# 📨 Step 7: Produce Test Data

Open Terminal 5:

```bash
cd $KAFKA_HOME

bin/kafka-console-producer.sh \
  --topic user-events \
  --bootstrap-server localhost:9092 << EOF
{"user_id":"user001","event_type":"login","timestamp":1699000000,"data":"successful login"}
{"user_id":"user002","event_type":"purchase","timestamp":1699000060,"data":"item: laptop"}
{"user_id":"user003","event_type":"logout","timestamp":1699000120,"data":"session ended"}
EOF
```

---

# ✅ Step 8: Verify Data in PostgreSQL

Query database:

```bash
sudo -u postgres psql -d kafkadb \
-c "SELECT * FROM user_events;"
```

Expected:

```text
user001
user002
user003
```

All records should appear successfully.

---

# 📁 Task 2: Configure File Sink Connector

---

# ⚙️ Step 1: Create File Sink Configuration

```bash
cat > config/file-sink-connector.properties << 'EOF'
name=file-sink-connector
connector.class=org.apache.kafka.connect.file.FileStreamSinkConnector
tasks.max=1
topics=user-events
file=/tmp/kafka-output.txt
key.converter=org.apache.kafka.connect.storage.StringConverter
value.converter=org.apache.kafka.connect.json.JsonConverter
value.converter.schemas.enable=false
EOF
```

---

# ▶️ Step 2: Start Both Connectors

Stop existing Connect worker:

```text
Ctrl + C
```

Restart:

```bash
cd $KAFKA_HOME

bin/connect-standalone.sh \
  config/connect-standalone-custom.properties \
  config/jdbc-sink-connector.properties \
  config/file-sink-connector.properties
```

---

# 📨 Step 3: Produce Additional Events

```bash
cd $KAFKA_HOME

bin/kafka-console-producer.sh \
  --topic user-events \
  --bootstrap-server localhost:9092 << EOF
{"user_id":"user004","event_type":"signup","timestamp":1699000180,"data":"new account created"}
{"user_id":"user005","event_type":"view","timestamp":1699000240,"data":"product page viewed"}
EOF
```

---

# 📄 Step 4: Verify File Output

Check file:

```bash
cat /tmp/kafka-output.txt
```

Expected:

```json
{"user_id":"user001","event_type":"login"}
{"user_id":"user002","event_type":"purchase"}
{"user_id":"user003","event_type":"logout"}
{"user_id":"user004","event_type":"signup"}
{"user_id":"user005","event_type":"view"}
```

---

# 🗄️ Step 5: Verify PostgreSQL Updates

Check record count:

```bash
sudo -u postgres psql -d kafkadb \
-c "SELECT COUNT(*) FROM user_events;"
```

Expected:

```text
5
```

---

# ✅ Verification

---

# 🔍 Connector Status Check

Monitor logs:

```bash
tail -f $KAFKA_HOME/logs/connect.log | grep -i "sink"
```

Look for:

✅ WorkerSinkTask

✅ Offset Commits

✅ Connector Started

---

# 🔄 End-to-End Verification

Produce unique message:

```bash
echo '{"user_id":"test999","event_type":"verification","timestamp":1699999999,"data":"end-to-end test"}' | \
bin/kafka-console-producer.sh \
--topic user-events \
--bootstrap-server localhost:9092
```

Verify PostgreSQL:

```bash
sudo -u postgres psql -d kafkadb \
-c "SELECT * FROM user_events WHERE user_id='test999';"
```

Verify File Sink:

```bash
grep "test999" /tmp/kafka-output.txt
```

Expected:

✅ Record appears in PostgreSQL

✅ Record appears in output file

---

# 📊 Performance Monitoring

Check consumer lag:

```bash
bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --describe \
  --group connect-file-sink-connector
```

Monitor:

* Current Offset
* Log End Offset
* Lag

---

# 🛠️ Troubleshooting

---

## ❌ Connector Fails to Start

Verify:

```bash
sudo systemctl status postgresql
```

Check plugin path:

```bash
echo $KAFKA_HOME
```

Ensure JDBC JAR exists:

```bash
ls connectors
```

---

## ❌ No Data in PostgreSQL

Verify table:

```bash
sudo -u postgres psql -d kafkadb -c "\dt"
```

Check logs:

```bash
tail -f logs/connect.log
```

Verify credentials:

```properties
connection.user=postgres
connection.password=
```

---

## ❌ File Sink Not Writing

Verify output file:

```bash
ls -lh /tmp/kafka-output.txt
```

Check permissions:

```bash
ls -ld /tmp
```

---

## ❌ Connection Refused

Verify Kafka:

```bash
netstat -tulnp | grep 9092
```

Verify ZooKeeper:

```bash
netstat -tulnp | grep 2181
```

---

# 📋 Lab Deliverables

At the end of this lab you should have:

📄 jdbc-sink-connector.properties

📄 file-sink-connector.properties

📄 connect-standalone-custom.properties

🗄️ PostgreSQL Database (kafkadb)

📊 user_events Table

📁 kafka-output.txt

🔌 Kafka Connect Worker

📨 user-events Topic

---

# 💡 Key Takeaways

### Kafka Connect

Provides integration without custom application code.

### JDBC Sink Connector

Streams Kafka data directly into relational databases.

### File Sink Connector

Stores streaming events into local files or cloud storage systems.

### Multiple Connectors

Multiple sinks can consume the same topic simultaneously.

### Standalone Mode

Ideal for development and testing environments.

---

# 🚀 Next Steps

Explore:

* Source Connectors
* Distributed Kafka Connect Mode
* Single Message Transforms (SMTs)
* Schema Registry Integration
* Debezium CDC Connectors
* JMX Monitoring
* Cloud Storage Connectors (AWS S3, Azure Blob, GCS)

---

# 🎓 Conclusion

You have successfully:

✅ Installed Kafka Connect

✅ Configured JDBC Sink Connector

✅ Streamed Kafka Data to PostgreSQL

✅ Configured File Sink Connector

✅ Verified Multi-Sink Data Streaming

✅ Monitored Connector Operations

✅ Implemented End-to-End Integration Pipelines

---

<div align="center">

# 🎉 Congratulations!

You have built a complete Kafka Connect integration pipeline capable of streaming real-time data from Kafka topics into multiple external systems.

### ⭐ Happy Learning & Happy Streaming! ⭐

</div>
