# 🚀 Real-Time Data Streaming with Apache Kafka

<div align="center">

# 📨 Apache Kafka Real-Time Streaming 

### Create • Stream • Consume • Monitor

![Apache Kafka](https://img.shields.io/badge/Apache-Kafka-black?style=for-the-badge\&logo=apachekafka)
![Java](https://img.shields.io/badge/OpenJDK-11-red?style=for-the-badge\&logo=openjdk)
![Linux](https://img.shields.io/badge/Linux-Ubuntu-orange?style=for-the-badge\&logo=linux)
![Streaming](https://img.shields.io/badge/Real--Time-Streaming-green?style=for-the-badge)
![PubSub](https://img.shields.io/badge/Pub--Sub-Messaging-blue?style=for-the-badge)
![Event Driven](https://img.shields.io/badge/Event-Driven-purple?style=for-the-badge)

</div>

---

# 📖 Overview

Apache Kafka is a distributed event-streaming platform used for building real-time data pipelines and streaming applications.

In this lab, you will learn how to:

✅ Install Apache Kafka

✅ Configure ZooKeeper and Kafka Broker

✅ Create and Manage Topics

✅ Produce Real-Time Messages

✅ Consume Messages

✅ Work with Consumer Groups

✅ Monitor Offsets and Lag

---

# 🎯 Learning Objectives

By completing this lab, you will be able to:

### ⚙️ Kafka Setup

* Install Apache Kafka
* Configure Kafka services
* Verify Kafka components

### 📨 Messaging

* Create Kafka topics
* Produce real-time events
* Consume events from topics

### 🔄 Streaming Concepts

* Understand Publish/Subscribe architecture
* Learn partition-based scalability
* Explore message keys and routing

### 👥 Consumer Groups

* Implement load balancing
* Track consumer offsets
* Monitor consumer lag

---

# 📚 Prerequisites

Before starting this lab, ensure you have:

* 🐧 Basic Linux command-line knowledge
* 💻 Familiarity with terminal operations
* 🌐 Basic networking concepts
* ☕ Java fundamentals
* 📡 Understanding of localhost and ports

---

# 🖥️ System Requirements

| Component | Requirement   |
| --------- | ------------- |
| OS        | Linux         |
| RAM       | Minimum 2GB   |
| Java      | OpenJDK 11    |
| Internet  | Required      |
| Kafka     | Version 3.6.0 |

---

# ⚙️ Environment Setup

---

# ☕ Install Java

Update repositories:

```bash
sudo apt update
```

Install OpenJDK:

```bash
sudo apt install -y openjdk-11-jdk
```

Verify installation:

```bash
java -version
```

Expected:

```text
openjdk version "11.x.x"
```

---

# ⬇️ Download and Install Kafka

Navigate to Home Directory:

```bash
cd ~
```

Download Kafka:

```bash
wget https://downloads.apache.org/kafka/3.6.0/kafka_2.13-3.6.0.tgz
```

Extract Archive:

```bash
tar -xzf kafka_2.13-3.6.0.tgz
```

Rename Directory:

```bash
mv kafka_2.13-3.6.0 kafka
```

Enter Kafka Directory:

```bash
cd kafka
```

---

# 🚀 Start Kafka Services

Kafka requires ZooKeeper and Kafka Broker.

---

## 🦓 Terminal 1 - Start ZooKeeper

```bash
cd ~/kafka

bin/zookeeper-server-start.sh config/zookeeper.properties
```

---

## 📨 Terminal 2 - Start Kafka Broker

```bash
cd ~/kafka

bin/kafka-server-start.sh config/server.properties
```

Keep both terminals running.

Open a third terminal for remaining tasks.

---

# 📂 Task 1: Create and Manage Kafka Topics

---

# 📝 Step 1: Create Topic

Create a topic called **user-events**:

```bash
cd ~/kafka

bin/kafka-topics.sh --create \
  --topic user-events \
  --bootstrap-server localhost:9092 \
  --partitions 3 \
  --replication-factor 1
```

### Parameter Explanation

| Parameter            | Description               |
| -------------------- | ------------------------- |
| --topic              | Topic Name                |
| --bootstrap-server   | Kafka Broker              |
| --partitions         | Parallel Processing Units |
| --replication-factor | Number of Copies          |

---

# 📋 Step 2: List Topics

```bash
bin/kafka-topics.sh \
--list \
--bootstrap-server localhost:9092
```

Expected:

```text
user-events
```

---

# 🔍 Step 3: Describe Topic

```bash
bin/kafka-topics.sh \
--describe \
--topic user-events \
--bootstrap-server localhost:9092
```

Verify:

✅ 3 Partitions

✅ Replication Factor = 1

✅ Leader Assignment

---

# 📨 Task 2: Produce Messages

---

# ▶️ Step 1: Start Producer

Open Terminal 4:

```bash
cd ~/kafka

bin/kafka-console-producer.sh \
  --topic user-events \
  --bootstrap-server localhost:9092
```

---

# ✍️ Step 2: Send Messages

Type each message and press Enter:

```json
{"user_id":101,"action":"login","timestamp":"2024-01-15T10:30:00"}
```

```json
{"user_id":102,"action":"page_view","timestamp":"2024-01-15T10:31:00"}
```

```json
{"user_id":101,"action":"purchase","timestamp":"2024-01-15T10:32:00"}
```

```json
{"user_id":103,"action":"logout","timestamp":"2024-01-15T10:33:00"}
```

---

# 🔑 Step 3: Producer with Keys

Stop Producer:

```text
Ctrl + C
```

Start Key-Based Producer:

```bash
bin/kafka-console-producer.sh \
  --topic user-events \
  --bootstrap-server localhost:9092 \
  --property "parse.key=true" \
  --property "key.separator=:"
```

Send:

```text
user101:{"action":"login","timestamp":"2024-01-15T10:35:00"}
```

```text
user102:{"action":"signup","timestamp":"2024-01-15T10:36:00"}
```

### Why Use Keys?

✅ Same user messages go to same partition

✅ Preserves ordering

✅ Improves processing efficiency

---

# 📥 Task 3: Consume Messages

---

# ▶️ Step 1: Consume From Beginning

Open Terminal 5:

```bash
cd ~/kafka

bin/kafka-console-consumer.sh \
  --topic user-events \
  --bootstrap-server localhost:9092 \
  --from-beginning
```

Expected:

```text
All previously produced messages
```

---

# ⚡ Step 2: Real-Time Consumption

Stop Consumer:

```text
Ctrl + C
```

Restart:

```bash
bin/kafka-console-consumer.sh \
  --topic user-events \
  --bootstrap-server localhost:9092
```

Now produce new messages and watch them appear instantly.

---

# 👥 Step 3: Consumer Groups

Start Consumer Group:

```bash
bin/kafka-console-consumer.sh \
  --topic user-events \
  --bootstrap-server localhost:9092 \
  --group analytics-group \
  --from-beginning
```

Benefits:

✅ Load Balancing

✅ Fault Tolerance

✅ Offset Tracking

---

# ⚖️ Step 4: Multiple Consumers

Open Terminal 6:

```bash
cd ~/kafka

bin/kafka-console-consumer.sh \
  --topic user-events \
  --bootstrap-server localhost:9092 \
  --group analytics-group
```

Observation:

Messages are distributed among consumers in the same group.

---

# 📊 Step 5: Consumer Group Details

List Groups:

```bash
bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --list
```

Describe Group:

```bash
bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --group analytics-group \
  --describe
```

Output Includes:

| Metric         | Description       |
| -------------- | ----------------- |
| Current Offset | Last Read Message |
| Log End Offset | Latest Message    |
| Lag            | Unread Messages   |
| Consumer ID    | Active Consumer   |

---

# ✅ Verification

---

## Verify Topic

```bash
bin/kafka-topics.sh \
--list \
--bootstrap-server localhost:9092
```

Expected:

```text
user-events
```

---

## Verify Topic Details

```bash
bin/kafka-topics.sh \
--describe \
--topic user-events \
--bootstrap-server localhost:9092
```

Expected:

✅ 3 Partitions

✅ Replication Factor 1

---

## Verify Produced Messages

```bash
bin/kafka-run-class.sh kafka.tools.GetOffsetShell \
  --broker-list localhost:9092 \
  --topic user-events
```

Expected:

```text
Non-zero offsets
```

---

## Verify Consumer Lag

```bash
bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --group analytics-group \
  --describe
```

Expected:

```text
Lag = 0 or low value
```

---

# 🔄 End-to-End Test

### Step 1

Start Consumer

### Step 2

Send Message

```json
{"test":"verification"}
```

### Step 3

Verify message appears within:

```text
1-2 Seconds
```

---

# 🛠️ Troubleshooting

---

## ❌ Kafka Won't Start

Check Running Processes:

```bash
ps aux | grep kafka
```

Kill Existing Processes:

```bash
pkill -f kafka
```

---

## ❌ ZooKeeper Connection Error

Check Port:

```bash
netstat -tuln | grep 2181
```

Restart ZooKeeper:

```bash
cd ~/kafka

bin/zookeeper-server-start.sh config/zookeeper.properties
```

---

## ❌ Consumer Not Receiving Messages

Verify:

* Correct Topic Name
* Correct Broker Address
* Producer Running
* Consumer Started

Try:

```bash
--from-beginning
```

---

## ❌ Out of Memory Error

Reduce Heap Size:

```bash
export KAFKA_HEAP_OPTS="-Xmx512M -Xms512M"
```

Restart Broker.

---

# 🧹 Cleanup

---

## Stop Kafka Broker

```bash
cd ~/kafka

bin/kafka-server-stop.sh
```

---

## Stop ZooKeeper

```bash
bin/zookeeper-server-stop.sh
```

---

## Delete Topic (Optional)

```bash
bin/kafka-topics.sh \
  --delete \
  --topic user-events \
  --bootstrap-server localhost:9092
```

---

# 🏆 Lab Deliverables

At the end of this lab you should have:

📨 user-events Topic

📂 3 Partitions

🦓 ZooKeeper Service

📡 Kafka Broker

👥 Consumer Group

📊 Offset Monitoring

⚡ Real-Time Message Streaming

---

# 🌍 Real-World Applications

### Event Streaming

* User Activity Tracking
* Application Logs
* Clickstream Data

### Analytics

* Real-Time Dashboards
* Customer Behavior Analysis
* Fraud Detection

### IoT

* Sensor Data Collection
* Smart Devices
* Telemetry Pipelines

### Financial Systems

* Transaction Processing
* Market Data Streaming

---

# 🎓 Conclusion

You have successfully:

✅ Installed Apache Kafka

✅ Configured ZooKeeper

✅ Created Topics

✅ Produced Real-Time Events

✅ Consumed Messages

✅ Worked with Consumer Groups

✅ Monitored Offsets and Lag

---

# 💡 Key Concepts Learned

### Topics

Logical channels for organizing messages.

### Partitions

Enable scalability and parallel processing.

### Producers

Applications that publish messages.

### Consumers

Applications that subscribe to messages.

### Consumer Groups

Provide load balancing and fault tolerance.

---

# 🚀 Next Steps

Explore:

* Kafka Streams
* Kafka Connect
* Schema Registry
* Multi-Broker Clusters
* Python Kafka Clients
* Java Kafka APIs
* Real-Time ETL Pipelines

---

<div align="center">

# 🎉 Congratulations!

You have successfully built and tested a real-time Apache Kafka streaming environment.

### ⭐ Happy Streaming & Happy Learning! ⭐

</div>
