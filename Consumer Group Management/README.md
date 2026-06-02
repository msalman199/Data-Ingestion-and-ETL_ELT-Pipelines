# 🚀 Consumer Group Management

<div align="center">

# 👥 Apache Kafka Consumer Group Management 

### Configure • Scale • Rebalance • Monitor

![Apache Kafka](https://img.shields.io/badge/Apache-Kafka-black?style=for-the-badge\&logo=apachekafka)
![Python](https://img.shields.io/badge/Python-kafka--python-blue?style=for-the-badge\&logo=python)
![Linux](https://img.shields.io/badge/Linux-Ubuntu-orange?style=for-the-badge\&logo=linux)
![ZooKeeper](https://img.shields.io/badge/ZooKeeper-Coordination-yellow?style=for-the-badge)
![Consumer Groups](https://img.shields.io/badge/Kafka-Consumer%20Groups-green?style=for-the-badge)
![Distributed Systems](https://img.shields.io/badge/Distributed-Processing-purple?style=for-the-badge)

</div>

---

# 📖 Overview

Consumer Groups are one of Kafka's most powerful features, enabling scalable, fault-tolerant message consumption across multiple consumers.

In this lab, you will:

✅ Configure Kafka Consumer Groups

✅ Understand Partition Assignment

✅ Monitor Consumer Offsets

✅ Observe Consumer Rebalancing

✅ Track Consumer Lag

✅ Reset Consumer Group Offsets

---

# 🎯 Learning Objectives

By the end of this lab, you will be able to:

### 👥 Consumer Group Management

* Create consumer groups
* Configure multiple consumers
* Manage group membership

### ⚖️ Partition Assignment

* Understand partition distribution
* Observe automatic balancing
* Monitor ownership changes

### 🔄 Rebalancing

* Trigger consumer rebalancing
* Analyze assignment changes
* Handle consumer join/leave events

### 📊 Monitoring

* Track offsets
* Monitor lag
* Verify message consumption

---

# 📚 Prerequisites

Before starting this lab, ensure you have:

* 🐧 Linux command-line experience
* 📨 Basic Kafka knowledge
* ⚙️ Understanding of topics and partitions
* ☕ Java 11+ installed
* 📡 Kafka broker running

---

# 🖥️ Environment Setup

---

# ⬇️ Install Apache Kafka

Download Kafka:

```bash
cd ~

wget https://archive.apache.org/dist/kafka/3.6.0/kafka_2.13-3.6.0.tgz
```

Extract Archive:

```bash
tar -xzf kafka_2.13-3.6.0.tgz
```

Navigate:

```bash
cd kafka_2.13-3.6.0
```

Configure Environment Variables:

```bash
export KAFKA_HOME=~/kafka_2.13-3.6.0

export PATH=$PATH:$KAFKA_HOME/bin
```

Verify:

```bash
echo $KAFKA_HOME
```

---

# 🚀 Start Kafka Services

---

## 🦓 Start ZooKeeper

```bash
$KAFKA_HOME/bin/zookeeper-server-start.sh \
-daemon $KAFKA_HOME/config/zookeeper.properties
```

Wait:

```bash
sleep 5
```

---

## 📨 Start Kafka Broker

```bash
$KAFKA_HOME/bin/kafka-server-start.sh \
-daemon $KAFKA_HOME/config/server.properties
```

Wait:

```bash
sleep 10
```

---

# 📂 Create Test Topic

Create topic with 4 partitions:

```bash
$KAFKA_HOME/bin/kafka-topics.sh --create \
  --bootstrap-server localhost:9092 \
  --topic orders \
  --partitions 4 \
  --replication-factor 1
```

Verify:

```bash
$KAFKA_HOME/bin/kafka-topics.sh --describe \
  --bootstrap-server localhost:9092 \
  --topic orders
```

Expected:

✅ 4 Partitions

✅ Replication Factor = 1

---

# 🏗️ Task 1: Configure Consumer Groups

---

# 📁 Step 1: Create Configuration Directory

```bash
mkdir -p ~/kafka-lab/consumers

cd ~/kafka-lab/consumers
```

---

# ⚙️ Create Consumer Configuration Files

---

## Consumer 1 Configuration

```bash
cat > consumer1.properties << 'EOF'
bootstrap.servers=localhost:9092
group.id=order-processing-group
key.deserializer=org.apache.kafka.common.serialization.StringDeserializer
value.deserializer=org.apache.kafka.common.serialization.StringDeserializer
auto.offset.reset=earliest
enable.auto.commit=true
auto.commit.interval.ms=1000
session.timeout.ms=10000
heartbeat.interval.ms=3000
EOF
```

---

## Consumer 2 Configuration

```bash
cat > consumer2.properties << 'EOF'
bootstrap.servers=localhost:9092
group.id=order-processing-group
key.deserializer=org.apache.kafka.common.serialization.StringDeserializer
value.deserializer=org.apache.kafka.common.serialization.StringDeserializer
auto.offset.reset=earliest
enable.auto.commit=false
session.timeout.ms=10000
heartbeat.interval.ms=3000
EOF
```

---

# 🐍 Step 2: Install Python Kafka Library

```bash
pip3 install kafka-python
```

Verify:

```bash
pip3 show kafka-python
```

---

# 📝 Create Consumer Script

Create file:

```bash
nano consumer_group_member.py
```

Key Features:

✅ Connect to Kafka

✅ Join Consumer Group

✅ Read Messages

✅ Display Partition Assignments

✅ Track Offsets

---

# ▶️ Step 3: Start Consumers

---

## Terminal 1

```bash
cd ~/kafka-lab/consumers

python3 consumer_group_member.py consumer-1
```

---

## Terminal 2

```bash
cd ~/kafka-lab/consumers

python3 consumer_group_member.py consumer-2
```

---

# 🔍 Step 4: Monitor Consumer Group

List Groups:

```bash
$KAFKA_HOME/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --list
```

Describe Group:

```bash
$KAFKA_HOME/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --group order-processing-group \
  --describe
```

Expected:

✅ Group Appears

✅ Partitions Assigned

✅ Offsets Visible

---

# 📨 Task 2: Test Consumer Rebalancing

---

# 📤 Step 1: Produce Test Messages

Generate 20 Orders:

```bash
for i in {1..20}; do
  echo "order-$i:Product purchased - Order ID: $i, Amount: \$$(($i * 10))" | \
  $KAFKA_HOME/bin/kafka-console-producer.sh \
    --bootstrap-server localhost:9092 \
    --topic orders \
    --property "parse.key=true" \
    --property "key.separator=:"
done
```

Observe:

📨 Messages distributed across partitions

👥 Consumers process assigned partitions

---

# ➕ Step 2: Add New Consumer

Open Terminal 3:

```bash
cd ~/kafka-lab/consumers

python3 consumer_group_member.py consumer-3
```

Observe:

⚠ Existing consumers pause briefly

🔄 Rebalance begins

📊 Partitions reassigned

▶ Processing resumes

---

# 🔍 Verify New Assignment

```bash
$KAFKA_HOME/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --group order-processing-group \
  --describe
```

Expected:

| Consumer Count | Possible Distribution |
| -------------- | --------------------- |
| 2 Consumers    | 2 + 2 Partitions      |
| 3 Consumers    | 2 + 1 + 1 Partitions  |

---

# ➖ Step 3: Remove Consumer

Stop Consumer 1:

```text
Ctrl + C
```

Observe:

🔄 Rebalance Triggered

📊 Partitions Redistributed

✅ No Message Loss

---

# 📈 Verify Rebalance

```bash
$KAFKA_HOME/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --group order-processing-group \
  --describe
```

Expected:

Remaining consumers own all partitions.

---

# 📊 Step 4: Monitor Consumer Lag

Create Monitoring Script:

```bash
nano monitor_lag.sh
```

Make Executable:

```bash
chmod +x monitor_lag.sh
```

Run:

```bash
./monitor_lag.sh
```

Displays:

* Current Offset
* Log End Offset
* Lag
* Consumer Ownership

---

# 🛑 Generate Lag

Stop All Consumers

Produce More Messages:

```bash
for i in {21..70}; do
  echo "order-$i:Product purchased - Order ID: $i, Amount: \$$(($i * 10))" | \
  $KAFKA_HOME/bin/kafka-console-producer.sh \
    --bootstrap-server localhost:9092 \
    --topic orders \
    --property "parse.key=true" \
    --property "key.separator=:"
done
```

Observe:

📈 Lag Increases

📨 Messages Remain in Topic

---

# 🔄 Step 5: Reset Consumer Offsets

---

## Reset to Earliest

```bash
$KAFKA_HOME/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --group order-processing-group \
  --topic orders \
  --reset-offsets \
  --to-earliest \
  --execute
```

---

## Reset to Specific Offset

```bash
$KAFKA_HOME/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --group order-processing-group \
  --topic orders \
  --reset-offsets \
  --to-offset 5 \
  --execute
```

---

# ✅ Verification

---

## Verify Consumer Group Exists

```bash
$KAFKA_HOME/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --list
```

Expected:

```text
order-processing-group
```

---

## Verify Partition Assignment

```bash
$KAFKA_HOME/bin/kafka-consumer-groups.sh \
  --group order-processing-group \
  --bootstrap-server localhost:9092 \
  --describe
```

Expected:

✅ All 4 Partitions Assigned

✅ Active Consumers Visible

---

## Verify Rebalancing

### Start 2 Consumers

Expected:

```text
Consumer A → 2 Partitions
Consumer B → 2 Partitions
```

### Add 3rd Consumer

Expected:

```text
Consumer A → 2 Partitions
Consumer B → 1 Partition
Consumer C → 1 Partition
```

### Remove Consumer

Expected:

Remaining consumers reclaim partitions automatically.

---

## Verify Message Consumption

Check Topic Offsets:

```bash
$KAFKA_HOME/bin/kafka-run-class.sh kafka.tools.GetOffsetShell \
  --broker-list localhost:9092 \
  --topic orders
```

Expected:

Non-zero offsets indicating successful processing.

---

# 🛠️ Troubleshooting

---

## ❌ Consumer Not Joining Group

Verify Broker:

```bash
jps | grep Kafka
```

Verify Port:

```bash
netstat -an | grep 9092
```

Review Consumer Logs.

---

## ❌ Rebalancing Too Slow

Reduce:

```properties
session.timeout.ms=5000
```

Ensure:

```properties
heartbeat.interval.ms < session.timeout.ms / 3
```

---

## ❌ Uneven Message Distribution

Verify:

```bash
$KAFKA_HOME/bin/kafka-topics.sh \
--describe \
--topic orders \
--bootstrap-server localhost:9092
```

Ensure:

✅ Multiple Partitions Exist

---

## ❌ Consumer Lag Increasing

Solutions:

* Add More Consumers
* Optimize Processing Logic
* Increase Fetch Size
* Tune Consumer Configuration

---

# 🏆 Lab Deliverables

At the end of this lab you should have:

📄 consumer1.properties

📄 consumer2.properties

📄 consumer_group_member.py

📄 monitor_lag.sh

📨 orders Topic

👥 order-processing-group

📊 Offset Monitoring

🔄 Rebalance Demonstration

---

# 💡 Key Takeaways

### Consumer Groups

Enable horizontal scaling of message processing.

### Rebalancing

Automatically redistributes partitions when consumers join or leave.

### Partition Ownership

Each partition belongs to exactly one consumer in a group.

### Lag Monitoring

Critical metric for production Kafka environments.

### Offset Management

Determines processing guarantees and recovery behavior.

---

# 🚀 Next Steps

Explore:

* Sticky Partition Assignor
* Round Robin Assignor
* Range Assignor
* Manual Offset Management
* Exactly-Once Processing
* Rebalance Listeners
* Multi-Broker Kafka Clusters
* Failure Recovery Scenarios

---

# 🎓 Conclusion

You have successfully:

✅ Configured Kafka Consumer Groups

✅ Started Multiple Consumers

✅ Observed Automatic Rebalancing

✅ Monitored Offsets and Lag

✅ Managed Consumer Group Membership

✅ Reset Consumer Offsets

✅ Verified Distributed Message Processing

---

<div align="center">

# 🎉 Congratulations!

You now understand how Kafka Consumer Groups enable scalable, fault-tolerant message processing in distributed streaming systems.

### ⭐ Happy Learning & Happy Streaming! ⭐

</div>
