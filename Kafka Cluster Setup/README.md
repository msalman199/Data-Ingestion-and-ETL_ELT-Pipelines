# 🚀 Kafka Cluster Setup

<div align="center">

# 📨 Apache Kafka Multi-Broker Cluster 

### Install • Configure • Test • Verify

![Apache Kafka](https://img.shields.io/badge/Apache-Kafka-black?style=for-the-badge\&logo=apachekafka)
![ZooKeeper](https://img.shields.io/badge/Apache-ZooKeeper-yellow?style=for-the-badge)
![Linux](https://img.shields.io/badge/Linux-Ubuntu-orange?style=for-the-badge\&logo=linux)
![Java](https://img.shields.io/badge/OpenJDK-11-red?style=for-the-badge\&logo=openjdk)
![Distributed Systems](https://img.shields.io/badge/Distributed-Systems-blue?style=for-the-badge)
![Messaging](https://img.shields.io/badge/Event-Streaming-green?style=for-the-badge)

</div>

---

# 📖 Overview

Apache Kafka is a distributed event-streaming platform used for building real-time data pipelines and streaming applications.

In this lab, you will deploy a complete **3-Broker Kafka Cluster** on a single Linux machine using **ZooKeeper** for coordination.

The lab covers:

✅ Kafka Installation

✅ ZooKeeper Configuration

✅ Multi-Broker Cluster Setup

✅ Topic Management

✅ Producer & Consumer Testing

✅ Cluster Resilience Verification

✅ Replication Validation

---

# 🎯 Learning Objectives

By the end of this lab, you will be able to:

### ⚙️ Installation

* Install Apache Kafka
* Install Java Runtime Environment
* Configure Linux environment variables

### 🏗️ Cluster Configuration

* Configure ZooKeeper
* Deploy multiple Kafka brokers
* Configure replication and partitions

### 📡 Messaging

* Create Kafka topics
* Produce messages
* Consume messages

### 🔍 Monitoring

* Verify broker communication
* Check cluster health
* Monitor partition leadership

### 🛡️ Fault Tolerance

* Simulate broker failures
* Verify automatic leader election
* Validate replication mechanisms

---

# 📚 Prerequisites

Before starting this lab ensure you have:

* 🐧 Basic Linux command-line skills
* 🌐 Networking fundamentals
* ☕ Java runtime knowledge
* ⚙️ Configuration file experience
* 🏗️ Distributed systems understanding

---

# 🖥️ Environment Requirements

| Requirement      | Specification             |
| ---------------- | ------------------------- |
| Operating System | Ubuntu 20.04+ / CentOS 7+ |
| RAM              | Minimum 4 GB              |
| Storage          | Minimum 20 GB             |
| Internet         | Required                  |
| Java             | OpenJDK 11                |

---

# 📦 Task 1: Install Kafka & ZooKeeper

---

# ☕ Step 1.1 Install Java

Kafka requires Java 8 or higher.

### Update Package Repository

```bash
sudo apt update
```

### Install OpenJDK 11

```bash
sudo apt install -y openjdk-11-jdk
```

### Verify Installation

```bash
java -version
```

Expected:

```text
openjdk version "11.x.x"
```

---

# ⬇️ Step 1.2 Download Kafka

Navigate to the installation directory.

```bash
cd /opt
```

Download Kafka:

```bash
sudo wget https://downloads.apache.org/kafka/3.6.0/kafka_2.13-3.6.0.tgz
```

Extract Archive:

```bash
sudo tar -xzf kafka_2.13-3.6.0.tgz
```

Create Symbolic Link:

```bash
sudo ln -s kafka_2.13-3.6.0 kafka
```

Set Ownership:

```bash
sudo chown -R $USER:$USER /opt/kafka_2.13-3.6.0
```

---

# 🌍 Step 1.3 Configure Environment Variables

Add Kafka to PATH:

```bash
echo 'export KAFKA_HOME=/opt/kafka' >> ~/.bashrc

echo 'export PATH=$PATH:$KAFKA_HOME/bin' >> ~/.bashrc
```

Reload Shell:

```bash
source ~/.bashrc
```

Verify:

```bash
echo $KAFKA_HOME
```

Expected:

```text
/opt/kafka
```

---

# 🏗️ Task 2: Configure Multi-Broker Kafka Cluster

---

# 🦓 Step 2.1 Configure ZooKeeper

Navigate to configuration directory:

```bash
cd $KAFKA_HOME/config
```

Create ZooKeeper data directory:

```bash
sudo mkdir -p /var/lib/zookeeper

sudo chown $USER:$USER /var/lib/zookeeper
```

Create configuration:

```bash
cat > zookeeper.properties << 'EOF'
dataDir=/var/lib/zookeeper
clientPort=2181
maxClientCnxns=0
admin.enableServer=false
tickTime=2000
initLimit=10
syncLimit=5
EOF
```

---

# 🖧 Step 2.2 Configure Broker 1

Create log directory:

```bash
sudo mkdir -p /var/lib/kafka-logs-1

sudo chown $USER:$USER /var/lib/kafka-logs-1
```

Broker Details:

| Setting   | Value                 |
| --------- | --------------------- |
| Broker ID | 1                     |
| Port      | 9092                  |
| Logs      | /var/lib/kafka-logs-1 |

Create:

```bash
cp server.properties server-1.properties
```

Modify:

```properties
broker.id=1
listeners=PLAINTEXT://localhost:9092
log.dirs=/var/lib/kafka-logs-1
zookeeper.connect=localhost:2181
```

---

# 🖧 Step 2.3 Configure Broker 2

Create Log Directory:

```bash
sudo mkdir -p /var/lib/kafka-logs-2
```

Broker Details:

| Setting   | Value                 |
| --------- | --------------------- |
| Broker ID | 2                     |
| Port      | 9093                  |
| Logs      | /var/lib/kafka-logs-2 |

```properties
broker.id=2
listeners=PLAINTEXT://localhost:9093
```

---

# 🖧 Step 2.4 Configure Broker 3

Create Log Directory:

```bash
sudo mkdir -p /var/lib/kafka-logs-3
```

Broker Details:

| Setting   | Value                 |
| --------- | --------------------- |
| Broker ID | 3                     |
| Port      | 9094                  |
| Logs      | /var/lib/kafka-logs-3 |

```properties
broker.id=3
listeners=PLAINTEXT://localhost:9094
```

---

# ▶️ Step 2.5 Start ZooKeeper

```bash
$KAFKA_HOME/bin/zookeeper-server-start.sh \
-daemon $KAFKA_HOME/config/zookeeper.properties
```

Wait:

```bash
sleep 5
```

Verify:

```bash
netstat -tuln | grep 2181
```

Expected:

```text
tcp 0 0 0.0.0.0:2181
```

---

# 🚀 Step 2.6 Start Kafka Brokers

Start Broker 1:

```bash
$KAFKA_HOME/bin/kafka-server-start.sh \
-daemon $KAFKA_HOME/config/server-1.properties
```

Start Broker 2:

```bash
$KAFKA_HOME/bin/kafka-server-start.sh \
-daemon $KAFKA_HOME/config/server-2.properties
```

Start Broker 3:

```bash
$KAFKA_HOME/bin/kafka-server-start.sh \
-daemon $KAFKA_HOME/config/server-3.properties
```

---

# 🔍 Step 2.7 Verify Services

Check ports:

```bash
netstat -tuln | grep -E '2181|9092|9093|9094'
```

Check Java processes:

```bash
jps
```

Expected:

```text
QuorumPeerMain
Kafka
Kafka
Kafka
```

---

# 📨 Task 3: Test Cluster Functionality

---

# 📂 Step 3.1 Create Replicated Topic

```bash
$KAFKA_HOME/bin/kafka-topics.sh \
--create \
--bootstrap-server localhost:9092 \
--replication-factor 3 \
--partitions 3 \
--topic test-cluster-topic
```

List Topics:

```bash
$KAFKA_HOME/bin/kafka-topics.sh \
--list \
--bootstrap-server localhost:9092
```

---

# 🔎 Step 3.2 Describe Topic

```bash
$KAFKA_HOME/bin/kafka-topics.sh \
--describe \
--bootstrap-server localhost:9092 \
--topic test-cluster-topic
```

Verify:

✅ 3 Partitions

✅ 3 Replicas

✅ ISR includes all brokers

✅ Distributed leaders

---

# 📤 Step 3.3 Test Producer

```bash
$KAFKA_HOME/bin/kafka-console-producer.sh \
--bootstrap-server localhost:9092 \
--topic test-cluster-topic
```

Sample Messages:

```text
Message 1: Testing broker 1
Message 2: Testing broker 2
Message 3: Testing broker 3
```

Exit:

```text
Ctrl + C
```

---

# 📥 Step 3.4 Test Consumer

Open another terminal:

```bash
$KAFKA_HOME/bin/kafka-console-consumer.sh \
--bootstrap-server localhost:9092 \
--topic test-cluster-topic \
--from-beginning
```

Expected:

```text
Message 1: Testing broker 1
Message 2: Testing broker 2
Message 3: Testing broker 3
```

---

# 👥 Step 3.5 Consumer Groups

Create Consumer Group:

```bash
$KAFKA_HOME/bin/kafka-console-consumer.sh \
--bootstrap-server localhost:9092 \
--topic test-cluster-topic \
--group test-consumer-group \
--from-beginning
```

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
--group test-consumer-group \
--describe
```

---

# 🛡️ Step 3.6 Test Cluster Resilience

Find Kafka Processes:

```bash
jps | grep Kafka
```

Stop Broker 3:

```bash
kill <BROKER_PID>
```

Check Topic Status:

```bash
$KAFKA_HOME/bin/kafka-topics.sh \
--describe \
--bootstrap-server localhost:9092 \
--topic test-cluster-topic
```

Observation:

✅ New leaders elected

✅ Cluster remains operational

✅ No data loss

Restart Broker:

```bash
$KAFKA_HOME/bin/kafka-server-start.sh \
-daemon $KAFKA_HOME/config/server-3.properties
```

---

# ✅ Verification

---

## Cluster Health

```bash
jps
```

```bash
netstat -tuln | grep -E '9092|9093|9094'
```

```bash
$KAFKA_HOME/bin/kafka-topics.sh \
--list \
--bootstrap-server localhost:9092
```

---

## Broker Metadata

```bash
$KAFKA_HOME/bin/kafka-broker-api-versions.sh \
--bootstrap-server localhost:9092 | head -20
```

---

## Replication Validation Script

Create verification script:

```bash
nano verify_cluster.sh
```

Add verification logic and run:

```bash
chmod +x verify_cluster.sh

./verify_cluster.sh
```

---

# 🎯 Expected Results

### ZooKeeper

✅ Running on Port 2181

### Brokers

✅ Broker 1 → 9092

✅ Broker 2 → 9093

✅ Broker 3 → 9094

### Topic

✅ test-cluster-topic exists

✅ 3 partitions

✅ Replication Factor = 3

### Cluster State

✅ All replicas in ISR

✅ Leader distribution active

✅ Produce & Consume successful

---

# 🛠️ Troubleshooting

---

## ❌ Broker Fails to Start

Check Logs:

```bash
tail -f /opt/kafka/logs/server.log
```

Possible Causes:

* Port conflict
* ZooKeeper unavailable
* Insufficient memory

---

## ❌ Cannot Connect to Broker

Verify Listener:

```bash
netstat -tuln | grep 9092
```

Firewall Check:

```bash
sudo ufw status
```

Connection Test:

```bash
telnet localhost 9092
```

---

## ❌ Under Replicated Partitions

Check:

```bash
$KAFKA_HOME/bin/kafka-topics.sh \
--describe \
--bootstrap-server localhost:9092 \
--under-replicated-partitions
```

Restart broker if required.

---

# 🧹 Cleanup

Stop Kafka:

```bash
pkill -f 'kafka.Kafka'
```

Stop ZooKeeper:

```bash
pkill -f 'org.apache.zookeeper'
```

Remove Data:

```bash
sudo rm -rf /var/lib/kafka-logs-*

sudo rm -rf /var/lib/zookeeper
```

---

# 🏆 Lab Deliverables

At the end of this lab you should have:

📄 server-1.properties

📄 server-2.properties

📄 server-3.properties

📄 zookeeper.properties

📄 verify_cluster.sh

📨 test-cluster-topic

🖧 3 Kafka Brokers

🦓 ZooKeeper Service

---

# 🎓 Conclusion

You have successfully:

✅ Installed Apache Kafka

✅ Installed ZooKeeper

✅ Built a 3-Broker Cluster

✅ Configured Replication

✅ Tested Producers & Consumers

✅ Simulated Broker Failure

✅ Verified Fault Tolerance

✅ Monitored Cluster Health

---

# 💡 Key Takeaways

* Kafka achieves fault tolerance through replication.
* ZooKeeper manages cluster metadata and leader election.
* Multiple brokers can run on a single machine using different ports.
* Partitions provide scalability and parallel processing.
* Replication ensures high availability and resiliency.

---

# 🚀 Next Steps

### Advanced Kafka Topics

* Kafka Streams
* Kafka Connect
* Schema Registry
* SSL/TLS Security
* SASL Authentication
* JMX Monitoring
* Disaster Recovery
* Multi-Node Production Clusters

---

<div align="center">

## 🎯 Congratulations!

You now have a fully functional Apache Kafka Cluster capable of supporting distributed event streaming workloads.

⭐ Happy Learning & Happy Streaming! ⭐

</div>
