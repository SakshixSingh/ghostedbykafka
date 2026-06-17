# Ghosted by Kafka

A hands-on Kafka demo built with [KafkaJS](https://kafka.js.org/) and Node.js. The app simulates **rider location updates** — producers publish rider names and locations to a Kafka topic, and consumers read those messages in real time.

This project is designed for learning: it covers Kafka **admin**, **producer**, and **consumer** clients, **topic partitioning**, and **consumer groups**.

---

## Table of Contents

- [Architecture](#architecture)
- [How It Works](#how-it-works)
- [Kafka Concepts Used](#kafka-concepts-used)
- [Prerequisites](#prerequisites)
- [Setup](#setup)
- [Running the Project](#running-the-project)
- [Project Structure](#project-structure)
- [Message Format](#message-format)
- [Partitioning Logic](#partitioning-logic)
- [Consumer Groups](#consumer-groups)
- [Troubleshooting](#troubleshooting)

---

## Architecture

```
┌─────────────┐     publish      ┌──────────────────┐     subscribe     ┌─────────────┐
│  producer.js│ ───────────────► │  rider-updates   │ ◄─────────────── │ consumer.js │
│  (stdin)    │                  │  (2 partitions)  │                  │ (group arg) │
└─────────────┘                  └──────────────────┘                  └─────────────┘
                                         ▲
                                         │ create topic
                                   ┌─────────────┐
                                   │   admin.js  │
                                   └─────────────┘

All clients share a single Kafka connection config in client.js
```

| File | Role |
|------|------|
| `client.js` | Shared Kafka client — broker address and `clientId` |
| `admin.js` | Creates the `rider-updates` topic with 2 partitions |
| `producer.js` | Interactive CLI — reads rider name + location, sends messages |
| `consumer.js` | Subscribes to `rider-updates` and logs incoming messages |

---

## How It Works

1. **Kafka broker** runs locally on `localhost:9092`.
2. **`admin.js`** connects to Kafka and creates the topic `rider-updates` with **2 partitions**. Partitions let Kafka scale reads/writes and preserve order *within* a partition.
3. **`producer.js`** connects, then waits for terminal input. Each line is parsed as `riderName location` (space-separated) and published as a JSON message.
4. **`consumer.js`** joins a **consumer group** (passed as a CLI argument), subscribes to the topic, and prints every message it receives — including partition number and offset.

---

## Kafka Concepts Used

### Broker
A Kafka server that stores and serves messages. This project connects to one broker at `localhost:9092`.

### Topic
A named stream of messages. Here: `rider-updates`.

### Partition
A topic is split into partitions for parallelism. This topic has **2 partitions**:
- **Partition 0** — messages where location is `north`
- **Partition 1** — all other locations

### Offset
A unique ID for each message within a partition. Consumers use offsets to track what they have already read.

### Producer
Publishes messages to a topic. The producer in this project explicitly chooses the partition based on location.

### Consumer & Consumer Group
Consumers read messages from topics. A **consumer group** is a set of consumers that share work:
- Each partition is assigned to **at most one consumer** in the group.
- With 2 partitions, you can run 2 consumers in the same group and each will handle one partition.
- Different groups each get **all** messages (broadcast behavior).

### Admin Client
Used for cluster management — here, only to create the topic before producing/consuming.

---

## Prerequisites

| Tool | Version | Purpose |
|------|---------|---------|
| [Node.js](https://nodejs.org/) | 18+ (20 LTS recommended) | Run the app |
| [Docker](https://www.docker.com/) | Latest | Run Kafka locally |

> **Note:** Node.js 24 works but may show a harmless `TimeoutNegativeWarning` from KafkaJS. Node 20 LTS avoids this.

---

## Setup

### 1. Clone and install dependencies

```bash
git clone https://github.com/SakshixSingh/ghostedbykafka.git
cd kafka-app
npm install
```

### 2. Start Kafka (Docker — KRaft mode)

The latest `confluentinc/cp-kafka` image uses **KRaft** (no Zookeeper). Run:

```bash
docker run -d --name kafka \
  -p 9092:9092 \
  -e KAFKA_NODE_ID=1 \
  -e KAFKA_PROCESS_ROLES=broker,controller \
  -e KAFKA_LISTENERS=PLAINTEXT://0.0.0.0:9092,CONTROLLER://0.0.0.0:9093 \
  -e KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://localhost:9092 \
  -e KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER \
  -e KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT \
  -e KAFKA_CONTROLLER_QUORUM_VOTERS=1@localhost:9093 \
  -e KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1 \
  confluentinc/cp-kafka
```

Verify Kafka is running:

```bash
docker ps
lsof -i :9092
```

### 3. Create the topic

```bash
node admin.js
```

Expected output:

```
Connnecting to Kafka...
Connected to Kafka
Creating Topics...
Topics created successfully...
Disconnecting from Kafka...
Disconnected from Kafka...
```

> Run `admin.js` only once (or again if you reset Kafka). Creating an existing topic is a no-op.

---

## Running the Project

Use **three terminals** for the full demo.

### Terminal 1 — Consumer (group A)

```bash
node consumer.js group-01
```

### Terminal 2 — Consumer (group B, optional)

Run a second consumer in a **different group** to see every message duplicated across groups:

```bash
node consumer.js group-02
```

Or run two consumers in the **same group** to see partition load-balancing:

```bash
# Terminal 2
node consumer.js group-01

# Terminal 3
node consumer.js group-01
```

With 2 partitions and 2 consumers in `group-01`, each consumer typically handles one partition.

### Terminal 3 — Producer

```bash
node producer.js
```

Type messages in the format:

```
RiderName location
```

Examples:

```
Sakshi north
Rahul south
Alex east
```

Press **Ctrl+C** to stop the producer (disconnects cleanly).

---

## Project Structure

```
kafka-app/
├── client.js      # Shared Kafka client config (brokers, clientId)
├── admin.js       # Topic creation (rider-updates, 2 partitions)
├── producer.js    # Interactive message producer (readline CLI)
├── consumer.js    # Message consumer (requires groupId CLI arg)
├── package.json
└── README.md
```

---

## Message Format

Each message published to `rider-updates` looks like:

```json
{
  "name": "Sakshi",
  "location": "north"
}
```

| Field | Source | Description |
|-------|--------|-------------|
| `key` | `"location-update"` | Message key (fixed for all messages) |
| `value` | JSON string | Rider name and location |
| `partition` | Chosen by producer | `0` if location is `north`, else `1` |

Consumer output example:

```json
{
  "partition": 0,
  "offset": "3",
  "value": "{\"name\":\"Sakshi\",\"location\":\"north\"}",
  "group": "group-01"
}
```

---

## Partitioning Logic

In `producer.js`, the partition is chosen manually:

```javascript
partition: location?.toLowerCase() === 'north' ? 0 : 1
```

| Input location | Partition |
|----------------|-----------|
| `north` (case-insensitive) | 0 |
| anything else (`south`, `east`, etc.) | 1 |

**Why partition manually?** This demo shows that you can route related data to the same partition so ordering is preserved for that subset (e.g. all "north" riders on partition 0). In production, Kafka can also auto-assign partitions via key hashing.

---

## Consumer Groups

`consumer.js` requires a group ID as the first CLI argument:

```bash
node consumer.js <groupId>
```

| Scenario | Command | Behavior |
|----------|---------|----------|
| Single consumer | `node consumer.js group-01` | Reads all partitions |
| Same group, 2 consumers | Two terminals, both `group-01` | Partitions split between consumers |
| Different groups | `group-01` and `group-02` | Each group receives every message |

The consumer uses `fromBeginning: true`, so on first join it reads **all historical messages** in the topic, not just new ones.

---

## Troubleshooting

### `ECONNREFUSED` on `localhost:9092`

Kafka is not running. Start the Docker container (see [Setup](#2-start-kafka-docker--kraft-mode)) and confirm with `docker ps`.

### `KAFKA_PROCESS_ROLES is not set`

You are using the latest Confluent image with old Zookeeper env vars. Use the KRaft `docker run` command above, not `KAFKA_ZOOKEEPER_CONNECT`.

### `Consumer groupId must be a non-empty string`

Pass a group ID when running the consumer:

```bash
node consumer.js group-01
```

### `TimeoutNegativeWarning` (Node.js 24)

Harmless KafkaJS + Node 24 compatibility warning. The app still works. Use Node 20 LTS to avoid it, or ignore the warning.

### KafkaJS partitioner warning

Informational only. Silence with:

```bash
KAFKAJS_NO_PARTITIONER_WARNING=1 node producer.js
```

### Producer input with spaces in name

Input is split on the **first space only** (`input.split(' ')`). Use single-word rider names, or update the parser to handle multi-word names.

### Reset everything

```bash
docker stop kafka && docker rm kafka
# Then re-run the docker run command and node admin.js
```

---

## Tech Stack

- **Runtime:** Node.js (ES modules)
- **Client library:** [KafkaJS](https://kafka.js.org/) v2.2.4
- **Broker:** Confluent Platform Kafka (Docker, KRaft mode)

---

## License

ISC
