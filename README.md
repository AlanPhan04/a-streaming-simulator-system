# Streaming Simulator System

This repository is a mini-project for the course **Distributed Systems (CO3071)** in semester 242 at **Ho Chi Minh University of Technology (HCMUT)**.

## 🔎 Overview

This project simulates a streaming data pipeline that mimics real-time data collection and processing. The data flow is as follows:

```
Producer → Kafka → Spark → PostgreSQL → Data Visualization
```

- **Producer**: Sends environmental data (Air, Earth, Water) to specific Kafka topics.
- **Kafka**: Acts as the messaging system that buffers and distributes data streams.
- **Spark**: Processes Kafka streams, applies transformations and imputations, and writes processed results to a database.
- **PostgreSQL**: Stores the final, processed dataset for querying or visualization.
- **Visualization**: Data stored in the database is visualized by `ClickHouse`.

---

## 🐳 Docker Setup

All core services — Kafka, Spark, and PostgreSQL — are defined in `compose.yaml`.

### Start All Services

```bash
docker compose up -d
```

### Start a Single Service

If you only want to start a specific service:

```bash
docker compose up <service_name> -d
```

### Stop all services
```bash
docker compose down
```

---

## ⚙️ Kafka Setup

The Kafka service is set up as a single instance that acts as both the **controller** and **broker** to keep things simple. It listens for data from the producer and passes it downstream to Spark for processing.

The setup for Kafka as said is written in the file `compose.yaml` and the ip address for bootstrap server is `localhost:9094`

## Spark Setup

The spark application is stored in directory `spark`. So that your spark application should be coded in that directory.

But first off, you must install necessary packages of spark with these commands:

```bash
cd spark
pip install requirements.txt
```

> ⚠️ **Important**: You must put the application entrance at the file `main.py` to start the application.

Okay, after you finished coding spark, you can start deploying with this command:

```bash
docker exec -it spark-master spark-submit \
  --master spark://spark-master:7077 \
  --packages  org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.5,org.postgresql:postgresql:42.2.27 \
  /opt/spark-app/main.py
```

## PostgreSQL Setup

There nothing much to say in this section. Just information to connect to postgres database server:

- username: postgres
- password: postgres
- ip: localhost:5432
- default database: postgres
- ip address for jdbc: jdbc:postgresql://localhost:5432/<your database>

If you want to you any others database, just go ahead and create any database in the database server.

---

## 📦 Producer

The `prod/` directory contains all the code related to the data producer, which sends synthetic environmental data to Kafka topics.

### Structure

- `main.py`: Main entry point to start the producer.
- `consumer_test.py`: A lightweight Kafka consumer to verify that data is being sent correctly.
- `configs/constants.py`: All critical Kafka-related configurations.

> ⚠️ **Important**: You should only modify settings in `configs/constants.py`. This file defines things like the Kafka bootstrap servers, topic names, serializers, etc.

#### Key Variables in `constants.py`:
- `KAFKA_BOOTSTRAP_SERVERS`
- `KAFKA_TOPICS`
- `PRODUCER_KEY_SERIALIZER`
- `PRODUCER_VALUE_SERIALIZER`
- `CONSUMER_KEY_DESERIALIZER`
- `CONSUMER_VALUE_DESERIALIZER`

---

### ▶️ Run the Producer

Install the necessary Python dependencies:

```bash
cd prod
pip install -r requirements.txt
```

Then run the producer:

```bash
python3 main.py
```

To test if the producer sent data correctly:

```bash
python3 consumer_test.py
```

---

## 🧪 Example Logs

Here are example logs from the producer and consumer for reference:

### ✅ Producer Logs

```bash
Sent RecordMetadata(topic='air', partition=2, offset=55927, ...)
Sent RecordMetadata(topic='air', partition=2, offset=55928, ...)
...
```

### ✅ Consumer Logs

```bash
[air] Key: SVDT3, Value: {...}
[earth] Key: SVDT3, Value: {...}
...
```
