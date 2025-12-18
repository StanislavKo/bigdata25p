# BigData25 POC – Streaming Analytics Architecture

![Overview](/img/architecture.png)

[Youtube](https://www.youtube.com/watch?v=7sp--pvwTXw&)

- [K8s](https://www.youtube.com/watch?v=7sp--pvwTXw&t=9s)
- [Kafka](https://www.youtube.com/watch?v=7sp--pvwTXw&t=15s)
- [Flink](https://www.youtube.com/watch?v=7sp--pvwTXw&t=29s)
- [Iceberg](https://www.youtube.com/watch?v=7sp--pvwTXw&t=37s)
- [ClickHouse](https://www.youtube.com/watch?v=7sp--pvwTXw&t=1m27s)
- [Airflow](https://www.youtube.com/watch?v=7sp--pvwTXw&t=1m53s)
- [Spark SQL](https://www.youtube.com/watch?v=7sp--pvwTXw&t=2m06s)
- [dbt](https://www.youtube.com/watch?v=7sp--pvwTXw&t=2m16s)
- [Trino](https://www.youtube.com/watch?v=7sp--pvwTXw&t=2m25s)
- [Superset](https://www.youtube.com/watch?v=7sp--pvwTXw&t=2m38s)

This repository contains an end-to-end programmatic Big Data POC demonstrating real-time ingestion, streaming analytics, lakehouse storage with Apache Iceberg, and analytical serving using ClickHouse.

The POC focuses on currency rates streaming data and shows how modern streaming, lakehouse, and analytics components can be integrated into a single architecture.

## High-Level Goals of the POC

- Demonstrate real-time ingestion from a WebSocket source
- Use Kafka as the central streaming backbone
- Persist raw streaming data into Iceberg bronze tables on AWS
- Perform stream processing and windowed analytics with Apache Flink
- Serve analytics through ClickHouse silver & gold layers
- Orchestrate batch & incremental workloads with Airflow and dbt
- Enable BI and SQL access via Superset and Trino

## Architecture Overview

The architecture is illustrated in architecture.png and can be logically divided into five layers:

- Streaming Ingestion
- Streaming Processing
- Lakehouse (Bronze)
- Analytics Serving (Silver / Gold)
- Orchestration & Consumption

## 1. Streaming Ingestion Layer

### Raw Currency Rates WebSocket

- External real-time data source producing currency rates via WebSocket.

### Custom Kafka Connect – WebSocket Source

- Custom Kafka Connect source connector that:
- - Connects to the WebSocket
- - Streams raw events into Kafka topics
- Acts as the entry point into the platform.

### Kafka

- Central event streaming platform.
- Decouples producers from consumers.
- Enables multiple downstream processing paths.

## 2. Streaming Processing Layer

### Apache Flink

- Consumes raw events from Kafka.
- Performs windowed aggregations (e.g. moving averages).
- Produces derived metrics back into Kafka (e.g. MA – Moving Average).

This allows:

- Real-time feature computation
- Separation of raw vs derived streams

## 3. Lakehouse – Bronze Layer (Iceberg)

Raw streaming data is persisted into Apache Iceberg tables for durability, replay, and batch analytics.

### Kafka Connect – Iceberg Sinks

Two alternative Iceberg sinks are demonstrated:

- AWS S3Tables Iceberg Sink
- AWS Glue Iceberg Sink

Both write:

- Immutable raw events
- Schema-evolved Iceberg tables
- Stored as Bronze layer

### Benefits

- Long-term storage
- Time-travel & schema evolution
- Compatible with Spark, Trino, Presto, dbt

## 4. Analytics Serving Layer

### ClickHouse – Silver Layer

- Kafka Connect ClickHouse Sink ingests:
- - Processed Kafka streams (e.g. Flink output)
- Used for low-latency analytical queries

### Transformations

- dbt is used to transform Silver → Gold
- Examples:
- - Moving Average Oscillator
- - MACD (incremental models)

### ClickHouse – Gold Layer

- Business-ready analytical tables
- Optimized for BI dashboards and SQL access

## 5. Orchestration & Consumption

### Airflow 3.1.3

Airflow orchestrates batch and incremental workloads:

- Spark SQL jobs over Iceberg
- Incremental ClickHouse transformations
- Python & HTTP sensors
- ClickHouse datasource integration

### Query & BI Tools

- Apache Superset 1.4 – dashboards over ClickHouse Gold
- Trino – SQL access to ClickHouse and Iceberg
- Presto – optional SQL engine
- dbt – analytics engineering layer

## Data Flow Summary

```
WebSocket
   ↓
Kafka Connect (WebSocket Source)
   ↓
Kafka
   ├──> Iceberg Bronze (S3Tables / Glue)
   ├──> Apache Flink (windowed analytics)
   │        ↓
   │      Kafka (derived metrics)
   │        ↓
   │   ClickHouse Silver
   │        ↓
   │      dbt
   │        ↓
   │   ClickHouse Gold
   ↓
BI / SQL (Superset, Trino)
```

## Known Limitations & POC Notes

This is a POC, not a production-ready platform. Known limitations include:

- WebSocket Kafka source does not auto-reconnect
- Iceberg Kafka sink does not register schemas in Schema Registry
- dbt uses single-source models
- AWS-managed Iceberg tables are used for simplicity

These constraints are intentional to keep the POC focused on architecture and integration patterns.

## Why This POC Matters

This project demonstrates:

- Modern streaming-first lakehouse architecture
- Kafka as a backbone for both real-time and batch
- Iceberg as a unifying storage layer
- ClickHouse as a high-performance analytical engine
- Clear separation of bronze / silver / gold layers

It is intended for:

- Data Engineers
- Streaming platform architects
- Analytics engineers exploring Iceberg + Kafka + ClickHouse
