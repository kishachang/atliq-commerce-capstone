# AtliQ Commerce — Phase 2 Real-Time Data Pipeline

## Overview

Phase 2 implements a real-time data engineering pipeline for AtliQ Commerce using Kafka, Azure Databricks Structured Streaming, Delta Lake, and Apache Airflow.

The pipeline processes order events continuously through Bronze, Silver, and Gold layers, while Airflow performs scheduled data-quality checks, table optimization, and daily aggregation.

## Architecture

```text
Python Producer
      ↓
Confluent Kafka
      ↓
Databricks Bronze
      ↓
Databricks Silver
      ↓
Databricks Gold
      ↓
Apache Airflow
```

## Technology Stack

- Python
- Confluent Cloud Kafka
- Azure Databricks
- Apache Spark Structured Streaming
- Delta Lake
- Unity Catalog
- Apache Airflow
- Docker
- PostgreSQL

## Streaming Pipeline

### Kafka Producer
Generates order lifecycle events and publishes them to the Kafka topic:

```text
atliq.orders.events
```

Events are keyed by `order_id` so events belonging to the same order are routed consistently and maintain ordering within a Kafka partition.

### Bronze Layer
Stores raw Kafka events together with Kafka metadata such as topic, partition, offset, timestamp, key, and value.

### Silver Layer
Transforms Bronze data into structured order events using explicit schema parsing, timestamp conversion, a 10-minute watermark, and `event_id` deduplication.

### Gold Layer
Produces 5-minute revenue aggregates for `payment_received` events, including `orders_paid` and `revenue`.

Gold windows may appear later than their actual 5-minute interval because Spark waits for potentially late-arriving events based on the watermark.

## Airflow Orchestration

Airflow runs an hourly DAG:

```text
check_fresh_events
        ↓
optimize_tables
        ↓
refresh_daily_summary
```

**check_fresh_events** fails when no events have arrived in the Silver table within the previous two hours.

**optimize_tables** runs `OPTIMIZE` against the Silver and Gold Delta tables.

**refresh_daily_summary** creates a daily summary containing orders placed, orders paid, orders cancelled, and revenue.

## Data Quality Validation

Two execution scenarios were tested:

1. Successful run with recent streaming events — all Airflow tasks completed successfully.
2. Producer stopped and data became stale — `check_fresh_events` failed and prevented downstream processing.

Screenshots of both runs are stored in the `screenshots/` directory.

## Batch vs Speed Lane

The batch lane provides complete and governed historical processing, while the speed lane provides low-latency operational insights from continuously arriving events.

Both approaches complement each other: streaming supports immediate visibility, while batch processing supports reconciliation, historical analysis, and authoritative reporting.

## Project Structure

```text
atliq-commerce-realtime/
│
├── producer/
├── databricks/
├── airflow/
│   └── dags/
├── screenshots/
├── .gitignore
└── README.md
```

## Security

Sensitive credentials such as Kafka API keys, secrets, and Databricks tokens are stored in environment variables and are not committed to the repository.
