# AtliQ Commerce Data Engineering Capstone

## Overview

This repository contains the complete AtliQ Commerce data engineering solution, implemented in two complementary phases.

### Phase 1 — Batch Data Engineering
Implements batch ingestion, transformation, dimensional modelling, orchestration, analytics, and reporting.

### Phase 2 — Real-Time Streaming
Implements real-time event processing using Confluent Kafka, Azure Databricks Structured Streaming, Delta Lake, and Apache Airflow.

## Architecture

```text
                    AtliQ Commerce
                          │
              ┌───────────┴───────────┐
              │                       │
              ▼                       ▼
       Phase 1 — Batch        Phase 2 — Streaming
              │                       │
        Batch Ingestion          Kafka Events
              │                       │
              ▼                       ▼
       Transformation       Bronze → Silver → Gold
              │                       │
              ▼                       ▼
       Analytics / BI        Airflow Orchestration

Repository Structure
atliq-commerce-capstone/
├── phase1_batch/
├── phase2_realtime/
├── README.md
└── .gitignore
Phase 1

See phase1_batch/README.md.

Phase 2

See phase2_realtime/README.md.

Security

Credentials, API keys, tokens, and local environment files are excluded from version control.