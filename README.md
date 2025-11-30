# PySpark Lakehouse Project

This project implements Task Group 2 using PySpark and Delta Lake.

## Prereqs
- Java 11+
- Python 3.9+
- Spark 3.3.x

Install dependencies:

```bash
pip install -r requirements.txt

Install Docker with Spark Image


Architecture Overview

Raw Layer :

CSV/JSON → SQLite 

Transform Layer : PySpark

Star schema:

    dim_job

    dim_candidate

    dim_date

    fact_applications

    fact_workflow_events

Analytics Layer : Spark SQL queries

Storage Format Choice :

    I chose Delta Lake because:

    ACID transactions

    Time travel

    Scalable metadata with checkpoints

    MERGE support (idempotency)

Scaling to 10TB

    Use:

    Spark Structured Streaming/ Databricks cloud

    Auto Loader

    Delta Lake partitions

    OPTIMIZE + ZORDER for performance


AI (ChatGPT) used for:

    Boilerplate SQL

    Drafting Python ingestion structure

    Generating README skeleton

    All logic, ETL modeling, and architectural decisions were manually validated.