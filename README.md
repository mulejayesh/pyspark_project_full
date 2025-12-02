# PySpark Lakehouse Project

This project implements Task Group 2 using PySpark and Delta Lake.

## Prereqs
- Java 11+
- Python 3.9+
- Spark 3.3.x

Install dependencies:

```bash
pip install -r requirements.txt
```

To Run Task1 

Run Command : ```bashpython3 task1.py```

output:
jobs -> 500 rows
candidates -> 2001 rows
education -> 2000 rows
applications -> 5000 rows
workflow_events -> 16769 rows
How many jobs are currently open?
(178,)
Top 5 departments by number of applications
('Marketing', 923)
('Product', 810)
('Engineering', 789)
('Sales', 761)
('Finance', 629)
Candidates who applied to more than 3 jobs
('0002572a-1130-48f5-9d5d-8f6533611134', 'Brian', 'Hines', 4)
('017cc74e-6f18-4343-bf41-5c985a8e8f05', 'Sara', 'Lee', 4)
('01e7cde7-33a2-4a5f-8683-4cc5757e2b43', 'Mary', 'Lambert', 4)
('01f268f5-6553-45aa-bdbc-34f8c8ddd9bb', 'Cindy', 'Mcintosh', 5)
('0240f59a-a638-4854-bb2b-8b80c08bd674', 'Gregory', 'Armstrong', 7)
('02b622e9-808e-48d2-9d26-4b1b6593b659', 'Joseph', 'Guerra', 4)
('02f4298f-6ffa-4356-8690-5ba517e03df5', 'James', 'Mcbride', 4)
('0316cde5-9590-4901-a5fb-24214d8e8fdb', 'Steven', 'Jr.', 4)
('0331bd0d-13ba-4d24-bebb-527c1438e799', 'Anne', 'Ramirez', 4)
there are more records also.

Task 2:

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

Execute below commands for Task2 :
```bash
docker exec -it spark-client spark-submit \
    --jars /home/jovyan/work/jars/delta-spark_2.12-3.1.0.jar,/home/jovyan/work/jars/delta-storage-3.1.0.jar \
    --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" \
    --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" \
    /home/jovyan/work/src/ingest.py



docker exec -it spark-client spark-submit \
    --jars /home/jovyan/work/jars/delta-spark_2.12-3.1.0.jar,/home/jovyan/work/jars/delta-storage-3.1.0.jar \
    --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" \
    --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" \
    --conf "spark.sql.legacy.timeParserPolicy=LEGACY" \
    /home/jovyan/work/src/transform.py



docker exec -it spark-client spark-submit \
    --jars /home/jovyan/work/jars/delta-spark_2.12-3.1.0.jar,/home/jovyan/work/jars/delta-storage-3.1.0.jar \
    --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" \
    --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" \
    --conf "spark.sql.legacy.timeParserPolicy=LEGACY" \
    /home/jovyan/work/src/data_quality.py
```

Task 3:

torage Format Choice :

    I chose Delta Lake because:

    ACID transactions

    Time travel

    Scalable metadata with checkpoints

    MERGE support (idempotency)

Scaling to 10TB

    Use:

    Spark Structured Streaming/ Databricks cloud

    Auto Loader or we can use more scalable approach with AWS EMR or other cloud providers.

    Delta Lake partitions with hire date or event date.

    OPTIMIZE + ZORDER for performance
    
    Reduce operations which do too much of Shuffles in partitions in spark.

    Use broadcast joins if some tables are smaller.

    Avoid data skew in case one partition has too much data we can repartition with salting mechanism.

    For 10TB of workflow_events, I’d keep the same Bronze–Silver–Gold Delta Lake design but move it to cloud storage (e.g. S3) and run the pipeline on an autoscaling Spark cluster (Databricks/EMR). I’d ingest events incrementally using Spark Structured Streaming or Auto Loader into a Delta Bronze table partitioned by event_date, then normalize and deduplicate into Silver.

    For Time-to-Hire, I’d pre-aggregate hired events in the 10TB table to a much smaller “per application” table, then join that with applications (possibly broadcasted) to compute time-to-hire per job and department. Data is partitioned by date, optimized into 256–512MB files, and Z-ordered on query keys like application_id. A medium-sized cluster (for example 8–16 workers with 16 vCPUs and 64–128GB RAM each) with adaptive query execution and autoscaling would comfortably handle daily recomputation of these metrics while keeping costs manageable.


AI (ChatGPT) used for:

    Boilerplate SQL

    Drafting Python ingestion structure

    Generating README skeleton

    All logic, ETL modeling, and architectural decisions were manually validated.
