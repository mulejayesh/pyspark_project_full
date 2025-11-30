import os
from pyspark.sql.functions import col, min as spark_min, datediff, date_format
from utils import get_spark, normalize_date_col

BRONZE = "lake/bronze"
SILVER = "lake/silver"
GOLD = "lake/gold"

spark = get_spark("transform")

# read bronze
jobs = spark.read.format("delta").load(os.path.join(BRONZE, "jobs"))
candidates = spark.read.format("delta").load(os.path.join(BRONZE, "candidates"))
education = spark.read.format("delta").load(os.path.join(BRONZE, "education"))
applications = spark.read.format("delta").load(os.path.join(BRONZE, "applications"))
events = spark.read.format("delta").load(os.path.join(BRONZE, "events"))

# silver
jobs_silver = normalize_date_col(jobs, 'posted_date', 'posted_date_clean')                 .drop('posted_date')                 .withColumnRenamed('posted_date_clean','posted_date')

applications_silver = normalize_date_col(applications, 'apply_date', 'apply_date_clean')                         .drop('apply_date')                         .withColumnRenamed('apply_date_clean','apply_date')

events_silver = normalize_date_col(events, 'event_timestamp', 'event_timestamp_clean')                     .drop('event_timestamp')                     .withColumnRenamed('event_timestamp_clean','event_timestamp')

jobs_silver.write.format("delta").mode("overwrite").save(os.path.join(SILVER, "jobs"))
applications_silver.write.format("delta").mode("overwrite").save(os.path.join(SILVER, "applications"))
events_silver.write.format("delta").mode("overwrite").save(os.path.join(SILVER, "events"))

# gold
dim_job = jobs_silver.select("job_id","title","department","posted_date")
dim_job.write.format("delta").mode("overwrite").save(os.path.join(GOLD,"dim_job"))

dim_candidate = candidates.select("candidate_id","first_name","last_name","email","phone","skills")
dim_candidate.write.format("delta").mode("overwrite").save(os.path.join(GOLD,"dim_candidate"))

dates = applications_silver.select(col("apply_date").alias("date_value")).union(
        events_silver.select(col("event_timestamp").alias("date_value"))
    ).distinct()

dim_date = dates.withColumn("date_key", date_format(col("date_value"),"yyyyMMdd").cast("int"))
dim_date.write.format("delta").mode("overwrite").save(os.path.join(GOLD,"dim_date"))

fact_applications = applications_silver.select("application_id","job_id","candidate_id","apply_date")
fact_applications.write.format("delta").mode("overwrite").save(os.path.join(GOLD,"fact_applications"))

fact_workflow = events_silver.select("application_id","old_status","new_status","event_timestamp")
fact_workflow.write.format("delta").mode("overwrite").save(os.path.join(GOLD,"fact_workflow_events"))

hire_dates = fact_workflow.filter(col("new_status")=="Hired")     .groupBy("application_id").agg(spark_min("event_timestamp").alias("hire_date"))

joined = fact_applications.join(hire_dates,"application_id","left")     .withColumn("time_to_hire_days", datediff(col("hire_date"), col("apply_date")))

joined.write.format("delta").mode("overwrite").save(os.path.join(GOLD,"metric_time_to_hire"))

print("Gold tables written to lake/gold/")
