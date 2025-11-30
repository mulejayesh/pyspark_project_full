from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, min as spark_min, datediff, to_date, coalesce
)

# -------------------------------------
# Spark Session (Delta-enabled)
# -------------------------------------
spark = (
    SparkSession.builder
    .appName("metric_time_to_hire")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()
)

# -------------------------------------
# Helper: Normalize date column
# -------------------------------------
def normalize_date(df, colname, outname):
    return df.withColumn(
        outname,
        coalesce(
            to_date(col(colname), "yyyy-MM-dd"),
            to_date(col(colname), "MM/dd/yyyy"),
            to_date(col(colname), "dd-MMM-yyyy")
        )
    )

# -------------------------------------
# Load Silver Layer Inputs
# -------------------------------------
SILVER = "lake/silver"
GOLD = "lake/gold"

applications = spark.read.format("delta").load(f"{SILVER}/applications")
events = spark.read.format("delta").load(f"{SILVER}/events")
jobs = spark.read.format("delta").load(f"{SILVER}/jobs")

# Ensure clean date types
applications = normalize_date(applications, "apply_date", "apply_date_clean") \
    .drop("apply_date") \
    .withColumnRenamed("apply_date_clean", "apply_date")

events = normalize_date(events, "event_timestamp", "event_timestamp_clean") \
    .drop("event_timestamp") \
    .withColumnRenamed("event_timestamp_clean", "event_timestamp")

# -------------------------------------
# 1. Extract Hire Dates (First Hired Status Per Application)
# -------------------------------------
hire_dates = (
    events.filter(col("new_status") == "Hired")
    .groupBy("application_id")
    .agg(
        spark_min("event_timestamp").alias("hire_date")
    )
)

# -------------------------------------
# 2. Join Applications with Hire Dates
# -------------------------------------
tth = (
    applications.join(hire_dates, "application_id", "left")
    .withColumn(
        "time_to_hire_days",
        datediff(col("hire_date"), col("apply_date"))
    )
)

# -------------------------------------
# 3. Save Base Time-to-Hire Table
# -------------------------------------
tth.write.format("delta").mode("overwrite").save(f"{GOLD}/metric_time_to_hire_per_application")

# -------------------------------------
# 4. Time to Hire per Job
# -------------------------------------
tth_by_job = (
    tth.groupBy("job_id")
    .avg("time_to_hire_days")
    .withColumnRenamed("avg(time_to_hire_days)", "avg_time_to_hire_days")
)

tth_by_job.write.format("delta").mode("overwrite").save(f"{GOLD}/metric_time_to_hire_per_job")

# -------------------------------------
# 5. Time to Hire per Department
# -------------------------------------
tth_with_job_info = tth.join(jobs, "job_id")

tth_by_dept = (
    tth_with_job_info.groupBy("department")
    .avg("time_to_hire_days")
    .withColumnRenamed("avg(time_to_hire_days)", "avg_time_to_hire_days")
)

tth_by_dept.write.format("delta").mode("overwrite").save(f"{GOLD}/metric_time_to_hire_per_department")

# -------------------------------------
# Done
# -------------------------------------
print("Time-to-Hire metrics generated successfully!")
print(f"- Per application:   {GOLD}/metric_time_to_hire_per_application")
print(f"- Per job:           {GOLD}/metric_time_to_hire_per_job")
print(f"- Per department:    {GOLD}/metric_time_to_hire_per_department")
