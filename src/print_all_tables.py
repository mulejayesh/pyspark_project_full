from pyspark.sql import SparkSession
import os

# ---------------------------------------
# Spark Session (Delta-enabled)
# ---------------------------------------
spark = (
    SparkSession.builder
    .appName("print_all_tables")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()
)

LAKE = "lake"

def print_table(path: str, name: str):
    """
    Load AND display a Delta table.
    """
    print(f"\n==============================")
    print(f"TABLE: {name}")
    print("==============================")

    try:
        df = spark.read.format("delta").load(path)
        df.show(50, truncate=False)
    except Exception as e:
        print(f"Could not load {name}: {e}")


# ---------------------------------------
# Print Bronze Tables
# ---------------------------------------
print("\n\n##########  BRONZE TABLES  ##########")

bronze_path = f"{LAKE}/bronze"
if os.path.exists(bronze_path):
    for tbl in os.listdir(bronze_path):
        print_table(f"{bronze_path}/{tbl}", f"bronze.{tbl}")
else:
    print("No bronze layer found.")

# ---------------------------------------
# Print Silver Tables
# ---------------------------------------
print("\n\n##########  SILVER TABLES  ##########")

silver_path = f"{LAKE}/silver"
if os.path.exists(silver_path):
    for tbl in os.listdir(silver_path):
        print_table(f"{silver_path}/{tbl}", f"silver.{tbl}")
else:
    print("No silver layer found.")

# ---------------------------------------
# Print Gold Tables
# ---------------------------------------
print("\n\n##########  GOLD TABLES  ##########")

gold_path = f"{LAKE}/gold"
if os.path.exists(gold_path):
    for tbl in os.listdir(gold_path):
        print_table(f"{gold_path}/{tbl}", f"gold.{tbl}")
else:
    print("No gold layer found.")

print("\nFinished printing all Lakehouse tables!\n")


# ---------------------------------------
# Print Time-to-Hire Metrics
# ---------------------------------------
print("\n\n##########  TIME-TO-HIRE METRICS  ##########")

# Per application
print_table(
    f"{gold_path}/metric_time_to_hire_per_application",
    "metric_time_to_hire_per_application"
)

# Per job
print_table(
    f"{gold_path}/metric_time_to_hire_per_job",
    "metric_time_to_hire_per_job"
)

# Per department
print_table(
    f"{gold_path}/metric_time_to_hire_per_department",
    "metric_time_to_hire_per_department"
)
