import os
from utils import get_spark

RAW_DIR = "/data"
BRONZE = "lake/bronze"

spark = get_spark("ingest")

def read_paths():
    jobs = spark.read.option("header", True).csv(os.path.join(RAW_DIR, "jobs.csv"))
    candidates = (
    spark.read.option("multiLine", True)  # <-- ADD THIS OPTION
    .json(os.path.join(RAW_DIR, "candidates.json"))
)
    education = spark.read.option("header", True).csv(os.path.join(RAW_DIR, "education.csv"))
    applications = spark.read.option("header", True).csv(os.path.join(RAW_DIR, "applications.csv"))
    events = spark.read.json(os.path.join(RAW_DIR, "workflow_events.jsonl"))
    return jobs, candidates, education, applications, events

def write_bronze():
    jobs, candidates, education, applications, events = read_paths()
    jobs.write.format("delta").mode("overwrite").save(os.path.join(BRONZE, "jobs"))
    candidates.write.format("delta").mode("overwrite").save(os.path.join(BRONZE, "candidates"))
    education.write.format("delta").mode("overwrite").save(os.path.join(BRONZE, "education"))
    applications.write.format("delta").mode("overwrite").save(os.path.join(BRONZE, "applications"))
    events.write.format("delta").mode("overwrite").save(os.path.join(BRONZE, "events"))

if __name__ == '__main__':
    write_bronze()
    print("Bronze data written to lake/bronze/")
