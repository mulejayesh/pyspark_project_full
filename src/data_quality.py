from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, max as spark_max, avg
import json
import os
from datetime import datetime


class DataQualityChecks:
    
    def __init__(self, base_path="lake", spark=None):
        self.base = base_path
        self.spark = spark or (
            SparkSession.builder
            .appName("ICIMS_DataQuality")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .getOrCreate()
        )

        self.results = []
        self.flags = []

        # ---> NEW (Great Expectations Export)
        self.expectations_suite = {
            "expectation_suite_name": "icims_data_quality_suite",
            "expectations": [],
            "meta": {"created": str(datetime.now())}
        }

    def _log(self, name, status, notes=None):
        self.results.append((name, status, notes))

        # ---> NEW: Add expectation entry
        self.expectations_suite["expectations"].append({
            "expectation_type": name,
            "success": status == "PASS",
            "notes": notes
        })

    def _flag(self, name, df):
        if df.count() > 0:
            self.flags.append((name, df))

    # ------------------------------------------------
    # 1️⃣ Uniqueness Check
    # ------------------------------------------------
    def check_uniqueness(self):
        df = self.spark.read.format("delta").load(f"{self.base}/silver/applications")

        dupes = (
            df.groupBy("application_id")
            .agg(count("*").alias("row_count"))
            .filter(col("row_count") > 1)
        )

        if dupes.count() > 0:
            self._log("expect_column_values_to_be_unique(application_id)",
                      "FAIL", 
                      f"{dupes.count()} duplicate application_ids found")
            self._flag("duplicate_application_id", dupes)
        else:
            self._log("expect_column_values_to_be_unique(application_id)", 
                      "PASS", 
                      "application_id is unique")

    # ------------------------------------------------
    # 2️⃣ Freshness
    # ------------------------------------------------
    def check_freshness(self):
        df = self.spark.read.format("delta").load(f"{self.base}/silver/events")
        latest = df.select(spark_max("event_timestamp").alias("latest")).collect()[0]["latest"]

        self._log("expect_table_row_freshness_to_be_recent", "INFO", f"Latest event timestamp: {latest}")

    # ------------------------------------------------
    # 3️⃣ Volume Anomaly Check
    # ------------------------------------------------
    def check_volume(self, tolerance=0.20):

        apps = self.spark.read.format("delta").load(f"{self.base}/silver/applications")
        count_now = apps.count()

        history_path = f"{self.base}/quality/volume_history"

        if os.path.exists(history_path):
            history = self.spark.read.format("delta").load(history_path)
            prev_avg = history.select(avg("row_count")).first()[0]
        else:
            prev_avg = None

        # Append new sample
        self.spark.createDataFrame([(count_now,)], ["row_count"]) \
            .write.mode("append").format("delta").save(history_path)

        if prev_avg is None:
            self._log("expect_table_row_count_to_have_baseline", "INFO",
                      f"Baseline created: {count_now}")
            return

        lower, upper = prev_avg * (1 - tolerance), prev_avg * (1 + tolerance)

        if count_now < lower or count_now > upper:
            self._log("expect_table_row_count_to_be_within_20pct", "WARN",
                      f"{count_now} deviates from baseline ({prev_avg:.2f})")
        else:
            self._log("expect_table_row_count_to_be_within_20pct", "PASS",
                      "Row count stable")

    # ------------------------------------------------
    # 4️⃣ Status Ordering Check
    # ------------------------------------------------
    def check_status_sequence(self):
        apps = self.spark.read.format("delta").load(f"{self.base}/silver/applications")
        events = self.spark.read.format("delta").load(f"{self.base}/silver/events")

        anomaly = (
            events.join(apps, "application_id")
            .filter((col("new_status") == "Hired") & (col("event_timestamp") < col("apply_date")))
        )

        if anomaly.count() > 0:
            self._log("expect_event_timestamp_to_be_after_application_date", 
                      "FAIL", 
                      f"{anomaly.count()} records failing workflow order")
            self._flag("hired_before_applied", anomaly)
        else:
            self._log("expect_event_timestamp_to_be_after_application_date", 
                      "PASS", 
                      "Workflow ordering valid")

    # ------------------------------------------------
    # RUN ALL CHECKS
    # ------------------------------------------------
    def run(self):
        print("\n=== Running Data Quality Checks ===")

        self.check_uniqueness()
        self.check_freshness()
        self.check_volume()
        self.check_status_sequence()

        # Save summary dataframe
        summary_df = self.spark.createDataFrame(self.results, ["check_name", "status", "details"])
        summary_df.write.format("delta").mode("overwrite").save(f"{self.base}/quality/summary")

        # Save flagged rows
        for name, df in self.flags:
            df.write.format("delta").mode("overwrite").save(f"{self.base}/quality/issues_{name}")

        # ---> NEW: Export Great Expectations compatible JSON
        ge_export_path = f"{self.base}/quality/great_expectations.json"
        with open(ge_export_path, "w") as f:
            json.dump(self.expectations_suite, f, indent=4)

        print(f"\n📄 Great Expectations export written to: {ge_export_path}")
        print("\n=== Data Quality Complete ===")

        return summary_df


# CLI
if __name__ == "__main__":
    dq = DataQualityChecks()
    dq.run()
