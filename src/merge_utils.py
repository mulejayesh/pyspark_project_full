from delta.tables import DeltaTable
from pyspark.sql import DataFrame

def merge_into(delta_path: str, source_df: DataFrame, key_cols: list):
    spark = source_df.sparkSession
    try:
        dt = DeltaTable.forPath(spark, delta_path)
        cond = " AND ".join([f"t.{k} = s.{k}" for k in key_cols])
        (dt.alias("t")
          .merge(source_df.alias("s"), cond)
          .whenMatchedUpdateAll()
          .whenNotMatchedInsertAll()
          .execute())
    except Exception:
        source_df.write.format("delta").mode("overwrite").save(delta_path)
