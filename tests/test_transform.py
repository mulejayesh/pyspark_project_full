import sys
import os
# Add the parent directory (work/) to the Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from src.utils import get_spark, normalize_date_col
from pyspark.sql import Row
from pyspark.sql.functions import col
from pyspark.sql.functions import datediff, min as spark_min

def test_transform_date_cleaning():
    spark = get_spark('test')
    df = spark.createDataFrame([
        Row(posted_date='2023-01-01'),
        Row(posted_date='01/05/2023'),
        Row(posted_date='05-Jan-2023')
    ])
    out = normalize_date_col(df, 'posted_date', 'clean')
    for r in out.collect():
        assert r['clean'] is not None

def test_join_logic_time_to_hire():
    spark = get_spark('test')

    apps = spark.createDataFrame([
        Row(application_id=1, apply_date='2023-01-01')
    ])

    events = spark.createDataFrame([
        Row(application_id=1, new_status='Hired', event_timestamp='2023-01-05')
    ])

    hires = events.groupBy('application_id').agg(spark_min('event_timestamp').alias('hire_date'))
    joined = apps.join(hires, 'application_id', 'left')         .withColumn('time_to_hire_days', datediff(col('hire_date'), col('apply_date')))

    r = joined.first()
    assert r.time_to_hire_days == 4
