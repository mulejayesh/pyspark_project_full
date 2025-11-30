from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date, coalesce


def get_spark(app_name):
    # Determine the configuration based on the environment (test or regular)
    if app_name == 'test':
        # Configuration specifically for local testing
        conf = (
            SparkSession.builder.appName(app_name)
            .master("local[*]")
            # FIX: Explicitly set driver binding address to solve BindException
            .config("spark.driver.bindAddress", "0.0.0.0") 
            .config("spark.driver.host", "127.0.0.1") # Often needed alongside bindAddress
            .getOrCreate()
        )
    else:
        # Regular production configuration
        conf = (
            SparkSession.builder.appName(app_name)
            # ... regular production settings here ...
            .getOrCreate()
        )
    return conf

def normalize_date_col(df, col_name, out_name):
    return df.withColumn(out_name,
                         coalesce(
                             to_date(df[col_name], 'yyyy-MM-dd'),
                             to_date(df[col_name], 'MM/dd/yyyy'),
                             to_date(df[col_name], 'dd-MMM-yyyy')
                         ))
