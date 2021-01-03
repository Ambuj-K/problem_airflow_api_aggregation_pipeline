import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, avg, date_format
from pyspark.sql.types import DoubleType, TimestampType

spark = SparkSession.builder \
    .config("spark.driver.maxResultSize", "2000m") \
    .config("spark.sql.shuffle.partitions", 4) \
    .getOrCreate()

pg_data_url = "jdbc:postgresql://localhost:5432/airflow"
pg_data_user = "airflow"
pg_data_password = "airflow"
pg_driver = 'org.postgresql.Driver'


def read_api_table_aggregate():
    """
                Pipeline of transformations for returning
                    - read api response local table staging_api_call
                    - run the aggregation logic
                    - bulk write to the aggregate_table

    """
    # read config based local table parallel
    spark.read.format("jdbc") \
        .option("driver", pg_driver) \
        .option("url", pg_data_url) \
        .option("user", pg_data_user) \
        .option("password", pg_data_password) \
        .option("dbtable", "staging_api_call") \
        .createOrReplaceTempView("api_table")

    # run the aggregate query
    intr_df = spark.sql("""Select date_val, delta_confirmed-total_confirmed as 
			from api_table 
			where delta_confirmed-total_confirmed is not null
			order by delta_confirmed-total_confirmed asc """)

    # write the aggregate query to the aggregate table
    intr_df.write.format("jdbc") \
        .option("driver", pg_driver) \
        .option("url", pg_data_url) \
        .option("user", pg_data_user) \
        .option("password", pg_data_password) \
        .option("dbtable", "aggregate_table") \
        .option("batchsize", 50000) \
        .mode("overwrite") \
        .save()

# call main function for the DAG pipeline call
read_api_table_aggregate()
