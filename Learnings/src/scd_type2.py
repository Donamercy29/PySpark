from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
spark = SparkSession.builder.appName("SCD2").getOrCreate()
schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("city", StringType(), True),
    StructField("start_date", StringType(), True),
    StructField("end_date", StringType(), True),
    StructField("active", StringType(), True)
])
existing_data = [(1, "John", "Chennai", "2025-01-01", None, "Y")]
existing_df = spark.createDataFrame(existing_data, schema)
new_data = [(1, "John", "Bangalore")]
new_df = spark.createDataFrame(new_data,["id", "name", "city"])
joined_df = existing_df.alias("old").join(new_df.alias("new"),"id")
changed_df = joined_df.filter(col("old.city") != col("new.city"))
expired_df = changed_df.select(
    col("id"),
    col("old.name").alias("name"),
    col("old.city").alias("city"),
    col("start_date"),
    current_date().alias("end_date"),
    lit("N").alias("active")
)
new_active_df = changed_df.select(
    col("id"),
    col("new.name").alias("name"),
    col("new.city").alias("city"),
    current_date().alias("start_date"),
    lit(None).cast("string").alias("end_date"),
    lit("Y").alias("active")
)
final_df = expired_df.union(new_active_df)
final_df.show()