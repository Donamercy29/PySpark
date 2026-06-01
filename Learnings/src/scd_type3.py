from pyspark.sql import SparkSession
from pyspark.sql.functions import *
spark = SparkSession.builder.appName("SCD3").getOrCreate()
existing_data = [
    (1, "John", "Chennai", None)
]

existing_df = spark.createDataFrame(
    existing_data,
    ["id", "name", "current_city", "previous_city"]
)
new_data = [
    (1, "John", "Bangalore")
]

new_df = spark.createDataFrame(
    new_data,
    ["id", "name", "city"]
)
joined_df = existing_df.alias("old").join(
    new_df.alias("new"),
    "id"
)
final_df = joined_df.select(
    col("id"),
    col("new.name").alias("name"),

    # new value becomes current
    col("new.city").alias("current_city"),

    # old current becomes previous
    col("old.current_city").alias("previous_city")
)
final_df.show()