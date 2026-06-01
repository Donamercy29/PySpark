from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("SCD1").getOrCreate()
existing_data = [
    (1, "John", "Chennai"),
    (2, "David", "Mumbai")
]
new_data = [
    (1, "John", "Bangalore")
]
existing_df = spark.createDataFrame(existing_data, ["id", "name", "place"])
new_df = spark.createDataFrame(new_data, ["id", "name", "place"])
final_df = (
    existing_df.alias("old").join(new_df.alias("new"), "id", "outer").selectExpr(
        "id",
        "coalesce(new.name, old.name) as name",
        "coalesce(new.place, old.place) as place"
    )
)
final_df.show()