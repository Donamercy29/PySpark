from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
spark = SparkSession.builder.appName("1_create_df").getOrCreate()
schema1 = StructType([
    StructField("customer", IntegerType(), True),
    StructField("product_model", StringType(), True),
    ])
data1 = [(1, "iphone13"), (1, "dell i5 core"), (2, "iphone13"),(2, "dell i5 core"),
         (3, "iphone13"), (3, "dell i5 core"), (1, "dell i3 core"), (1, "hp i5 core"),
         (1, "iphone14"), (3, "iphone14"), (4, "iphone13")]
df1 = spark.createDataFrame(data1, schema1)
data2 = [("iphone13",), ("dell i5 core",), ("dell i3 core",), ("hp i5 core",),("iphone14",)]
schema2 = StructType([StructField("product_model", StringType(), True)])
df2 = spark.createDataFrame(data2, schema2)
#Find the customers who have bought only iphone13
result = (
    df1.groupBy("customer")
       .agg(
           count_distinct("product_model").alias("product_count"),
           collect_set("product_model").alias("products")
       )
       .filter(
           (col("product_count") == 1) &
           (array_contains(col("products"), "iphone13"))
       )
)

result.show()
#Find customers who upgraded from product iphone13 to product iphone14
df3 = (
    df1.groupBy("customer")
       .agg(collect_set("product_model").alias("products"))
       .filter(
           array_contains(col("products"), "iphone13") &
           array_contains(col("products"), "iphone14")
       )
       .select("customer")
)

df3.show()
#Find customers who have bought all models in the new Product Data
count = df2.select("product_model").distinct().count()
df4 = (
    df1.groupBy("customer")
       .agg(
           count_distinct("product_model").alias("product_count")
       )
       .filter(col("product_count") == count)
       .select("customer")
)

df4.show()