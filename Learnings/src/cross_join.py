from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("cross_join").getOrCreate()
color = [("Red",), ("Blue",)]
df1 = spark.createDataFrame(color, ["color"])
size = [("S",), ("M",), ("L",)]
df2 = spark.createDataFrame(size, ["Size"])
result = df1.crossJoin(df2)
result.show()
