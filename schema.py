from pyspark.sql import SparkSession
from pyspark.sql.types import *
spark = SparkSession.builder.appName("Schema").getOrCreate()
schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("salary", DoubleType(), True)
])
data = [
    (101, "Dona", 50000.0),
    (102, "John", 45000.0)
]
df = spark.createDataFrame(data, schema)
df.show()
df.printSchema()