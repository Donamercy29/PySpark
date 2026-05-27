from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
spark = SparkSession.builder.appName("Schema1").getOrCreate()
schema = StructType([
    StructField("name", StringType(), True),
    StructField("id", IntegerType(), True),
    StructField("dept", StringType(), True)
]
)
data = [
    ("James", 111, "HR"),
    ("Michael", 222, "IT"),
    ("Robert", 333, "SALES"),
    ("Maria", 444, "IT"),
    ("Jen", 555, "HR")
]

df = spark.createDataFrame(data, schema)
df.printSchema()
df.display()