from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
spark = SparkSession.builder.appName("nested").getOrCreate()
schema = StructType([
    StructField("Name",StructType([
        StructField("firstname", StringType(), False),
        StructField("middle", StringType(), False),
        StructField("lastname", StringType(), False),
    ])),
    StructField("id", IntegerType(), False),
    StructField("dept", StringType(), True)]
)
data = [
    (("James", "Will", "Smith"), 111, "HR"),
    (("Michael", "Rose", "Dan"), 222, "IT"),
    (("Robert", "Ray", "William"), 333, "SALES"),
    (("Maria", "Anne", "Jones"), 444, "IT"),
    (("Jen", "Mary", "Brown"), 555, "HR")
]
df = spark.createDataFrame(data, schema)
df.printSchema()
df.show()