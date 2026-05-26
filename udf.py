from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
spark = SparkSession.builder.appName("UDF").getOrCreate()

data = [("Dona", 80), ("John", 40), ("Theo", 65)]
df = spark.createDataFrame(data, ["Name", "Marks"])
def result(mark):
    if mark >= 50:
        return "Pass"
    else:
        return "Fail"
result_udf = udf(result, StringType())
df1 = df.withColumn("Result",result_udf("Marks"))
df1.show()