from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType
from pyspark.sql.functions import udf
from pyspark.sql.types import IntegerType

spark = SparkSession.builder.appName("udf1").getOrCreate()
data = [(1, "Dona", 2000, 500), (2, "Mercy", 3000, 1000)]
col = ["id", "Name", "Salary", "Bonus"]
df = spark.createDataFrame(data, col)
@udf(returnType=IntegerType())
def totalpay(s, b):
    return s+b
df.select('*', totalpay(df.Salary, df.Bonus).alias("TotalPay")).show()
