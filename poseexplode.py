from pyspark.sql import SparkSession
from pyspark.sql.functions import posexplode_outer
spark = SparkSession.builder.appName("explode1").getOrCreate()
data = [(1, ["Python", "SQL", "Spark"]),(2, None),(3, [])]
df = spark.createDataFrame(data, ["id", "skills"])
df1 = df.select("id", posexplode_outer("skills"))
df1.show()