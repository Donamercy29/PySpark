from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, explode_outer
spark = SparkSession.builder.appName("explode").getOrCreate()
data = ((1, ["Python", "SQL", "Spark"]), (2, ["Java", "AWS"]), (3, None))
df = spark.createDataFrame(data, ["id", "skills"])
#df1 = df.select("id", explode("Skills"))
df1 = df.select("id", explode_outer("skills").alias("skill"))
df1.show()