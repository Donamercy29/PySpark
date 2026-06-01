from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("left_join").getOrCreate()
emp = [(1, "John", 101), (2, "David", 102), (3, "Sara", 103)]
col1 = ["id", "name", "dept_id"]
df1 = spark.createDataFrame(emp, col1)
dept_val = [(101, "HR"), (102, "IT")]
col2 = ["dept_id", "name"]
df2 = spark.createDataFrame(dept_val, col2)
result = df1.join(df2, "dept_id", "left_outer")
result.show()