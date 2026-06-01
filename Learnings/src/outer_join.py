from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("crossjoin").getOrCreate()
emp = [(1, "John", 101), (2, "David", 102), (3, "Sara", 103)]
col = ["emp_id", "name", "dept_id"]
df1 = spark.createDataFrame(emp, col)
dept1 = [(101, "HR"), (102, "IT"), (104, "Finance")]
dept_col = ["dept_id","name"]
df2 = spark.createDataFrame(dept1, dept_col)

result = df1.join(df2, "dept_id","outer")
result.show()
