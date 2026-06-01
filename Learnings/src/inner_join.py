from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("InnerJoin").getOrCreate()
emp = [(1, "John", "101"), (1, "David", "102"), (3, "Sara", 103), (4, "Mike", 104)]
emp_col = ["id", "name", "dept_id"]
emp_df = spark.createDataFrame(emp, emp_col)
dept = [(101, "HR"), (102, "IT"), (105, "Finance")]
dept_col = ["dept_id", "dept_name"]
dept_df = spark.createDataFrame(dept, dept_col)

result=emp_df.join(dept_df, "dept_id", "inner")
result.show()