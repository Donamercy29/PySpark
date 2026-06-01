from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import re

spark = SparkSession.builder.appName("Assignment4").getOrCreate()

# 1. Read JSON file provided in the attachment using dynamic function
employee_df = spark.read.option("multiline", "true").json("nested_json_file.json")
employee_df.show(truncate=False)
employee_df.printSchema()

# 2. Flatten the dataframe
flattened_df = (
    employee_df
    .withColumn("employee", explode("employees"))
    .select(
        col("id"),
        col("properties.name").alias("name"),
        col("properties.storeSize").alias("storeSize"),
        col("employee.empId").alias("empId"),
        col("employee.empName").alias("empName")
    )
)
flattened_df.show(truncate=False)

# 3. Find record count when flattened and when not flattened
original_count = employee_df.count()
flattened_count = flattened_df.count()
print("Original Count :", original_count)
print("Flattened Count :", flattened_count)
print("Difference :", flattened_count - original_count)

# 4. Difference between explode, explode_outer and posexplode
employee_df.select(
    explode("employees").alias("employee")
).show(truncate=False)
print("Using explode_outer")
employee_df.select(
    explode_outer("employees").alias("employee")
).show(truncate=False)
employee_df.select(
    posexplode("employees")
).show(truncate=False)

# 5. Filter the id which is equal to 1001
filtered_df = employee_df.filter(
    col("id") == 1001
)
filtered_df.show(truncate=False)

# 6. Convert column names from camel case to snake case
def camel_to_snake(column_name):
    return re.sub(r'(?<!^)(?=[A-Z])', '_', column_name).lower()
new_columns = [
    camel_to_snake(column)
    for column in flattened_df.columns
]
snake_case_df = flattened_df.toDF(*new_columns)
print("Question 6")
snake_case_df.show(truncate=False)

# 7. Add a new column named load_date with current date
load_date_df = snake_case_df.withColumn(
    "load_date",
    current_date()
)
load_date_df.show(truncate=False)

# 8. Create year, month and day columns from load_date
final_df = (
    load_date_df
    .withColumn("year", year("load_date"))
    .withColumn("month", month("load_date"))
    .withColumn("day", dayofmonth("load_date"))
)
final_df.show(truncate=False)


# 9. Write dataframe to table employee.employee_detail
spark.sql("CREATE DATABASE IF NOT EXISTS employee")
final_df.write.mode("overwrite").format("json").partitionBy("year", "month", "day").saveAsTable("employee.employee_details")
