from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.functions import current_date
spark = SparkSession.builder.appName("EmployeeAssignment").enableHiveSupport().getOrCreate()

employee_columns = [
    ("employee_id", IntegerType()),
    ("employee_name", StringType()),
    ("department", StringType()),
    ("state", StringType()),
    ("salary", IntegerType()),
    ("age", IntegerType())
]
employee_schema = StructType([
    StructField(col_name, dtype, True)
    for col_name, dtype in employee_columns
])
employee_data = [
    (11, "james", "D101", "ny", 9000, 34),
    (12, "michel", "D101", "ny", 8900, 32),
    (13, "robert", "D102", "ca", 7900, 29),
    (14, "scott", "D103", "ca", 8000, 36),
    (15, "jen", "D102", "ny", 9500, 38),
    (16, "jeff", "D103", "uk", 9100, 35),
    (17, "maria", "D101", "ny", 7900, 40)
]
employee_df = spark.createDataFrame(employee_data, employee_schema)
department_columns = [
    ("dept_id", StringType()),
    ("dept_name", StringType())
]
department_schema = StructType([
    StructField(col_name, dtype, True)
    for col_name, dtype in department_columns
])
department_data = [
    ("D101", "sales"),
    ("D102", "finance"),
    ("D103", "marketing"),
    ("D104", "hr"),
    ("D105", "support")
]
department_df = spark.createDataFrame(
    department_data,
    department_schema
)
country_columns = [
    ("country_code", StringType()),
    ("country_name", StringType())
]
country_schema = StructType([
    StructField(col_name, dtype, True)
    for col_name, dtype in country_columns
])
country_data = [
    ("ny", "newyork"),
    ("ca", "California"),
    ("uk", "Russia")
]
country_df = spark.createDataFrame(
    country_data,
    country_schema
)

# 2. Find Avg Salary of Each Department

avg_salary_df = (
    employee_df
    .groupBy("department")
    .agg(avg("salary").alias("avg_salary"))
)
avg_salary_df.show()

# 3. Employee Name and Department Namewhose name starts with 'm'
employee_m_df = (
    employee_df.alias("e")
    .join(
        department_df.alias("d"),
        col("e.department") == col("d.dept_id"),
        "inner"
    )
    .filter(lower(col("employee_name")).startswith("m"))
    .select(
        "employee_name",
        "dept_name"
    )
)
employee_m_df.show()


# 4. Add Bonus Column = Salary * 2

employee_bonus_df = employee_df.withColumn(
    "bonus",
    col("salary") * 2
)
employee_bonus_df.show()

#5. Reorder the column names of employee_df columns as (employee_id,employee_name,salary,State,Age,department)
reordered_df = employee_df.select(
    "employee_id",
    "employee_name",
    "salary",
    "state",
    "age",
    "department"
)
reordered_df.show()

#6. Give the result of an inner join, left join, and right join when joining employee_df with department_df in a dynamic way
join_types = ["inner", "left", "right"]
for join_type in join_types:
    result_df = employee_df.join(
        department_df,
        employee_df.department == department_df.dept_id,
        join_type
    )
    result_df.show()

#7. Derive a new data frame with country_name instead of State in employee_df
#Eg(11,“james”,”D101”,”newyork”,8900,32)
employee_country_df = (
    employee_df.alias("e")
    .join(
        country_df.alias("c"),
        col("e.state") == col("c.country_code"),
        "inner"
    )
    .select(
        "employee_id",
        "employee_name",
        "department",
        col("country_name").alias("state"),
        "salary",
        "age"
    )
)
employee_country_df.show()

#8. convert all the column names into lowercase from the result of question 7in a dynamic way, add the load_date column with the current date
lowercase_df = employee_country_df.toDF(
    *[col_name.lower() for col_name in employee_country_df.columns]
)
final_df = lowercase_df.withColumn(
    "load_date",
    current_date()
)
final_df.show()

#9. create 2 external tables with parquet, CSV format with the same name database name, and 2 different table names as CSV and parquet format.
final_df.write.mode("overwrite").parquet("output/employee_parquet")
final_df.write.mode("overwrite").option("header", "true").csv("output/employee_csv")