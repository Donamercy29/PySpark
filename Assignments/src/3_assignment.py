from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
spark = SparkSession.builder.appName("UserActivity").enableHiveSupport().getOrCreate()

# 1. Create a Data Frame with custom schema creation by using StructType and StructField
schema = StructType([
    StructField("log_id", IntegerType(), True),
    StructField("user_id", IntegerType(), True),
    StructField("action", StringType(), True),
    StructField("timestamp", StringType(), True)
])
data = [
    (1, 101, 'login', '2023-09-05 08:30:00'),
    (2, 102, 'click', '2023-09-06 12:45:00'),
    (3, 101, 'click', '2023-09-07 14:15:00'),
    (4, 103, 'login', '2023-09-08 09:00:00'),
    (5, 102, 'logout', '2023-09-09 17:30:00'),
    (6, 101, 'click', '2023-09-10 11:20:00'),
    (7, 103, 'click', '2023-09-11 10:15:00'),
    (8, 102, 'click', '2023-09-12 13:10:00')
]
user_activity_df = spark.createDataFrame(data, schema)
user_activity_df.show(truncate=False)

# 2. Column names should be log_id, user_id, user_activity, time_stamp using dynamic function
new_columns = ["log_id", "user_id", "user_activity", "time_stamp"]
renamed_df = user_activity_df.toDF(*new_columns)
renamed_df.show(truncate=False)

#Convert time_stamp to timestamp datatype for further processing
activity_df = renamed_df.withColumn("time_stamp",
    to_timestamp(col("time_stamp"), "yyyy-MM-dd HH:mm:ss")
)

# 3. Calculate the number of actions performed by each user in the last 7 days
max_date = activity_df.select(max("time_stamp")).collect()[0][0]
last_7_days_df = activity_df.filter(
    col("time_stamp") >= date_sub(lit(max_date), 7)
)
actions_per_user_df = (
    last_7_days_df
    .groupBy("user_id")
    .agg(count("*").alias("action_count"))
)
actions_per_user_df.show()

# 4. Convert the time_stamp column to login_date column with YYYY-MM-DD format and DateType
login_date_df = activity_df.withColumn(
    "login_date",
    to_date(col("time_stamp"))
)
login_date_df.show(truncate=False)
login_date_df.printSchema()

# 5. Write the DataFrame as a CSV file with different write options
(login_date_df.write.mode("overwrite").option("header", True)
 .option("delimiter", ",")
 .option("nullValue", "NULL")
 .csv("user_activity_csv"))

# 6. Write as a Managed Table with database name 'user' and table name 'login_details'
df.write.mode("overwrite").saveAsTable("login_details")
