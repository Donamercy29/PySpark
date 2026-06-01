#Find employee who have "python" skill in the array column
from pyspark.sql import SparkSession
from pyspark.sql.functions import array, array_contains, array_position, array_remove
spark = SparkSession.builder.appName("array").getOrCreate()
emp_data = [("Dona", "Python", "SQL", "Git"),("John", "SQL", "Spark", "PySpark"),("Theo", "Python", "SQL", "Git")]
col = ["Name", "Skill1", "Skill2", "Skill3"]
df = spark.createDataFrame(emp_data, col)
df1 = df.withColumn("Skill", array("Skill1", "Skill2", "Skill3"))
#result = df1.filter(array_contains("Skill", "Python"))
#Find employee more than 2 skills
#result = df1.filter(array_length("Skill") > 2)
#result = df1.withColumn("Skill_position", array_position("Skill", "SQL"))
#Find Employee who do not have skill spark
#result = df1.filter(~array_contains("Skill", "Spark"))
#Find position of "SQL" inside skill array.
#result = df1.withColumn("Position", array_position("Skill", "SQL"))
#Remove "Git" from employee skills.
result = df1.withColumn("Remove_Git", array_remove("Skill", "Git"))
result.show(truncate=False)
