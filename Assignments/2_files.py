from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import udf, col
spark = SparkSession.builder.appName("CreditCard").getOrCreate()
schema = StructType([StructField("card_number", StringType(), True)])
data = [("1234567891234567",),("5678912345671234",),("9123456712345678",),("1234567812341122",),("1234567812341342",)]
credit_card_df = spark.createDataFrame(data, schema)
#1. Create a Dataframe as credit_card_df with different read methods
#credit_card_df1 = spark.read.csv("credit_cards.csv", header=True)
#credit_card_df2 = spark.read.json("credit_cards.json")
#credit_card_df_parquet = spark.read.parquet("credit_card_parquet")

#2. print number of partitions
partitions = credit_card_df.rdd.getNumPartitions()
print("Number of Partitions:", credit_card_df.rdd.getNumPartitions())

#3. Increase the partition size to 5
repartitioned = credit_card_df.repartition(5)
print("Number of Partitions after repartition:",
      repartitioned.rdd.getNumPartitions())
repartitioned.show()

#4. Decrease the partition size back to its original partition size
coalesced_df = repartitioned.coalesce(partitions)
print("Number of Partitions after coalesce:",
      coalesced_df.rdd.getNumPartitions())

#5.Create a UDF to print only the last 4 digits marking the remaining digits as *
def mask_card(card_number):
    if card_number is None:
        return None
    return "*" * (len(card_number) - 4) + card_number[-4:]
mask_card_udf = udf(mask_card, StringType())

#6.output should have 2 columns as card_number, masked_card_number(with output of question 2)
final_df = credit_card_df.withColumn(
    "masked_card_number",
    mask_card_udf(col("card_number"))
)

final_df.show(truncate=False)