import os
import shutil
from pyspark.sql import SparkSession, functions as F


months = {
    1: "january",
    2: "february",
    3: "march",
    4: "april",
    5: "may",
    6: "june",
    7: "july",
    8: "august",
    9: "september",
    10: "october",
    11: "november",
    12: "december",
}

spark = SparkSession\
    .builder\
    .master("local[1]")\
    .appName("SparkByExamples.com")\
    .getOrCreate()

data = spark.read.format("csv") \
      .option("header", True) \
      .load("csv/database.csv")

data.departure = data.select(F.to_timestamp("departure"))

for i in months:
    print(months[i])

    data.where(F.month("departure") == i) \
        .write.csv(f"csv/month/{months[i]}")

    os.system(f"cat csv/month/{months[i]}/p* > csv/month/{months[i]}.csv")
    shutil.rmtree(f"csv/month/{months[i]}")
    print("Done!!!\n")
