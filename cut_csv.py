import os
import shutil
import calendar
from pyspark.sql import SparkSession, functions as F


spark = SparkSession\
    .builder\
    .master("local[1]")\
    .appName("SparkByExamples.com")\
    .getOrCreate()

data = spark.read.format("csv") \
      .option("header", True) \
      .load("csv/database.csv")

data.departure = data.select(F.to_timestamp("departure"))

months = [i.lower() for i in calendar.month_name if i != '']


for idx, month in enumerate(months):
    data.where(F.month("departure") == idx) \
        .write.csv(f"csv/month/{month}")

    os.system(f"cat csv/month/{month}/p* > csv/month/{month}.csv")
    shutil.rmtree(f"csv/month/{month}")
