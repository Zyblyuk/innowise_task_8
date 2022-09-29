from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def create_count_csv(month: str) -> bool:
    """Create count file csv"""

    spark = SparkSession \
        .builder \
        .master("local[1]") \
        .appName("SparkByExamples.com") \
        .getOrCreate()

    df = spark.read.format("csv") \
        .option("header", True) \
        .load(f"csv/month/{month}.csv")

    df.groupBy("departure_name") \
        .count() \
        .select(
        F.col("departure_name").alias("name"),
        F.col("count").alias("departure_count")
    ).alias('d') \
        .join(
        df.groupBy("return_name")
        .count()
        .select(
            F.col("return_name").alias("name"),
            F.col("count").alias("return_count")
        ).alias('r'),
        F.col('d.name') == F.col('r.name')
    ).select("d.name", "departure_count", "return_count") \
        .toPandas() \
        .to_csv(f"csv/month/{month}_count.csv")
    return True
