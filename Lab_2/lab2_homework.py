from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    to_timestamp,
    window,
    count,
    avg,
    round as _round,
    desc
)

spark = (
    SparkSession.builder
    .appName("Lab2-Homework")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

df = spark.read.json("transactions_10k.jsonl")

df = df.withColumn(
    "timestamp",
    to_timestamp(col("timestamp"), "yyyy-MM-dd HH:mm:ss")
)

print("Record count:", df.count())

# --------------------------------------------------
# Homework 1
# Find the hour in which store Gdańsk had the lowest average transaction amount
# --------------------------------------------------

print("\nHomework 1")

gdansk_lowest_avg = (
    df.filter(col("store") == "Gdańsk")
      .groupBy(window("timestamp", "1 hour"))
      .agg(
          _round(avg("amount"), 2).alias("avg_amount")
      )
      .select(
          col("window.start").alias("from"),
          col("window.end").alias("to"),
          "avg_amount"
      )
      .orderBy("avg_amount")
      .limit(1)
)

gdansk_lowest_avg.show(truncate=False)

# --------------------------------------------------
# Homework 2
# Count transactions per category from 09:00–09:30
# --------------------------------------------------

print("\nHomework 2")

category_counts = (
    df.filter(
        (col("timestamp") >= "2026-04-12 09:00:00") &
        (col("timestamp") < "2026-04-12 09:30:00")
    )
    .groupBy("category")
    .agg(
        count("*").alias("tx_count")
    )
    .orderBy("category")
)

category_counts.show(truncate=False)

# --------------------------------------------------
# Homework 3
# Peak 15-minute transaction window
# --------------------------------------------------

print("\nHomework 3")

peak_quarter_hour = (
    df.groupBy(
        window("timestamp", "15 minutes")
    )
    .agg(
        count("*").alias("tx_count")
    )
    .select(
        col("window.start").alias("from"),
        col("window.end").alias("to"),
        "tx_count"
    )
    .orderBy(desc("tx_count"))
    .limit(1)
)

peak_quarter_hour.show(truncate=False)

spark.stop()