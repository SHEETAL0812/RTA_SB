from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, to_timestamp, window,
    count, sum as _sum, round as _round,
    to_json, struct, lit
)
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

KAFKA_PACKAGE = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0"

spark = (
    SparkSession.builder
    .appName("Lab4-Homework")
    .config("spark.jars.packages", KAFKA_PACKAGE)
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

tx_schema = StructType([
    StructField("tx_id", StringType()),
    StructField("user_id", StringType()),
    StructField("amount", DoubleType()),
    StructField("store", StringType()),
    StructField("category", StringType()),
    StructField("timestamp", StringType()),
])

kafka_raw = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "broker:9092")
    .option("subscribe", "transactions")
    .option("startingOffsets", "earliest")
    .load()
)

df = (
    kafka_raw
    .select(from_json(col("value").cast("string"), tx_schema).alias("tx"))
    .select("tx.*")
    .withColumn("timestamp", to_timestamp("timestamp"))
)

# Homework 1:
# Sliding window: 2 minutes window size, 1 minute step, per store.

windowed_homework = (
    df.withWatermark("timestamp", "30 seconds")
    .groupBy(
        window("timestamp", "2 minutes", "1 minute"),
        "store"
    )
    .agg(
        count("tx_id").alias("tx_count"),
        _round(_sum("amount"), 2).alias("total_amount")
    )
)

window_query = (
    windowed_homework.writeStream
    .format("console")
    .outputMode("append")
    .option("truncate", False)
    .option("checkpointLocation", "/tmp/chk_lab4_homework_sliding_final")
    .start()
)

# Homework 2:
# Add ratio = amount / 400.0 to the alerts stream.

alerts_homework = (
    df.filter(col("amount") > 3000)
    .select(
        to_json(
            struct(
                "tx_id",
                "user_id",
                "amount",
                "store",
                "category",
                col("timestamp").cast("string").alias("timestamp"),
                lit("HIGH").alias("alert_level"),
                _round((col("amount") / 400.0), 2).alias("ratio")
            )
        ).alias("value")
    )
)

alert_query = (
    alerts_homework.writeStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "broker:9092")
    .option("topic", "alerts")
    .option("checkpointLocation", "/tmp/chk_lab4_homework_alerts_final")
    .outputMode("append")
    .start()
)

# Homework 3 answer:
# When the producer is stopped and we wait about 2 minutes, new results stop appearing
# because no new Kafka events are arriving. In append mode, Spark outputs windowed
# results only after the watermark considers the window closed. Some final delayed
# results may appear briefly, but after the watermark has advanced and no new data
# arrives, the stream becomes quiet.

print("Lab 4 homework streams started.")
print("Stop manually with Ctrl+C when testing is finished.")

try:
    spark.streams.awaitAnyTermination()
except KeyboardInterrupt:
    print("Stopping streams...")
    window_query.stop()
    alert_query.stop()
    spark.stop()