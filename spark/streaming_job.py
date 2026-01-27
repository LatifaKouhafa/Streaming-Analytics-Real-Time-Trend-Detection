from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp, window, date_format, regexp_replace
from pyspark.sql.types import StructType, StructField, StringType

TOPIC = "tweets_stream"
KAFKA_BOOTSTRAP = "kafka:29092"  

schema = StructType([
    StructField("text", StringType(), True),
    StructField("keyword", StringType(), True),
    StructField("timestamp", StringType(), True),
])

spark = (
    SparkSession.builder
    .appName("streaming-trend-detection")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# 1) Read from Kafka (stream)
raw = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
    .option("subscribe", TOPIC)
    .option("startingOffsets", "latest")
    .load()
)

# 2) Parse JSON
json_df = raw.selectExpr("CAST(value AS STRING) as value_str")

parsed = (
    json_df
    .select(from_json(col("value_str"), schema).alias("data"))
    .select(
        col("data.text").alias("text"),
        col("data.keyword").alias("keyword"),
        col("data.timestamp").alias("timestamp_str"),
    )
)

with_time = parsed.withColumn(
    "event_time",
    to_timestamp(col("timestamp_str"), "yyyy-MM-dd'T'HH:mm:ss.SSSSSSXXX")
)
agg = (
    with_time
    .withWatermark("event_time", "10 seconds")
    .groupBy(
        window(col("event_time"), "1 minute"),
        col("keyword")
    )
    .count()
    .select(
        col("window.start").alias("minute_start"),
        col("window.end").alias("minute_end"),
        col("keyword"),
        col("count")
    )
)

def write_to_postgres(batch_df, batch_id):
    (
        batch_df.write
        .format("jdbc")
        .option("url", "jdbc:postgresql://postgres:5432/streaming")
        .option("dbtable", "trends_per_minute")
        .option("user", "demo")
        .option("password", "demo")
        .option("driver", "org.postgresql.Driver")
        .mode("append")
        .save()
    )

query = (
    agg.writeStream
    .outputMode("append")
    .foreachBatch(write_to_postgres)
    .option("checkpointLocation", "/tmp/checkpoints/trends")
    .trigger(processingTime="10 seconds")
    .start()
)

query.awaitTermination()


