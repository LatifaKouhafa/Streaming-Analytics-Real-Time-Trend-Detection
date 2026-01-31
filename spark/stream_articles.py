"""
Spark Structured Streaming: Kafka -> Language Detection -> Sentiment -> Live Console Metrics
-----------------------------------------------------------------------------------------

This Spark job consumes article events from a Kafka topic ("articles") in real time, enriches
each event with:
  1) detected language (langdetect)
  2) sentiment score + label (VADER, English-only)

Then it continuously prints a few live analytics to the console:
  - sentiment distribution (positive / negative / neutral)
  - language distribution
  - hourly ingestion volume

Notes:
- Spark runs inside Docker, so Kafka is reached via: kafka:29092 (Docker network listener).
- Sentiment is computed only when detected_lang == "en" (VADER is tuned for English).
- For non-English articles, we default sentiment to "neutral" to avoid misleading scores.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, window, udf, concat_ws, when
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

from langdetect import detect
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer


# -------------------------------------------------------------------
# Configuration
# -------------------------------------------------------------------
TOPIC = "articles"


# -------------------------------------------------------------------
# Spark session
# -------------------------------------------------------------------
spark = SparkSession.builder.appName("ArticlesStreamProcessing").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")


# -------------------------------------------------------------------
# Step 0: Define UDFs (must be defined before usage)
# -------------------------------------------------------------------
def detect_lang(text: str) -> str:
    """
    Detect the language of a text using langdetect.
    Returns "unknown" if detection fails or input is not usable.
    """
    try:
        return detect(text)
    except Exception:
        return "unknown"


detect_lang_udf = udf(detect_lang, StringType())


@udf(DoubleType())
def vader_compound(text: str) -> float:
    """
    Compute VADER compound sentiment score in [-1, +1].
    Returns 0.0 for empty / null texts.

    NOTE: VADER is optimized for English, so we will only call this UDF for English texts.
    """
    if text is None or text.strip() == "":
        return 0.0

    analyzer = SentimentIntensityAnalyzer()
    return float(analyzer.polarity_scores(text)["compound"])


@udf(StringType())
def sentiment_label(score: float) -> str:
    """
    Convert a VADER compound score into a discrete label.

    Common thresholds:
      - score >=  0.05  -> positive
      - score <= -0.05  -> negative
      - otherwise       -> neutral
    """
    if score is None:
        return "neutral"
    if score >= 0.05:
        return "positive"
    if score <= -0.05:
        return "negative"
    return "neutral"



def write_to_postgres(batch_df, batch_id):
    (
        batch_df
        .select(
            "title",
            "source",
            "source_country",
            "detected_lang",
            "sentiment",
            "sentiment_score",
            "event_time"
        )
        .write
        .format("jdbc")
        .option("url", "jdbc:postgresql://postgres:5432/streaming")
        .option("dbtable", "articles_enriched")
        .option("user", "demo")
        .option("password", "demo")
        .option("driver", "org.postgresql.Driver")
        .mode("append")
        .save()
    )


# -------------------------------------------------------------------
# Step 1: Read Kafka stream (Docker network listener)
# -------------------------------------------------------------------
kafka_df = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "kafka:29092")
    .option("subscribe", TOPIC)
    .option("startingOffsets", "earliest")
    .load()
)

# Use Kafka message timestamp as event_time (stable & consistent)
kafka_with_time = kafka_df.selectExpr(
    "CAST(value AS STRING) as json_str",
    "timestamp as event_time"
)


# -------------------------------------------------------------------
# Step 2: Parse JSON payload into a structured DataFrame
# -------------------------------------------------------------------
schema = StructType([
    StructField("article_id", StringType(), True),
    StructField("tweet_id", StringType(), True),
    StructField("theme", StringType(), True),
    StructField("source_country", StringType(), True),
    StructField("keyword", StringType(), True),
    StructField("title", StringType(), True),
    StructField("text", StringType(), True),
    StructField("url", StringType(), True),
    StructField("created_at", StringType(), True),
    StructField("published_at", StringType(), True),
    StructField("lang", StringType(), True),
    StructField("source", StringType(), True),
    StructField("provider", StringType(), True),
])

events = kafka_with_time.select(
    from_json(col("json_str"), schema).alias("e"),
    col("event_time")
).select("e.*", "event_time")


# -------------------------------------------------------------------
# Step 3: Enrichment - language detection (title + text)
# -------------------------------------------------------------------
events_lang = events.withColumn(
    "detected_lang",
    detect_lang_udf(concat_ws(" ", col("title"), col("text")))
)


# -------------------------------------------------------------------
# Step 4: Enrichment - sentiment analysis (English only)
# -------------------------------------------------------------------
events_sent = (
    events_lang
    .withColumn(
        "sentiment_score",
        when(
            col("detected_lang") == "en",
            vader_compound(concat_ws(" ", col("title"), col("text")))
        ).otherwise(None)
    )
    .withColumn(
        "sentiment",
        when(
            col("detected_lang") == "en",
            sentiment_label(col("sentiment_score"))
        ).otherwise("neutral")
    )
)


# -------------------------------------------------------------------
# Step 5: Streaming metrics (console sinks)
# -------------------------------------------------------------------

# (A) Sentiment distribution (live counts)
sentiment_stats = (
    events_sent
    .groupBy("sentiment")
    .count()
    .orderBy(col("count").desc())
)

sentiment_stats.writeStream \
    .outputMode("complete") \
    .format("console") \
    .option("truncate", "false") \
    .start()


# (B) Language distribution (live counts)
lang_stats = (
    events_lang
    .groupBy("detected_lang")
    .count()
    .orderBy(col("count").desc())
)

lang_stats.writeStream \
    .outputMode("complete") \
    .format("console") \
    .option("truncate", "false") \
    .start()


# (C) Ingestion volume per hour (based on Kafka ingestion timestamp)
counts_per_hour = (
    events
    .groupBy(window(col("event_time"), "1 hour"))
    .count()
    .orderBy(col("window"))
)

counts_per_hour.writeStream \
    .outputMode("complete") \
    .format("console") \
    .option("truncate", "false") \
    .start()

events_sent.writeStream \
    .foreachBatch(write_to_postgres) \
    .outputMode("append") \
    .option("checkpointLocation", "/tmp/checkpoints/articles_enriched") \
    .start()

# -------------------------------------------------------------------
# Step 6: Keep the streaming queries running
# -------------------------------------------------------------------
# Since we started multiple streaming queries, use awaitAnyTermination().
spark.streams.awaitAnyTermination()
