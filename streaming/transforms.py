"""
Streaming Transformation Logic for Orders Pipeline.

Contains pure DataFrame transformation functions for:
1. Parsing Kafka raw payloads into Valid vs Dead-Letter records.
2. Stateful deduplication bounded by event-time watermarking.
3. Tumbling window aggregations for city-level metrics.
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from schemas import ORDER_PAYLOAD_SCHEMA


def parse_raw_kafka_stream(kafka_df: DataFrame):
    """
    Parses binary Kafka records (key, value, partition, offset) into structured columns.

    Splits incoming stream into two distinct DataFrames:
      - valid_df: Well-formed events with parsed timestamps and valid schemas.
      - dead_letter_df: Corrupted JSON strings or schema contract violations.

    Why this design?
      In production streaming, one unparseable byte sequence shouldn't bring down
      the entire pipeline. Corrupted records are isolated to a Dead-Letter Queue (DLQ)
      for forensic auditing and alerting while healthy traffic continues flowing.
    """
    # 1. Unpack raw Kafka binary attributes
    raw_df = kafka_df.select(
        F.col("key").cast("string").alias("kafka_key"),
        F.col("value").cast("string").alias("raw_value"),
        F.col("partition").alias("kafka_partition"),
        F.col("offset").alias("kafka_offset"),
        F.col("timestamp").alias("kafka_timestamp")
    )

    # 2. Parse JSON with columnNameOfCorruptRecord to prevent parser crashes on bad syntax
    parsed_df = raw_df.withColumn(
        "parsed",
        F.from_json(
            F.col("raw_value"),
            ORDER_PAYLOAD_SCHEMA,
            {"columnNameOfCorruptRecord": "_corrupt_record"}
        )
    )

    # 3. Derive parsed timestamp to validate temporal format
    parsed_df = parsed_df.withColumn(
        "parsed_event_time",
        F.to_timestamp(F.col("parsed.event_time"))
    )

    # 4. Strict validation predicate
    is_valid = (
        F.col("parsed._corrupt_record").isNull() &
        F.col("parsed.order_id").isNotNull() &
        (F.length(F.col("parsed.order_id")) == 36) &
        F.col("parsed.user_id").isNotNull() &
        F.col("parsed.amount").isNotNull() &
        (F.col("parsed.amount") > 0.0) &
        F.col("parsed.city").isNotNull() &
        F.col("parsed.status").isin(["CREATED", "PAID", "FAILED", "CANCELLED"]) &
        F.col("parsed_event_time").isNotNull()
    )

    # 5. Build Valid DataFrame
    valid_df = parsed_df.filter(is_valid).select(
        F.col("parsed.order_id").alias("order_id"),
        F.col("parsed.user_id").alias("user_id"),
        F.round(F.col("parsed.amount"), 2).alias("amount"),
        F.col("parsed.city").alias("city"),
        F.col("parsed.status").alias("status"),
        F.col("parsed_event_time").alias("event_time"),
        F.col("kafka_partition"),
        F.col("kafka_offset")
    )

    # 6. Build Dead-Letter DataFrame with contextual diagnostics
    dead_letter_df = parsed_df.filter(~is_valid).select(
        F.coalesce(F.col("parsed._corrupt_record"), F.col("raw_value")).alias("raw_payload"),
        F.when(F.col("parsed._corrupt_record").isNotNull(), "MALFORMED_JSON_SYNTAX")
         .when(F.col("parsed.order_id").isNull(), "MISSING_ORDER_ID")
         .when(F.length(F.col("parsed.order_id")) != 36, "INVALID_ORDER_ID_UUID")
         .when(F.col("parsed.amount").isNull(), "MISSING_OR_NON_NUMERIC_AMOUNT")
         .when(F.col("parsed.amount") <= 0.0, "NON_POSITIVE_AMOUNT")
         .when(F.col("parsed_event_time").isNull(), "INVALID_TIMESTAMP_FORMAT")
         .otherwise("SCHEMA_VALIDATION_FAILURE").alias("error_reason"),
        F.col("kafka_key"),
        F.col("kafka_partition").cast("string"),
        F.col("kafka_offset").cast("string"),
        F.current_timestamp().alias("ingest_time")
    )

    return valid_df, dead_letter_df


def deduplicate_orders(df: DataFrame, watermark_duration: str = "2 minutes") -> DataFrame:
    """
    Deduplicates orders by `order_id` within the specified watermark.

    Trade-off & Interview Explanation:
      - Watermarking: `withWatermark("event_time", watermark_duration)` establishes a
        moving temporal threshold: `watermark = max(event_time) - watermark_duration`.
      - State Management: Structured Streaming stores seen order_ids in an internal
        state store (in-memory / RocksDB). Without a watermark, Spark would retain every
        order_id forever, eventually causing OutOfMemoryError.
      - With a 2-minute watermark, Spark safely purges state keys older than the watermark,
        keeping memory overhead strictly bounded.
      - Any duplicate event arriving within 2 minutes of the original will be dropped.
    """
    watermarked_df = df.withWatermark("event_time", watermark_duration)

    # In Spark Structured Streaming, deduplicating on a streaming dataset with a watermark
    # requires event_time to be present in the deduplication keys or watermark constraint.
    return watermarked_df.dropDuplicates(["order_id", "event_time"])


def aggregate_windowed_orders(
    df: DataFrame,
    window_duration: str = "1 minute"
) -> DataFrame:
    """
    Computes 1-minute tumbling window aggregations per city.

    Metrics calculated:
      - total_orders: total count of orders in the window.
      - total_revenue: sum of transaction amounts (rounded to 2 decimal places).
      - counts per status: paid_orders, created_orders, failed_orders, cancelled_orders.

    Interview Note on Append Mode:
      When writing windowed aggregations in 'append' mode, Spark emits an aggregation row
      ONLY after the watermark has passed the end of the window:
        `watermark > window.end`.
      This guarantees the window is finalized and no further updates will arrive.
    """
    windowed = df.groupBy(
        F.window(F.col("event_time"), window_duration),
        F.col("city")
    ).agg(
        F.count("order_id").alias("total_orders"),
        F.round(F.sum("amount"), 2).alias("total_revenue"),
        F.count(F.when(F.col("status") == "PAID", 1)).alias("paid_orders"),
        F.count(F.when(F.col("status") == "CREATED", 1)).alias("created_orders"),
        F.count(F.when(F.col("status") == "FAILED", 1)).alias("failed_orders"),
        F.count(F.when(F.col("status") == "CANCELLED", 1)).alias("cancelled_orders")
    )

    # Flatten window boundaries into explicit timestamp columns for downstream queries
    return windowed.select(
        F.col("window.start").alias("window_start"),
        F.col("window.end").alias("window_end"),
        F.col("city"),
        F.col("total_orders"),
        F.col("total_revenue"),
        F.col("paid_orders"),
        F.col("created_orders"),
        F.col("failed_orders"),
        F.col("cancelled_orders")
    )
