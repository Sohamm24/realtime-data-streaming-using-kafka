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

