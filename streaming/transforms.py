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

