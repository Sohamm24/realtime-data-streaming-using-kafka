"""
Explicit PySpark Schemas for Kafka Ingestion and Dead-Letter Queue.

We explicitly define schemas without relying on schema inference:
1. Schema inference requires reading ahead, which is impossible or unsafe in streaming.
2. An explicit schema guarantees deterministic typing and strict runtime contract enforcement.
3. Includes `_corrupt_record` to safely catch unparseable/malformed JSON strings.
"""

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    TimestampType
)

# Explicit schema for incoming order events.
# Includes `_corrupt_record` to capture malformed JSON syntax without throwing exceptions.
ORDER_PAYLOAD_SCHEMA = StructType([
    StructField("order_id", StringType(), True),
    StructField("user_id", StringType(), True),
    StructField("amount", DoubleType(), True),
    StructField("city", StringType(), True),
    StructField("status", StringType(), True),
    StructField("event_time", StringType(), True),  # Read as string first, then cast to timestamp
    StructField("_corrupt_record", StringType(), True)
])

# Dead-letter schema for recording faulty payloads
DEAD_LETTER_SCHEMA = StructType([
    StructField("raw_payload", StringType(), False),
    StructField("error_reason", StringType(), False),
    StructField("kafka_key", StringType(), True),
    StructField("kafka_partition", StringType(), True),
    StructField("kafka_offset", StringType(), True),
    StructField("ingest_time", TimestampType(), False)
])
