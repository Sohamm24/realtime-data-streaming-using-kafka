"""
Unit tests for PySpark Streaming Transformations.

Tests:
1. Parsing of valid vs malformed Kafka payloads (DLQ routing).
2. Stateful deduplication by order_id and event_time.
3. 1-minute tumbling window aggregations.
"""

from datetime import datetime, timezone
import json
import pytest
import sys
import os

# PySpark session fixture for unit testing transformations in local[1] mode
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    LongType,
    TimestampType
)

# Add streaming directory to path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "streaming")))

from transforms import (
    parse_raw_kafka_stream,
    deduplicate_orders,
    aggregate_windowed_orders
)


@pytest.fixture(scope="session")
def spark():
    """Initializes a lightweight local SparkSession for unit tests."""
    session = (
        SparkSession.builder
        .master("local[1]")
        .appName("UnitTest-Transforms")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.default.parallelism", "1")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


def create_mock_kafka_df(spark, records):
    """Helper to simulate raw binary Kafka DataFrame."""
    schema = StructType([
        StructField("key", StringType(), True),
        StructField("value", StringType(), True),
        StructField("partition", IntegerType(), True),
        StructField("offset", LongType(), True),
        StructField("timestamp", TimestampType(), True)
    ])
    return spark.createDataFrame(records, schema)


def test_parse_valid_and_malformed_records(spark):
    """Verify clean orders go to valid_df while corrupt payloads route to DLQ."""
    valid_order = {
        "order_id": "12345678-1234-1234-1234-123456789abc",
        "user_id": "usr_1001",
        "amount": 299.50,
        "city": "Mumbai",
        "status": "PAID",
        "event_time": "2026-10-15T12:00:00+00:00"
    }

    malformed_json = "{bad_json: missing_quotes"
    missing_order_id = json.dumps({"city": "Delhi", "amount": 150.00, "status": "PAID"})
    bad_amount = json.dumps({
        "order_id": "87654321-4321-4321-4321-cba987654321",
        "user_id": "usr_1002",
        "amount": "NOT_NUMBER",
        "city": "Bengaluru",
        "status": "PAID",
        "event_time": "2026-10-15T12:00:05+00:00"
    })

    now = datetime.now(timezone.utc)
    mock_data = [
        ("Mumbai", json.dumps(valid_order), 0, 1, now),
        ("Delhi", malformed_json, 1, 2, now),
        ("Delhi", missing_order_id, 1, 3, now),
        ("Bengaluru", bad_amount, 2, 4, now)
    ]

    mock_df = create_mock_kafka_df(spark, mock_data)
    valid_df, dlq_df = parse_raw_kafka_stream(mock_df)

    valid_rows = valid_df.collect()
    dlq_rows = dlq_df.collect()

    # 1. Exactly 1 valid record
    assert len(valid_rows) == 1
    assert valid_rows[0]["order_id"] == "12345678-1234-1234-1234-123456789abc"
    assert valid_rows[0]["amount"] == 299.50
    assert valid_rows[0]["city"] == "Mumbai"

    # 2. Exactly 3 records in DLQ
    assert len(dlq_rows) == 3
    reasons = [r["error_reason"] for r in dlq_rows]
    assert "MALFORMED_JSON_SYNTAX" in reasons
    assert "MISSING_ORDER_ID" in reasons


def test_deduplicate_orders(spark):
    """Verify deduplication removes duplicate records with identical order_id."""
    ts = datetime(2026, 10, 15, 12, 0, 0)
    data = [
        ("uuid-1", "usr_1", 100.0, "Mumbai", "PAID", ts, 0, 1),
        ("uuid-1", "usr_1", 100.0, "Mumbai", "PAID", ts, 0, 2),  # Duplicate
        ("uuid-2", "usr_2", 200.0, "Delhi", "PAID", ts, 1, 3)
    ]
    schema = StructType([
        StructField("order_id", StringType(), False),
        StructField("user_id", StringType(), False),
        StructField("amount", StringType(), False),
        StructField("city", StringType(), False),
        StructField("status", StringType(), False),
        StructField("event_time", TimestampType(), False),
        StructField("kafka_partition", IntegerType(), False),
        StructField("kafka_offset", LongType(), False)
    ])

    df = spark.createDataFrame(data, schema)
    # Using batch deduplication behavior tested against transforms.py
    deduped = deduplicate_orders(df, watermark_duration="2 minutes")
    assert deduped.count() == 2


def test_aggregate_windowed_orders(spark):
    """Verify 1-minute tumbling aggregations calculate revenue and status counts."""
    t1 = datetime(2026, 10, 15, 12, 0, 10)
    t2 = datetime(2026, 10, 15, 12, 0, 25)
    t3 = datetime(2026, 10, 15, 12, 1, 15)  # Next window

    data = [
        ("id-1", "u1", 500.0, "Mumbai", "PAID", t1, 0, 1),
        ("id-2", "u2", 300.0, "Mumbai", "CREATED", t2, 0, 2),
        ("id-3", "u3", 250.0, "Delhi", "PAID", t1, 1, 3),
        ("id-4", "u4", 1000.0, "Mumbai", "PAID", t3, 0, 4)
    ]
    schema = StructType([
        StructField("order_id", StringType(), False),
        StructField("user_id", StringType(), False),
        StructField("amount", StringType(), False),
        StructField("city", StringType(), False),
        StructField("status", StringType(), False),
        StructField("event_time", TimestampType(), False),
        StructField("kafka_partition", IntegerType(), False),
        StructField("kafka_offset", LongType(), False)
    ])

    df = spark.createDataFrame(data, schema)
    agg_df = aggregate_windowed_orders(df, window_duration="1 minute")

    results = agg_df.collect()
    assert len(results) == 3

    mumbai_w1 = [r for r in results if r["city"] == "Mumbai" and r["window_start"].minute == 0][0]
    assert mumbai_w1["total_orders"] == 2
    assert mumbai_w1["total_revenue"] == 800.0
    assert mumbai_w1["paid_orders"] == 1
    assert mumbai_w1["created_orders"] == 1
