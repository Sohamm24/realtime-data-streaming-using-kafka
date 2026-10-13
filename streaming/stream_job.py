"""
Structured Streaming Driver for Real-Time Orders Pipeline.

Reads from Kafka, parses payloads, isolates dead letters to ./data/dead_letter,
deduplicates within a 2-minute watermark, computes 1-minute tumbling aggregations,
and writes to Delta Lake tables (orders_clean and orders_agg) with checkpointing.
"""

import logging
import os
import sys

from pyspark.sql import SparkSession
from transforms import (
    parse_raw_kafka_stream,
    deduplicate_orders,
    aggregate_windowed_orders
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s"
)
logger = logging.getLogger("StreamJob")


def create_spark_session() -> SparkSession:
    """
    Initializes SparkSession configured for local streaming execution with Delta Lake.
    """
    master = os.getenv("SPARK_MASTER", "local[2]")
    driver_mem = os.getenv("SPARK_DRIVER_MEMORY", "2g")

    logger.info(f"Creating SparkSession: master={master}, driver_mem={driver_mem}")

    builder = (
        SparkSession.builder
        .appName("RealtimeOrdersStreaming")
        .master(master)
        .config("spark.driver.memory", driver_mem)
        # Packages required for Kafka streaming and Delta Lake format
        .config(
            "spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,io.delta:delta-spark_2.12:3.2.0"
        )
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        # Keep shuffle partitions aligned with local core count to prevent empty partition overhead
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true")
    )

    return builder.getOrCreate()


def main():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    # Read configuration from environment
    kafka_broker = os.getenv("KAFKA_BROKER_INTERNAL", "kafka:29092")
    kafka_topic = os.getenv("KAFKA_TOPIC", "orders")

