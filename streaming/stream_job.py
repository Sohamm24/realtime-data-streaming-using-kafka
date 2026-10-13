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
    watermark_duration = os.getenv("WATERMARK_DURATION", "2 minutes")
    window_duration = os.getenv("WINDOW_DURATION", "1 minute")

    data_dir = os.getenv("DATA_DIR", "/app/data")
    checkpoint_dir = os.getenv("CHECKPOINT_DIR", f"{data_dir}/checkpoints")
    delta_clean_dir = os.getenv("DELTA_CLEAN_DIR", f"{data_dir}/delta/orders_clean")
    delta_agg_dir = os.getenv("DELTA_AGG_DIR", f"{data_dir}/delta/orders_agg")
    dead_letter_dir = os.getenv("DEAD_LETTER_DIR", f"{data_dir}/dead_letter")

    enable_console = os.getenv("ENABLE_CONSOLE_SINK", "true").lower() == "true"

    logger.info("==================================================================")
    logger.info(f"Connecting to Kafka topic '{kafka_topic}' on '{kafka_broker}'")
    logger.info(f"Delta Clean sink: {delta_clean_dir}")
    logger.info(f"Delta Agg sink:   {delta_agg_dir}")
    logger.info(f"Dead Letter sink: {dead_letter_dir}")
    logger.info(f"Checkpoints dir:  {checkpoint_dir}")
    logger.info(f"Watermark: {watermark_duration} | Window: {window_duration}")
    logger.info("==================================================================")

    # 1. Ingest raw byte stream from Kafka
    # maxOffsetsPerTrigger caps rate per micro-batch to prevent OOM on local 8GB machines
    raw_kafka_stream = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", kafka_broker)
        .option("subscribe", kafka_topic)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .option("maxOffsetsPerTrigger", 500)
        .load()
    )

    # 2. Split raw stream into Valid Orders and Corrupt/Malformed Dead Letters
    valid_orders_df, dead_letter_df = parse_raw_kafka_stream(raw_kafka_stream)

    queries = []

    # 3. Sink: Dead-Letter Queue
    # Writes malformed JSON or schema contract violations as JSON files for auditing
    logger.info("Starting Dead-Letter Sink...")
    dlq_query = (
        dead_letter_df.writeStream
        .format("json")
        .outputMode("append")
        .option("path", dead_letter_dir)
        .option("checkpointLocation", f"{checkpoint_dir}/dead_letter")
        .trigger(processingTime="5 seconds")
        .queryName("dead_letter_sink")
        .start()
    )
    queries.append(dlq_query)

    # 4. Stateful Deduplication
    # Deduplicates on order_id within event_time watermark
    deduped_orders_df = deduplicate_orders(
        valid_orders_df,
        watermark_duration=watermark_duration
    )

    # 5. Sink: Clean Deduplicated Orders (Delta Lake)
    logger.info("Starting Delta Clean Orders Sink...")
    clean_query = (
        deduped_orders_df.writeStream
        .format("delta")
        .outputMode("append")
        .option("path", delta_clean_dir)
        .option("checkpointLocation", f"{checkpoint_dir}/orders_clean")
        .trigger(processingTime="5 seconds")
        .queryName("clean_orders_sink")
        .start()
    )
    queries.append(clean_query)

    # 6. Windowed Aggregations (1-minute tumbling windows grouped by city)
    windowed_agg_df = aggregate_windowed_orders(
        deduped_orders_df,
        window_duration=window_duration
    )

    # 7. Sink: Windowed Aggregates (Delta Lake)
    # Append mode emits window results only after watermark has passed window_end
    logger.info("Starting Delta Windowed Aggregates Sink...")
    agg_query = (
        windowed_agg_df.writeStream
        .format("delta")
        .outputMode("append")
        .option("path", delta_agg_dir)
        .option("checkpointLocation", f"{checkpoint_dir}/orders_agg")
        .trigger(processingTime="5 seconds")
        .queryName("windowed_agg_sink")
        .start()
    )
    queries.append(agg_query)

    # 8. Optional Console Sink for live inspection (confirms Phase 1, Step 3)
    if enable_console:
        logger.info("Starting Console Sink for live row confirmation...")
        console_query = (
            deduped_orders_df.writeStream
            .format("console")
            .outputMode("append")
            .option("truncate", "false")
            .trigger(processingTime="5 seconds")
            .queryName("console_sink")
            .start()
        )
        queries.append(console_query)

    logger.info(f"All {len(queries)} streaming queries active. Awaiting termination...")

    try:
        spark.streams.awaitAnyTermination()
    except KeyboardInterrupt:
        logger.info("Streaming job interrupted. Stopping queries...")
        for q in queries:
            if q.isActive:
                q.stop()
        spark.stop()
        logger.info("Spark session cleanly shut down.")


if __name__ == "__main__":
    main()
