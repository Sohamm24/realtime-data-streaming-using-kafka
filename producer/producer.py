"""
Kafka Order Event Producer.

Publishes simulated orders to Kafka topic `orders` with message key = city.
Reads configuration from environment variables or .env.
"""

import logging
import os
import time
from typing import Optional

from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

from events import OrderEventGenerator

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s"
)
logger = logging.getLogger("OrderProducer")


def get_kafka_producer(broker_address: str, retries: int = 15, delay: int = 2) -> KafkaProducer:
    """
    Connects to Kafka with retry backoff to tolerate container boot ordering.
    """
    for attempt in range(1, retries + 1):
        try:
            logger.info(f"Attempting Kafka connection to {broker_address} (try {attempt}/{retries})...")
            producer = KafkaProducer(
                bootstrap_servers=broker_address,
                key_serializer=lambda k: k.encode("utf-8") if k else b"",
                value_serializer=lambda v: v.encode("utf-8") if isinstance(v, str) else v,
                acks="all",  # Strongest durability guarantee
                retries=3,
                linger_ms=10  # Micro-batching for throughput
            )
            logger.info("Successfully connected to Kafka broker.")
            return producer
        except NoBrokersAvailable as e:
            logger.warning(f"Kafka broker not ready yet ({e}). Waiting {delay}s...")
            time.sleep(delay)

    raise ConnectionError(f"Could not connect to Kafka at {broker_address} after {retries} attempts.")



