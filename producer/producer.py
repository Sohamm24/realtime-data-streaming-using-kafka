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


def run_producer(
    broker_address: Optional[str] = None,
    topic: Optional[str] = None,
    event_rate: Optional[int] = None,
    max_events: Optional[int] = None
) -> None:
    """
    Main publisher loop.

    Args:
        broker_address: Kafka bootstrap server (defaults to KAFKA_BROKER_INTERNAL or KAFKA_BROKER_EXTERNAL).
        topic: Kafka topic name (defaults to KAFKA_TOPIC).
        event_rate: Number of events per second (defaults to EVENT_RATE).
        max_events: Optional total events to emit before stopping (useful for tests/demos).
    """
    broker = broker_address or os.getenv("KAFKA_BROKER_INTERNAL", os.getenv("KAFKA_BROKER_EXTERNAL", "localhost:9092"))
    topic_name = topic or os.getenv("KAFKA_TOPIC", "orders")
    rate = int(event_rate or os.getenv("EVENT_RATE", "10"))
    late_pct = float(os.getenv("LATE_EVENT_PCT", "0.05"))
    dup_pct = float(os.getenv("DUPLICATE_EVENT_PCT", "0.03"))
    mal_pct = float(os.getenv("MALFORMED_EVENT_PCT", "0.01"))

    logger.info(
        f"Starting producer -> Topic: '{topic_name}' | Broker: {broker} | Rate: {rate} eps | "
        f"Faults: Late={late_pct:.1%}, Dup={dup_pct:.1%}, Malformed={mal_pct:.1%}"
    )

    generator = OrderEventGenerator(
        late_pct=late_pct,
        duplicate_pct=dup_pct,
        malformed_pct=mal_pct
    )

    producer = get_kafka_producer(broker)
    interval = 1.0 / max(rate, 1)

    total_emitted = 0
    tag_counts = {"NORMAL": 0, "LATE": 0, "DUPLICATE": 0, "MALFORMED": 0}

    try:
        while True:
            key, payload, tag = generator.next_event()
            # Send with city as partition key
            producer.send(topic=topic_name, key=key, value=payload)

            total_emitted += 1
            tag_counts[tag] = tag_counts.get(tag, 0) + 1

            if total_emitted % (rate * 5) == 0:  # Log summary roughly every 5 seconds
                logger.info(
                    f"Emitted {total_emitted} events -> "
                    f"Normal: {tag_counts['NORMAL']} | Late: {tag_counts['LATE']} | "
                    f"Dup: {tag_counts['DUPLICATE']} | Malformed: {tag_counts['MALFORMED']}"
                )

            if max_events and total_emitted >= max_events:
                logger.info(f"Reached max_events limit ({max_events}). Stopping.")
                break

            time.sleep(interval)

    except KeyboardInterrupt:
        logger.info("Producer interrupted by user.")
    finally:
        logger.info("Flushing producer buffer...")
        producer.flush()
        producer.close()
        logger.info("Producer shut down cleanly.")


if __name__ == "__main__":
    run_producer()
