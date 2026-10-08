"""
Unit tests for Event Generator and Fault Injection.
"""

from datetime import datetime, timezone, timedelta
import json
import pytest
import sys
import os

# Add producer directory to path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "producer")))

from events import (
    generate_clean_order,
    OrderEventGenerator,
    CITIES,
    ORDER_STATUSES
)


def test_clean_order_structure():
    """Verify clean orders conform to required schema and types."""
    order = generate_clean_order()

    assert "order_id" in order and len(order["order_id"]) == 36  # UUID
    assert order["user_id"].startswith("usr_")
    assert isinstance(order["amount"], float)
    assert order["amount"] > 0
    assert order["city"] in CITIES
    assert order["status"] in ORDER_STATUSES

    # Verify event_time parses as valid ISO-8601 UTC timestamp
    parsed_time = datetime.fromisoformat(order["event_time"])
    assert parsed_time is not None


def test_late_event_injection():
    """Verify late event injector produces timestamps 2-10 minutes in the past."""
    generator = OrderEventGenerator(
        late_pct=1.0,
        duplicate_pct=0.0,
        malformed_pct=0.0
    )

    now = datetime.now(timezone.utc)
    for _ in range(20):
        key, payload, tag = generator.next_event()
        assert tag == "LATE"
        order = json.loads(payload)
        event_time = datetime.fromisoformat(order["event_time"])
        lag = (now - event_time).total_seconds()
        # Should be between 119 and 605 seconds due to slight execution delta
        assert 110 <= lag <= 610
        assert key == order["city"]


def test_duplicate_event_injection():
    """Verify duplicate events replay an order_id from recent history."""
    generator = OrderEventGenerator(
        late_pct=0.0,
        duplicate_pct=1.0,
        malformed_pct=0.0
    )

    # First event initializes history when history is empty
    key1, payload1, tag1 = generator.next_event()
    assert tag1 == "NORMAL"
    first_order = json.loads(payload1)

    # Second event should replay from history
    key2, payload2, tag2 = generator.next_event()
    assert tag2 == "DUPLICATE"
    second_order = json.loads(payload2)
    assert second_order["order_id"] == first_order["order_id"]


def test_malformed_event_injection():
    """Verify malformed injector produces corrupt JSON or invalid schemas."""
    generator = OrderEventGenerator(
        late_pct=0.0,
        duplicate_pct=0.0,
        malformed_pct=1.0
    )

    for _ in range(20):
        key, payload, tag = generator.next_event()
        assert tag == "MALFORMED"
        assert key in CITIES

        is_malformed = False
        try:
            parsed = json.loads(payload)
            # If valid JSON, it must violate mandatory schema fields or types
            missing_fields = not all(k in parsed for k in ["order_id", "user_id", "amount", "event_time"])
            bad_type = not isinstance(parsed.get("amount"), (int, float))
            is_malformed = missing_fields or bad_type
        except json.JSONDecodeError:
            is_malformed = True

        assert is_malformed, f"Payload was unexpectedly valid: {payload}"
