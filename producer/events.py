"""
Event Generator and Fault Injection Module.

Simulates e-commerce / UPI-style transactions across Indian metros with
configurable fault injection (late events, duplicate events, malformed payloads).
"""

from datetime import datetime, timezone, timedelta
import json
import random
from typing import Dict, Any, Tuple, Optional
import uuid

# 10 Indian metropolitan cities for realistic e-commerce simulation
CITIES = [
    "Mumbai",
    "Delhi",
    "Bengaluru",
    "Hyderabad",
    "Chennai",
    "Kolkata",
    "Pune",
    "Ahmedabad",
    "Jaipur",
    "Lucknow"
]

# Order lifecycle statuses
ORDER_STATUSES = ["CREATED", "PAID", "FAILED", "CANCELLED"]
# Weights corresponding to typical UPI/card checkout funnel
STATUS_WEIGHTS = [0.15, 0.70, 0.10, 0.05]


def generate_clean_order(
    fixed_time: Optional[datetime] = None
) -> Dict[str, Any]:
    """
    Generates a valid, well-formed order event.

    Args:
        fixed_time: Optional explicit timestamp (useful for deterministic testing).
    """
    event_time = fixed_time or datetime.now(timezone.utc)
    return {
        "order_id": str(uuid.uuid4()),
        "user_id": f"usr_{random.randint(1000, 9999)}",
        "amount": round(random.uniform(99.0, 4999.0), 2),
        "city": random.choice(CITIES),
        "status": random.choices(ORDER_STATUSES, weights=STATUS_WEIGHTS, k=1)[0],
        "event_time": event_time.isoformat()
    }


class OrderEventGenerator:
    """
    Stateful generator that injects:
      1. Late events (~5% by default): event_time set 2-10 minutes in the past.
      2. Duplicate events (~3% by default): re-transmits an existing order_id.
      3. Malformed events (~1% by default): corrupt JSON or missing fields.
    """

    def __init__(
        self,
        late_pct: float = 0.05,
        duplicate_pct: float = 0.03,
        malformed_pct: float = 0.01,
        history_size: int = 50
    ):
        self.late_pct = late_pct
        self.duplicate_pct = duplicate_pct
        self.malformed_pct = malformed_pct
        self.history_size = history_size
        # Ring buffer of recent valid orders to sample duplicates from
        self.order_history = []

    def next_event(self) -> Tuple[str, str, str]:
        """
        Generates the next event tuple: (kafka_key, serialized_payload, event_tag).
        
        event_tag indicates: 'NORMAL', 'LATE', 'DUPLICATE', or 'MALFORMED'.
        """
        roll = random.random()

        # Cumulative probability boundaries
        malformed_thresh = self.malformed_pct
        duplicate_thresh = malformed_thresh + self.duplicate_pct
        late_thresh = duplicate_thresh + self.late_pct

        # 1. Fault: Malformed payload
        if roll < malformed_thresh:
            return self._generate_malformed_event()

        # 2. Fault: Duplicate event
        if roll < duplicate_thresh:
            if self.order_history:
                replayed_order = random.choice(self.order_history)
                key = replayed_order.get("city", "Mumbai")
                payload = json.dumps(replayed_order)
                return key, payload, "DUPLICATE"
            # If history is empty, fall through and create a clean order to seed history
            order = generate_clean_order()
            self._record_history(order)
            return order["city"], json.dumps(order), "NORMAL"

        # 3. Fault: Late event
        if roll < late_thresh:
            # 2 to 10 minutes (120 to 600 seconds) in the past
            lag_seconds = random.randint(120, 600)
            late_time = datetime.now(timezone.utc) - timedelta(seconds=lag_seconds)
            order = generate_clean_order(fixed_time=late_time)
            self._record_history(order)
            return order["city"], json.dumps(order), "LATE"

        # 4. Standard on-time event
        order = generate_clean_order()
        self._record_history(order)
        return order["city"], json.dumps(order), "NORMAL"

    def _generate_malformed_event(self) -> Tuple[str, str, str]:
        """Injects either broken JSON syntax or schema corruption."""
        malform_type = random.choice(["corrupt_json", "missing_fields", "invalid_type"])
        city = random.choice(CITIES)

        if malform_type == "corrupt_json":
            # Unterminated / syntactically broken JSON string
            payload = f'{{"order_id": "{uuid.uuid4()}", "city": "{city}", "amount": 250.00'
            return city, payload, "MALFORMED"

        if malform_type == "missing_fields":
            # Missing mandatory order_id or event_time
            payload = json.dumps({
                "city": city,
                "amount": 99.99,
                "status": "PAID"
                # missing order_id, user_id, event_time
            })
            return city, payload, "MALFORMED"

        # invalid_type: amount is a string or non-numeric
        payload = json.dumps({
            "order_id": str(uuid.uuid4()),
            "user_id": "usr_corrupted",
            "amount": "NOT_A_NUMBER",
            "city": city,
            "status": "PAID",
            "event_time": datetime.now(timezone.utc).isoformat()
        })
        return city, payload, "MALFORMED"

    def _record_history(self, order: Dict[str, Any]) -> None:
        """Maintains bounded ring buffer of recent events."""
        self.order_history.append(order)
        if len(self.order_history) > self.history_size:
            self.order_history.pop(0)
