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


class OrderEventGenerator:
    """Generates simulated e-commerce order events with controlled anomalies."""
    def __init__(self, late_pct=0.05, duplicate_pct=0.03, malformed_pct=0.01):
        self.late_pct = late_pct
        self.duplicate_pct = duplicate_pct
        self.malformed_pct = malformed_pct
