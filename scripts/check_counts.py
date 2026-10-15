"""
Check Counts and Data Consistency Script.

Validates reconciliation across:
  - Kafka Partition Offsets (total emitted messages)
  - Clean Deduplicated Records (Delta: orders_clean)
  - Duplicate Records Dropped
  - Malformed Records Captured (DLQ: dead_letter)
  - Windowed Aggregations (Delta: orders_agg)

Used for interview verification and testing recovery guarantees without data loss or double-counting.
"""

import json
import os
import sys
import duckdb


def get_data_dir() -> str:
    if os.path.exists("/app/data"):
        return "/app/data"
    if os.path.exists("./data"):
        return "./data"
    return "data"


def main():
    data_dir = get_data_dir()
    clean_path = os.path.join(data_dir, "delta", "orders_clean").replace("\\", "/")
    agg_path = os.path.join(data_dir, "delta", "orders_agg").replace("\\", "/")
    dead_letter_path = os.path.join(data_dir, "dead_letter").replace("\\", "/")

    con = duckdb.connect()
    try:
        con.execute("INSTALL delta; LOAD delta;")
    except Exception as e:
        print(f"[Warning] Delta extension note: {e}")

    print("\n================================================================================")
    print("  AUDIT & RECONCILIATION: REAL-TIME STREAMING INTEGRITY REPORT")
    print("================================================================================\n")

    # 1. Clean Orders Metrics
    total_clean = 0
    unique_orders = 0
    try:
        res = con.execute(f"SELECT COUNT(*), COUNT(DISTINCT order_id) FROM delta_scan('{clean_path}')").fetchone()
        total_clean = res[0]
        unique_orders = res[1]
    except Exception as e:
        print(f"Could not read orders_clean: {e}")

    # 2. Dead Letter Metrics
    total_dlq = 0
    try:
        res = con.execute(f"SELECT COUNT(*) FROM read_json_auto('{dead_letter_path}/*.json')").fetchone()
        total_dlq = res[0]
    except Exception as e:
        print(f"Could not read dead_letter: {e}")

    # 3. Aggregation Windows Metrics
    total_windows = 0
    sum_agg_orders = 0
    total_revenue = 0.0
    try:
        res = con.execute(f"""
            SELECT 
                COUNT(*), 
                COALESCE(SUM(total_orders), 0), 
                COALESCE(ROUND(SUM(total_revenue), 2), 0.0)
            FROM delta_scan('{agg_path}')
        """).fetchone()
        total_windows = res[0]
        sum_agg_orders = res[1]
        total_revenue = res[2]
    except Exception as e:
        print(f"Could not read orders_agg: {e}")

    duplicate_discrepancy = total_clean - unique_orders

if __name__ == "__main__":
    audit_pipeline()
