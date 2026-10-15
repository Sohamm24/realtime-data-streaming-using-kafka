"""
Query Output Script.

Queries Delta Lake tables (orders_agg, orders_clean) and dead-letter store
using DuckDB delta_scan. Can run either on the host or inside the container.
"""

import os
import sys
import duckdb


def get_data_dir() -> str:
    """Detects whether running inside Docker container or directly on host."""
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

    print(f"\n================================================================================")
    print(f"  REAL-TIME ORDERS STREAMING: DELTA LAKE & DLQ INSPECTOR")
    print(f"  Data Directory: {data_dir}")
    print(f"================================================================================\n")

    con = duckdb.connect()
    try:
        con.execute("INSTALL delta; LOAD delta;")
    except Exception as e:
        print(f"[Warning] Could not load Delta extension directly: {e}")

    # 1. Inspect Windowed Aggregates
    print("--------------------------------------------------------------------------------")
    print(" 1. LATEST 1-MINUTE WINDOWED AGGREGATES (DELTA: orders_agg)")
    print("--------------------------------------------------------------------------------")
    try:
        con.sql(f"""
            SELECT 
                window_start,
                window_end,
                city,
                total_orders,
                total_revenue,
                paid_orders,
                created_orders,
                failed_orders,
                cancelled_orders
            FROM delta_scan('{agg_path}')
            ORDER BY window_start DESC, total_revenue DESC
            LIMIT 10
        """).show()
    except Exception as e:
        print(f"No windowed aggregates available yet: {e}")

    # 2. City Revenue Summary Across All Windows
    print("\n--------------------------------------------------------------------------------")
    print(" 2. CUMULATIVE REVENUE & STATUS DISTRIBUTION BY CITY")
    print("--------------------------------------------------------------------------------")
    try:
        con.sql(f"""
            SELECT 
                city,
                SUM(total_orders) AS cumulative_orders,
                ROUND(SUM(total_revenue), 2) AS cumulative_revenue_inr,
                SUM(paid_orders) AS total_paid,
                SUM(failed_orders) AS total_failed,
                SUM(cancelled_orders) AS total_cancelled
            FROM delta_scan('{agg_path}')
            GROUP BY city
            ORDER BY cumulative_revenue_inr DESC
        """).show()
    except Exception as e:
        print(f"Error querying city summary: {e}")

    # 3. Sample Clean Deduplicated Orders
    print("\n--------------------------------------------------------------------------------")
    print(" 3. SAMPLE CLEAN DEDUPLICATED ORDERS (DELTA: orders_clean)")
    print("--------------------------------------------------------------------------------")
    try:
        con.sql(f"""
            SELECT 
                order_id,
                user_id,
                amount,
                city,
                status,
                event_time,
                kafka_partition,
                kafka_offset
            FROM delta_scan('{clean_path}')
            ORDER BY event_time DESC
            LIMIT 5
        """).show()
    except Exception as e:
        print(f"Error querying clean orders: {e}")

    # 4. Dead-Letter Queue Diagnostics
    print("\n--------------------------------------------------------------------------------")
    print(" 4. DEAD-LETTER QUEUE DIAGNOSTICS (DLQ: dead_letter)")
    print("--------------------------------------------------------------------------------")
    try:
        # Read JSON partition files directly
        dlq_glob = f"{dead_letter_path}/*.json"
        con.sql(f"""
            SELECT 
                error_reason,
                COUNT(*) AS occurrences,
                MIN(ingest_time) AS first_seen,
                MAX(ingest_time) AS last_seen
            FROM read_json_auto('{dlq_glob}')
            GROUP BY error_reason
            ORDER BY occurrences DESC
        """).show()

        print("\nSample Malformed Records from DLQ:")
        con.sql(f"""
            SELECT 
                raw_payload,
                error_reason,
                kafka_partition,
                kafka_offset
            FROM read_json_auto('{dlq_glob}')
            ORDER BY ingest_time DESC
            LIMIT 3
        """).show()
    except Exception as e:
        print(f"No dead letter files recorded yet: {e}")

    print("================================================================================\n")


if __name__ == "__main__":
    main()
