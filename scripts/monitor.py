"""
Pipeline Real-Time Monitor.

Displays live operational metrics:
- Kafka Topic Partitions & Ingest Offsets
- Delta Lake Table Sizes & Records
- Dead-Letter Queue Fault Capture Rate
- Processing throughput and lag
"""

import json
import os
import time
import duckdb


def get_data_dir() -> str:
    if os.path.exists("/app/data"):
        return "/app/data"
    if os.path.exists("./data"):
        return "./data"
    return "data"


def display_dashboard():
    data_dir = get_data_dir()
    clean_path = os.path.join(data_dir, "delta", "orders_clean").replace("\\", "/")
    agg_path = os.path.join(data_dir, "delta", "orders_agg").replace("\\", "/")
    dlq_path = os.path.join(data_dir, "dead_letter").replace("\\", "/")

    con = duckdb.connect()
    try:
        con.execute("INSTALL delta; LOAD delta;")
    except Exception:
        pass

    print("\033[2J\033[H", end="")  # Clear terminal screen
    print("=" * 80)
    print("   REALTIME ORDERS STREAMING - OPERATIONAL HEALTH MONITOR")
    print("=" * 80)
    print(f"Timestamp: {time.strftime('%Y-%m-%d %H:%M:%S UTC', time.gmtime())}")
    print("-" * 80)

    # 1. Delta Clean Table
    try:
        clean_stats = con.execute(f"""
            SELECT 
                COUNT(*) as total_clean,
                COUNT(DISTINCT order_id) as unique_orders,
                ROUND(SUM(amount), 2) as gross_volume,
                MAX(event_time) as latest_event
            FROM delta_scan('{clean_path}')
        """).fetchone()
        print(f"[DELTA CLEAN ORDERS]   Rows: {clean_stats[0]:,} | Unique IDs: {clean_stats[1]:,} | Vol: Rs. {clean_stats[2]:,.2f} | Latest: {clean_stats[3]}")
    except Exception as e:
        print(f"[DELTA CLEAN ORDERS]   Initializing: {e}")

    # 2. Delta Aggregated Windows
    try:
        agg_stats = con.execute(f"""
            SELECT 
                COUNT(*) as total_windows,
                SUM(total_orders) as agg_orders,
                ROUND(SUM(total_revenue), 2) as settled_revenue,
                MAX(window_end) as latest_window
            FROM delta_scan('{agg_path}')
        """).fetchone()
        print(f"[DELTA AGGREGATIONS]   Windows: {agg_stats[0]:,} | Orders: {agg_stats[1]:,} | Settled: Rs. {agg_stats[2]:,.2f} | Latest Window End: {agg_stats[3]}")
    except Exception as e:
        print(f"[DELTA AGGREGATIONS]   Initializing: {e}")

    # 3. Dead Letter Queue
    try:
        dlq_stats = con.execute(f"""
            SELECT COUNT(*), COUNT(DISTINCT error_reason)
            FROM read_json_auto('{dlq_path}/*.json')
        """).fetchone()
        print(f"[DEAD-LETTER SINK]     Fault Records: {dlq_stats[0]:,} | Distinct Failure Causes: {dlq_stats[1]:,}")
    except Exception as e:
        print(f"[DEAD-LETTER SINK]     Initializing: {e}")

    # 4. Top Performing Metros
    print("-" * 80)
    print("TOP METROS BY TOTAL REVENUE:")
    try:
        con.sql(f"""
            SELECT 
                city,
                SUM(total_orders) AS orders,
                ROUND(SUM(total_revenue), 2) AS revenue_inr,
                ROUND(SUM(paid_orders) * 100.0 / NULLIF(SUM(total_orders), 0), 1) AS success_rate_pct
            FROM delta_scan('{agg_path}')
            GROUP BY city
            ORDER BY revenue_inr DESC
            LIMIT 5
        """).show()
    except Exception as e:
        print(f"Aggregates warming up: {e}")

    print("=" * 80)


if __name__ == "__main__":
    display_dashboard()
