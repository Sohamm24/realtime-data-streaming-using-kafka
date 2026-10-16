# Real-Time Orders Streaming Lakehouse (`realtime-orders-streaming`)

A production-grade, 100% local streaming pipeline processing simulated e-commerce and UPI order events. Built with **Apache Kafka (KRaft mode)**, **Apache Spark Structured Streaming (PySpark)**, **Delta Lake**, and **DuckDB**, orchestrated entirely within a single `docker-compose.yml`.

---

## Architecture Overview

```mermaid
flowchart TD
    subgraph Data_Generation["Event Producer (Python 3.11)"]
        G["OrderEventGenerator"] -->|Inject Faults:\n~5% Late, ~3% Dup, ~1% Malformed| P["KafkaProducer\n(Key = City)"]
    end

    subgraph Messaging_Backbone["Distributed Messaging (KRaft Mode)"]
        P -->|Internal: kafka:29092| K["Kafka Broker: orders\n(3 Partitions, Key = City)"]
        KUI["Kafka UI (Port 8081)"] -.->|Inspect Partitions & Offsets| K
    end

    subgraph Stream_Engine["PySpark Structured Streaming (local[2])"]
        K -->|readStream\nmaxOffsetsPerTrigger=500| S["stream_job.py"]
        S -->|Explicit Schema\nwith _corrupt_record| SPLIT{"Schema & Syntax\nValidation"}

        SPLIT -->|Malformed JSON\nor Missing Fields| DLQ["Dead-Letter Sink\n(Format: JSON)"]
        SPLIT -->|Well-Formed Events| DEDUP["Stateful Deduplication\n(Watermark: 2 min, Key: order_id)"]

        DEDUP -->|Clean Records| CLEAN_SINK["Delta Sink: orders_clean\n(Append Mode, Checkpointed)"]
        DEDUP -->|Tumbling Window: 1 min| WIN["Windowed Aggregations\n(Grouped by City)"]
        WIN -->|Watermark Passed Window End| AGG_SINK["Delta Sink: orders_agg\n(Append Mode, Checkpointed)"]
    end

    subgraph Storage_Layer["Local Lakehouse Storage (Mimicking S3 / ADLS)"]
        DLQ --> FS_DLQ["./data/dead_letter/"]
        CLEAN_SINK --> FS_CLEAN["./data/delta/orders_clean/"]
        AGG_SINK --> FS_AGG["./data/delta/orders_agg/"]
        S -.->|Offset & State Recovery| FS_CHK["./data/checkpoints/"]
    end

    subgraph Analytics_Consumption["Consumption & Monitoring"]
        FS_CLEAN -.-> DUCK["DuckDB Analytics Engine\n(delta_scan)"]
        FS_AGG -.-> DUCK
        FS_DLQ -.-> DUCK
        DUCK --> QRY["scripts/query_output.py"]
        DUCK --> CNT["scripts/check_counts.py"]
        DUCK --> MON["scripts/monitor.py"]
    end

    style Data_Generation fill:#E3F2FD,stroke:#1E88E5,color:#0D47A1
    style Messaging_Backbone fill:#FFF3E0,stroke:#FB8C00,color:#E65100
    style Stream_Engine fill:#F3E5F5,stroke:#8E24AA,color:#4A148C
    style Storage_Layer fill:#E8F5E9,stroke:#43A047,color:#1B5E20
    style Analytics_Consumption fill:#ECEFF1,stroke:#546E7A,color:#263238
```

---

## How It Runs Locally

Designed to run smoothly on standard laptops (e.g., 8 GB RAM, 4 CPU cores) with zero cloud dependencies:

| Container | Image | Purpose | Port / Resources |
| :--- | :--- | :--- | :--- |
| **`orders-kafka`** | `apache/kafka:3.7.1` | KRaft broker (no ZooKeeper), auto-creates topic `orders` with 3 partitions. Dual listeners. | `localhost:9092` (host), `kafka:29092` (internal) (~350 MB RAM) |
| **`orders-kafka-ui`**| `provectuslabs/kafka-ui:v0.7.2` | Web dashboard for inspecting topics, partitions, and live payloads. | [http://localhost:8081](http://localhost:8081) (~300 MB RAM) |
| **`orders-producer`**| `dataengineerproject2-producer` (Python 3.11-slim) | Generates UPI orders keyed by city with injected faults (~5% late, ~3% dup, ~1% malformed). | Internal (~20 MB RAM) |
| **`orders-spark`**   | `dataengineerproject2-spark` (Spark 3.5.1) | PySpark Structured Streaming engine in `local[2]` mode with Delta Lake 3.2.0. | Internal (~1.5 GB RAM) |

### Local Storage Layout (Standing in for S3 / ADLS)
All storage is mounted under `./data` on the host machine:
- `./data/delta/orders_clean/`: Bronze/Silver layer storing deduplicated, parsed valid orders in Delta format.
- `./data/delta/orders_agg/`: Gold layer storing 1-minute tumbling window metrics (revenue, order counts by status per city) in Delta format.
- `./data/dead_letter/`: Quarantined malformed or schema-violating payloads with diagnostic error reasons.
- `./data/checkpoints/`: Write-Ahead Log (WAL) and state stores for each streaming sink enabling fault-tolerant restart.

---

