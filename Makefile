.PHONY: up down clean test produce stream query counts monitor status help

help:
	@echo "Available commands:"
	@echo "  make up       - Start Kafka (KRaft), Kafka UI, and topic initialization"
	@echo "  make produce  - Build and start simulated UPI/orders event producer"
	@echo "  make stream   - Build and start PySpark Structured Streaming job"
	@echo "  make query    - Run DuckDB inspection on Delta tables & Dead-Letter store"
	@echo "  make counts   - Run audit and reconciliation check (clean vs dedup vs DLQ)"
	@echo "  make monitor  - View real-time terminal operational health dashboard"
	@echo "  make test     - Run full pytest test suite (generator & PySpark transforms)"
	@echo "  make status   - Show running Docker containers and health"
	@echo "  make down     - Stop all containers gracefully"
	@echo "  make clean    - Stop containers and purge local data/checkpoint directories"

up:
	docker compose up -d kafka kafka-ui init-kafka

produce:
	docker compose up -d --build producer

stream:
	docker compose up -d --build spark

query:
	docker exec -it orders-spark python3 /app/scripts/query_output.py

counts:
	docker exec -it orders-spark python3 /app/scripts/check_counts.py

monitor:
	docker exec -it orders-spark python3 /app/scripts/monitor.py

test:
	docker exec orders-spark python3 -m pytest /app/tests -v

status:
	docker compose ps

down:
	docker compose down

clean:
	docker compose down -v
	rm -rf data/checkpoints data/delta data/dead_letter
