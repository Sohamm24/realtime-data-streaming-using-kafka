# ==============================================================================
# Real-Time Orders Streaming Lakehouse Makefile
# ==============================================================================

.PHONY: help up down restart ps logs build clean

help:
	@echo "Available commands:"
	@echo "  make up           - Start Kafka, Spark, and producer containers"
	@echo "  make down         - Stop and remove all containers"
	@echo "  make ps           - View running services"
	@echo "  make logs         - Stream container logs"
