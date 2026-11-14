# Distributed Streaming Analytics Platform

**Status:** Locally tested replay-safe event processor with an optional Kafka consumer adapter. Not deployed on Azure or verified against a live Kafka broker.

## Problem / solution
Duplicate and malformed document events can corrupt analytical counts. This implementation validates JSON events, deduplicates event IDs with durable SQLite state, stores malformed events in a dead-letter table, and atomically maintains category counts.

**Architecture:** Kafka (optional) / JSONL -> validation -> transactional deduplication -> aggregates + dead-letter table.

## Quickstart
```bash
python -m scripts.run_demo
python -m pytest -q
python -m scripts.benchmark --events 10000
```

## Optional Kafka integration
`pip install kafka-python`; configure `KAFKA_BOOTSTRAP_SERVERS` and optionally `KAFKA_TOPIC`. Run `python -m integrations.kafka_consumer`. The adapter commits offsets only after a database transaction. A production deployment needs a shared durable state store and partition-aware scaling.

## Engineering considerations
At-least-once event delivery and idempotent aggregates; persistent state across restarts; replay and duplicate tests; dead-letter handling. No end-to-end exactly-once or live Azure Event Hubs claims. Azure Event Hubs Kafka access requires SASL/TLS configuration not included in this local example.

## Interview topics
Consumer groups, partition ordering, offset management, replay and idempotency, backpressure, consumer lag, watermarking, checkpointing, DLQ design, observability and operational SLAs.

## Metrics policy
The benchmark measures local synthetic JSONL/SQLite throughput only. It does not establish Kafka, Spark, Azure, or production performance.