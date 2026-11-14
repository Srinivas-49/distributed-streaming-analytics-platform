"""Optional Kafka adapter. Requires kafka-python and an independently configured broker."""
import json
import os
from kafka import KafkaConsumer
from src.streaming import EventProcessor

def main():
    bootstrap = os.environ["KAFKA_BOOTSTRAP_SERVERS"]
    topic = os.environ.get("KAFKA_TOPIC", "documents")
    consumer = KafkaConsumer(
        topic, bootstrap_servers=bootstrap.split(","), enable_auto_commit=False,
        auto_offset_reset="earliest",
        value_deserializer=lambda b: json.loads(b.decode("utf-8"))
    )
    processor = EventProcessor(os.environ.get("STATE_DB", "stream.db"))
    try:
        for record in consumer:
            status = processor.process(record.value)
            if status in ("processed", "duplicate", "dead_letter"):
                consumer.commit()  # commit local state before broker offset
    finally:
        processor.close()
        consumer.close()

if __name__ == "__main__":
    main()
