import os
import json
from kafka import KafkaConsumer

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
TOPIC = os.getenv("PRODUCTS_TOPIC", "products.created")
GROUP_ID = "print-consumer"

def main():
    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id=GROUP_ID,
        auto_offset_reset="latest",  # only new messages from now on
        value_deserializer=lambda b: json.loads(b.decode("utf-8")),
        key_deserializer=lambda b: b.decode("utf-8") if b else None,
    )

    print(f"Listening on topic: {TOPIC}")
    for msg in consumer:
        print("got event, we could mail people on each purchase :) ", msg.value)

if __name__ == "__main__":
    main()