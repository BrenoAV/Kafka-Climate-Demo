import json
from kafka import KafkaProducer


def publish_message(
    producer_instance: KafkaProducer, topic_name: str, key: str, value: dict
):
    try:
        key_bytes = bytes(key, encoding="utf-8")
        value_bytes = json.dumps(value, indent=2).encode("utf-8")
        producer_instance.send(topic=topic_name, key=key_bytes, value=value_bytes)
        producer_instance.flush()
        print(f"Message published successfully on the {topic_name}")
    except Exception as exc:
        print(f"Exception in publishing the message: {exc}")


def connect_kafka_producer():
    _producer = None
    try:
        _producer = KafkaProducer(
            bootstrap_servers=["localhost:9092"], api_version=(0, 10)
        )
    except Exception as exc:
        print(f"Exception while connecting Kafka: {exc}")
    finally:
        return _producer
