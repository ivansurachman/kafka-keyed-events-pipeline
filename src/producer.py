"""Producer: kirim 1 event / 5 detik, key = user, partition ditentukan oleh key."""
import json
import random
import time
from datetime import datetime, timezone

from confluent_kafka import KafkaException, Producer
from faker import Faker

from src import config

log = config.get_logger("producer")
fake = Faker()
ACTIONS = ["view", "click", "add_to_cart", "purchase"]


def build_event(user_key: str) -> dict:
    return {
        "event_id": fake.uuid4(),
        "user": user_key,
        "name": fake.name(),
        "action": random.choice(ACTIONS),
        "amount": round(random.uniform(1, 500), 2),
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


def on_delivery(err, msg) -> None:
    if err is not None:
        log.error("delivery gagal: %s", err)
        return
    log.info(
        "key=%s -> partition=%d offset=%d",
        msg.key().decode(), msg.partition(), msg.offset(),
    )


def run() -> None:
    producer = Producer({
        "bootstrap.servers": config.BOOTSTRAP_SERVERS,
        "broker.address.family": "v4",
        # murmur2 = algoritma partitioner default Java client. Default librdkafka = CRC32.
        "partitioner": "murmur2_random",
        "acks": "all",
        "enable.idempotence": True,
    })
    log.info("producer start -> topic=%s", config.TOPIC)
    try:
        while True:
            key = random.choice(config.USER_KEYS)
            try:
                producer.produce(
                    config.TOPIC,
                    key=key,
                    value=json.dumps(build_event(key)),
                    on_delivery=on_delivery,
                )
            except (BufferError, KafkaException) as exc:
                log.error("produce gagal: %s", exc)
            producer.flush(5)  # tunggu ack + jalankan callback
            time.sleep(config.PRODUCE_INTERVAL_SEC)
    except KeyboardInterrupt:
        log.info("stop diminta")
    finally:
        producer.flush(10)


if __name__ == "__main__":
    run()
