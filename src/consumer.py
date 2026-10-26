"""Consumer: baca events_topic, agregasi per user, simpan ke SQLite.

Jalankan: python -m src.consumer --name consumer-1
"""
import argparse
import json
import sqlite3
from collections import Counter

from confluent_kafka import Consumer, KafkaError, KafkaException

from src import config
from src.storage import EventStore

log = config.get_logger("consumer")


def on_assign(_consumer, partitions) -> None:
    log.info("ASSIGNED partitions=%s", sorted(p.partition for p in partitions))


def on_revoke(_consumer, partitions) -> None:
    log.info("REVOKED  partitions=%s", sorted(p.partition for p in partitions))


def handle_message(msg, store: EventStore, counts: Counter, name: str) -> None:
    user_key = msg.key().decode()
    event = json.loads(msg.value())
    store.save(msg.partition(), msg.offset(), user_key, event, name)
    counts[user_key] += 1
    log.info(
        "partition=%d offset=%d key=%s action=%s | total %s=%d",
        msg.partition(), msg.offset(), user_key, event.get("action"),
        user_key, counts[user_key],
    )


def run(name: str) -> None:
    consumer = Consumer({
        "bootstrap.servers": config.BOOTSTRAP_SERVERS,
        "broker.address.family": "v4",
        "group.id": config.GROUP_ID,
        "client.id": name,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,  # commit manual setelah data aman di sink
    })
    store = EventStore(config.DB_PATH)
    counts: Counter = Counter()
    consumer.subscribe([config.TOPIC], on_assign=on_assign, on_revoke=on_revoke)
    log.info("%s start | group=%s", name, config.GROUP_ID)

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                if msg.error().fatal():
                    raise KafkaException(msg.error())
                log.error("consumer error: %s", msg.error())
                continue
            try:
                handle_message(msg, store, counts, name)
            except (json.JSONDecodeError, UnicodeDecodeError, AttributeError) as exc:
                log.warning("payload invalid, skip (offset=%d): %s", msg.offset(), exc)
            except sqlite3.Error as exc:
                log.error("gagal simpan ke sink, TIDAK di-commit: %s", exc)
                continue
            consumer.commit(message=msg, asynchronous=False)
    except KeyboardInterrupt:
        log.info("stop diminta")
    finally:
        consumer.close()  # trigger rebalance bersih ke consumer lain
        store.close()
        log.info("%s ringkasan event per user: %s", name, dict(counts))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--name", default="consumer-1", help="nama consumer (client.id)")
    run(parser.parse_args().name)
