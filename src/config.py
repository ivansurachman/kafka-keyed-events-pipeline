"""Konfigurasi bersama (dari .env) + setup logging."""
import logging
import os

from dotenv import load_dotenv

load_dotenv()

BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS", "localhost:29092,localhost:29093,localhost:29094"
)
TOPIC = os.getenv("KAFKA_TOPIC", "events_topic")
GROUP_ID = os.getenv("KAFKA_GROUP_ID", "events_group")
PRODUCE_INTERVAL_SEC = int(os.getenv("PRODUCE_INTERVAL_SEC", "5"))
DB_PATH = os.getenv("DB_PATH", "output/events.db")
# Hardcode: set key menentukan pemetaan partition di report (lihat report.md)
USER_KEYS = [f"user_{i}" for i in range(1, 6)]


def get_logger(name: str) -> logging.Logger:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)-7s | %(name)s | %(message)s",
    )
    return logging.getLogger(name)
