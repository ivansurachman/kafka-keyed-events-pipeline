"""Sink SQLite. PK (partition, offset) -> idempotent kalau event diproses ulang."""
import sqlite3
from pathlib import Path

SCHEMA = """
CREATE TABLE IF NOT EXISTS events (
    kafka_partition INTEGER NOT NULL,
    kafka_offset    INTEGER NOT NULL,
    user_key        TEXT    NOT NULL,
    action          TEXT,
    amount          REAL,
    event_ts        TEXT,
    consumer        TEXT    NOT NULL,
    processed_at    TEXT    DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (kafka_partition, kafka_offset)
)
"""


class EventStore:
    def __init__(self, db_path: str):
        Path(db_path).parent.mkdir(parents=True, exist_ok=True)
        # timeout besar: beberapa consumer menulis ke file yang sama
        self._conn = sqlite3.connect(db_path, timeout=30)
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute(SCHEMA)
        self._conn.commit()

    def save(self, partition: int, offset: int, user_key: str,
             event: dict, consumer: str) -> None:
        with self._conn:  # auto commit / rollback
            self._conn.execute(
                "INSERT OR IGNORE INTO events (kafka_partition, kafka_offset, user_key,"
                " action, amount, event_ts, consumer) VALUES (?, ?, ?, ?, ?, ?, ?)",
                (partition, offset, user_key, event.get("action"),
                 event.get("amount"), event.get("timestamp"), consumer),
            )

    def close(self) -> None:
        self._conn.close()
