"""Bukti pembagian data: query sink SQLite. Jalankan: python -m src.stats"""
import sqlite3

from src import config

QUERIES = {
    "Event per consumer per partition": """
        SELECT consumer, kafka_partition, COUNT(*) AS n
        FROM events GROUP BY consumer, kafka_partition ORDER BY consumer, kafka_partition""",
    "Key -> partition (harus 1 key = 1 partition)": """
        SELECT user_key, kafka_partition, COUNT(*) AS n
        FROM events GROUP BY user_key, kafka_partition ORDER BY user_key""",
    "Agregasi per user": """
        SELECT user_key, COUNT(*) AS n, ROUND(SUM(amount), 2) AS total_amount
        FROM events GROUP BY user_key ORDER BY user_key""",
}

if __name__ == "__main__":
    with sqlite3.connect(config.DB_PATH) as conn:
        for title, sql in QUERIES.items():
            print(f"\n== {title}")
            for row in conn.execute(sql):
                print("  ", row)
