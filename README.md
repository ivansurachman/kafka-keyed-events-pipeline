# Kafka Keyed Events Pipeline

Pipeline Kafka end-to-end: producer Faker (key-based partitioning) → consumer group → sink SQLite.
Cluster KRaft 3 broker di Docker, dipantau lewat Kafbat UI.

```
Producer (1 event/5 detik, key = user_N)
   └─► events_topic (3 partition, RF 3)
          └─► Consumer group events_group (≥ 2 consumer)
                 └─► SQLite output/events.db
```

Hasil observasi (penentuan partition, replication, consumer group) ada di [report.md](report.md).

## Prasyarat

- Docker + Docker Compose (disarankan RAM untuk Docker ±3 GB, karena ada 3 broker)
- Python 3.x

## Struktur

```
docker-compose.yml   # 3 broker KRaft, kafka-init (buat topic), Kafbat UI
requirements.txt
.env.example         # template konfigurasi (di-commit)
.env                 # salinan lokal (git-ignored)
src/
  config.py          # konfigurasi dari .env + logging
  producer.py        # producer: key-based partitioning (murmur2_random)
  consumer.py        # consumer: agregasi per user + commit manual
  storage.py         # sink SQLite (PK = partition + offset, idempotent)
  stats.py           # query bukti distribusi data
output/
  events.db          # data sink (dibuat saat consumer jalan; git-ignored, ada di ZIP)
evidence/            # log eksperimen + stats.txt
screenshot/          # screenshot Kafbat UI
report.md            # laporan observasi
```

## Menjalankan

```bash
python -m venv .venv
source .venv/bin/activate        # Windows: .venv\Scripts\activate
pip install -r requirements.txt
cp .env.example .env             # Windows: copy .env.example .env

docker compose up -d
docker logs kafka-init           # pastikan events_topic terbuat (3 partition, RF 3)
```

Kafbat UI: http://localhost:8080

Jalankan tiap perintah di terminal terpisah:

```bash
python -m src.consumer --name consumer-1
python -m src.consumer --name consumer-2
python -m src.producer
```

Hentikan dengan `Ctrl+C`. Lihat hasil agregasi dari sink:

```bash
python -m src.stats
```

## Mengulang eksperimen (dengan log bukti)

Reset dulu supaya log dan `stats.py` konsisten:

```bash
docker compose down -v
rm -f output/events.db*          # PowerShell: Remove-Item output/events.db* -ErrorAction SilentlyContinue
mkdir -p evidence                # PowerShell: New-Item -ItemType Directory -Force evidence
docker compose up -d
```

Simpan log tiap proses ke `evidence/`:

| Shell | Perintah |
|---|---|
| PowerShell 7 | `python -m src.consumer --name consumer-1 2>&1 \| Tee-Object evidence/consumer-1.log` |
| bash | `python -m src.consumer --name consumer-1 2>&1 \| tee -i evidence/consumer-1.log` |

(Ganti nama untuk `consumer-2` dan `python -m src.producer` → `producer.log`. `tee -i` mencegah `tee` ikut mati saat `Ctrl+C`.)

Urutan:

1. Jalankan `consumer-1`, tunggu `ASSIGNED [0, 1, 2]`.
2. Jalankan `consumer-2`. Amati `REVOKED`/`ASSIGNED` di kedua terminal (rebalance). Screenshot Kafbat UI: Consumers → `events_group`.
3. Jalankan producer, biarkan beberapa menit. Screenshot pembagian partition dan lag.
4. `Ctrl+C` `consumer-2` (producer tetap jalan). Amati `consumer-1` mengambil alih partition 2.
5. Tunggu ±1–2 menit, lalu `Ctrl+C` **producer**.
6. Tunggu ±10 detik, lalu `Ctrl+C` `consumer-1`.
7. `python -m src.stats > evidence/stats.txt`
8. Screenshot Kafbat UI: Topics → `events_topic` → Overview (partition, replica, ISR).

Producer dihentikan sebelum `consumer-1` supaya semua event sudah terbaca dan jumlah di sink sama dengan jumlah yang dikirim.

## Konfigurasi

Semua lewat `.env` (lihat `.env.example`): alamat broker, nama topic, group id, interval producer, path database, serta jumlah partition dan replication factor untuk `kafka-init`. Set key (`user_1` … `user_5`) di `src/config.py` sengaja tidak lewat `.env`, karena menentukan pemetaan partition di report.

## Troubleshooting

- **Log penuh `Connect to ipv6#[::1] ... failed` (Windows):** `localhost` ter-resolve ke IPv6, sementara port Docker hanya IPv4. Tambahkan `"broker.address.family": "v4"` di konfigurasi `Producer` dan `Consumer`.
- **Topic berubah setelah edit `.env`:** `kafka-init` memakai `--if-not-exists`, jadi topic lama tidak diubah. Jalankan `docker compose down -v` lalu `up -d`.
- **`GETPID ... Coordinator load in progress` di awal producer:** sementara, cluster baru menyala; producer retry otomatis.