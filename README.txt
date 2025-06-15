# Real-Time Crypto Streaming Pipeline (BTC-USD)

This project is a real-time cryptocurrency data pipeline that streams BTC-USD price data from Coinbase WebSocket, publishes it to Apache Kafka, and stores it in a Supabase PostgreSQL database every 30 seconds. Built using Python, Kafka, Docker, and Supabase.

---

##  Project Flow

1. **Coinbase WebSocket**: Streams BTC-USD ticker data.
2. **Kafka Producer**: Sends ticker data every 30 seconds to a Kafka topic (`crypto_prices`).
3. **Kafka Consumer**: Listens to `crypto_prices` and stores data into Supabase PostgreSQL.
4. **Supabase**: Stores and allows querying of historical crypto prices.

---

##  Technologies Used

- Python 3.10+
- Apache Kafka & Zookeeper (via Docker)
- WebSocket (`websocket-client`)
- Kafka (`kafka-python`)
- Supabase PostgreSQL
- psycopg2
- Docker & Docker Compose
- Git / GitHub

---


##  Download Sample Data
- [BTC-USD 1 Hour Snapshot (CSV)](https://raw.githubusercontent.com/Manisukavasi/real-time-crypto-streaming/refs/heads/main/exports/btc_prices_20250615.csv)

