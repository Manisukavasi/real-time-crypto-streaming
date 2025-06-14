from kafka import KafkaConsumer
import json
import sqlite3

# Create SQLite database and table (if not exists)
conn = sqlite3.connect("crypto_data.db")
cursor = conn.cursor()
cursor.execute("""
    CREATE TABLE IF NOT EXISTS btc_prices (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        price REAL,
        timestamp TEXT
    )
""")
conn.commit()
conn.close()

# Connect to Kafka topic
consumer = KafkaConsumer(
    'crypto_prices',
    bootstrap_servers='localhost:9092',
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

print("Listening for btc_usd price data...")

# Listen and insert into DB
for message in consumer:
    price_data = message.value
    price = price_data.get('price')
    timestamp = price_data.get('time')

    print(f"[Saved] ${price} at {timestamp}")

    # Insert into SQLite
    conn = sqlite3.connect("crypto_data.db")
    cursor = conn.cursor()
    cursor.execute("INSERT INTO btc_prices (price, timestamp) VALUES (?, ?)", (price, timestamp))
    conn.commit()
    conn.close()
