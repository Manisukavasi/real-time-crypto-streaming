import time
from kafka import KafkaConsumer
import psycopg2
import json
from datetime import datetime

# Database config and connection (replace with actual details)
DB_CONFIG = {
    'host': 'aws-0-us-east-2.pooler.supabase.com',
    'port': '6543',
    'dbname': 'postgres',
    'user': 'postgres.livzreipoiisqxmfwfwt',
    'password': 'Dataproject#@1',
}
conn = psycopg2.connect(**DB_CONFIG)
cur = conn.cursor()

# Kafka consumer
consumer = KafkaConsumer(
    'crypto_prices',
    bootstrap_servers='localhost:9092',
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

print("Listening for BTC-USD price data...")

last_saved_time = 0  # Epoch time in seconds

for message in consumer:
    current_time = time.time()
    if current_time - last_saved_time >= 30:
        data = message.value
        price = float(data['price'])
        timestamp = datetime.utcnow()

        cur.execute(
            "INSERT INTO btc_usd_prices (price, timestamp) VALUES (%s, %s)",
            (price, timestamp)
        )
        conn.commit()

        print(f"Saved price: {price} at {timestamp}")
        last_saved_time = current_time
