from kafka import KafkaConsumer
import json
import psycopg2

# Supabase PostgreSQL DB credentials
DB_CONFIG = {
    'host': 'aws-0-us-east-2.pooler.supabase.com',
    'port': 6543,
    'database': 'postgres',
    'user': 'postgres.livzreipoiisqxmfwfwt',
    'password': 'Dataproject#@1'
}

# Connect to Supabase PostgreSQL
conn = psycopg2.connect(**DB_CONFIG)
cursor = conn.cursor()

# Create Kafka consumer
consumer = KafkaConsumer(
    'crypto_prices',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='latest',
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

print("[Listening] for BTC-USD price data...")

try:
    for message in consumer:
        data = message.value
        price = data.get("price")

        if price:
            cursor.execute(
                "INSERT INTO btc_usd_prices (price) VALUES (%s)",
                (price,)
            )
            conn.commit()
            print(f"[Inserted] BTC-USD price: {price}")

except KeyboardInterrupt:
    print("\n[Exit] Stopping consumer...")

finally:
    cursor.close()
    conn.close()
