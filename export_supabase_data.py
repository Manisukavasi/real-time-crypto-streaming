import psycopg2
import pandas as pd

# Supabase DB config (fill these in)
DB_CONFIG = {
    'host': 'aws-0-us-east-2.pooler.supabase.com',
    'port': '6543',
    'dbname': 'postgres',
    'user': 'postgres.livzreipoiisqxmfwfwt',
    'password': 'Dataproject#@1'
}

try:
    # Connect and fetch data
    conn = psycopg2.connect(**DB_CONFIG)
    query = "SELECT * FROM btc_usd_prices ORDER BY timestamp ASC;"
    df = pd.read_sql(query, conn)

    # Save to CSV
    df.to_csv("btc_usd_prices_hourly.csv", index=False)
    print("✅ Exported to btc_usd_prices_hourly.csv")
    conn.close()

except Exception as e:
    print("❌ Export failed:", e)
