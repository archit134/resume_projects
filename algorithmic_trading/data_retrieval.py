import pandas as pd
import psycopg2
from decouple import config
from alpaca.data.historical import StockHistoricalDataClient
from alpaca.data.requests import StockBarsRequest
from alpaca.data.timeframe import TimeFrame
from datetime import datetime, timedelta

# Alpaca API credentials
API_KEY = config("ALPACA_KEY")
SECRET_KEY = config("ALPACA_SECRET")

# Initialize Alpaca client
client = StockHistoricalDataClient(API_KEY, SECRET_KEY)

# Calculate start and end dates for older data
end_date = datetime.now() - timedelta(days=365)  # Adjust the date to be a year ago
start_date = end_date - timedelta(days=100)

# Format dates as strings
start = start_date.strftime('%Y-%m-%d')
end = end_date.strftime('%Y-%m-%d')

# Print start and end dates for verification
print(f"Fetching data from {start} to {end}")

# Fetch minute-level data from Alpaca
def fetch_alpaca_data(symbols, start, end):
    try:
        request_params = StockBarsRequest(
            symbol_or_symbols=symbols,
            timeframe=TimeFrame.Minute,
            start=start,
            end=end
        )

        bars = client.get_stock_bars(request_params).df
        data = {}
        for symbol in symbols:
            df = bars.xs(symbol, level='symbol').reset_index()
            df.rename(columns={'timestamp': 'timestamp', 'open': 'open', 'high': 'high', 'low': 'low', 'close': 'close', 'volume': 'volume'}, inplace=True)
            df['symbol'] = symbol
            data[symbol] = df
        return data
    except Exception as e:
        print(f"Error fetching data from Alpaca: {e}")
        return None

# Connect to TimescaleDB
conn = psycopg2.connect(
    dbname='algo_trading',
    user='Archit',
    password='Archit@1',
    host='localhost',
    port='5432'
)
cursor = conn.cursor()

# Create table if it doesn't exist
cursor.execute("""
    CREATE TABLE IF NOT EXISTS alpaca_minute_data (
        timestamp TIMESTAMPTZ,
        symbol TEXT,
        open FLOAT,
        high FLOAT,
        low FLOAT,
        close FLOAT,
        volume BIGINT,
        PRIMARY KEY (timestamp, symbol)
    );
    SELECT create_hypertable('alpaca_minute_data', 'timestamp', if_not_exists => TRUE);
""")
conn.commit()

# Insert minute-level data into TimescaleDB
def insert_data_to_db(data):
    if data:
        for symbol, df in data.items():
            for index, row in df.iterrows():
                cursor.execute("""
                    INSERT INTO alpaca_minute_data (timestamp, symbol, open, high, low, close, volume)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (timestamp, symbol) DO NOTHING;
                """, (row['timestamp'], row['symbol'], row['open'], row['high'], row['low'], row['close'], row['volume']))
        conn.commit()

# Fetch and insert data
symbols = ['MCD', 'PEP', 'KO']
data = fetch_alpaca_data(symbols, start, end)
insert_data_to_db(data)

# Close connection
cursor.close()
conn.close()
