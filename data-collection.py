import os
import requests
from datetime import datetime, timedelta
from cassandra.cluster import Cluster

# Set up connection to Cassandra
cluster = Cluster(['127.0.0.1'])
session = cluster.connect()

# Set up Alpha Vantage API parameters
api_key = os.environ.get('ALPHA_VANTAGE_API_KEY')
symbol = 'AAPL'
interval = '5min'

# Set up Cassandra keyspace and table
session.execute("""
    CREATE KEYSPACE IF NOT EXISTS stock_data 
    WITH replication = {'class':'SimpleStrategy', 'replication_factor':1};
""")
session.execute("""
    CREATE TABLE IF NOT EXISTS stock_data.time_series (
        symbol text,
        timestamp timestamp,
        open double,
        high double,
        low double,
        close double,
        volume int,
        PRIMARY KEY (symbol, timestamp)
    );
""")

# Define function to collect and store stock data


def collect_stock_data():
    # Get current date and time
    now = datetime.now()

    # Calculate start and end times for API request
    end_time = now - timedelta(minutes=5)
    start_time = end_time - timedelta(days=1)

    # Format times for API request
    start_str = start_time.strftime('%Y-%m-%d %H:%M:%S')
    end_str = end_time.strftime('%Y-%m-%d %H:%M:%S')

    # Make API request for stock data
    url = f'https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY&symbol={symbol}&interval={interval}&apikey={api_key}&outputsize=compact&datatype=json&from={start_str}&to={end_str}'
    response = requests.get(url)

    # Parse API response and store data in Cassandra
    data = response.json()['Time Series (5min)']
    for timestamp, values in data.items():
        open_price = float(values['1. open'])
        high_price = float(values['2. high'])
        low_price = float(values['3. low'])
        close_price = float(values['4. close'])
        volume = int(values['5. volume'])
        session.execute(f"""
            INSERT INTO stock_data.time_series (symbol, timestamp, open, high, low, close, volume)
            VALUES ('{symbol}', '{timestamp}', {open_price}, {high_price}, {low_price}, {close_price}, {volume});
        """)

    print(
        f"Stock data collected and stored for {start_time.strftime('%Y-%m-%d %H:%M:%S')} to {end_time.strftime('%Y-%m-%d %H:%M:%S')}")


# Call function to collect and store stock data
collect_stock_data()
