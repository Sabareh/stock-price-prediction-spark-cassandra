import pandas as pd
from cassandra.cluster import Cluster

# Connect to Cassandra
cluster = Cluster(['127.0.0.1'])
session = cluster.connect()

# Get stock data from Cassandra
rows = session.execute('SELECT * FROM stock_data')
df = pd.DataFrame(rows)

# Clean the data
df = df.drop_duplicates()
df = df.dropna()

# Handle missing values
df = df.fillna(method='ffill')
df = df.fillna(method='bfill')

# Normalize the data
df = (df - df.mean()) / df.std()

# Store the preprocessed data in Cassandra
for index, row in df.iterrows():
    session.execute(
        """
        INSERT INTO preprocessed_data (timestamp, symbol, open, high, low, close, volume)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        """,
        (row['timestamp'], row['symbol'], row['open'],
         row['high'], row['low'], row['close'], row['volume'])
    )
