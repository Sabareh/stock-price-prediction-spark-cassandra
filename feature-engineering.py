import pandas as pd
import nltk
from textblob import TextBlob
from cassandra.cluster import Cluster

# Connect to Cassandra
cluster = Cluster(['127.0.0.1'])
session = cluster.connect()

# Get preprocessed data from Cassandra
rows = session.execute('SELECT * FROM preprocessed_data')
df = pd.DataFrame(rows)

# Calculate technical indicators
df['SMA_10'] = df['close'].rolling(window=10).mean()
df['SMA_30'] = df['close'].rolling(window=30).mean()
df['EMA_10'] = df['close'].ewm(span=10, adjust=False).mean()
df['EMA_30'] = df['close'].ewm(span=30, adjust=False).mean()
df['RSI'] = talib.RSI(df['close'].values, timeperiod=14)
df['MACD'], _, _ = talib.MACD(
    df['close'].values, fastperiod=12, slowperiod=26, signalperiod=9)

# Add sentiment analysis
nltk.download('vader_lexicon')
df['sentiment_score'] = df['headline'].apply(
    lambda x: TextBlob(x).sentiment.polarity)

# Add macroeconomic indicators
rows = session.execute('SELECT * FROM macro_data')
macro_df = pd.DataFrame(rows)
macro_df = macro_df[['timestamp', 'unemployment_rate',
                     'inflation_rate', 'gdp_growth']]
df = pd.merge(df, macro_df, on='timestamp', how='left')

# Store the feature engineered data in Cassandra
for index, row in df.iterrows():
    session.execute(
        """
        INSERT INTO feature_data (timestamp, symbol, open, high, low, close, volume, sma_10, sma_30, ema_10, ema_30, rsi, macd, sentiment_score, unemployment_rate, inflation_rate, gdp_growth)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """,
        (row['timestamp'], row['symbol'], row['open'], row['high'], row['low'], row['close'], row['volume'], row['SMA_10'], row['SMA_30'], row['EMA_10'],
         row['EMA_30'], row['RSI'], row['MACD'], row['sentiment_score'], row['unemployment_rate'], row['inflation_rate'], row['gdp_growth'])
    )
