from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider

# Define the Cassandra keyspace and table
KEYSPACE = "yourkeyspace"
TABLE = "stock_data"

# Set up the Cassandra connection
auth_provider = PlainTextAuthProvider(username="username", password="password")
cluster = Cluster(["localhost"], auth_provider=auth_provider)
session = cluster.connect()

# Create the keyspace if it doesn't exist
session.execute(
    f"CREATE KEYSPACE IF NOT EXISTS {KEYSPACE} WITH REPLICATION = {{ 'class' : 'SimpleStrategy', 'replication_factor' : 1 }}")

# Create the table if it doesn't exist
session.execute(
    f"CREATE TABLE IF NOT EXISTS {KEYSPACE}.{TABLE} (symbol text, timestamp text, open double, high double, low double, close double, volume double, PRIMARY KEY (symbol, timestamp))")

# Close the Cassandra connection
cluster.shutdown()
