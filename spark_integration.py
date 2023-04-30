# This script performs data analysis and predictions using Spark and Cassandra. 
# It integrates the previously developed scripts and provides an end-to-end pipeline for stock price prediction. 

from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegressionModel
from pyspark.sql.functions import col
from pyspark.sql.types import *

# Define the schema for the stock data
schema = StructType([
    StructField("symbol", StringType()),
    StructField("timestamp", StringType()),
    StructField("open", DoubleType()),
    StructField("high", DoubleType()),
    StructField("low", DoubleType()),
    StructField("close", DoubleType()),
    StructField("volume", DoubleType())
])

# Connect to Cassandra and read in the stock data
spark = SparkSession.builder.appName("StockPrediction").config(
    "spark.cassandra.connection.host", "localhost").getOrCreate()
df = spark.read.format("org.apache.spark.sql.cassandra").options(
    table="stock_data", keyspace="mykeyspace").schema(schema).load()

# Perform data preprocessing and feature engineering
df = df.drop("symbol")
df = df.withColumn("timestamp", col("timestamp").cast("timestamp"))
# perform other preprocessing steps
# perform feature engineering

# Split the data into training and testing sets
(trainingData, testData) = df.randomSplit([0.7, 0.3])

# Train the model
assembler = VectorAssembler(
    inputCols=["open", "high", "low", "close", "volume"], outputCol="features")
trainingData = assembler.transform(trainingData)
lrModel = LinearRegressionModel.load("lr_model")
predictions = lrModel.transform(testData)

# Evaluate the model
evaluator = RegressionEvaluator(
    labelCol="close", predictionCol="prediction", metricName="rmse")
rmse = evaluator.evaluate(predictions)
print("Root Mean Squared Error (RMSE) on test data = %g" % rmse)

# Save the predictions to Cassandra
predictions.write.format("org.apache.spark.sql.cassandra").mode(
    "append").options(table="predictions", keyspace="mykeyspace").save()
