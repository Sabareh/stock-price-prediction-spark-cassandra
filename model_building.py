from pyspark.sql.functions import col
from pyspark.ml.regression import LinearRegression
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.tuning import ParamGridBuilder, CrossValidator
from pyspark.ml.pipeline import Pipeline

# Load preprocessed data from Cassandra
preprocessed_data = spark.read.format("org.apache.spark.sql.cassandra").options(
    table="preprocessed_data", keyspace="stock_data"
).load()

# Select relevant columns for model training
model_data = preprocessed_data.select(
    col("date"),
    col("symbol"),
    col("label"),
    col("norm_open"),
    col("norm_high"),
    col("norm_low"),
    col("norm_close"),
    col("norm_volume"),
    col("norm_sma"),
    col("norm_rsi"),
    col("norm_macd"),
    col("norm_sentiment"),
    col("norm_unemployment_rate"),
    col("norm_inflation_rate"),
)

# Assemble features into a single vector column
feature_cols = [
    "norm_open",
    "norm_high",
    "norm_low",
    "norm_close",
    "norm_volume",
    "norm_sma",
    "norm_rsi",
    "norm_macd",
    "norm_sentiment",
    "norm_unemployment_rate",
    "norm_inflation_rate",
]
assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")
model_data = assembler.transform(model_data)

# Split data into training and testing sets
train_data, test_data = model_data.randomSplit([0.8, 0.2], seed=42)

# Define machine learning algorithm
lr = LinearRegression(featuresCol="features", labelCol="label")

# Define hyperparameter grid for cross-validation
param_grid = ParamGridBuilder() \
    .addGrid(lr.maxIter, [10, 20]) \
    .addGrid(lr.regParam, [0.1, 0.01]) \
    .addGrid(lr.elasticNetParam, [0.0, 0.5, 1.0]) \
    .build()

# Define cross-validation object
evaluator = RegressionEvaluator(predictionCol="prediction", labelCol="label")
cv = CrossValidator(estimator=lr, estimatorParamMaps=param_grid,
                    evaluator=evaluator, numFolds=5)

# Train and evaluate model using cross-validation
cv_model = cv.fit(train_data)
predictions = cv_model.transform(test_data)
rmse = evaluator.evaluate(predictions, {evaluator.metricName: "rmse"})
r2 = evaluator.evaluate(predictions, {evaluator.metricName: "r2"})

# Print evaluation metrics
print(f"Root Mean Squared Error: {rmse}")
print(f"R-squared: {r2}")

# Save model to Cassandra
cv_model.bestModel.write().format("org.apache.spark.sql.cassandra").options(
    table="stock_model", keyspace="stock_data"
).save()
