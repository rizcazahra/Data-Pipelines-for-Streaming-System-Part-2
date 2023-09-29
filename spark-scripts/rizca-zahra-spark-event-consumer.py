import pyspark
import os
from dotenv import load_dotenv
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, window, sum
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, TimestampType

dotenv_path = Path("/opt/app/.env")
load_dotenv(dotenv_path=dotenv_path)

spark_hostname = os.getenv("SPARK_MASTER_HOST_NAME")
spark_port = os.getenv("SPARK_MASTER_PORT")
kafka_host = os.getenv("KAFKA_HOST")
kafka_topic = os.getenv("KAFKA_TOPIC_NAME")

spark_host = f"spark://{spark_hostname}:{spark_port}"

os.environ[
    "PYSPARK_SUBMIT_ARGS"
] = "--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.2"

spark = SparkSession.builder \
    .appName("DibimbingStreaming") \
    .master(spark_host) \
    .getOrCreate()

spark.conf.set("spark.sql.shuffle.partitions", "2")  # Set the number of shuffle partitions

# Define the Kafka schema to read JSON messages
kafka_schema = StructType([
    StructField("order_id", StringType(), True),
    StructField("customer_id", IntegerType(), True),
    StructField("furniture", StringType(), True),
    StructField("color", StringType(), True),
    StructField("price", FloatType(), True),
    StructField("ts", TimestampType(), True)
])

# Read data from Kafka
stream_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", f"{kafka_host}:9092") \
    .option("subscribe", kafka_topic) \
    .option("startingOffsets", "latest") \
    .load()

# Parse JSON data
parsed_df = stream_df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), kafka_schema).alias("data"))

# Perform aggregation for daily total purchases
windowed_df = parsed_df \
    .withWatermark("ts", "10 minutes") \
    .groupBy(window(col("ts"), "1 day").alias("window")) \
    .agg(sum(col("price")).alias("daily_total"))

# Select the desired columns for output
output_df = windowed_df.select("window.start", "daily_total")

# Write the result to the console
query = output_df.writeStream \
    .format("console") \
    .outputMode("append") \
    .start()

query.awaitTermination()
