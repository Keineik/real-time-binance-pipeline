from pyspark.sql import SparkSession
import os
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-10_2.12:3.5.5,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.5 pyspark-shell'

spark =  SparkSession.builder \
    .appName("BTCUSDT Bonus") \
    .config("spark.streaming.stopGracefullyOnShutdown", "true") \
    .config("spark.jars.packages", "org.apache.spark:spark-streaming-kafka-0-10_2.12:3.5.5") \
    .master("spark://localhost:7077") \
    .getOrCreate()

df = spark \
  .readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "localhost:9092") \
  .option("subscribe", "btc-price") \
  .option("startingOffsets", "earliest") \
  .load()

schema = StructType() \
    .add("symbol", StringType()) \
    .add("price", StringType()) \
    .add("event-time", StringType())

parsed_df = df.selectExpr("CAST(value AS STRING) as json_value") \
    .select(from_json(col("json_value"), schema).alias("data")) \
    .select("data.symbol", "data.price", "data.event-time")

query = parsed_df.writeStream \
    .format("console") \
    .option("truncate", "false") \
    .start()

query.awaitTermination()
spark.stop()