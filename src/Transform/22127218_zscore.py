from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, col, avg, expr,to_json, struct, date_trunc, explode)
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType, ArrayType

KAFKA_BROKER = "kafka:9092"

def main():
    # Initialize Spark
    spark = SparkSession.builder\
                        .appName("BTC Price Z-score")\
                        .config("spark.streaming.stopGracefullyOnShutdown", "true")\
                        .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true")\
                        .config("spark.cleaner.referenceTracking.cleanCheckpoints", "true")\
                        .config("spark.cores.max", "3")\
                        .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    
    # Define schema for incoming data
    btc_price_schema = StructType([
        StructField("symbol", StringType(), False),
        StructField("price", StringType(), False),
        StructField("timestamp", TimestampType(), False)
    ])
    
    moving_stats_schema = StructType([
        StructField("timestamp", TimestampType(), False),
        StructField("symbol", StringType(), False),
        StructField("stats", ArrayType(StructType([
            StructField("window", StringType(), False),
            StructField("avg_price", DoubleType(), False),
            StructField("std_price", DoubleType(), False)
        ])))
    ])
    
    # Read btc-price topic
    raw_price = spark.readStream\
                     .format("kafka")\
                     .option("kafka.bootstrap.servers", KAFKA_BROKER)\
                     .option("subscribe", "btc-price")\
                     .option("startingOffsets", "latest")\
                     .load()
    
    price_stream = raw_price\
            .selectExpr("CAST(value AS STRING) as json_value")\
            .select(from_json("json_value", btc_price_schema).alias("data"))\
            .select(
                col("data.symbol"),
                col("data.price").cast("double").alias("price"),
                col("data.timestamp")
            )\
            .withColumn("timestamp", date_trunc("second", col("timestamp")))  # Normalize timestamp
    
    # Group by symbol and the rounded timestamp, then compute average price
    # This ensures each (symbol, timestamp) pair has only one record
    price_stream = price_stream \
        .withWatermark("timestamp", "10 seconds") \
        .groupBy("symbol", "timestamp") \
        .agg(avg("price").alias("price"))
                
    # Read btc-price-moving topic
    raw_moving = spark.readStream\
        .format("kafka")\
        .option("kafka.bootstrap.servers", KAFKA_BROKER)\
        .option("subscribe", "btc-price-moving")\
        .option("startingOffsets", "latest")\
        .load()

    moving_stream = raw_moving\
        .selectExpr("CAST(value AS STRING) as json_value")\
        .select(from_json("json_value", moving_stats_schema).alias("data"))\
        .select(
            col("data.timestamp"),
            col("data.symbol"),
            explode(col("data.stats")).alias("stats") 
        )\
        .select(
            col("timestamp"),
            col("symbol"),
            col("stats.window").alias("window"),
            col("stats.avg_price").alias("avg_price"),
            col("stats.std_price").alias("std_price")
        )\
        .withWatermark("timestamp", "10 seconds")
    
     # Join on timestamp and symbol
    joined_stream = price_stream.join(
        moving_stream,
        on = ["timestamp", "symbol"]
    )
    
    # Compute z-score: (price - avg)/ std (handle std = 0)
    enriched = joined_stream.withColumn(
        "zscore_price",
        expr("CASE when std_price = 0 OR std_price IS NULL THEN 0 ELSE (price - avg_price)/ std_price END")
    )
    
    # Group by timestamp and symbol, collect list of zscores per window
    grouped_result = enriched\
            .select(
                col("timestamp"),
                col("symbol"),
                struct(
                    col("window"),
                    col("zscore_price"),
                ).alias("zscore_struct")
            )\
            .groupBy("timestamp", "symbol")\
            .agg(expr("collect_list(zscore_struct) as zscores"))
            
    
    # Convert to final output format
    output = grouped_result.select(to_json(struct("*")).alias("value"))
    
    # Write to Kafka topic btc-price-zscore
    query = output.writeStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BROKER) \
        .option("topic", "btc-price-zscore") \
        .option("checkpointLocation", "/tmp/checkpoints/btc-price-zscore") \
        .outputMode("append") \
        .start()

    query.awaitTermination()

if __name__ == "__main__":
    main()