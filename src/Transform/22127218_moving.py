from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, col, window, avg, stddev,
    to_json, struct, lit, collect_list)
from pyspark.sql.types import StructType, StructField, StringType, TimestampType

def main():
    # Initialize Spark
    spark = SparkSession.builder\
                        .appName("BTC Price Analytics")\
                        .config("spark.streaming.stopGracefullyOnShutdown", "true")\
                        .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true")\
                        .config("spark.sql.streaming.statefulOperator.allowMultiple", "true")\
                        .config("spark.cleaner.referenceTracking.cleanCheckpoints", "true")\
                        .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    
    # Define schema for incoming data
    schema = StructType([
        StructField("symbol", StringType(), False),
        StructField("price", StringType(), False),
        StructField("timestamp", TimestampType(), False)
    ])
    
    # Read from Kafka source
    kafka_stream = spark.readStream\
                        .format("kafka")\
                        .option("kafka.bootstrap.servers", "kafka:9092")\
                        .option("subscribe", "btc-price")\
                        .option("startingOffsets", "latest")\
                        .load()
    
    # Parse JSON data
    parsed_stream = kafka_stream\
                    .selectExpr("CAST(value AS STRING) as json_value")\
                    .select(from_json("json_value", schema).alias("data"))\
                    .selectExpr(
                        "data.symbol", 
                        "DOUBLE(data.price) as price", 
                        "data.timestamp")\
                    .withWatermark("timestamp", "10 seconds")
    

    def calc_moving_stats(df, window_name, window_length):
        # Calculate moving average and standard deviation
        return df\
            .groupBy(
                window("timestamp", window_length), 
                "symbol")\
            .agg(
                avg("price").alias("avg_price"),
                stddev("price").alias("std_price"))\
            .select(
                col("window").alias("tumbling_window"),
                col("symbol"),
                col("window.end").alias("timestamp"),
                struct(
                    lit(window_name).alias("window"),
                    col("avg_price"),
                    col("std_price")
                ).alias("stats"))
    
    # Define the time windows for moving statistics
    # windows = {"30s": "30 seconds", "1m": "1 minute", "5m": "5 minutes", "15m": "15 minutes", "30m": "30 minutes", "1h": "1 hour"}
    windows = {"5s": "5 seconds", "10s": "10 seconds", "20s": "20 seconds", "40s": "40 seconds"}

    # Calculate moving statistics for each window
    windowed_streams = {k: calc_moving_stats(parsed_stream, k, v) for k, v in windows.items()}

    # Combine all windowed streams into one
    combined_stream = windowed_streams["5s"]
    for k, v in windows.items():
        if k == "5s": continue
        combined_stream = combined_stream.unionByName(windowed_streams[k])

    # Group by timestamp and symbol to collect all windows for the same key
    grouped_stream = combined_stream\
        .groupBy(
            window(combined_stream.tumbling_window, "5 seconds"), "symbol", "timestamp")\
        .agg(collect_list("stats").alias("stats"))\
        .select(
            col("timestamp"),
            col("symbol"),
            col("stats"),
        )
    
    # Format the output to json
    output_stream = grouped_stream.select(to_json(struct("*")).alias("value"))

    # Write the results to Kafka
    query = output_stream.writeStream\
                .format("kafka")\
                .option("kafka.bootstrap.servers", "kafka:9092")\
                .option("topic", "btc-price-moving")\
                .option("checkpointLocation", "/tmp/checkpoints/btc-price-moving")\
                .outputMode("append")\
                .start()

    query.awaitTermination()

if __name__ == "__main__":
    main()