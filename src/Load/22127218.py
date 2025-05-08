from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, explode
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, ArrayType, DoubleType

def main():
    # Khởi tạo Spark session
    spark = SparkSession.builder \
        .appName("LoadStage-ZScoreToMongoDB") \
        .config("spark.mongodb.output.uri", "mongodb://localhost:27017/crypto") \
        .config("spark.streaming.stopGracefullyOnShutdown", "true") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    # Định nghĩa schema cho dữ liệu zscore
    zscore_schema = StructType([
        StructField("timestamp", TimestampType(), False),
        StructField("symbol", StringType(), False),
        StructField("zscores", ArrayType(
            StructType([
                StructField("window", StringType(), False),
                StructField("zscore_price", DoubleType(), False)
            ])
        ), False)
    ])

    # Đọc từ Kafka topic 'btc-price-zscore'
    raw_zscore = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:9092") \
        .option("subscribe", "btc-price-zscore") \
        .option("startingOffsets", "latest") \
        .load()

    # Phân tích dữ liệu JSON
    parsed_stream = raw_zscore.selectExpr("CAST(value AS STRING) as json") \
        .select(from_json(col("json"), zscore_schema).alias("data")) \
        .select(
            col("data.timestamp"),
            col("data.symbol"),
            explode(col("data.zscores")).alias("zscore")
        ) \
        .select(
            col("timestamp"),
            col("symbol"),
            col("zscore.window").alias("window"),
            col("zscore.zscore_price").alias("zscore_price")
        ) \
        .withWatermark("timestamp", "10 seconds")  # Handle late data (up to 10s)

    # Ghi từng window vào MongoDB collection riêng biệt
    window_values = ["30s", "1m", "5m", "15m", "30m", "1h"]
    queries = []
    for window in window_values:
        filtered = parsed_stream.filter(col("window") == window)
        query = filtered.writeStream \
            .format("mongodb") \
            .option("checkpointLocation", f"/tmp/checkpoints/mongo-{window}") \
            .option("collection", f"btc-price-zscore-{window}") \
            .outputMode("append") \
            .start()
        queries.append(query)

    # Đợi tất cả truy vấn kết thúc
    for q in queries:
        q.awaitTermination()

if __name__ == "__main__":
    main()
