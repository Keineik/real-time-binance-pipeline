from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, explode
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, ArrayType, DoubleType

KAFKA_BROKER = "localhost:9094"
MONGODB_URI = "mongodb://localhost:27017"
MONGODB_DATABASE = "crypto"

def main():
    # Khởi tạo Spark session
    spark = SparkSession.builder \
        .appName("LoadStage-ZScoreToMongoDB") \
        .config("spark.mongodb.write.connection.uri", MONGODB_URI) \
        .config("spark.streaming.stopGracefullyOnShutdown", "true") \
        .config("spark.cores.max", "3")\
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
        .option("kafka.bootstrap.servers", KAFKA_BROKER) \
        .option("subscribe", "btc-price-zscore") \
        .option("startingOffsets", "latest") \
        .load()

    print("Reading from Kafka topic 'btc-price-zscore'...")

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
    window_values = ["5s", "10s", "20s", "40s"]
    for window in window_values:
        print(f"Writing to MongoDB collection for window: {window}...")
        filtered = parsed_stream.filter(col("window") == window)
        filtered.writeStream \
            .format("mongodb") \
            .option("checkpointLocation", f"/tmp/checkpoints/mongo-{window}") \
            .option("spark.mongodb.connection.uri", MONGODB_URI) \
            .option("spark.mongodb.database", MONGODB_DATABASE) \
            .option("spark.mongodb.collection", f"btc-price-zscore-{window}") \
            .outputMode("append") \
            .start()

    # Đợi tất cả truy vấn kết thúc
    spark.streams.awaitAnyTermination()
    

if __name__ == "__main__":
    main()
