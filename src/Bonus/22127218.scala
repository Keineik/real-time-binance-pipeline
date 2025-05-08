import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, from_json}
import org.apache.spark.sql.streaming.{GroupState, GroupStateTimeout, OutputMode}
import org.apache.spark.sql.types.{StructType, StringType, DoubleType, TimestampType}
import java.sql.Timestamp
import scala.collection.mutable
import java.time.Duration
import java.time.format.DateTimeFormatter

object BonusPriceStream {
    // Case classes to represent the data structure
    case class PriceEvent(timestamp: Timestamp, price: Double)
    case class Output(timestamp: Timestamp, windowLength: Double)
    case class EventStatus(
        event: PriceEvent,
        higherFound: Boolean = false, 
        lowerFound: Boolean = false,
        processed: Boolean = false
    )
    case class EventState(events: List[EventStatus])

    /**
      * Find the first higher and lower prices within a 20-second window
      *
      * @param values New incoming price events received from Kafka
      * @param state GroupState to maintain the state of the events
      * @return Iterator of tuples containing the topic and JSON string with the first higher and lower prices
      */
    def findFirstPriceShifts(
        values: Iterator[PriceEvent],
        state: GroupState[EventState]
    ): Iterator[(String, String)] = {
        // Initialize the state
        val incoming = values.toList.sortBy(_.timestamp.getTime)
                            .map(event => EventStatus(event))
        val existing = if (state.exists) state.get.events else List()
        val allEvents = existing ++ incoming
        
        // Get current processing time
        val currentTime = incoming.lastOption.map(_.event.timestamp)
                .getOrElse(new Timestamp(System.currentTimeMillis()))
        
        // Mutable array, same as vector in C++
        val results = mutable.ArrayBuffer[(String, String)]()

        // Create a DateTimeFormatter for formatting timestamps
        val formatter = DateTimeFormatter.ISO_INSTANT
        
        // Update existing events with new data (check for price shifts)
        val processedEvents = allEvents.map { event =>
            if (event.processed) event
            else {
                // Find events that are within the 20-second window
                val eligible = allEvents.filter(s => 
                    s.event.timestamp.after(event.event.timestamp) &&
                    Duration.between(event.event.timestamp.toInstant, s.event.timestamp.toInstant).getSeconds <= 20
                )
                
                // Check for higher price if not found yet
                val higherFound = if (!event.higherFound) {
                    eligible.find(_.event.price > event.event.price).map { found =>
                        val length = Duration.between(event.event.timestamp.toInstant, found.event.timestamp.toInstant).toMillis / 1000.0
                        val json = s"""{"timestamp":"${formatter.format(event.event.timestamp.toInstant())}","higher_window":$length}"""
                        results += (("btc-price-higher", json))
                        true
                    }.getOrElse(event.higherFound) // No new higher found
                } else event.higherFound
                
                // Check for lower price if not found yet
                val lowerFound = if (!event.lowerFound) {
                    eligible.find(_.event.price < event.event.price).map { found =>
                        val length = Duration.between(event.event.timestamp.toInstant, found.event.timestamp.toInstant).toMillis / 1000.0
                        val json = s"""{"timestamp":"${formatter.format(event.event.timestamp.toInstant())}","lower_window":$length}"""
                        results += (("btc-price-lower", json))
                        true
                    }.getOrElse(event.lowerFound) // No new lower found
                } else event.lowerFound
                
                // Check if window is complete (event is >30s old (20s + 10 grace) or both price shifts found)
                val windowComplete = Duration.between(event.event.timestamp.toInstant, currentTime.toInstant).getSeconds > 30 || 
                                    (higherFound && lowerFound)
                                    
                // Emit placeholders for shifts not found if window complete
                if (windowComplete && !event.processed) {
                    if (!higherFound) {
                        val json = s"""{"timestamp":"${formatter.format(event.event.timestamp.toInstant())}","higher_window":20.0}"""
                        results += (("btc-price-higher", json))
                    }
                    
                    if (!lowerFound) {
                        val json = s"""{"timestamp":"${formatter.format(event.event.timestamp.toInstant())}","lower_window":20.0}"""
                        results += (("btc-price-lower", json))
                    }
                }
                
                // Yield the event with updated status
                event.copy(higherFound = higherFound, lowerFound = lowerFound, processed = windowComplete)
            }
        }
        
        // Filter out processed events
        state.update(EventState(processedEvents.filter(e => 
            !e.processed || // Keep unprocessed events
            Duration.between(e.event.timestamp.toInstant, currentTime.toInstant).getSeconds <= 30 // Keep events within the 30s window
        )))
        
        // Yield results
        results.iterator
    }

    /**
      * Write the DataFrame to Kafka topic
      *
      * @param df DataFrame to write
      * @param topic Kafka topic to write to
      */
    def writeToKafka(df: DataFrame, topic: String): Unit = {
        df.writeStream
            .format("kafka")
            .option("kafka.bootstrap.servers", "kafka:9092")
            .option("topic", topic)
            .outputMode("append")
            .option("checkpointLocation", s"/tmp/checkpoints/$topic")
            .start()
    }

    def main(args: Array[String]): Unit = {
        val spark = SparkSession.builder
            .appName("BTC Price Streaming Bonus")
            .config("spark.streaming.stopGracefullyOnShutdown", "true")
            .master("spark://spark-master:7077")
            .getOrCreate()
        spark.sparkContext.setLogLevel("ERROR")
        
        import spark.implicits._

        val schema = new StructType()
            .add("timestamp", TimestampType)
            .add("price", StringType)

        // Read from Kafka topic
        val rawDF = spark.readStream
            .format("kafka")
            .option("subscribe", "btc-price")
            .option("startingOffsets", "latest")
            .option("kafka.bootstrap.servers", "kafka:9092")
            .load()

        // Parse the JSON data
        // Add watermark to handle late data (10 seconds)
        val parsedDF = rawDF.select(from_json(col("value").cast("string"), schema).alias("data"))
            .select("data.*")
            .withColumn("price", col("price").cast(DoubleType))
            .filter(col("price").isNotNull)
            .withWatermark("timestamp", "10 seconds")

        // Add a key to the DataFrame (a key is required for groupByKey)
        // This is necessary for the flatMapGroupsWithState function
        // We use a constant key "btc" for all events
        val keyedDF = parsedDF.as[PriceEvent].map(p => ("btc", p)).toDF("key", "event")

        // Group by key and apply the stateful processing
        // Use flatMapGroupsWithState to maintain state across batches
        val outputDF = keyedDF.as[(String, PriceEvent)]
            .groupByKey(_._1)
            .flatMapGroupsWithState[EventState, (String, String)](OutputMode.Append, GroupStateTimeout.ProcessingTimeTimeout) {
                case (key: String, iter: Iterator[(String, PriceEvent)], state: GroupState[EventState]) => 
                    findFirstPriceShifts(iter.map(_._2), state)
            }.toDF("topic", "value")
        
        val higherDF = outputDF.filter(col("topic") === "btc-price-higher")
        val lowerDF = outputDF.filter(col("topic") === "btc-price-lower")

        val higherQuery = writeToKafka(higherDF, "btc-price-higher")
        val lowerQuery = writeToKafka(lowerDF, "btc-price-lower")

        spark.streams.awaitAnyTermination()
        spark.stop()
    }
}