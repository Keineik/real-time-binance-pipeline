import requests
import json
import time
from datetime import datetime, timezone
from kafka import KafkaProducer
import logging

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Constants
BINANCE_API_URL = "https://api.binance.com/api/v3/ticker/price?symbol=BTCUSDT"
KAFKA_TOPIC = "btc-price"
KAFKA_BOOTSTRAP_SERVERS = ['localhost:9092']
POLLING_INTERVAL = 0.5  # 100ms

def setup_kafka_producer():
    """Initialize and return a Kafka producer instance"""
    try:
        producer = KafkaProducer (
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            acks='all')
        logger.info("Kafka producer initialized successfully")
        return producer
    except Exception as e:
        logger.error(f"Failed to initialize Kafka producer: {e}")
        raise

def fetch_btc_price():
    """Fetch BTC/USDT price from Binance API using direct URL"""
    try:
        response = requests.get(BINANCE_API_URL, timeout=5)
        response.raise_for_status()  # Raise exception for HTTP errors
        data = response.json()
        return data
    except requests.exceptions.RequestException as e:
        logger.error(f"Error fetching data from Binance API: {e}")
        return None

def validate_response(data):
    """Validate that the response has the expected format:
       {"symbol": <string>, "price": <float>}
    """
    if not data:
        return False
    
    if not isinstance(data, dict):
        logger.error(f"Unexpected response type: {type(data)}")
        return False
        
    if 'symbol' not in data or 'price' not in data:
        logger.error(f"Missing required fields in response: {data}")
        return False
        
    try:
        # Verify price is a valid float
        float(data['price'])
        return True
    except (ValueError, TypeError):
        logger.error(f"Price is not a valid float: {data['price']}")
        return False

def add_timestamp(data):
    """Add event-time field with ISO8601 formatted timestamp"""
    data['timestamp'] = datetime.now(timezone.utc).isoformat()
    return data

def publish_to_kafka(producer, data):
    """Publish data to Kafka topic"""
    try:
        future = producer.send(KAFKA_TOPIC, data)
        # Wait for the message to be delivered
        record_metadata = future.get(timeout=10)
        logger.debug(f"Message sent to {record_metadata.topic}, partition: {record_metadata.partition}")
        return True
    except Exception as e:
        logger.error(f"Error publishing to Kafka: {e}")
        return False

def main():
    """Main function to run the extract process"""
    producer = setup_kafka_producer()
    
    logger.info(f"Starting BTC price extractor - polling every {POLLING_INTERVAL}s")
    
    try:
        while True:
            start_time = time.time()
            
            # Fetch data
            data = fetch_btc_price()
            
            # Process and publish if valid
            if data and validate_response(data):
                data = add_timestamp(data)
                publish_to_kafka(producer, data)
                logger.info(f"Published: {data}")
            
            # Calculate sleep time to maintain desired frequency
            elapsed = time.time() - start_time
            sleep_time = max(0, POLLING_INTERVAL - elapsed)
            
            if sleep_time > 0:
                time.sleep(sleep_time)
    
    except KeyboardInterrupt:
        logger.info("Extractor stopped by user")
    finally:
        if producer:
            producer.flush()
            producer.close()
            logger.info("Kafka producer closed")

if __name__ == "__main__":
    main()