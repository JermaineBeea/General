import json
import logging
import os
import time
from kafka import KafkaConsumer
from sqlalchemy import create_engine, text

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Configuration
environment = 'dev' if os.getenv('USER', '') != '' else 'prod'

if environment == 'dev':
    KAFKA_SERVERS = ['localhost:19092']
    TOPIC_NAME = 'tick-data-dev'
    DATABASE_URL = "postgresql://postgres:postgres@localhost:15432/wtc_analytics"
else:
    KAFKA_SERVERS = ['redpanda-0:9092']
    TOPIC_NAME = 'tick-data'
    DATABASE_URL = "postgresql://postgres:postgres@postgres:5432/wtc_analytics"

# Wait for services to be ready
logger.info("Waiting for services to start...")
time.sleep(10)

# Create database table
logger.info("Creating database table...")
engine = create_engine(DATABASE_URL)
with engine.connect() as conn:
    conn.execute(text("""
        CREATE TABLE IF NOT EXISTS forex_data.forex_raw (
            timestamp TIMESTAMP,
            pair_name VARCHAR(10),
            bid_price FLOAT,
            ask_price FLOAT,
            spread FLOAT,
            PRIMARY KEY (timestamp, pair_name)
        )
    """))
    conn.commit()
logger.info("Table created successfully")

# Connect to Kafka
logger.info("Connecting to Kafka...")
consumer = KafkaConsumer(
    TOPIC_NAME,
    bootstrap_servers=KAFKA_SERVERS,
    group_id='forex-consumer',
    auto_offset_reset='earliest',
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)
logger.info("Connected to Kafka")

# Consume and insert data
logger.info("Starting to consume messages...")
message_count = 0
batch = []
BATCH_SIZE = 100

try:
    for message in consumer:
        data = message.value
        batch.append(data)
        
        if len(batch) >= BATCH_SIZE:
            # Insert batch to database
            with engine.connect() as conn:
                for record in batch:
                    conn.execute(text("""
                        INSERT INTO forex_data.forex_raw 
                        (timestamp, pair_name, bid_price, ask_price, spread)
                        VALUES (:timestamp, :pair_name, :bid_price, :ask_price, :spread)
                        ON CONFLICT (timestamp, pair_name) DO NOTHING
                    """), record)
                conn.commit()
            
            message_count += len(batch)
            logger.info(f"Inserted {message_count} records")
            batch = []
            
except KeyboardInterrupt:
    logger.info("Shutting down...")
finally:
    # Insert remaining records
    if batch:
        with engine.connect() as conn:
            for record in batch:
                conn.execute(text("""
                    INSERT INTO forex_data.forex_raw 
                    (timestamp, pair_name, bid_price, ask_price, spread)
                    VALUES (:timestamp, :pair_name, :bid_price, :ask_price, :spread)
                    ON CONFLICT (timestamp, pair_name) DO NOTHING
                """), record)
            conn.commit()
        message_count += len(batch)
    
    consumer.close()
    logger.info(f"Total records inserted: {message_count}")