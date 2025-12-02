import json
import logging
from kafka import KafkaConsumer
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
import time
import os

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)
logging.getLogger('kafka').setLevel(level=logging.ERROR)

# Environment configuration
environment = 'dev' if os.getenv('USER', '') != '' else 'prod'

if environment == 'dev':
    KAFKA_SERVERS = ['localhost:19092', 'localhost:29092', 'localhost:39092']
    DATABASE_URL = "postgresql://postgres:postgres@localhost:15432/wtc_analytics"
    TOPIC_NAME = 'tick-data-dev'
else:
    KAFKA_SERVERS = ['redpanda-0:9092', 'redpanda-1:9092', 'redpanda-2:9092']
    DATABASE_URL = "postgresql://postgres:postgres@postgres:5432/wtc_analytics"
    TOPIC_NAME = 'tick-data'

logger.info(f"Environment: {environment}")
logger.info(f"Kafka servers: {KAFKA_SERVERS}")
logger.info(f"Topic: {TOPIC_NAME}")

# Wait for PostgreSQL to be ready
logger.info('Waiting for PostgreSQL to be ready...')
time.sleep(10)

# Create database engine
engine = create_engine(DATABASE_URL)
Session = sessionmaker(bind=engine)

# Create forex_raw table if it doesn't exist
def create_table():
    """Create the forex_raw table in the forex_data schema"""
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS forex_data.forex_raw (
        id SERIAL PRIMARY KEY,
        timestamp TIMESTAMP NOT NULL,
        pair_name VARCHAR(20) NOT NULL,
        bid_price DECIMAL(10, 4) NOT NULL,
        ask_price DECIMAL(10, 4) NOT NULL,
        spread DECIMAL(10, 4) NOT NULL,
        ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    
    CREATE INDEX IF NOT EXISTS idx_forex_raw_timestamp ON forex_data.forex_raw(timestamp);
    CREATE INDEX IF NOT EXISTS idx_forex_raw_pair_name ON forex_data.forex_raw(pair_name);
    """
    
    with engine.connect() as connection:
        connection.execute(text(create_table_sql))
        connection.commit()
    
    logger.info("Table forex_data.forex_raw created successfully")

def insert_tick_data(session, tick_data):
    """Insert tick data into the database"""
    insert_sql = """
    INSERT INTO forex_data.forex_raw (timestamp, pair_name, bid_price, ask_price, spread)
    VALUES (:timestamp, :pair_name, :bid_price, :ask_price, :spread)
    """
    
    session.execute(text(insert_sql), {
        'timestamp': tick_data['timestamp'],
        'pair_name': tick_data['pair_name'],
        'bid_price': tick_data['bid_price'],
        'ask_price': tick_data['ask_price'],
        'spread': tick_data['spread']
    })

def consume_forex_data():
    """Main consumer function"""
    # Create table
    create_table()
    
    # Create Kafka consumer
    logger.info("Creating Kafka consumer...")
    consumer = KafkaConsumer(
        TOPIC_NAME,
        bootstrap_servers=KAFKA_SERVERS,
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='forex-postgres-consumer',
        value_deserializer=lambda x: json.loads(x.decode('utf-8')),
        consumer_timeout_ms=60000  # Exit after 60 seconds of no messages
    )
    
    logger.info(f"Consumer subscribed to topic: {TOPIC_NAME}")
    
    session = Session()
    message_count = 0
    batch_size = 100
    
    try:
        for message in consumer:
            tick_data = message.value
            
            # Insert into database
            insert_tick_data(session, tick_data)
            message_count += 1
            
            # Commit in batches
            if message_count % batch_size == 0:
                session.commit()
                logger.info(f"Processed {message_count} messages")
        
        # Final commit
        session.commit()
        logger.info(f"Total messages processed: {message_count}")
        
    except KeyboardInterrupt:
        logger.info("Consumer interrupted by user")
    except Exception as e:
        logger.error(f"Error consuming messages: {e}", exc_info=True)
        session.rollback()
    finally:
        session.close()
        consumer.close()
        logger.info("Consumer closed")

if __name__ == "__main__":
    logger.info("Starting Forex consumer...")
    consume_forex_data()
    logger.info("Forex consumer finished")