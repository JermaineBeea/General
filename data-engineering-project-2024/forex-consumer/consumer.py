import json
import logging
import os
from kafka import KafkaConsumer
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)
logging.getLogger('kafka').setLevel(logging.ERROR)

# Environment configuration
environment = 'dev' if os.getenv('USER', '') != '' else 'prod'

# Kafka configuration
if environment == 'dev':
    KAFKA_SERVERS = ['localhost:19092', 'localhost:29092', 'localhost:39092']
    TOPIC_NAME = 'tick-data-dev'
    DATABASE_URL = "postgresql://postgres:postgres@localhost:15432/wtc_analytics"
else:
    KAFKA_SERVERS = ['redpanda-0:9092', 'redpanda-1:9092', 'redpanda-2:9092']
    TOPIC_NAME = 'tick-data'
    DATABASE_URL = "postgresql://postgres:postgres@postgres:5432/wtc_analytics"

# Database setup
engine = create_engine(DATABASE_URL)
Session = sessionmaker(bind=engine)

def create_forex_table():
    """Create the forex_raw table if it doesn't exist"""
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS forex_data.forex_raw (
        id SERIAL PRIMARY KEY,
        timestamp TIMESTAMP NOT NULL,
        pair_name VARCHAR(10) NOT NULL,
        bid_price DECIMAL(10, 4) NOT NULL,
        ask_price DECIMAL(10, 4) NOT NULL,
        spread DECIMAL(10, 4) NOT NULL,
        ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """
    
    with engine.connect() as connection:
        connection.execute(text(create_table_sql))
        connection.commit()
    
    logger.info("forex_raw table created/verified")

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
    """Main consumer loop"""
    # Create table
    create_forex_table()
    
    # Create Kafka consumer
    consumer = KafkaConsumer(
        TOPIC_NAME,
        bootstrap_servers=KAFKA_SERVERS,
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='forex-consumer-group',
        value_deserializer=lambda m: json.loads(m.decode('utf-8'))
    )
    
    logger.info(f"Connected to Kafka, consuming from topic: {TOPIC_NAME}")
    
    session = Session()
    batch_size = 100
    batch = []
    
    try:
        for message in consumer:
            tick_data = message.value
            batch.append(tick_data)
            
            # Insert in batches for better performance
            if len(batch) >= batch_size:
                for tick in batch:
                    insert_tick_data(session, tick)
                session.commit()
                logger.info(f"Inserted batch of {len(batch)} records")
                batch = []
        
    except KeyboardInterrupt:
        logger.info("Consumer interrupted by user")
    except Exception as e:
        logger.error(f"Error consuming messages: {e}")
        session.rollback()
    finally:
        # Insert any remaining records
        if batch:
            for tick in batch:
                insert_tick_data(session, tick)
            session.commit()
            logger.info(f"Inserted final batch of {len(batch)} records")
        
        session.close()
        consumer.close()
        logger.info("Consumer closed")

if __name__ == "__main__":
    consume_forex_data()