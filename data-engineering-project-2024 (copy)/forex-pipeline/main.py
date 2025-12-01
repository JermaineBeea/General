import json
import logging
import os
import signal
import sys
import time
from datetime import datetime

from kafka import KafkaConsumer, KafkaProducer
import psycopg2
from psycopg2.extras import execute_values

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger('forex-pipeline')


def get_kafka_bootstrap():
    # Try internal (docker network) first, then host-exposed ports
    internal = ['redpanda-0:9092', 'redpanda-1:9092', 'redpanda-2:9092']
    external = ['localhost:19092', 'localhost:29092', 'localhost:39092']
    return internal + external


KAFKA_SERVERS = get_kafka_bootstrap()
TOPIC_IN = os.getenv('TOPIC_IN', 'tick-data')
TOPIC_IN_DEV = os.getenv('TOPIC_IN_DEV', 'tick-data-dev')
TOPIC_OUT = os.getenv('TOPIC_OUT', 'forex_raw')

# Postgres connection parameters. These match the project's docker-compose defaults.
PG_HOST = os.getenv('PG_HOST', 'postgres')
PG_PORT = int(os.getenv('PG_PORT', '5432'))
PG_DB = os.getenv('PG_DB', 'wtc_analytics')
PG_USER = os.getenv('PG_USER', 'postgres')
PG_PASS = os.getenv('PG_PASS', 'postgres')


shutdown_requested = False


def handle_sigterm(signum, frame):
    global shutdown_requested
    logger.info('Signal received, shutting down')
    shutdown_requested = True


signal.signal(signal.SIGINT, handle_sigterm)
signal.signal(signal.SIGTERM, handle_sigterm)


def ensure_table(cur):
    cur.execute("""
    CREATE TABLE IF NOT EXISTS forex_data.raw_ticks (
        id SERIAL PRIMARY KEY,
        ts TIMESTAMP,
        pair_name TEXT,
        bid_price NUMERIC,
        ask_price NUMERIC,
        spread NUMERIC,
        raw JSONB
    );
    """)


def connect_pg(retries=5, wait=3):
    for i in range(retries):
        try:
            conn = psycopg2.connect(host=PG_HOST, port=PG_PORT, dbname=PG_DB, user=PG_USER, password=PG_PASS)
            conn.autocommit = True
            logger.info('Connected to Postgres')
            return conn
        except Exception as e:
            logger.warning('Postgres connection failed (%s). Retrying in %s seconds...', e, wait)
            time.sleep(wait)
    logger.error('Failed to connect to Postgres after retries')
    raise RuntimeError('Postgres connection failed')


def run_consumer():
    consumer = KafkaConsumer(
        bootstrap_servers=KAFKA_SERVERS,
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        consumer_timeout_ms=5000,
    )

    # subscribe to both the production and dev topics (one may exist depending on env)
    consumer.subscribe([TOPIC_IN, TOPIC_IN_DEV])

    producer = KafkaProducer(bootstrap_servers=KAFKA_SERVERS, value_serializer=lambda v: json.dumps(v).encode('utf-8'))

    conn = connect_pg()
    cur = conn.cursor()
    ensure_table(cur)

    logger.info('Starting to consume from %s and %s', TOPIC_IN, TOPIC_IN_DEV)

    buffer = []

    try:
        while not shutdown_requested:
            empty = True
            for msg in consumer:
                empty = False
                logger.debug('Received message: %s', msg.value)
                data = msg.value
                # parse timestamp
                try:
                    ts = datetime.strptime(data.get('timestamp'), '%Y-%m-%d %H:%M:%S.%f')
                except Exception:
                    try:
                        ts = datetime.strptime(data.get('timestamp'), '%Y-%m-%d %H:%M:%S')
                    except Exception:
                        ts = None

                pair = data.get('pair_name')
                bid = data.get('bid_price')
                ask = data.get('ask_price')
                spread = data.get('spread')

                # enqueue into buffer for DB
                buffer.append((ts, pair, bid, ask, spread, json.dumps(data)))

                # publish to forex_raw
                try:
                    producer.send(TOPIC_OUT, value=data)
                except Exception as e:
                    logger.exception('Failed to produce to %s: %s', TOPIC_OUT, e)

                # flush to db in batches
                if len(buffer) >= 50:
                    execute_values(cur, "INSERT INTO forex_data.raw_ticks (ts, pair_name, bid_price, ask_price, spread, raw) VALUES %s", buffer)
                    logger.info('Inserted %d rows into Postgres', len(buffer))
                    buffer.clear()

            if empty:
                # no messages during consumer poll, sleep briefly
                time.sleep(1)

    except Exception as e:
        logger.exception('Unexpected error in consumer: %s', e)

    finally:
        # flush remaining buffer
        if buffer:
            execute_values(cur, "INSERT INTO forex_data.raw_ticks (ts, pair_name, bid_price, ask_price, spread, raw) VALUES %s", buffer)
            logger.info('Inserted final %d rows into Postgres', len(buffer))

        try:
            producer.flush(10)
        except Exception:
            pass
        try:
            consumer.close()
        except Exception:
            pass
        try:
            cur.close()
            conn.close()
        except Exception:
            pass


if __name__ == '__main__':
    try:
        run_consumer()
        logger.info('Consumer exited normally')
        sys.exit(0)
    except Exception as e:
        logger.exception('Consumer failed: %s', e)
        sys.exit(2)
