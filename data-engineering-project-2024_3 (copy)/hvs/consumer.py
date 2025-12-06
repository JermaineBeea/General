from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement
from kafka import KafkaConsumer
import json
import time

# --- CONFIG ---
SCYLLA_HOSTS = ["localhost"] 
KEYSPACE = "cdr_keyspace"

KAFKA_BOOTSTRAP_SERVERS = ["redpanda:9092"]
TOPICS = ["cdr_data", "cdr_voice"]

TABLES = {
    "cdr_data": """
        CREATE TABLE IF NOT EXISTS cdr_data (
            msisdn text,
            tower_id int,
            up_bytes bigint,
            down_bytes bigint,
            data_type text,
            ip_address text,
            website_url text,
            event_datetime timestamp,
            PRIMARY KEY (msisdn, event_datetime)
        )
    """,
    "cdr_voice": """
        CREATE TABLE IF NOT EXISTS cdr_voice (
            msisdn text,
            tower_id int,
            call_type text,
            dest_nr text,
            call_duration_sec int,
            start_time timestamp,
            PRIMARY KEY (msisdn, start_time)
        )
    """
}

# --- SCYLLA FUNCTIONS ---
def connect_scylla(hosts):
    cluster = Cluster(hosts)
    session = cluster.connect()
    return session

def create_keyspace(session):
    session.execute(f"""
        CREATE KEYSPACE IF NOT EXISTS {KEYSPACE}
        WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': 1}}
    """)
    session.set_keyspace(KEYSPACE)

def create_tables(session):
    for table_name, ddl in TABLES.items():
        session.execute(ddl)
        print(f"Table {table_name} ready.")

# --- KAFKA FUNCTIONS ---
def consume_and_insert(session):
    consumer = KafkaConsumer(
        *TOPICS,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        auto_offset_reset='earliest',
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        enable_auto_commit=True,
        group_id='hvs-group'
    )

    print(f"Subscribed to topics: {TOPICS}")
    
    for message in consumer:
        topic = message.topic
        record = message.value
        
        if topic == "cdr_data":
            query = """
                INSERT INTO cdr_data (msisdn, tower_id, up_bytes, down_bytes, data_type, ip_address, website_url, event_datetime)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """
            session.execute(query, (
                record.get("msisdn"),
                record.get("tower_id"),
                record.get("up_bytes"),
                record.get("down_bytes"),
                record.get("data_type"),
                record.get("ip_address"),
                record.get("website_url"),
                record.get("event_datetime")
            ))
        elif topic == "cdr_voice":
            query = """
                INSERT INTO cdr_voice (msisdn, tower_id, call_type, dest_nr, call_duration_sec, start_time)
                VALUES (%s, %s, %s, %s, %s, %s)
            """
            session.execute(query, (
                record.get("msisdn"),
                record.get("tower_id"),
                record.get("call_type"),
                record.get("dest_nr"),
                record.get("call_duration_sec"),
                record.get("start_time")
            ))
        print(f"Inserted record into {topic}")

# --- MAIN ---
def main():
    print("Connecting to Scylla...")
    session = connect_scylla(SCYLLA_HOSTS)
    create_keyspace(session)
    create_tables(session)
    
    print("Starting Kafka consumer...")
    try:
        consume_and_insert(session)
    except KeyboardInterrupt:
        print("Stopping HVS consumer...")
    finally:
        session.shutdown()

if __name__ == "__main__":
    main()
