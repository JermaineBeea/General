import os, json, datetime, logging
from kafka import KafkaConsumer
from cassandra.cluster import Cluster
from prometheus_client import start_http_server, Counter, Gauge
import ast 

# ---- Config
BOOTSTRAP = os.getenv("BOOTSTRAP", "localhost:19092")   # Redpanda external
TOPICS = [os.getenv("TOPIC_DATA", "cdr_data"),
          os.getenv("TOPIC_VOICE", "cdr_voice")]
SCYLLA_HOST = os.getenv("SCYLLA_HOST", "::1")
SCYLLA_PORT = int(os.getenv("SCYLLA_PORT", "9042"))

WAK_PER_ZAR = float(os.getenv("WAK_PER_ZAR", "1.0"))    # adjust if WAK != ZAR
BYTES_PER_GB = int(os.getenv("BYTES_PER_GB", "1000000000"))  # decimal GB

# ---- Logging
logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s [%(levelname)s] %(message)s")

# ---- Scylla client
cluster = Cluster(contact_points=[SCYLLA_HOST], port=SCYLLA_PORT)
session = cluster.connect("cdr_keyspace")

# ---- Prepared statements
upd_data = session.prepare("""
UPDATE daily_data_summary
SET total_up_bytes = total_up_bytes + ?, 
    total_down_bytes = total_down_bytes + ?, 
    total_cost_wak = total_cost_wak + ?
WHERE msisdn = ? AND event_date = ? AND data_type = ?;
""")

upd_voice = session.prepare("""
UPDATE daily_voice_summary
SET total_duration_sec = total_duration_sec + ?, 
    total_cost_wak = total_cost_wak + ?
WHERE msisdn = ? AND event_date = ? AND call_type = ?;
""")

# ---- Helpers
def to_usage_date(ts_str):
    try:
        dt = datetime.datetime.fromisoformat(ts_str.replace("Z",""))
    except ValueError:
        dt = datetime.datetime.strptime(ts_str, "%Y-%m-%d %H:%M:%S")
    return dt.date()

def data_cost_wak(total_bytes):
    zar = (total_bytes * 49.0) / BYTES_PER_GB
    return int(round(zar * WAK_PER_ZAR))

def voice_cost_wak(seconds):
    zar = seconds / 60.0
    return int(round(zar * WAK_PER_ZAR))

# ---- Prometheus metrics
cdr_data_updates = Counter('cdr_data_updates_total', 'Number of data CDR updates')
cdr_voice_updates = Counter('cdr_voice_updates_total', 'Number of voice CDR updates')
cdr_last_update = Gauge('cdr_last_update_timestamp', 'Unix timestamp of last CDR processed')

# Start Prometheus metrics server on port 8000
start_http_server(8000)

# ---- Unified consumer
consumer = KafkaConsumer(
    *TOPICS,
    bootstrap_servers=[BOOTSTRAP],
    value_deserializer=lambda m: json.loads(m.decode("utf-8")),
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    group_id="cdr-summary-unified"
)

logging.info("Unified consumer started. Listening to topics: %s", TOPICS)

try:
     for msg in consumer:
        # Attempt to deserialize JSON safely
        try:
            # First try JSON
            v = json.loads(msg.value.decode("utf-8"))
        except json.JSONDecodeError:
            try:
                # Fallback: Python dict string
                v = ast.literal_eval(msg.value.decode("utf-8"))
            except Exception as e:
                logging.warning("Skipping bad message: %s", msg.value)
                continue

        topic = msg.topic


        if topic == "cdr_data":
            usage_date = to_usage_date(v["event_datetime"])
            msisdn = v["msisdn"]
            data_type = v["data_type"]
            up_b = int(v["up_bytes"])
            down_b = int(v["down_bytes"])
            cost_wak = data_cost_wak(up_b + down_b)
            session.execute(upd_data, (up_b, down_b, cost_wak, msisdn, usage_date, data_type))
            logging.info("Updated data summary for %s on %s [%s]", msisdn, usage_date, data_type)
            cdr_data_updates.inc()
            cdr_last_update.set_to_current_time()

        elif topic == "cdr_voice":
            usage_date = to_usage_date(v["start_time"])
            msisdn = v["msisdn"]
            call_type = v["call_type"]
            dur = int(v["call_duration_sec"])
            cost_wak = voice_cost_wak(dur)
            session.execute(upd_voice, (dur, cost_wak, msisdn, usage_date, call_type))
            logging.info("Updated voice summary for %s on %s [%s]", msisdn, usage_date, call_type)
            cdr_voice_updates.inc()
            cdr_last_update.set_to_current_time()

except KeyboardInterrupt:
    logging.info("Stopping unified consumer...")
finally:
    consumer.close()
    cluster.shutdown()


# from cassandra.cluster import Cluster
# from cassandra.query import SimpleStatement
# from kafka import KafkaConsumer
# import json
# import time

# # --- CONFIG ---
# SCYLLA_HOSTS = ["localhost"] 
# KEYSPACE = "cdr_keyspace"

# KAFKA_BOOTSTRAP_SERVERS = ["redpanda:9092"]
# TOPICS = ["cdr_data", "cdr_voice"]

# TABLES = {
#     "cdr_data": """
#         CREATE TABLE IF NOT EXISTS cdr_data (
#             msisdn text,
#             tower_id int,
#             up_bytes bigint,
#             down_bytes bigint,
#             data_type text,
#             ip_address text,
#             website_url text,
#             event_datetime timestamp,
#             PRIMARY KEY (msisdn, event_datetime)
#         )
#     """,
#     "cdr_voice": """
#         CREATE TABLE IF NOT EXISTS cdr_voice (
#             msisdn text,
#             tower_id int,
#             call_type text,
#             dest_nr text,
#             call_duration_sec int,
#             start_time timestamp,
#             PRIMARY KEY (msisdn, start_time)
#         )
#     """
# }

# # --- SCYLLA FUNCTIONS ---
# def connect_scylla(hosts):
#     cluster = Cluster(hosts)
#     session = cluster.connect()
#     return session

# def create_keyspace(session):
#     session.execute(f"""
#         CREATE KEYSPACE IF NOT EXISTS {KEYSPACE}
#         WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': 1}}
#     """)
#     session.set_keyspace(KEYSPACE)

# def create_tables(session):
#     for table_name, ddl in TABLES.items():
#         session.execute(ddl)
#         print(f"Table {table_name} ready.")

# # --- KAFKA FUNCTIONS ---
# def consume_and_insert(session):
#     consumer = KafkaConsumer(
#         *TOPICS,
#         bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
#         auto_offset_reset='earliest',
#         value_deserializer=lambda m: json.loads(m.decode('utf-8')),
#         enable_auto_commit=True,
#         group_id='hvs-group'
#     )

#     print(f"Subscribed to topics: {TOPICS}")
    
#     for message in consumer:
#         topic = message.topic
#         record = message.value
        
#         if topic == "cdr_data":
#             query = """
#                 INSERT INTO cdr_data (msisdn, tower_id, up_bytes, down_bytes, data_type, ip_address, website_url, event_datetime)
#                 VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
#             """
#             session.execute(query, (
#                 record.get("msisdn"),
#                 record.get("tower_id"),
#                 record.get("up_bytes"),
#                 record.get("down_bytes"),
#                 record.get("data_type"),
#                 record.get("ip_address"),
#                 record.get("website_url"),
#                 record.get("event_datetime")
#             ))
#         elif topic == "cdr_voice":
#             query = """
#                 INSERT INTO cdr_voice (msisdn, tower_id, call_type, dest_nr, call_duration_sec, start_time)
#                 VALUES (%s, %s, %s, %s, %s, %s)
#             """
#             session.execute(query, (
#                 record.get("msisdn"),
#                 record.get("tower_id"),
#                 record.get("call_type"),
#                 record.get("dest_nr"),
#                 record.get("call_duration_sec"),
#                 record.get("start_time")
#             ))
#         print(f"Inserted record into {topic}")

# # --- MAIN ---
# def main():
#     print("Connecting to Scylla...")
#     session = connect_scylla(SCYLLA_HOSTS)
#     create_keyspace(session)
#     create_tables(session)
    
#     print("Starting Kafka consumer...")
#     try:
#         consume_and_insert(session)
#     except KeyboardInterrupt:
#         print("Stopping HVS consumer...")
#     finally:
#         session.shutdown()

# if __name__ == "__main__":
#     main()
