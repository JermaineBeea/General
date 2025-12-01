import json
import os
import time
from kafka import KafkaConsumer, KafkaProducer


environment = 'dev' if os.getenv('USER', '') != '' else 'prod'

if environment == 'dev':
    KAFKA_SERVERS = ['localhost:19092', 'localhost:29092', 'localhost:39092', 'localhost:49092', 'localhost:59092']
    SOURCE_TOPIC = 'tick-data-dev'
else:
    KAFKA_SERVERS = ['redpanda-0:9092', 'redpanda-1:9092', 'redpanda-2:9092']
    SOURCE_TOPIC = 'tick-data'

TARGET_TOPIC = 'forex_raw'


def consume_and_produce():
    consumer = KafkaConsumer(
        SOURCE_TOPIC,
        bootstrap_servers=KAFKA_SERVERS,
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        auto_offset_reset='earliest',
        group_id='forex-pipeline-consumer'
    )
    
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    
    print(f"Started consuming from {SOURCE_TOPIC} and producing to {TARGET_TOPIC}")
    
    count = 0
    try:
        for message in consumer:
            tick_data = message.value
            
            # Produce to forex_raw topic
            producer.send(TARGET_TOPIC, value=tick_data)
            count += 1
            
            if count % 1000 == 0:
                print(f"Processed {count} messages")
                producer.flush()
    
    except KeyboardInterrupt:
        print("Consumer stopped.")
    finally:
        producer.flush(timeout=10)
        producer.close()
        consumer.close()
        print(f"Completed consuming, total messages processed: {count}")


if __name__ == '__main__':
    consume_and_produce()
