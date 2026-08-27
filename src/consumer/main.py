import os
import json
import time
from confluent_kafka import Consumer, KafkaError, KafkaException
import psycopg2
from psycopg2.extras import Json

KAFKA_BROKER = os.environ.get("KAFKA_BROKER", "localhost:9092")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "source.public.orders")
KAFKA_GROUP_ID = os.environ.get("KAFKA_GROUP_ID", "warehouse-ingest-group")

DB_HOST = os.environ.get("DB_HOST", "localhost")
DB_PORT = os.environ.get("DB_PORT", "5432")
DB_USER = os.environ.get("DB_USER", "user")
DB_PASSWORD = os.environ.get("DB_PASSWORD", "password")
DB_NAME = os.environ.get("DB_NAME", "warehouse_db")

def get_db_connection():
    while True:
        try:
            conn = psycopg2.connect(
                host=DB_HOST,
                port=DB_PORT,
                user=DB_USER,
                password=DB_PASSWORD,
                dbname=DB_NAME
            )
            return conn
        except psycopg2.OperationalError as e:
            print(f"Waiting for warehouse database: {e}")
            time.sleep(2)

def create_kafka_consumer():
    conf = {
        'bootstrap.servers': KAFKA_BROKER,
        'group.id': KAFKA_GROUP_ID,
        'auto.offset.reset': 'earliest',
        'enable.auto.commit': False,
        'session.timeout.ms': 10000
    }
    return Consumer(conf)

def process_message(msg, cursor):
    if msg is None:
        return False
    if msg.error():
        if msg.error().code() == KafkaError._PARTITION_EOF:
            return False
        else:
            raise KafkaException(msg.error())

    # Get Kafka metadata for exact-once deduplication
    topic = msg.topic()
    partition = msg.partition()
    offset = msg.offset()
    offset_id = f"{topic}-{partition}-{offset}"
    
    val = msg.value()
    if val is None:
        return False
        
    payload_str = val.decode('utf-8')
    try:
        payload = json.loads(payload_str)
    except json.JSONDecodeError:
        print(f"Failed to decode JSON: {payload_str}")
        return False

    # Extract source event time if available, else use kafka timestamp
    source_ts_ms = payload.get("source", {}).get("ts_ms")
    if source_ts_ms:
        event_time = psycopg2.TimestampFromTicks(source_ts_ms / 1000.0)
    else:
        # fallback to current time
        event_time = psycopg2.TimestampFromTicks(time.time())

    # Idempotent insert into bronze layer.
    # If the message is redelivered (same topic/partition/offset), we DO NOTHING.
    cursor.execute("""
        INSERT INTO bronze.raw_events (offset_id, event_time, topic, payload)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (offset_id) DO NOTHING
    """, (offset_id, event_time, topic, Json(payload)))

    return True

def main():
    print("Starting Kafka consumer...")
    consumer = create_kafka_consumer()
    
    # Wait for Kafka to be ready
    while True:
        try:
            metadata = consumer.list_topics(timeout=5.0)
            if KAFKA_TOPIC in metadata.topics:
                break
            print(f"Topic {KAFKA_TOPIC} not found yet. Retrying...")
            time.sleep(2)
        except Exception as e:
            print(f"Kafka not ready: {e}. Retrying...")
            time.sleep(2)

    consumer.subscribe([KAFKA_TOPIC])
    conn = get_db_connection()
    cursor = conn.cursor()

    batch_size = 100
    msg_count = 0

    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                if msg_count > 0:
                    conn.commit()
                    consumer.commit(asynchronous=False)
                    print(f"Committed {msg_count} messages (timeout flush).")
                    msg_count = 0
                continue

            processed = process_message(msg, cursor)
            if processed:
                msg_count += 1

            # Commit offsets and DB transaction in batches
            if msg_count >= batch_size:
                conn.commit()
                consumer.commit(asynchronous=False)
                print(f"Committed {msg_count} messages to bronze layer.")
                msg_count = 0

    except KeyboardInterrupt:
        print("Aborted by user")
    finally:
        if msg_count > 0:
            conn.commit()
            consumer.commit(asynchronous=False)
        cursor.close()
        conn.close()
        consumer.close()

if __name__ == "__main__":
    main()
