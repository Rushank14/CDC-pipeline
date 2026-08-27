import os
import time
import uuid
import random
import psycopg2
from threading import Thread

DB_HOST = os.environ.get("DB_HOST", "localhost")
DB_PORT = os.environ.get("DB_PORT", "5432")
DB_USER = os.environ.get("DB_USER", "user")
DB_PASSWORD = os.environ.get("DB_PASSWORD", "password")
DB_NAME = os.environ.get("DB_NAME", "source_db")
CHAOS_MODE = os.environ.get("CHAOS_MODE", "true").lower() == "true"

def get_connection():
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
            print(f"Waiting for database: {e}")
            time.sleep(2)

def simulate_order_lifecycle():
    conn = get_connection()
    conn.autocommit = True
    cursor = conn.cursor()
    
    order_id = str(uuid.uuid4())
    user_id = str(uuid.uuid4())
    amount = round(random.uniform(10.0, 500.0), 2)
    
    # 1. INSERT
    cursor.execute("""
        INSERT INTO orders (order_id, user_id, amount, status)
        VALUES (%s, %s, %s, 'PENDING')
    """, (order_id, user_id, amount))
    print(f"INSERTED order {order_id}")
    
    if CHAOS_MODE:
        time.sleep(random.uniform(0.1, 2.0))
    else:
        time.sleep(1)
        
    # 2. UPDATE
    cursor.execute("""
        UPDATE orders SET status = 'SHIPPED', updated_at = CURRENT_TIMESTAMP
        WHERE order_id = %s
    """, (order_id,))
    print(f"UPDATED order {order_id} to SHIPPED")
    
    if CHAOS_MODE:
        time.sleep(random.uniform(0.1, 2.0))
    else:
        time.sleep(1)
        
    # 3. DELETE or Final UPDATE
    if random.random() > 0.8:
        cursor.execute("DELETE FROM orders WHERE order_id = %s", (order_id,))
        print(f"DELETED order {order_id}")
    else:
        cursor.execute("""
            UPDATE orders SET status = 'DELIVERED', updated_at = CURRENT_TIMESTAMP
            WHERE order_id = %s
        """, (order_id,))
        print(f"UPDATED order {order_id} to DELIVERED")

    cursor.close()
    conn.close()

def main():
    print("Starting data generator...")
    while True:
        if CHAOS_MODE:
            # Concurrent transactions to simulate load
            threads = []
            for _ in range(random.randint(1, 5)):
                t = Thread(target=simulate_order_lifecycle)
                t.start()
                threads.append(t)
            for t in threads:
                t.join()
        else:
            simulate_order_lifecycle()
            
        time.sleep(random.uniform(0.5, 3.0))

if __name__ == "__main__":
    main()
