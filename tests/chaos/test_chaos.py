import subprocess
import time
import psycopg2

def run_cmd(cmd):
    print(f"Running: {cmd}")
    subprocess.run(cmd, shell=True, check=True)

def get_db_connection(port, dbname):
    return psycopg2.connect(
        host="localhost",
        port=port,
        user="user",
        password="password",
        dbname=dbname
    )

def fetch_table_state(conn, table_name, is_warehouse=False):
    cursor = conn.cursor()
    if is_warehouse:
        query = f"SELECT order_id, status FROM {table_name} ORDER BY order_id"
    else:
        query = f"SELECT order_id, status FROM {table_name} ORDER BY order_id"
        
    cursor.execute(query)
    results = cursor.fetchall()
    cursor.close()
    return {str(row[0]): row[1] for row in results}

def main():
    print("=== Starting Chaos Test ===")
    
    # 1. Let the system run for a bit to generate load
    print("Waiting for 15 seconds to let generator create load...")
    time.sleep(15)
    
    # 2. Kill the consumer mid-flight
    print("Simulating consumer crash...")
    run_cmd("docker stop consumer")
    
    # 3. Wait while the generator continues to write to source
    print("Waiting for 10 seconds while consumer is dead...")
    time.sleep(10)
    
    # 4. Restart consumer
    print("Restarting consumer...")
    run_cmd("docker start consumer")
    
    print("Stopping generator to freeze source state...")
    run_cmd("docker kill generator")

    print("Waiting 60 seconds for consumer to restart, clear Kafka session timeout, rebalance, and flush all messages...")
    time.sleep(60)

    # 6. Run dbt to process the bronze -> silver layer
    print("Running dbt to perform idempotent merge...")
    run_cmd("docker run --rm --user root --network host -e DB_HOST=localhost -e DB_NAME=warehouse_db -e DB_USER=user -e DB_PASSWORD=password -v $(pwd)/src/dbt_project:/dbt:z -w /dbt ghcr.io/dbt-labs/dbt-postgres:1.7.3 run --profiles-dir .")
    
    # 7. Compare State
    print("Validating exact-once idempotency...")
    source_conn = get_db_connection(5432, "source_db")
    warehouse_conn = get_db_connection(5433, "warehouse_db")
    
    source_state = fetch_table_state(source_conn, "public.orders")
    warehouse_state = fetch_table_state(warehouse_conn, "silver.dim_orders", is_warehouse=True)
    
    source_count = len(source_state)
    warehouse_count = len(warehouse_state)
    
    print(f"Source Orders Count: {source_count}")
    print(f"Warehouse Orders Count: {warehouse_count}")
    
    assert source_count > 0, "No data was generated!"
    
    mismatches = 0
    for order_id, status in source_state.items():
        if order_id not in warehouse_state:
            print(f"MISSING: {order_id} not in warehouse!")
            mismatches += 1
        elif warehouse_state[order_id] != status:
            print(f"STATE MISMATCH: {order_id} Source: {status}, Warehouse: {warehouse_state[order_id]}")
            mismatches += 1
            
    if source_count != warehouse_count or mismatches > 0:
        print("[FAIL] CHAOS TEST FAILED: Pipeline is not idempotent or dropped data.")
        exit(1)
    else:
        print("[SUCCESS] CHAOS TEST PASSED: State exactly matches despite crashes and restarts!")
        
    source_conn.close()
    warehouse_conn.close()

if __name__ == "__main__":
    main()
