import logging
import sys
import os
from pyspark.sql.functions import col

# Add src to path so we can import modules
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

from src.logistics_iceberg_app import config, spark_session

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def run_time_travel_demo():
    spark = spark_session.create_spark_session()
    table_name = f"{config.CATALOG_NAME}.db.delivery_logs"
    
    print(f"\n{'='*80}")
    print(f"ICEBERG TIME TRAVEL DEMO: {table_name}")
    print(f"{'='*80}\n")

    # 1. Get Snapshots
    print("Fetching snapshots to identify interesting points in time...")
    snapshots_df = spark.table(f"{table_name}.snapshots").select(
        "committed_at", "snapshot_id", "operation", "summary"
    ).orderBy("committed_at")
    
    snapshots = snapshots_df.collect()
    
    # We want to find:
    # 1. A snapshot where we ADDED a record (operation='append')
    # 2. The snapshot immediately BEFORE that (to show it wasn't there)
    # 3. The snapshot where we DELETED it (operation='delete')
    
    append_snapshot = None
    delete_snapshot = None
    
    for s in snapshots:
        if s['operation'] == 'append':
            append_snapshot = s
        elif s['operation'] == 'delete':
            delete_snapshot = s
            
    if not append_snapshot:
        print("Could not find an 'append' operation to demonstrate time travel. Run crud_demo.py first!")
        return

    print(f"Found APPEND snapshot: {append_snapshot['snapshot_id']} (at {append_snapshot['committed_at']})")
    
    # ---------------------------------------------------------
    # 2. Query: AFTER INSERT
    # ---------------------------------------------------------
    print(f"\n--- Querying VERSION AS OF {append_snapshot['snapshot_id']} (After Insert) ---")
    df_after_insert = spark.read.option("snapshot-id", append_snapshot['snapshot_id']).table(table_name)
    
    # We know the inserted ID was 99999999.0 from crud_demo
    target_id = 99999999.0
    
    count_after = df_after_insert.filter(col("delivery_id") == target_id).count()
    print(f"Count of ID {target_id}: {count_after}")
    if count_after > 0:
        df_after_insert.filter(col("delivery_id") == target_id).select("delivery_id", "delivery_cost", "delivery_status").show()

    # ---------------------------------------------------------
    # 3. Query: BEFORE INSERT (Time Travel Backwards)
    # ---------------------------------------------------------
    # We'll try to find the parent of the append snapshot, or just pick an earlier one
    # For simplicity, let's just use the timestamp of the append snapshot minus 1 second
    # Or better, use the snapshot ID of the previous snapshot in our list
    
    prev_snapshot = None
    for i, s in enumerate(snapshots):
        if s['snapshot_id'] == append_snapshot['snapshot_id'] and i > 0:
            prev_snapshot = snapshots[i-1]
            break
            
    if prev_snapshot:
        print(f"\n--- Querying VERSION AS OF {prev_snapshot['snapshot_id']} (Before Insert) ---")
        df_before = spark.read.option("snapshot-id", prev_snapshot['snapshot_id']).table(table_name)
        count_before = df_before.filter(col("delivery_id") == target_id).count()
        print(f"Count of ID {target_id}: {count_before}")
        df_before.filter(col("delivery_id") == target_id).select("delivery_id", "delivery_cost", "delivery_status").show()
    else:
        print("Could not find a previous snapshot.")

    # ---------------------------------------------------------
    # 4. Query: AFTER DELETE (Current State)
    # ---------------------------------------------------------
    if delete_snapshot:
        print(f"\n--- Querying VERSION AS OF {delete_snapshot['snapshot_id']} (After Delete) ---")
        df_after_delete = spark.read.option("snapshot-id", delete_snapshot['snapshot_id']).table(table_name)
        count_delete = df_after_delete.filter(col("delivery_id") == target_id).count()
        print(f"Count of ID {target_id}: {count_delete}")
        df_after_delete.filter(col("delivery_id") == target_id).select("delivery_id", "delivery_cost", "delivery_status").show()

    spark.stop()

if __name__ == "__main__":
    run_time_travel_demo()
