import logging
import sys
import os
import random
import time

# Add src to path so we can import modules
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

from src.logistics_iceberg_app import config, spark_session

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def run_crud_demo():
    spark = spark_session.create_spark_session()
    table_name = f"{config.CATALOG_NAME}.delivery_logs"
    
    print(f"\n{'='*80}")
    print(f"ICEBERG CRUD DEMO: {table_name}")
    print(f"{'='*80}\n")

    # ---------------------------------------------------------
    # 1. INITIAL STATE
    # ---------------------------------------------------------
    print("--- 1. INITIAL STATE ---")
    initial_count = spark.table(table_name).count()
    print(f"Initial Row Count: {initial_count}")
    
    # Show a specific record we might update later (e.g., first one)
    target_row = spark.sql(f"SELECT * FROM {table_name} LIMIT 1").collect()[0]
    target_id = target_row['delivery_id']
    print(f"Sample Record (ID: {target_id}):")
    spark.sql(f"SELECT delivery_id, delivery_partner, delivery_status, delivery_cost FROM {table_name} WHERE delivery_id = {target_id}").show()

    # ---------------------------------------------------------
    # 2. INSERT (Add a new record)
    # ---------------------------------------------------------
    print("\n--- 2. INSERT OPERATION ---")
    # Schema says delivery_id is double. We'll use a large number.
    new_id = 99999999.0
    print(f"Inserting new record with ID: {new_id}")
    
    # Construct a dummy record matching the schema
    # Note: We need to match the schema columns. 
    # Based on previous output, we have columns like delivery_id, delivery_partner, vehicle_type, etc.
    # We'll use a SQL INSERT for simplicity.
    
    # Let's get the columns first to ensure we insert correctly
    columns = spark.table(table_name).columns
    # Create dummy values for all columns
    # We'll just select the first row, modify the ID, and insert it back as a new row
    
    spark.sql(f"""
        INSERT INTO {table_name}
        SELECT 
            {new_id} as delivery_id,
            delivery_partner,
            package_type,
            vehicle_type,
            delivery_mode,
            region,
            weather_condition,
            distance_km,
            package_weight_kg,
            delivery_time_hours,
            expected_time_hours,
            delayed,
            delivery_status,
            delivery_rating,
            999.99 as delivery_cost -- Distinctive cost
        FROM {table_name} LIMIT 1
    """)
    
    print("Insert committed.")
    print(f"New Row Count: {spark.table(table_name).count()}")
    spark.sql(f"SELECT delivery_id, delivery_cost FROM {table_name} WHERE delivery_id = {new_id}").show()

    # ---------------------------------------------------------
    # 3. UPDATE (Modify an existing record)
    # ---------------------------------------------------------
    print("\n--- 3. UPDATE OPERATION ---")
    print(f"Updating cost for record ID: {target_id}")
    
    # Iceberg supports row-level updates
    spark.sql(f"""
        UPDATE {table_name}
        SET delivery_cost = 0.0, delivery_status = 'UPDATED'
        WHERE delivery_id = {target_id}
    """)
    
    print("Update committed.")
    spark.sql(f"SELECT delivery_id, delivery_partner, delivery_status, delivery_cost FROM {table_name} WHERE delivery_id = {target_id}").show()

    # ---------------------------------------------------------
    # 4. DELETE (Remove a record)
    # ---------------------------------------------------------
    print("\n--- 4. DELETE OPERATION ---")
    print(f"Deleting the newly inserted record ID: {new_id}")
    
    spark.sql(f"DELETE FROM {table_name} WHERE delivery_id = {new_id}")
    
    print("Delete committed.")
    print(f"Final Row Count: {spark.table(table_name).count()}")
    
    # Verify it's gone
    count_new = spark.sql(f"SELECT * FROM {table_name} WHERE delivery_id = {new_id}").count()
    print(f"Records with ID {new_id}: {count_new}")

    # ---------------------------------------------------------
    # 5. METADATA INSPECTION (Show what happened)
    # ---------------------------------------------------------
    print("\n--- 5. ICEBERG SNAPSHOTS (Audit Trail) ---")
    # Show last 5 snapshots to see the sequence of operations
    spark.sql(f"""
        SELECT committed_at, snapshot_id, operation, summary['added-records'] as added, summary['deleted-records'] as deleted
        FROM {table_name}.snapshots
        ORDER BY committed_at DESC
        LIMIT 5
    """).show(truncate=False)

    spark.stop()

if __name__ == "__main__":
    run_crud_demo()
