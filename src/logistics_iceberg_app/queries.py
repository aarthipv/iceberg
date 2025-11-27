from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import logging

logger = logging.getLogger(__name__)

def run_sample_queries(spark: SparkSession, table_name: str = "logistics.delivery_logs") -> None:
    """
    Runs sample analytics and metadata queries on the Iceberg table.
    """
    logger.info(f"Reading from Iceberg table '{table_name}'...")
    df = spark.table(table_name)
    
    # 1. Count total rows
    total_rows = df.count()
    print(f"\n--- Total Rows: {total_rows} ---")
    
    # 2. Basic Analytics
    # Attempt to find relevant columns dynamically or use common ones if they exist
    columns = [c.lower() for c in df.columns]
    
    # Example: Group by a categorical column if present (e.g., 'city', 'partner', 'status')
    # We'll look for 'city' or 'partner' or similar.
    group_col = None
    for candidate in ['city', 'partner', 'agent_id', 'status', 'vehicle_type']:
        if candidate in columns:
            group_col = candidate
            break
            
    if group_col:
        print(f"\n--- Top 5 entries by {group_col} ---")
        df.groupBy(group_col).count().orderBy(F.col("count").desc()).show(5)
    else:
        print("\n(No suitable categorical column found for grouping example)")

    # Example: Summary stats for numerical columns
    # We'll look for 'delivery_time', 'distance', 'cost', 'rating'
    num_cols = [c for c in columns if c in ['delivery_time', 'distance', 'cost', 'rating', 'order_amount', 'time_taken(min)']]
    if num_cols:
        print(f"\n--- Summary Statistics for {num_cols} ---")
        df.select(num_cols).summary().show()
        
    # 3. Metadata Queries
    print("\n--- Iceberg Metadata: History ---")
    try:
        spark.table(f"{table_name}.history").show(truncate=False)
    except Exception as e:
        logger.warning(f"Could not query history: {e}")

    print("\n--- Iceberg Metadata: Snapshots ---")
    try:
        spark.table(f"{table_name}.snapshots").select("committed_at", "snapshot_id", "operation", "summary").show(truncate=False)
    except Exception as e:
        logger.warning(f"Could not query snapshots: {e}")
