from pyspark.sql import SparkSession
import pandas as pd
import logging
from . import config

logger = logging.getLogger(__name__)

def ingest_to_iceberg_table(
    spark: SparkSession,
    df: pd.DataFrame,
    table_name: str = "logistics.delivery_logs",
) -> None:
    """
    Converts the pandas DataFrame to a Spark DataFrame and writes it
    into an Iceberg table within the configured catalog.
    Creates the table if it does not exist.
    """
    logger.info("Converting Pandas DataFrame to Spark DataFrame...")
    
    # 1) Convert pandas -> Spark DataFrame
    # Ensure column names are clean (no spaces, lower_snake_case)
    df.columns = [c.strip().replace(" ", "_").lower() for c in df.columns]
    
    # Handle potential type issues if needed (e.g. object -> string)
    # For simplicity, we let Spark infer.
    spark_df = spark.createDataFrame(df)
    
    logger.info(f"Writing to Iceberg table '{table_name}'...")
    
    # 3) Write into Iceberg using Spark
    # Using writeTo(...).createOrReplace() as it handles both creation and replacement nicely.
    try:
        # Ensure namespace exists (for Hive catalog)
        # table_name is expected to be "catalog.namespace.table"
        parts = table_name.split('.')
        if len(parts) == 3:
            catalog = parts[0]
            namespace = parts[1]
            spark.sql(f"CREATE DATABASE IF NOT EXISTS {catalog}.{namespace}")
        
        spark_df.writeTo(table_name) \
            .using("iceberg") \
            .createOrReplace()
            
        logger.info(f"Successfully wrote data to '{table_name}'.")
        
    except Exception as e:
        logger.error(f"Failed to write to Iceberg table: {e}")
        raise
