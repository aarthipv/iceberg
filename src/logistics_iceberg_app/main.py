import argparse
import logging
import sys
import os

# Add src to path so we can import modules if running as script
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

from src.logistics_iceberg_app import config, spark_session, kaggle_loader, ingest, queries

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def main():
    parser = argparse.ArgumentParser(description="Logistics Iceberg App CLI")
    parser.add_argument("--file-path", help="Path to the specific file in the Kaggle dataset", default=None)
    parser.add_argument("--warehouse-path", help="Path to the Iceberg warehouse", default=None)
    
    args = parser.parse_args()
    
    # 1. Create Spark Session
    logger.info("Initializing Spark Session...")
    spark = spark_session.create_spark_session(warehouse_path=args.warehouse_path)
    
    try:
        # 2. Load Dataset
        logger.info("Loading dataset from Kaggle...")
        df_pandas = kaggle_loader.load_delivery_dataset(file_path=args.file_path)
        
        # 3. Ingest to Iceberg
        logger.info("Ingesting data into Iceberg...")
        table_name = f"{config.CATALOG_NAME}.delivery_logs"
        ingest.ingest_to_iceberg_table(spark, df_pandas, table_name)
        
        # 4. Run Queries
        logger.info("Running analytics and metadata queries...")
        queries.run_sample_queries(spark, table_name)
        
        logger.info("Workflow completed successfully.")
        
    except Exception as e:
        logger.error(f"Workflow failed: {e}")
        sys.exit(1)
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
