from pyspark.sql import SparkSession
from . import config
import logging

logger = logging.getLogger(__name__)

def create_spark_session(warehouse_path: str | None = None) -> SparkSession:
    """
    Creates a SparkSession configured for Iceberg with a Hadoop catalog.
    
    Args:
        warehouse_path: Optional override for the warehouse path.
                        If None, uses the configuration default.
    """
    if warehouse_path is None:
        warehouse_path = config.get_warehouse_path()
        
    catalog_name = config.CATALOG_NAME
    
    logger.info(f"Creating SparkSession with Iceberg catalog '{catalog_name}' at '{warehouse_path}'")

    # Iceberg Spark Runtime package
    # Using 3.5_2.12 as a safe default for Spark 3.5. Adjust if using a different Spark version.
    # For a generic "Spark 3.x" requirement, 3.5 is a good target.
    iceberg_pkg = "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.3"

    builder = SparkSession.builder \
        .appName("LogisticsIcebergApp") \
        .master("local[*]") \
        .config("spark.jars.packages", iceberg_pkg) \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config(f"spark.sql.catalog.{catalog_name}", "org.apache.iceberg.spark.SparkCatalog") \
        .config(f"spark.sql.catalog.{catalog_name}.type", "hadoop") \
        .config(f"spark.sql.catalog.{catalog_name}.warehouse", warehouse_path)
        
    spark = builder.getOrCreate()
    return spark
