import os

# Default configuration
DEFAULT_WAREHOUSE_PATH = "file:///tmp/logistics_iceberg_warehouse"
CATALOG_NAME = "logistics"
DATASET_HANDLE = "kundanbedmutha/delivery-logistics-dataset-india-multi-partner"

def get_warehouse_path() -> str:
    """Returns the warehouse path from env var or default."""
    return os.environ.get("ICEBERG_WAREHOUSE_PATH", DEFAULT_WAREHOUSE_PATH)
