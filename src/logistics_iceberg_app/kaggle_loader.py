import kagglehub
from kagglehub import KaggleDatasetAdapter
import pandas as pd
import logging
from . import config

logger = logging.getLogger(__name__)

def load_delivery_dataset(file_path: str | None = None) -> pd.DataFrame:
    """
    Uses kagglehub.load_dataset with KaggleDatasetAdapter.PANDAS
    to load the 'kundanbedmutha/delivery-logistics-dataset-india-multi-partner' dataset.
    
    Args:
        file_path: Optional path to a specific file within the dataset.
                   If None, it relies on kagglehub to pick the default or requires explicit path if multiple files exist.
                   For this specific dataset, we might need to specify the CSV if there are multiple.
                   
    Returns:
        pd.DataFrame: The loaded dataset.
    """
    dataset_handle = config.DATASET_HANDLE
    
    logger.info(f"Loading dataset '{dataset_handle}' using kagglehub...")
    if file_path:
        logger.info(f"Targeting specific file path: {file_path}")
    else:
        logger.info("No specific file path provided, loading default.")

    # The dataset contains a CSV file. We must specify it if file_path is not provided.
    if file_path is None or file_path == "":
        file_path = "Delivery_Logistics.csv"

    try:
        df = kagglehub.load_dataset(
            KaggleDatasetAdapter.PANDAS,
            dataset_handle,
            file_path,
        )
        
        logger.info(f"Successfully loaded dataset. Shape: {df.shape}")
        return df
        
    except Exception as e:
        logger.error(f"Failed to load dataset: {e}")
        raise
