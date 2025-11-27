# Logistics Iceberg App

This project demonstrates how to build a modern data engineering pipeline using Python, Apache Spark, and Apache Iceberg. It downloads a logistics dataset from Kaggle, ingests it into an Iceberg table, and performs analytical queries.

## Features

- **Data Loading**: Automatically downloads the "Delivery Logistics Dataset" from Kaggle using `kagglehub`.
- **Data Storage**: Uses Apache Iceberg as the table format for reliable, ACID-compliant storage.
- **Processing Engine**: Uses Apache Spark (PySpark) for data ingestion and querying.
- **Analytics**: Demonstrates sample SQL queries and Iceberg metadata inspection (history, snapshots).

## Prerequisites

- **Python 3.10+**
- **Java 11+** (Required for Apache Spark)
- **Kaggle Account**: You may need to configure Kaggle credentials if the dataset requires it (though `kagglehub` often handles public datasets seamlessly).

## Setup

1.  **Clone the repository** (if applicable) or navigate to the project directory.

2.  **Create a virtual environment**:
    ```bash
    python3 -m venv venv
    source venv/bin/activate
    ```

3.  **Install dependencies**:
    ```bash
    pip install -r requirements.txt
    ```

## Usage

Run the main application using the CLI entrypoint:

```bash
python -m src.logistics_iceberg_app.main
```

### Optional Arguments

- `--file-path`: Specify a particular CSV file within the Kaggle dataset if needed.
- `--warehouse-path`: Override the default Iceberg warehouse location (default: `/tmp/logistics_iceberg_warehouse`).

Example:
```bash
python -m src.logistics_iceberg_app.main --warehouse-path /tmp/my_custom_warehouse
```

## Project Structure

- `src/logistics_iceberg_app/`: Source code.
    - `config.py`: Configuration settings.
    - `spark_session.py`: SparkSession creation with Iceberg config.
    - `kaggle_loader.py`: Kaggle dataset loading logic.
    - `ingest.py`: Logic to write Pandas DataFrames to Iceberg.
    - `queries.py`: Sample analytics and metadata queries.
    - `main.py`: Main execution flow.

## Notes

- The application runs Spark in `local[*]` mode.
- The Iceberg catalog is configured as a Hadoop catalog pointing to the local filesystem.
