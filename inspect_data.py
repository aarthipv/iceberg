from src.logistics_iceberg_app import spark_session

spark = spark_session.create_spark_session()
table_name = "logistics.db.delivery_logs"

print(f"--- Schema of {table_name} ---")
spark.table(table_name).printSchema()

print(f"\n--- Sample Data from {table_name} ---")
spark.table(table_name).show(20, truncate=False)

print(f"\n--- Total Count ---")
print(spark.table(table_name).count())
