from src.logistics_iceberg_app import spark_session

spark = spark_session.create_spark_session()

print("Catalogs:")
spark.sql("SHOW CATALOGS").show()

print("\nDatabases in 'logistics':")
try:
    spark.sql("SHOW DATABASES IN logistics").show()
except Exception as e:
    print(e)

print("\nTables in 'logistics.db':")
try:
    spark.sql("SHOW TABLES IN logistics.db").show()
except Exception as e:
    print(e)

print("\nTrying to access 'logistics.db.delivery_logs.snapshots':")
try:
    spark.table("logistics.db.delivery_logs.snapshots").show(truncate=False)
except Exception as e:
    print(e)
