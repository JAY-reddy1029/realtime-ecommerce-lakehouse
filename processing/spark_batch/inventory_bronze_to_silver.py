from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, current_timestamp

spark = SparkSession.builder \
    .appName("InventoryBronzeToSilver") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

BRONZE_PATH = "/app/data/bronze/inventory"
SILVER_PATH = "/app/data/silver/inventory"

print("\nReading Bronze inventory table...")
bronze_df = spark.read.format("delta").load(BRONZE_PATH)
bronze_count = bronze_df.count()
print(f"Bronze row count : {bronze_count}")

print("\nCleaning data...")

silver_df = bronze_df \
    .dropDuplicates(["event_id"]) \
    .filter(col("event_id").isNotNull()) \
    .filter(col("product_id").isNotNull()) \
    .filter(col("warehouse_id").isNotNull()) \
    .filter(col("quantity").isNotNull()) \
    .filter(col("quantity") > 0) \
    .filter(col("action").isin("restock", "sale", "return", "adjustment")) \
    .withColumn("timestamp", to_timestamp(col("timestamp"))) \
    .withColumn("processed_timestamp", current_timestamp()) \
    .drop("kafka_partition", "kafka_offset", "ingestion_timestamp")

silver_count = silver_df.count()

print(f"Silver row count : {silver_count}")
print(f"Rows removed     : {bronze_count - silver_count}")

print("\nWriting Silver inventory table...")
silver_df.write \
    .format("delta") \
    .mode("overwrite") \
    .save(SILVER_PATH)

print("\nSilver inventory table written successfully.")
print(f"Path: {SILVER_PATH}\n")