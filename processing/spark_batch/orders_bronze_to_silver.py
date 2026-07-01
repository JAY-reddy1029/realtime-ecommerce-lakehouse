from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, current_timestamp
from pyspark.sql.types import DoubleType

spark = SparkSession.builder \
    .appName("OrdersBronzeToSilver") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

BRONZE_PATH = "/app/data/bronze/orders"
SILVER_PATH = "/app/data/silver/orders"

print("\nReading Bronze orders table...")
bronze_df = spark.read.format("delta").load(BRONZE_PATH)
bronze_count = bronze_df.count()
print(f"Bronze row count : {bronze_count}")

print("\nCleaning data...")

silver_df = bronze_df \
    .dropDuplicates(["order_id"]) \
    .filter(col("order_id").isNotNull()) \
    .filter(col("user_id").isNotNull()) \
    .filter(col("product_id").isNotNull()) \
    .filter(col("amount").isNotNull()) \
    .filter(col("amount").cast(DoubleType()) > 0) \
    .filter(col("status").isNotNull()) \
    .filter(col("status").isin(
        "placed", "confirmed", "shipped", "delivered", "cancelled"
    )) \
    .withColumn("timestamp", to_timestamp(col("timestamp"))) \
    .withColumn("processed_timestamp", current_timestamp()) \
    .drop("kafka_partition", "kafka_offset", "ingestion_timestamp")

silver_count = silver_df.count()

print(f"Silver row count : {silver_count}")
print(f"Rows removed     : {bronze_count - silver_count}")

print("\nWriting Silver orders table...")
silver_df.write \
    .format("delta") \
    .mode("overwrite") \
    .save(SILVER_PATH)

print("\nSilver orders table written successfully.")
print(f"Path: {SILVER_PATH}\n")