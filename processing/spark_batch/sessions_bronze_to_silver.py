from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, current_timestamp

spark = SparkSession.builder \
    .appName("SessionsBronzeToSilver") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

BRONZE_PATH = "/app/data/bronze/sessions"
SILVER_PATH = "/app/data/silver/sessions"

print("\nReading Bronze sessions table...")
bronze_df = spark.read.format("delta").load(BRONZE_PATH)
bronze_count = bronze_df.count()
print(f"Bronze row count : {bronze_count}")

print("\nCleaning data...")

silver_df = bronze_df \
    .dropDuplicates(["session_id"]) \
    .filter(col("session_id").isNotNull()) \
    .filter(col("user_id").isNotNull()) \
    .filter(col("page_viewed").isNotNull()) \
    .filter(col("duration_seconds").isNotNull()) \
    .filter(col("duration_seconds") > 0) \
    .filter(col("device").isin("mobile", "desktop", "tablet")) \
    .withColumn("timestamp", to_timestamp(col("timestamp"))) \
    .withColumn("processed_timestamp", current_timestamp()) \
    .drop("kafka_partition", "kafka_offset", "ingestion_timestamp")

silver_count = silver_df.count()

print(f"Silver row count : {silver_count}")
print(f"Rows removed     : {bronze_count - silver_count}")

print("\nWriting Silver sessions table...")
silver_df.write \
    .format("delta") \
    .mode("overwrite") \
    .save(SILVER_PATH)

print("\nSilver sessions table written successfully.")
print(f"Path: {SILVER_PATH}\n")