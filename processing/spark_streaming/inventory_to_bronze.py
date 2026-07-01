from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

spark = SparkSession.builder \
    .appName("InventoryToBronze") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

inventory_schema = StructType([
    StructField("event_id", StringType()),
    StructField("product_id", StringType()),
    StructField("warehouse_id", StringType()),
    StructField("quantity", IntegerType()),
    StructField("action", StringType()),
    StructField("timestamp", StringType()),
])

raw_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:29092") \
    .option("subscribe", "inventory_updates") \
    .option("startingOffsets", "earliest") \
    .load()

bronze_stream = raw_stream.select(
    from_json(col("value").cast("string"), inventory_schema).alias("data"),
    col("partition").alias("kafka_partition"),
    col("offset").alias("kafka_offset"),
).select(
    "data.*",
    "kafka_partition",
    "kafka_offset",
).withColumn("ingestion_timestamp", current_timestamp())

print("Streaming inventory into Delta Lake Bronze layer...")
print("Table path: /app/data/bronze/inventory")
print("Press Ctrl+C to stop.\n")

query = bronze_stream.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", "/app/data/checkpoints/inventory_bronze") \
    .start("/app/data/bronze/inventory")

query.awaitTermination()