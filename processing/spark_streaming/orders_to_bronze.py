from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

spark = SparkSession.builder \
    .appName("OrdersToBronze") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

order_schema = StructType([
    StructField("order_id", StringType()),
    StructField("user_id", StringType()),
    StructField("product_id", StringType()),
    StructField("amount", DoubleType()),
    StructField("status", StringType()),
    StructField("timestamp", StringType()),
])

raw_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:29092") \
    .option("subscribe", "orders") \
    .option("startingOffsets", "earliest") \
    .load()

bronze_stream = raw_stream.select(
    from_json(col("value").cast("string"), order_schema).alias("data"),
    col("partition").alias("kafka_partition"),
    col("offset").alias("kafka_offset"),
).select(
    "data.*",
    "kafka_partition",
    "kafka_offset",
).withColumn("ingestion_timestamp", current_timestamp())

print("Streaming orders into Delta Lake Bronze layer...")
print("Table path: /app/data/bronze/orders")
print("Press Ctrl+C to stop.\n")

query = bronze_stream.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", "/app/data/checkpoints/orders_bronze") \
    .start("/app/data/bronze/orders")

query.awaitTermination()