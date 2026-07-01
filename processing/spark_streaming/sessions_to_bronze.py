from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

spark = SparkSession.builder \
    .appName("SessionsToBronze") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

session_schema = StructType([
    StructField("session_id", StringType()),
    StructField("user_id", StringType()),
    StructField("page_viewed", StringType()),
    StructField("duration_seconds", IntegerType()),
    StructField("device", StringType()),
    StructField("timestamp", StringType()),
])

raw_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:29092") \
    .option("subscribe", "user_sessions") \
    .option("startingOffsets", "earliest") \
    .load()

bronze_stream = raw_stream.select(
    from_json(col("value").cast("string"), session_schema).alias("data"),
    col("partition").alias("kafka_partition"),
    col("offset").alias("kafka_offset"),
).select(
    "data.*",
    "kafka_partition",
    "kafka_offset",
).withColumn("ingestion_timestamp", current_timestamp())

print("Streaming sessions into Delta Lake Bronze layer...")
print("Table path: /app/data/bronze/sessions")
print("Press Ctrl+C to stop.\n")

query = bronze_stream.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", "/app/data/checkpoints/sessions_bronze") \
    .start("/app/data/bronze/sessions")

query.awaitTermination()