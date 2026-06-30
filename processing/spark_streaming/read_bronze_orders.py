from pyspark.sql import SparkSession
from pyspark.sql.functions import desc

spark = SparkSession.builder \
    .appName("ReadBronzeOrders") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

df = spark.read.format("delta").load("/app/data/bronze/orders")

print(f"\nTotal rows currently in Bronze orders table: {df.count()}\n")

print("Most recent 10 orders ingested:")
df.orderBy(desc("ingestion_timestamp")).show(10, truncate=False)

print("Schema:")
df.printSchema()