from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("VerifyBronzeTables") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

tables = {
    "orders":    "/app/data/bronze/orders",
    "sessions":  "/app/data/bronze/sessions",
    "inventory": "/app/data/bronze/inventory",
}

print("\n" + "="*60)
print("  BRONZE LAYER VERIFICATION REPORT")
print("="*60)

for name, path in tables.items():
    df = spark.read.format("delta").load(path)
    count = df.count()
    cols = len(df.columns)
    print(f"\n  {name.upper()} TABLE")
    print(f"  Rows    : {count}")
    print(f"  Columns : {cols}")
    print(f"  Schema  : {', '.join(df.columns)}")

print("\n" + "="*60)
print("  All 3 Bronze tables verified successfully.")
print("="*60 + "\n")