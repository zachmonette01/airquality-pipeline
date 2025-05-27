"""
transform/json_to_parquet.py
----------------------------
• Reads every AirNow JSON file in s3a://raw-data/
• Handles the JSON ‑‑multiline array format
• Cleans column names, adds ingest_ts
• Writes Parquet to s3a://curated-data/

Run inside the Spark container:
    docker exec -it spark \
      /opt/bitnami/spark/bin/spark-submit /workspace/transform/json_to_parquet.py
"""

import os
from pyspark.sql import SparkSession, functions as F

# ---------------------------------------------------------------------------
# 1 Connection details for MinIO
# ---------------------------------------------------------------------------
ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minio")
SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minio123")
ENDPOINT   = os.getenv("MINIO_ENDPOINT", "http://host.docker.internal:9000")

RAW_PATH     = "s3a://raw-data/*.json"
CURATED_PATH = "s3a://curated-data/airnow"

# ---------------------------------------------------------------------------
# 2 Spark session with S3A configs
# ---------------------------------------------------------------------------
spark = (
    SparkSession.builder.appName("airquality-json→parquet")
    .config("spark.hadoop.fs.s3a.endpoint", ENDPOINT)
    .config("spark.hadoop.fs.s3a.access.key", ACCESS_KEY)
    .config("spark.hadoop.fs.s3a.secret.key", SECRET_KEY)
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

# ---------------------------------------------------------------------------
# 3 Read JSON with multiline = true
# ---------------------------------------------------------------------------
print("🔄  Reading raw JSON from", RAW_PATH)
df = (
    spark.read.format("json")
    .option("multiline", "true")   # <-- key fix for array‑style JSON
    .option("mode", "PERMISSIVE")
    .load(RAW_PATH)
)

# Drop the _corrupt_record helper column if it exists
if "_corrupt_record" in df.columns:
    df = df.filter(F.col("_corrupt_record").isNull()).drop("_corrupt_record")

if df.rdd.isEmpty():
    print("⚠️  No valid JSON rows found; exiting.")
    spark.stop()
    raise SystemExit

# ---------------------------------------------------------------------------
# 4 Transform: lower‑case cols, add timestamp
# ---------------------------------------------------------------------------
df_clean = (
    df.select([F.col(c).alias(c.lower()) for c in df.columns])
      .withColumn("ingest_ts", F.current_timestamp())
)

# ---------------------------------------------------------------------------
# 5 Write Parquet
# ---------------------------------------------------------------------------
print(f"💾  Writing cleaned Parquet to {CURATED_PATH}")
df_clean.write.mode("overwrite").parquet(CURATED_PATH)
print("✅  Parquet written successfully!")

spark.stop()
