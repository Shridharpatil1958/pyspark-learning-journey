"""
Day 15 — Structured Streaming: reading a growing folder of files as a stream.

How to try this yourself:
1. Run this script — it starts watching ./stream_input/ for new CSV files.
2. While it's running, copy a CSV file into ./stream_input/ (matching the
   schema below) — you'll see new results appear in the console output.
3. Press Ctrl+C to stop.
"""

import os
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

spark = SparkSession.builder.appName("Day15-StructuredStreaming").getOrCreate()

os.makedirs("stream_input", exist_ok=True)

# Streaming reads need an explicit schema (no inferSchema on data that hasn't arrived yet)
schema = StructType([
    StructField("region", StringType(), True),
    StructField("product", StringType(), True),
    StructField("revenue", IntegerType(), True),
])

# --- Set up the stream (same API shape as spark.read, just "readStream") ---
stream_df = spark.readStream.csv("stream_input", schema=schema, header=True)

# --- Transformations — identical to everything learned in batch (Days 1-14) ---
result = stream_df.groupBy("region").sum("revenue")

# --- Write the stream out — "writeStream" instead of "write" ---
query = (
    result.writeStream
    .outputMode("complete")   # rewrite the full aggregated result each update
    .format("console")        # print to console for this demo
    .start()
)

print("Streaming query started. Drop CSV files into ./stream_input/ to see updates.")
print("Example file content to drop in:")
print("region,product,revenue")
print("North,Widget,500")

query.awaitTermination()
