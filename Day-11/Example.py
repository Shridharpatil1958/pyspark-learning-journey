"""
Day 11 — repartition() vs coalesce(): controlling partition count.
Run: python example.py
"""

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Day11-RepartitionVsCoalesce").getOrCreate()

data = [
    ("North", "Widget", 1200),
    ("North", "Gadget", 300),
    ("South", "Widget", 800),
    ("South", "Gadget", 1500),
    ("East", "Widget", 400),
] * 2000
df = spark.createDataFrame(data, ["region", "product", "revenue"])

print(f"Original partition count: {df.rdd.getNumPartitions()}")

# --- repartition: full shuffle, can increase or decrease ---
more_partitions = df.repartition(8)
print(f"After repartition(8): {more_partitions.rdd.getNumPartitions()}")

by_column = df.repartition("region")
print(f"After repartition('region'): {by_column.rdd.getNumPartitions()}")

# --- coalesce: cheap merge, can only decrease ---
fewer_partitions = df.coalesce(2)
print(f"After coalesce(2): {fewer_partitions.rdd.getNumPartitions()}")

# Trying to INCREASE with coalesce does nothing useful — it's capped at the current count
attempted_increase = fewer_partitions.coalesce(10)
print(f"coalesce(10) on a 2-partition df stays at: {attempted_increase.rdd.getNumPartitions()}")

# --- Practical use: controlling output file count when writing ---
print("Writing WITHOUT coalesce (one file per partition)...")
df.write.mode("overwrite").parquet("output_many_files")

print("Writing WITH coalesce(2) first (just 2 output files)...")
df.coalesce(2).write.mode("overwrite").parquet("output_few_files")

spark.stop()
