"""
Day 05 — Parquet: writing, reading, overwrite/append, partitioned writes.
Run: python example.py
"""

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Day05-Parquet").getOrCreate()

data = [
    ("North", "Widget", 1200),
    ("North", "Gadget", 300),
    ("South", "Widget", 800),
    ("South", "Gadget", 1500),
    ("East", "Widget", 400),
]
df = spark.createDataFrame(data, ["region", "product", "revenue"])

# --- Basic write and read ---
df.write.mode("overwrite").parquet("sales_data.parquet")
df2 = spark.read.parquet("sales_data.parquet")

print("=== Read back from Parquet (schema came from the file, no inferSchema needed) ===")
df2.printSchema()
df2.show()

# --- Partitioned write: splits output into folders by region ---
df.write.mode("overwrite").partitionBy("region").parquet("sales_by_region")

print("=== Reading back the partitioned data, filtering on the partition column ===")
partitioned = spark.read.parquet("sales_by_region")
partitioned.filter(partitioned.region == "North").show()

# --- Append mode: add more rows without overwriting ---
more_data = [("East", "Gadget", 900)]
more_df = spark.createDataFrame(more_data, ["region", "product", "revenue"])
more_df.write.mode("append").parquet("sales_data.parquet")

print("=== After appending, sales_data.parquet has the extra row too ===")
spark.read.parquet("sales_data.parquet").show()

spark.stop()
