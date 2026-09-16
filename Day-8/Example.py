"""
Day 08 — .explain() for query plans, and .cache() for reuse.
Run: python example.py
"""

import time
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Day08-ExplainAndCaching").getOrCreate()

data = [
    ("North", "Widget", 1200),
    ("North", "Gadget", 300),
    ("South", "Widget", 800),
    ("South", "Gadget", 1500),
    ("East", "Widget", 400),
] * 1000  # repeat to make the example slightly more realistic in size
df = spark.createDataFrame(data, ["region", "product", "revenue"])

# --- .explain(): see the query plan before/without fully running it ---
result = df.filter(df.revenue > 500).groupBy("region").sum("revenue")

print("=== Physical plan (read bottom to top) ===")
result.explain()

print("=== Formatted plan (easier to read) ===")
result.explain(mode="formatted")

# --- Caching: reuse a filtered DataFrame across multiple aggregations ---
filtered = df.filter(df.revenue > 500).cache()

start = time.time()
filtered.count()  # first action: computes AND caches the result
print(f"First count (computes + caches): {time.time() - start:.3f}s")

start = time.time()
filtered.groupBy("region").sum("revenue").show()  # reuses cached data
print(f"groupBy after cache: {time.time() - start:.3f}s")

start = time.time()
filtered.groupBy("product").avg("revenue").show()  # also reuses cached data
print(f"another groupBy after cache: {time.time() - start:.3f}s")

filtered.unpersist()  # free the memory once done

spark.stop()
