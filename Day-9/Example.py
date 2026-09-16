"""
Day 09 — broadcast joins: forcing a small table to be copied to every
executor instead of shuffling both tables.
Run: python example.py
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import broadcast

spark = SparkSession.builder.appName("Day09-BroadcastJoins").getOrCreate()

# Small "dimension" table — a handful of regions
regions = spark.createDataFrame(
    [("North", "Zone A"), ("South", "Zone B"), ("East", "Zone C")],
    ["region", "zone"],
)

# Larger "fact" table — many sales rows
sales_data = [
    ("North", "Widget", 1200),
    ("North", "Gadget", 300),
    ("South", "Widget", 800),
    ("South", "Gadget", 1500),
    ("East", "Widget", 400),
] * 1000
sales = spark.createDataFrame(sales_data, ["region", "product", "revenue"])

# --- Regular join (Spark decides automatically whether to broadcast) ---
auto_result = sales.join(regions, on="region")

print("=== Plan WITHOUT explicit broadcast (Spark may still auto-broadcast) ===")
auto_result.explain()

# --- Explicit broadcast join ---
broadcast_result = sales.join(broadcast(regions), on="region")

print("=== Plan WITH explicit broadcast (look for BroadcastHashJoin) ===")
broadcast_result.explain()

print("=== Result (same output either way, only the execution plan differs) ===")
broadcast_result.groupBy("zone").sum("revenue").show()

# --- Checking / adjusting the auto-broadcast threshold ---
current_threshold = spark.conf.get("spark.sql.autoBroadcastJoinThreshold")
print(f"Current autoBroadcastJoinThreshold: {current_threshold}")

spark.stop()
