"""
Day 10 — the Spark UI: how to find it and generate some activity to watch.
Run: python example.py
While it's running, open the printed URL (usually http://localhost:4040)
in a browser to see the Jobs, Stages, SQL, and Storage tabs live.
"""

import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import broadcast

spark = SparkSession.builder.appName("Day10-SparkUI").getOrCreate()

print(f"Spark UI available at: {spark.sparkContext.uiWebUrl}")
print("Open that URL in a browser while this script runs to watch it live.\n")

# Bigger dataset so there's something visible in the UI
sales_data = [
    ("North", "Widget", 1200),
    ("North", "Gadget", 300),
    ("South", "Widget", 800),
    ("South", "Gadget", 1500),
    ("East", "Widget", 400),
] * 5000
sales = spark.createDataFrame(sales_data, ["region", "product", "revenue"])

regions = spark.createDataFrame(
    [("North", "Zone A"), ("South", "Zone B"), ("East", "Zone C")],
    ["region", "zone"],
)

# A cached DataFrame -> check the Storage tab after this runs
filtered = sales.filter(sales.revenue > 500).cache()
filtered.count()
print("Cached 'filtered' — check the Storage tab in the UI.")

# A shuffle-heavy groupBy -> check the Stages tab for an Exchange step
print("Running a groupBy (creates a shuffle stage)...")
filtered.groupBy("region").sum("revenue").show()

# A broadcast join -> check the SQL tab, look for BroadcastHashJoin
print("Running a broadcast join...")
sales.join(broadcast(regions), on="region").groupBy("zone").sum("revenue").show()

print("\nPausing 30s so you have time to click through the UI tabs...")
time.sleep(30)

filtered.unpersist()
spark.stop()
