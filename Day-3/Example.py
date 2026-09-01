"""
Day 03 — window functions: rank, dense_rank, row_number, running totals.
Run: python example.py
"""

from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import rank, dense_rank, row_number, sum as spark_sum, desc

spark = SparkSession.builder.appName("Day03-WindowFunctions").getOrCreate()

data = [
    ("North", "Gadget", 1200, 1),
    ("North", "Widget", 300, 2),
    ("North", "Thingy", 300, 3),   # tied with Widget on revenue
    ("South", "Gadget", 1500, 4),
    ("South", "Widget", 800, 5),
    ("East", "Widget", 400, 6),
]
columns = ["region", "product", "revenue", "order_id"]
df = spark.createDataFrame(data, columns)

# --- Ranking within each region, ordered by revenue descending ---
region_window = Window.partitionBy("region").orderBy(desc("revenue"))

ranked = (
    df.withColumn("rank", rank().over(region_window))
      .withColumn("dense_rank", dense_rank().over(region_window))
      .withColumn("row_number", row_number().over(region_window))
)

print("=== Rank / dense_rank / row_number within each region ===")
ranked.orderBy("region", "rank").show()

# --- Running total of revenue within each region, ordered by order_id ---
running_window = Window.partitionBy("region").orderBy("order_id")
running = df.withColumn("running_total", spark_sum("revenue").over(running_window))

print("=== Running total of revenue within each region ===")
running.orderBy("region", "order_id").show()

spark.stop()
