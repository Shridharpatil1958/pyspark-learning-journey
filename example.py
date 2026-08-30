"""
Day 01 — first PySpark script.
Demonstrates: SparkSession, reading data, transformations vs actions.

Run: python example.py
(Creates its own sample data, so no external file needed.)
"""

from pyspark.sql import SparkSession

# Entry point to Spark
spark = SparkSession.builder.appName("Day01-Intro").getOrCreate()

# Sample data — normally you'd use spark.read.csv(...)
data = [
    ("North", "Widget", 1200),
    ("North", "Gadget", 300),
    ("South", "Widget", 800),
    ("South", "Gadget", 1500),
    ("East", "Widget", 400),
]
columns = ["region", "product", "revenue"]

df = spark.createDataFrame(data, columns)

print("=== Schema ===")
df.printSchema()

# --- Transformations below: none of this runs yet, Spark just plans it ---
big_sales = (
    df.filter(df.revenue > 500)
      .groupBy("region")
      .sum("revenue")
      .orderBy("sum(revenue)", ascending=False)
)

# --- This is the ACTION: everything above finally executes here ---
print("=== Regions with revenue > 500, summed and sorted ===")
big_sales.show()

spark.stop()
