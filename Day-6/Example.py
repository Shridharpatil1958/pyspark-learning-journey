"""
Day 06 — UDFs: wrapping custom Python logic for use on DataFrame columns.
Run: python example.py
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, when, col
from pyspark.sql.types import StringType

spark = SparkSession.builder.appName("Day06-UDFs").getOrCreate()

data = [
    ("North", "Widget", 1200),
    ("North", "Gadget", 300),
    ("South", "Widget", 800),
    ("South", "Gadget", 1500),
    ("East", "Widget", 400),
]
df = spark.createDataFrame(data, ["region", "product", "revenue"])


# --- Approach 1: plain function wrapped with udf() ---
def categorize(revenue):
    if revenue > 1000:
        return "High"
    elif revenue > 500:
        return "Medium"
    return "Low"


categorize_udf = udf(categorize, StringType())

print("=== Using a UDF to categorize revenue ===")
df.withColumn("revenue_tier", categorize_udf(df.revenue)).show()


# --- Approach 2: decorator style ---
@udf(returnType=StringType())
def categorize_decorated(revenue):
    if revenue > 1000:
        return "High"
    elif revenue > 500:
        return "Medium"
    return "Low"


print("=== Same thing, decorator style ===")
df.withColumn("revenue_tier", categorize_decorated(df.revenue)).show()


# --- For comparison: the SAME logic using only built-ins (faster, preferred) ---
print("=== Same result using built-in when/otherwise instead of a UDF ===")
df.withColumn(
    "revenue_tier",
    when(col("revenue") > 1000, "High")
    .when(col("revenue") > 500, "Medium")
    .otherwise("Low"),
).show()

spark.stop()
