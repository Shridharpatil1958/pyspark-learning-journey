"""
Day 16 — common PySpark coding interview problems, with solutions.
Try each problem yourself first, then compare with the solution below it.
Run: python example.py
"""

from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import (
    col, count, desc, row_number, sum as spark_sum, when, avg,
)

spark = SparkSession.builder.appName("Day16-InterviewPrep").getOrCreate()

sales = spark.createDataFrame(
    [
        (1, "North", "Widget", 1200),
        (2, "North", "Gadget", 300),
        (3, "North", "Gizmo", 800),
        (4, "South", "Widget", 800),
        (5, "South", "Gadget", 1500),
        (6, "East", "Widget", 400),
        (6, "East", "Widget", 400),   # duplicate row on purpose
        (7, "East", "Gadget", None),  # null revenue on purpose
    ],
    ["order_id", "region", "product", "revenue"],
)

# ---------------------------------------------------------------
# Problem 1: Remove duplicate rows.
# ---------------------------------------------------------------
print("=== Problem 1: remove duplicates ===")
deduped = sales.dropDuplicates()
deduped.show()

# ---------------------------------------------------------------
# Problem 2: Find the top 2 products by revenue in each region.
# (Classic "top N per group" question.)
# ---------------------------------------------------------------
print("=== Problem 2: top 2 products per region ===")
w = Window.partitionBy("region").orderBy(desc("revenue"))
top2 = (
    deduped.withColumn("rn", row_number().over(w))
    .filter(col("rn") <= 2)
    .drop("rn")
)
top2.orderBy("region").show()

# ---------------------------------------------------------------
# Problem 3: Count nulls in each column.
# ---------------------------------------------------------------
print("=== Problem 3: null count per column ===")
deduped.select(
    [spark_sum(when(col(c).isNull(), 1).otherwise(0)).alias(c) for c in deduped.columns]
).show()

# ---------------------------------------------------------------
# Problem 4: Total and average revenue per region, highest total first.
# ---------------------------------------------------------------
print("=== Problem 4: total and average revenue per region ===")
(
    deduped.groupBy("region")
    .agg(spark_sum("revenue").alias("total"), avg("revenue").alias("average"))
    .orderBy(desc("total"))
    .show()
)

# ---------------------------------------------------------------
# Problem 5: Find regions with more than 2 orders (HAVING equivalent).
# ---------------------------------------------------------------
print("=== Problem 5: regions with more than 2 orders ===")
(
    deduped.groupBy("region")
    .agg(count("*").alias("order_count"))
    .filter(col("order_count") > 2)
    .show()
)

# ---------------------------------------------------------------
# Problem 6: Same as Problem 4, but written in Spark SQL.
# ---------------------------------------------------------------
print("=== Problem 6: Problem 4 via Spark SQL ===")
deduped.createOrReplaceTempView("sales")
spark.sql("""
    SELECT region,
           SUM(revenue) AS total,
           AVG(revenue) AS average
    FROM sales
    GROUP BY region
    ORDER BY total DESC
""").show()

spark.stop()
