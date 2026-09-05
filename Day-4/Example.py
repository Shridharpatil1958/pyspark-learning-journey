"""
Day 04 — Spark SQL: temp views and spark.sql().
Run: python example.py
"""

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Day04-SparkSQL").getOrCreate()

customers = spark.createDataFrame(
    [(1, "Aman"), (2, "Priya"), (3, "Rahul")],
    ["cust_id", "name"],
)
orders = spark.createDataFrame(
    [(101, 1, 500), (102, 1, 700), (103, 2, 300), (104, 2, 900)],
    ["order_id", "cust_id", "amount"],
)

# Register both DataFrames as SQL-queryable views
customers.createOrReplaceTempView("customers")
orders.createOrReplaceTempView("orders")

print("=== Total spend per customer, via Spark SQL ===")
spark.sql("""
    SELECT c.name, SUM(o.amount) AS total_spent
    FROM customers c
    JOIN orders o ON c.cust_id = o.cust_id
    GROUP BY c.name
    ORDER BY total_spent DESC
""").show()

print("=== Same result, via the DataFrame API (proves they're equivalent) ===")
from pyspark.sql.functions import sum as spark_sum, desc

(
    customers.join(orders, on="cust_id")
    .groupBy("name")
    .agg(spark_sum("amount").alias("total_spent"))
    .orderBy(desc("total_spent"))
    .show()
)

# spark.sql() result is a normal DataFrame — keep chaining on it
big_spenders = spark.sql("SELECT * FROM orders WHERE amount > 400")
print("=== Orders over 400, filtered further with the DataFrame API ===")
big_spenders.filter(big_spenders.cust_id == 1).show()

spark.stop()
