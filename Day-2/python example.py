"""
Day 02 — joins and handling nulls.
Run: python example.py
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as spark_sum, when

spark = SparkSession.builder.appName("Day02-JoinsAndNulls").getOrCreate()

customers = spark.createDataFrame(
    [(1, "Aman"), (2, "Priya"), (3, "Rahul")],
    ["cust_id", "name"],
)

# cust_id 4 has no matching customer -> will show how joins/nulls behave
orders = spark.createDataFrame(
    [(101, 1, 500), (102, 1, 700), (103, 4, 300)],
    ["order_id", "cust_id", "amount"],
)

print("=== Inner join: only customers who have orders ===")
customers.join(orders, on="cust_id", how="inner").show()

print("=== Left join: every customer, orders columns null if none ===")
left = customers.join(orders, on="cust_id", how="left")
left.show()

print("=== Count nulls per column (on the left-joined result) ===")
left.select([
    spark_sum(when(col(c).isNull(), 1).otherwise(0)).alias(c)
    for c in left.columns
]).show()

print("=== Fill missing amount with 0 ===")
left.na.fill({"amount": 0}).show()

print("=== Drop rows where amount is null ===")
left.na.drop(subset=["amount"]).show()

spark.stop()
