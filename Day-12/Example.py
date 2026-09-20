"""
Day 12 — handling skewed joins with salting, and AQE as a simpler first option.
Run: python example.py
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import concat, lit, rand, floor, col, explode, array, sum as spark_sum

spark = SparkSession.builder.appName("Day12-SkewedJoinsSalting").getOrCreate()

# --- Simulate a skewed dataset: cust_id 1 has WAY more orders than the rest ---
skewed_data = [(1, i, 100) for i in range(5000)]          # customer 1: 5000 orders
skewed_data += [(2, i, 100) for i in range(10)]            # customer 2: only 10 orders
skewed_data += [(3, i, 100) for i in range(10)]            # customer 3: only 10 orders
orders = spark.createDataFrame(skewed_data, ["cust_id", "order_id", "amount"])

customers = spark.createDataFrame(
    [(1, "BigCorp"), (2, "SmallCo"), (3, "TinyInc")],
    ["cust_id", "name"],
)

print(f"Orders per customer (this imbalance is the skew problem):")
orders.groupBy("cust_id").count().show()

# --- Option 1: turn on AQE, let Spark auto-handle skew ---
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")

print("=== Join with AQE skew handling enabled ===")
result_aqe = orders.join(customers, on="cust_id")
result_aqe.groupBy("name").agg(spark_sum("amount").alias("total")).show()

# --- Option 2: manual salting ---
NUM_SALTS = 10

orders_salted = orders.withColumn(
    "salted_key",
    concat(col("cust_id").cast("string"), lit("_"), floor(rand() * NUM_SALTS).cast("int"))
)

customers_salted = (
    customers.withColumn("salt", explode(array([lit(i) for i in range(NUM_SALTS)])))
    .withColumn("salted_key", concat(col("cust_id").cast("string"), lit("_"), col("salt")))
)

print("=== Join using manual salting on the join key ===")
result_salted = orders_salted.join(customers_salted, on="salted_key")
result_salted.groupBy("name").agg(spark_sum("amount").alias("total")).show()

spark.stop()
