"""
Day 07 — Week 1 mini project.
Combines: SparkSession, joins, null handling, UDFs, window functions,
Spark SQL, and Parquet output into one small pipeline.

Run: python example.py
"""

from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import udf, sum as spark_sum, rank, desc, col
from pyspark.sql.types import StringType

spark = SparkSession.builder.appName("Day07-MiniProject").getOrCreate()

# --- Sample "source" data (normally you'd read these from CSV files) ---
customers = spark.createDataFrame(
    [(1, "Aman", "North"), (2, "Priya", "North"),
     (3, "Rahul", "South"), (4, "Sneha", "South"), (5, "Vikram", "East")],
    ["cust_id", "name", "region"],
)

orders = spark.createDataFrame(
    [(101, 1, 500), (102, 1, 700), (103, 2, 300),
     (104, 3, 1500), (105, 4, None), (106, 5, 400)],  # note: one null amount
    ["order_id", "cust_id", "amount"],
)

# --- Step 1: join customers with their orders ---
joined = customers.join(orders, on="cust_id", how="left")

# --- Step 2: handle nulls — treat missing amount as 0 ---
cleaned = joined.na.fill({"amount": 0})

# --- Step 3: total spend per customer ---
totals = cleaned.groupBy("cust_id", "name", "region").agg(
    spark_sum("amount").alias("total_spent")
)


# --- Step 4: UDF to categorize spend level ---
@udf(returnType=StringType())
def categorize(total):
    if total > 1000:
        return "High"
    elif total > 400:
        return "Medium"
    return "Low"


enriched = totals.withColumn("spend_tier", categorize(col("total_spent")))

# --- Step 5: window function — rank customers within each region ---
region_window = Window.partitionBy("region").orderBy(desc("total_spent"))
ranked = enriched.withColumn("rank_in_region", rank().over(region_window))

print("=== Final enriched + ranked result ===")
ranked.orderBy("region", "rank_in_region").show()

# --- Same aggregation step, shown via Spark SQL for comparison (Day 4) ---
cleaned.createOrReplaceTempView("cleaned_orders")
print("=== Same totals, via Spark SQL ===")
spark.sql("""
    SELECT cust_id, name, region, SUM(amount) AS total_spent
    FROM cleaned_orders
    GROUP BY cust_id, name, region
    ORDER BY total_spent DESC
""").show()

# --- Step 6: write final result to Parquet, partitioned by region (Day 5) ---
ranked.write.mode("overwrite").partitionBy("region").parquet("week1_result")

print("=== Read back from Parquet to confirm it saved correctly ===")
spark.read.parquet("week1_result").orderBy("region", "rank_in_region").show()

spark.stop()
