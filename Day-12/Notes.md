# Day 12 — Handling skewed joins (salting)

## The problem: data skew

If one key (e.g. one big customer) has way more rows than others, that
key's data lands on one partition during a join. Every other executor
finishes in seconds; the overloaded one takes far longer, and the whole
job waits on it. Visible in the Spark UI (Day 10) as one task taking much
longer than the rest in a stage.

## Why broadcast joins don't fix this

Broadcasting (Day 09) works when an entire table is small. Skew is
different — both tables can be huge, it's just that one key value within
them is wildly overrepresented. Broadcasting doesn't help with an
imbalance inside a large table.

## The fix: salting

Split the skewed key into several fake sub-keys so its rows spread across
multiple partitions instead of piling onto one.

```python
from pyspark.sql.functions import concat, lit, rand, floor, col, explode, array

NUM_SALTS = 10

# Add a random salt (0-9) to the skewed side
orders_salted = orders.withColumn(
    "salted_key",
    concat(col("cust_id"), lit("_"), floor(rand() * NUM_SALTS).cast("int"))
)

# Explode the small side so every salt value gets a matching row
customers_salted = customers.withColumn(
    "salt", explode(array([lit(i) for i in range(NUM_SALTS)]))
).withColumn(
    "salted_key", concat(col("cust_id"), lit("_"), col("salt"))
)

result = orders_salted.join(customers_salted, on="salted_key")
```

Rows for the huge customer now spread across NUM_SALTS partitions instead
of overwhelming one.

## Simpler first option: Adaptive Query Execution (AQE)

Modern Spark (3.0+) can auto-detect and split skewed partitions on its own:

```python
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
```

**Rule of thumb**: try AQE first (often already on by default) — only
hand-roll salting if AQE isn't sufficient, since salting adds real
complexity to the code.

## Questions / things to explore next

- Reading/writing real cloud storage paths (S3 / ADLS / GCS)
- Delta Lake basics (if working in Databricks)
- Structured Streaming (if the job ever needs real-time data)
- 
