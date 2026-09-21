# Day 14 — Delta Lake basics

## What problem it solves

Plain Parquet (Day 05) has no built-in change tracking. A failed write can
leave corrupted/partial data, there's no way to see yesterday's version of
the data, and updating or deleting specific rows means rewriting the whole
file.

## The core idea

Delta Lake = Parquet files + a transaction log (`_delta_log` folder next to
the data) that tracks every change made. That log is what enables
everything below.

## Reading and writing — nearly identical to Parquet

```python
df.write.format("delta").mode("overwrite").save("sales_delta")
df2 = spark.read.format("delta").load("sales_delta")
```

## What Delta adds over plain Parquet

**ACID transactions** — a write either fully succeeds or fully fails, no
partial/corrupted data from a crashed job.

**Time travel** — query the data as it looked earlier:

```python
old_version = spark.read.format("delta").option("versionAsOf", 3).load("sales_delta")
old_data = spark.read.format("delta").option("timestampAsOf", "2026-09-01").load("sales_delta")
```

**UPDATE / DELETE / MERGE on specific rows** — no full-file rewrite needed:

```python
from delta.tables import DeltaTable

delta_table = DeltaTable.forPath(spark, "sales_delta")

delta_table.update(condition="region = 'North'", set={"revenue": "revenue * 1.1"})
delta_table.delete(condition="revenue < 100")

# merge (upsert) — very common for daily incremental loads
delta_table.alias("target").merge(
    new_data.alias("source"), "target.cust_id = source.cust_id"
).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
```

## When to actually use this

Default table format in Databricks — comes up constantly there. Outside
Databricks, plain Parquet is still perfectly fine and more universal.

## Questions / things to explore next

- Structured Streaming — processing data as it arrives instead of in batches
- Schema evolution / enforcement in Delta tables
- Z-ordering and OPTIMIZE for Delta table performance tuning
- 
