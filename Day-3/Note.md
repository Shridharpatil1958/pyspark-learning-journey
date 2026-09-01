# Day 03 — Window functions

## Why not just groupBy?

`groupBy` collapses rows into one summary row per group. A window function
keeps every original row, but adds a new column calculated relative to that
row's group — e.g. its rank within its region, or a running total.

## Defining a window

```python
from pyspark.sql import Window
from pyspark.sql.functions import desc

region_window = Window.partitionBy("region").orderBy(desc("revenue"))
```

- `partitionBy` — how to group the rows (like groupBy's column)
- `orderBy` — how to order rows within each group (needed for rank/running total)

## Ranking functions

```python
from pyspark.sql.functions import rank, dense_rank, row_number

df.withColumn("rank", rank().over(region_window))
df.withColumn("dense_rank", dense_rank().over(region_window))
df.withColumn("row_number", row_number().over(region_window))
```

- `rank()` — ties share a rank, next rank skips (1,2,2,4)
- `dense_rank()` — ties share a rank, no skip (1,2,2,3)
- `row_number()` — always unique, breaks ties arbitrarily (1,2,3,4)

## Running totals

```python
from pyspark.sql.functions import sum as spark_sum

running_window = Window.partitionBy("region").orderBy("order_id")
df.withColumn("running_total", spark_sum("revenue").over(running_window))
```

## When to use this

Anything needing per-row context from its group instead of one collapsed
summary row: "top 3 products per region," "running total per customer,"
"rank employees by sales within their branch."

## Questions / things to explore next

- Spark SQL syntax (`spark.sql()` with temp views) as an alternative to the DataFrame API
- Reading/writing Parquet files
- `.explain()` to see how Spark plans to execute a query
