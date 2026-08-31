# Day 02 — Joins and handling nulls

## Joins

Combine two DataFrames on a common key, same idea as SQL joins.

```python
df1.join(df2, on="key_column", how="inner")
```

- **inner** — only rows where the key exists in both DataFrames
- **left** — all rows from the left DataFrame; unmatched right-side columns become null
- **right** — all rows from the right DataFrame; unmatched left-side columns become null
- **full** (outer) — all rows from both; null wherever there's no match

## Handling nulls

Real-world data is full of missing values. PySpark's `df.na` namespace handles this:

```python
df.na.drop()                          # drop rows with ANY null
df.na.drop(subset=["amount"])         # drop rows only if this column is null
df.na.fill({"amount": 0})             # fill nulls with a default value
```

**Rule of thumb:**
- `drop()` when a missing value makes the row useless for the analysis
- `fill()` when there's a sensible default to substitute

## Counting nulls per column

```python
from pyspark.sql.functions import col, sum as spark_sum, when

df.select([
    spark_sum(when(col(c).isNull(), 1).otherwise(0)).alias(c)
    for c in df.columns
]).show()
```

## Questions / things to explore next

- Window functions (running totals, rankings within groups)
- Spark SQL syntax as an alternative to the DataFrame API
- Reading/writing Parquet files
