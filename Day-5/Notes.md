# Day 05 — Parquet files

## Why Parquet over CSV

- CSV is row-based plain text — reading even one column means scanning every
  row.
- Parquet is columnar and binary — reading one column only touches that
  column's data on disk.
- Parquet stores the schema (column names + types) inside the file, so no
  `inferSchema=True` needed.
- Parquet compresses much better than plain text, so files are smaller.

## Reading and writing

```python
df.write.parquet("sales_data.parquet")

df2 = spark.read.parquet("sales_data.parquet")   # schema comes from the file
```

## Overwrite vs append

Default write mode errors if the path already exists — specify a mode:

```python
df.write.mode("overwrite").parquet("sales_data.parquet")  # replace existing data
df.write.mode("append").parquet("sales_data.parquet")     # add to existing data
```

## Partitioned writes

Split output into folders by a column, so future reads can skip folders
that don't match a filter:

```python
df.write.partitionBy("region").parquet("sales_by_region")
```

Creates:
```
sales_by_region/region=North/
sales_by_region/region=South/
sales_by_region/region=East/
```

Reading with a filter on `region` lets Spark skip irrelevant folders
entirely — much faster than scanning everything.

## Questions / things to explore next

- `.explain()` to see how Spark actually plans/optimizes a query
- User-defined functions (UDFs) for custom row-level logic
- Caching (`.cache()` / `.persist()`) for DataFrames reused multiple times
