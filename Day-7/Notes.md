# Day 07 — Week 1 mini project

## Goal

Combine everything from Days 1–6 into one small realistic pipeline:
read two CSVs, join them, handle nulls, categorize with a UDF, rank with a
window function, and write the final result to Parquet.

## Pipeline

```
sales.csv + customers.csv
        |
   join on customer_id
        |
   fill missing amounts (na.fill)
        |
   UDF: categorize spend (High / Medium / Low)
        |
   rank customers within each region (window function)
        |
   write final result to Parquet, partitioned by region
```

## What each step reuses from earlier days

- **Day 1** — SparkSession, reading CSVs, lazy evaluation (nothing runs
  until the final `.show()` / write)
- **Day 2** — join + `na.fill()` for missing amounts
- **Day 4** — could rewrite the join/aggregation as Spark SQL instead
  (both approaches shown in `example.py`)
- **Day 3** — `Window.partitionBy("region").orderBy(desc("total_spent"))`
  to rank customers per region
- **Day 6** — a UDF to categorize each customer's spend level
- **Day 5** — final write as partitioned Parquet

## Takeaway

A "real" PySpark task is rarely just one technique — it's usually a chain
of read → clean → join → transform → aggregate → write, with each step
being one of the building blocks learned this week.

## Questions / things to explore next (week 2)

- `.explain()` to see how Spark actually plans/optimizes this pipeline
- Caching (`.cache()`) if `enriched` is reused multiple times downstream
- Reading from/writing to a real cloud storage path (S3 / ADLS) instead of
  local files

