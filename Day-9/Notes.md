# Day 09 — Broadcast joins

## The problem

A normal join triggers a shuffle (`Exchange`, from Day 8) — both tables get
redistributed across the network so matching keys land together. Expensive,
especially at scale.

## The trick: broadcast joins

If one table is small enough to fit in memory, Spark can copy it to every
executor instead of shuffling both tables. Each executor then joins its
local slice of the big table against the small table locally — no network
shuffle needed for the big table.

```python
from pyspark.sql.functions import broadcast

result = big_orders_df.join(broadcast(small_customers_df), on="cust_id")
```

`broadcast()` tells Spark: "don't shuffle this one, copy it everywhere."

## Spark does this automatically below a size threshold

```python
spark.conf.get("spark.sql.autoBroadcastJoinThreshold")            # check current (default 10MB)
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", 50*1024*1024)  # raise threshold
```

Use `broadcast()` explicitly when:
- a table is just over the auto-threshold but you know it's still safely small
- you want to force it rather than trust Spark's size estimate (which can be
  wrong on complex plans)

## Confirming it worked

Check `.explain()` (Day 8) for `BroadcastHashJoin` instead of a regular
`SortMergeJoin` + `Exchange`.

## Rule of thumb

- Small lookup/dimension table joining a big fact table → broadcast the
  small one.
- Two large tables joining each other → broadcasting isn't an option
  (would blow up memory on every executor) — a shuffle join is unavoidable.

## Questions / things to explore next

- The Spark UI (web interface) for visually inspecting jobs, stages, and shuffles
- Reading/writing real cloud storage paths (S3 / ADLS / GCS)
- Salting keys to handle skewed joins (when one key has way more rows than others)
