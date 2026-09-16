# Day 08 — .explain() and caching

## .explain() — seeing the query plan

Because of lazy evaluation, transformations just build a plan. `.explain()`
prints that plan without necessarily running it fully.

```python
result = df.filter(df.revenue > 500).groupBy("region").sum("revenue")
result.explain()
```

Read the physical plan **bottom to top** — that's execution order:

```
== Physical Plan ==
*(2) HashAggregate(keys=[region], functions=[sum(revenue)])
+- Exchange hashpartitioning(region, 200)
   +- *(1) HashAggregate(keys=[region], functions=[partial_sum(revenue)])
      +- *(1) Filter (revenue > 500)
         +- *(1) FileScan csv
```

1. `FileScan` — read the file
2. `Filter` — apply the condition
3. `HashAggregate` (partial) — sum per-partition first
4. `Exchange` — shuffle data across the network
5. `HashAggregate` (final) — combine into the final result

## The key thing to look for: Exchange

`Exchange` = a network shuffle, the most expensive operation in Spark.
`groupBy`, `join`, and `orderBy` typically trigger one. When a job is slow,
`.explain()` is often how you find where the shuffle is happening.

```python
result.explain(mode="formatted")   # cleaner breakdown (Spark 3.0+)
result.explain(True)               # shows parsed / analyzed / optimized / physical plans
```

## Caching — avoiding repeated recomputation

Because of laziness, reusing the same DataFrame multiple times recomputes
it from scratch each time. `.cache()` keeps it in memory after the first
computation.

```python
filtered = df.filter(df.revenue > 500).cache()

filtered.count()                                    # triggers + caches it
filtered.groupBy("region").sum("revenue").show()     # reuses cached data
filtered.groupBy("product").avg("revenue").show()    # reuses cached data

filtered.unpersist()   # free the memory when done
```

**Rule of thumb**: cache when a DataFrame is reused 2+ times downstream;
skip it for anything used only once — it just wastes memory.

## Questions / things to explore next

- Reading/writing to real cloud storage paths (S3 / ADLS / GCS)
- Broadcast joins for speeding up joins with a small table
- Spark UI (the web interface) for visually inspecting jobs and stages
