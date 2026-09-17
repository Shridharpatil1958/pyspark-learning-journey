# Day 11 — repartition() vs coalesce()

## Why partition count matters

Data is split into partitions, one per executor task (Day 01). Too few
partitions wastes cluster parallelism; too many tiny partitions adds
overhead that outweighs the benefit.

## repartition(n) — full reshuffle

```python
df2 = df.repartition(8)          # spread into 8 partitions, evenly
df3 = df.repartition("region")   # partition by a column's values
```

- Full shuffle — every row moves across the network (an `Exchange`, Day 08).
- Can increase OR decrease partition count.
- Expensive, but produces evenly-sized partitions.
- Use before heavy work (big groupBy) on badly imbalanced partitions, or to
  partition by a column before a join.

## coalesce(n) — cheap merge, no shuffle

```python
df2 = df.coalesce(2)
```

- Only DECREASES partition count — can't use it to increase.
- Merges existing partitions together, no full shuffle — much cheaper.
- Common use: after a wide operation leaves you with 200 tiny partitions,
  merge down before writing output.

## Most common real use: controlling output file count

```python
df.write.parquet("output")              # might create 200 small files
df.coalesce(4).write.parquet("output")  # 4 well-sized files instead
```

## Rule of thumb

| Situation | Use |
|---|---|
| Need more partitions / better parallelism | repartition() |
| Need fewer partitions, sizes don't need to be even | coalesce() |
| Reducing before a final write | coalesce() (cheaper) |
| Partitioning by a column before a join/groupBy | repartition("column") |

## Questions / things to explore next

- Handling skewed joins directly (salting keys)
- Reading/writing real cloud storage paths (S3 / ADLS / GCS)
- Delta Lake basics (if working in Databricks)
