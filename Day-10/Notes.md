# Day 10 — The Spark UI

## What it is

A web dashboard Spark runs automatically for every application, usually at
`http://localhost:4040`, showing real-time job progress. On a cloud
cluster (Databricks etc.) it's linked from the cluster/job page instead.

```python
spark = SparkSession.builder.appName("MyApp").getOrCreate()
print(spark.sparkContext.uiWebUrl)
```

## Key tabs

- **Jobs** — every action (`.show()`, `.count()`, a write) is one job; shows
  duration and pass/fail.
- **Stages** — jobs break into stages, split at shuffle boundaries
  (`Exchange`, from Day 08). A much slower stage than the rest = bottleneck.
- **SQL/DataFrame** — the visual version of `.explain()` (Day 08) — draws
  the execution plan as a diagram with row counts flowing through each step.
- **Storage** — shows what's currently cached (`.cache()`, Day 08) and how
  much memory it's using.

## What to look for

- **Data skew** — if most tasks in a stage finish instantly but one takes
  far longer, one partition has way more data than the others (e.g. one
  region holds 90% of rows). Common real-world problem.
- **Lots of shuffle-heavy stages** — may mean a broadcast join (Day 09)
  or query restructuring would help.
- **Spill to disk** — data didn't fit in memory, so Spark wrote to disk
  (slow). Often fixed with more partitions or more memory.

## Why this matters for interviews

"How would you debug a slow Spark job?" is a common interview question.
Expected answer: check the Spark UI — Stages tab for skew/slow stages, SQL
tab for the plan, look for unnecessary shuffles or a missing broadcast join.

## Questions / things to explore next

- Handling skewed joins directly (salting keys)
- Reading/writing real cloud storage paths (S3 / ADLS / GCS)
- Partition tuning — `repartition()` vs `coalesce()`
