# Day 01 — Intro to PySpark, architecture, lazy evaluation

## Why PySpark?

Pandas loads everything into one machine's memory. Spark splits data into
partitions and spreads the work across multiple machines (executors) so it
can handle datasets too big for one computer.

## Architecture

- **Driver** — runs my actual PySpark code, sends instructions out.
- **Cluster manager** — decides which executor gets which piece of work.
- **Executors** — each holds one partition of the data and runs tasks on it,
  in parallel with the other executors.

## Lazy evaluation (the big habit to remember)

- **Transformations** (`.filter()`, `.select()`, `.groupBy()`, `.withColumn()`,
  `.orderBy()`) don't run immediately — Spark just records a plan.
- **Actions** (`.show()`, `.count()`, `.collect()`) are what actually trigger
  execution of the whole plan, all at once.
- Why: laziness lets Spark see the full chain of steps and optimize before
  running anything.

## SparkSession

Every program starts with:

```python
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("MyApp").getOrCreate()
```

This is the entry point / remote control for the whole cluster.

## DataFrames

As a data analyst I'll mostly use DataFrames (not raw RDDs) — rows + named
columns, like pandas or Excel, but partitioned across executors.

Key operations:
- Read: `spark.read.csv(path, header=True, inferSchema=True)`
- Transform: `.select()`, `.filter()`, `.groupBy()`, `.orderBy()`, `.withColumn()`
- Act: `.show()`, `.count()`, `.collect()`

## Questions / things to explore next

- SQL inside PySpark (`createOrReplaceTempView` + `spark.sql()`)
- Joins between DataFrames
- Handling nulls / missing data
