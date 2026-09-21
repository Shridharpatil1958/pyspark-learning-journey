# Day 15 — Structured Streaming

## The core mental model

Think of a stream as an infinite, ever-growing table. New data arriving
just appends new rows; the query re-runs incrementally on just the new
rows, instead of running once on a fixed file like batch processing.

```
Batch:      read file -> process -> write output -> DONE
Streaming:  read new data -> process -> write -> repeat forever
```

## Syntax — almost identical to batch, swap read for readStream

```python
df = spark.read.csv("sales_folder/")                                  # batch
stream_df = spark.readStream.csv("sales_folder/", schema=known_schema) # streaming
```

Streaming reads need an explicit schema — Spark can't `inferSchema=True`
on data that hasn't arrived yet.

## Transformations — unchanged from Days 1-14

```python
result = stream_df.filter(stream_df.revenue > 500).groupBy("region").sum("revenue")
```

`filter`, `groupBy`, joins, window functions, even Spark SQL all work the
same way on a streaming DataFrame.

## Running it — writeStream instead of write

```python
query = (
    result.writeStream
    .outputMode("complete")
    .format("console")     # for testing; use "parquet"/"delta" for real output
    .start()
)
query.awaitTermination()
```

## Output modes (the genuinely new concept)

- `append` — only new rows since the last update (most common)
- `complete` — rewrite the entire result table each time (needed for
  aggregations like groupBy, since past rows can still change)
- `update` — write only the rows that changed

## Realistic expectation for a fresher analyst role

Most entry-level analyst work is batch (Days 1-14's CSV/Parquet
pipelines). Streaming shows up more in data engineering roles. Knowing
the concept — same DataFrame API, applied incrementally to an
ever-growing table — is usually enough for an interview question even
without deep hands-on streaming experience.

## Questions / things to explore next

- Watermarking — handling late-arriving data in streams
- Trigger intervals — controlling how often a stream checks for new data
- Interview-prep: common PySpark interview questions with example answers
- 
