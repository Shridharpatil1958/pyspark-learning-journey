# Day 16 — PySpark interview prep

Common questions based on Days 1-15, with short answers.

## Core concepts

**What is lazy evaluation, and why does Spark use it?**
Transformations (filter, select, groupBy) only build a plan; nothing runs
until an action (show, count, write). Spark waits so it can see the whole
chain of steps and optimize it before executing. (Day 01)

**Transformation vs action?**
A transformation returns a new DataFrame and is lazy. An action triggers
execution and returns a result or writes output. (Day 01)

**Explain the Spark architecture.**
The driver runs your code and builds the plan. The cluster manager assigns
work. Executors hold partitions of the data and run tasks in parallel.
(Day 01)

**RDD vs DataFrame?**
DataFrames have named columns and a schema, and Spark can optimize them.
RDDs are lower-level with no schema. Analysts mostly use DataFrames.
(Day 01)

## Performance

**What is a shuffle, and why is it expensive?**
Moving data across the network so related rows land on the same partition.
groupBy, join, and orderBy trigger it. It appears as `Exchange` in
`.explain()`. (Day 08)

**repartition() vs coalesce()?**
repartition does a full shuffle and can increase or decrease partitions.
coalesce only decreases and avoids a full shuffle, so it's cheaper. Use
coalesce before a write to control the number of output files. (Day 11)

**What is a broadcast join?**
Spark copies a small table to every executor so the big table needn't be
shuffled. Use it when one side is small, like a lookup table. (Day 09)

**How would you debug a slow Spark job?**
Open the Spark UI. Check the Stages tab for one very slow task (skew) or
spill to disk. Check the SQL tab or `.explain()` for unnecessary shuffles.
Consider a broadcast join, caching, or fixing partition counts. (Day 10)

**What is data skew, and how do you fix it?**
One key holds far more rows than others, so one task runs much longer.
Try Adaptive Query Execution first; use salting if that isn't enough.
(Day 12)

**When would you cache a DataFrame?**
When it's reused two or more times downstream. Don't cache something used
once. (Day 08)

## Data handling

**Parquet vs CSV?**
Parquet is columnar, compressed, and stores the schema in the file. Spark
reads only the columns it needs, so it's faster and smaller. (Day 05)

**Why avoid UDFs?**
Rows round-trip out to Python, and the optimizer can't see inside them.
Prefer built-ins like when().otherwise() where possible. (Day 06)

**rank() vs dense_rank() vs row_number()?**
On ties: rank skips numbers (1,2,2,4), dense_rank doesn't (1,2,2,3),
row_number is always unique (1,2,3,4). (Day 03)

**What does Delta Lake add over Parquet?**
ACID transactions, time travel, and row-level update, delete, and merge,
all powered by a transaction log. (Day 14)

**Batch vs streaming?**
Batch processes a fixed dataset once. Structured Streaming treats data as
an ever-growing table and processes new rows incrementally, using
readStream and writeStream. (Day 15)

## Tips for answering

- Lead with the one-sentence definition, then give a concrete example.
- Tie answers to a real scenario: "In a pipeline I built..." (Day 07's
  mini project is a good one to reference).
- If you don't know something, explain how you'd find out (Spark UI,
  `.explain()`, docs) rather than guessing.

## Questions / things to explore next

- Practice the coding problems in example.py without looking at solutions
- Mock interview: explain Day 07's pipeline out loud in 2 minutes
- Learn a few common SQL-style questions (top N per group, dedup, running totals)
 
