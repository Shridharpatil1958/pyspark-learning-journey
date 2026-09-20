# Day 13 — Reading/writing cloud storage (S3, ADLS, GCS)

## Key insight: only the path prefix changes

Everything learned so far (`spark.read.csv()`, `spark.read.parquet()`,
`.write.parquet()`) works identically — just swap the path.

```python
# Local
df = spark.read.parquet("sales_data.parquet")

# Amazon S3 (note: s3a://, not s3://)
df = spark.read.parquet("s3a://my-bucket/sales_data/")

# Azure Data Lake Storage
df = spark.read.parquet("abfss://container@account.dfs.core.windows.net/sales_data/")

# Google Cloud Storage
df = spark.read.parquet("gs://my-bucket/sales_data/")
```

## Authentication

Not passed in the path — configured separately.

```python
# For learning only — not how production does it
spark.conf.set("fs.s3a.access.key", "YOUR_ACCESS_KEY")
spark.conf.set("fs.s3a.secret.key", "YOUR_SECRET_KEY")
```

In a real company setup, credentials are almost never hardcoded — the
cluster/environment itself is already authorized (IAM role on AWS, managed
identity on Azure, etc.), so the path alone is enough.

## Required libraries

S3 access needs extra JARs (`hadoop-aws`) not included in Spark by
default. Pre-installed on managed platforms (Databricks, EMR); added via
`--packages` if running Spark yourself.

## The takeaway for interviews / on the job

The DataFrame API itself never changes — only the path scheme and how
credentials are supplied. That's the concept that actually matters, not
memorizing connector JAR names.

## Questions / things to explore next

- Delta Lake basics (if working in Databricks) — ACID transactions, time travel
- Structured Streaming — processing data as it arrives instead of in batches
- Partition pruning when reading a large cloud dataset with filters
- 
