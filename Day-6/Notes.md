# Day 06 — User-Defined Functions (UDFs)

## Why UDFs exist

Spark has many built-in functions (`upper()`, `round()`, `when()`, etc.) that
run natively and fast. A UDF lets you plug in custom Python logic when no
built-in covers what you need — but it's slower, because each row's data
has to round-trip out to Python and back.

**Rule of thumb**: always check if a built-in function already does the job
before writing a UDF. Built-ins are almost always faster, and Spark's
optimizer can't see inside a UDF to optimize around it.

## Basic syntax

```python
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

def categorize(revenue):
    if revenue > 1000:
        return "High"
    elif revenue > 500:
        return "Medium"
    return "Low"

categorize_udf = udf(categorize, StringType())

df = df.withColumn("revenue_tier", categorize_udf(df.revenue))
```

## Decorator style (cleaner)

```python
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

@udf(returnType=StringType())
def categorize(revenue):
    if revenue > 1000:
        return "High"
    elif revenue > 500:
        return "Medium"
    return "Low"

df = df.withColumn("revenue_tier", categorize(df.revenue))
```

## Caveats

- Must specify `returnType` explicitly — Spark can't infer it from Python.
- UDFs are a "black box" to Spark's query optimizer — use only when there's
  genuinely no built-in alternative.
- For heavier numeric work, pandas UDFs (vectorized, much faster) are worth
  learning next, but plain UDFs are the right starting point.

## Questions / things to explore next

- `.explain()` — see how Spark plans/optimizes a query
- Caching (`.cache()` / `.persist()`) for DataFrames reused multiple times
- Pandas UDFs for faster vectorized custom logic
