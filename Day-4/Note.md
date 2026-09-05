# Day 04 — Spark SQL

## The core idea

Register a DataFrame as a temporary view, then query it with plain SQL via
`spark.sql()`. Under the hood, SQL and the DataFrame method-chaining API
(`.filter()`, `.groupBy()`, etc.) compile to the exact same execution plan —
they're just two syntaxes for the same thing.

## Basic usage

```python
df.createOrReplaceTempView("sales")

result = spark.sql("""
    SELECT region, SUM(revenue) AS total_revenue
    FROM sales
    WHERE revenue > 500
    GROUP BY region
    ORDER BY total_revenue DESC
""")

result.show()
```

- `spark.sql(...)` always returns a normal DataFrame, so `.filter()`,
  `.show()`, etc. all still work on the result.
- `createOrReplaceTempView` — view only exists for this SparkSession, gone
  when the program ends.
- `createGlobalTempView` — survives across sessions in the same Spark app
  (rarely needed early on).

## Joining views

Register each DataFrame as its own view, then join like normal SQL tables:

```python
customers.createOrReplaceTempView("customers")
orders.createOrReplaceTempView("orders")

spark.sql("""
    SELECT c.name, SUM(o.amount) AS total_spent
    FROM customers c
    JOIN orders o ON c.cust_id = o.cust_id
    GROUP BY c.name
    ORDER BY total_spent DESC
""").show()
```

## When to use SQL vs the DataFrame API

- SQL: quick when the query is naturally SQL-shaped (joins, group by, having)
  or when coming from a SQL background.
- DataFrame API: easier to build dynamically in Python (loops, conditionals,
  reusable functions).

## Questions / things to explore next

- Reading/writing Parquet files
- `.explain()` to see how Spark actually plans to execute a query
- User-defined functions (UDFs) for custom logic
