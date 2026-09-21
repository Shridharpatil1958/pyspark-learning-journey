"""
Day 14 — Delta Lake basics: write/read, time travel, update/delete/merge.

NOTE: requires the delta-spark package, which isn't installed by default.
Install with:
    pip install delta-spark
And build the SparkSession with Delta support as shown below (this is the
one place the setup differs from a plain SparkSession.builder call).
"""

from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip

builder = (
    SparkSession.builder.appName("Day14-DeltaLakeBasics")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
)
spark = configure_spark_with_delta_pip(builder).getOrCreate()

# --- Write and read, same shape as Parquet but format="delta" ---
data = [("North", "Widget", 1200), ("South", "Gadget", 1500)]
df = spark.createDataFrame(data, ["region", "product", "revenue"])

df.write.format("delta").mode("overwrite").save("sales_delta")
print("=== Version 0: initial write ===")
spark.read.format("delta").load("sales_delta").show()

# --- Make a second write to create a new version ---
more_data = [("East", "Widget", 400)]
more_df = spark.createDataFrame(more_data, ["region", "product", "revenue"])
more_df.write.format("delta").mode("append").save("sales_delta")

print("=== Version 1: after appending a row ===")
spark.read.format("delta").load("sales_delta").show()

# --- Time travel: read version 0 even though we're now on version 1 ---
print("=== Time travel back to version 0 ===")
spark.read.format("delta").option("versionAsOf", 0).load("sales_delta").show()

# --- Update / delete specific rows without rewriting the whole file ---
from delta.tables import DeltaTable

delta_table = DeltaTable.forPath(spark, "sales_delta")

print("=== After updating North's revenue by +10% ===")
delta_table.update(condition="region = 'North'", set={"revenue": "revenue * 1.1"})
delta_table.toDF().show()

print("=== After deleting rows with revenue < 500 ===")
delta_table.delete(condition="revenue < 500")
delta_table.toDF().show()

# --- Merge (upsert): update existing rows, insert new ones, in one step ---
incoming = spark.createDataFrame(
    [("North", "Widget", 999), ("West", "Gizmo", 250)],  # North exists, West is new
    ["region", "product", "revenue"],
)

print("=== After merge (upsert) with incoming data ===")
(
    delta_table.alias("target")
    .merge(incoming.alias("source"), "target.region = source.region")
    .whenMatchedUpdateAll()
    .whenNotMatchedInsertAll()
    .execute()
)
delta_table.toDF().show()

spark.stop()
