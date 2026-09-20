"""
Day 13 — cloud storage paths (S3 / ADLS / GCS).

NOTE: unlike previous days, this script is a REFERENCE, not something that
runs end-to-end without real cloud credentials and a real bucket. Run the
local-file part directly; the cloud parts are shown as commented examples
of the exact syntax you'd use.
"""

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Day13-CloudStorage").getOrCreate()

# --- This part actually runs: local read/write, same API as cloud storage ---
data = [("North", "Widget", 1200), ("South", "Gadget", 1500)]
df = spark.createDataFrame(data, ["region", "product", "revenue"])

df.write.mode("overwrite").parquet("local_output.parquet")
df2 = spark.read.parquet("local_output.parquet")
print("=== Read back from local Parquet (same API works for cloud paths) ===")
df2.show()

# --- Reference only: the same code, pointed at S3 instead ---
#
# spark.conf.set("fs.s3a.access.key", "YOUR_ACCESS_KEY")
# spark.conf.set("fs.s3a.secret.key", "YOUR_SECRET_KEY")
#
# df.write.mode("overwrite").parquet("s3a://my-bucket/sales_data/")
# df_from_s3 = spark.read.parquet("s3a://my-bucket/sales_data/")
# df_from_s3.show()

# --- Reference only: Azure Data Lake Storage ---
#
# df.write.mode("overwrite").parquet(
#     "abfss://container@account.dfs.core.windows.net/sales_data/"
# )

# --- Reference only: Google Cloud Storage ---
#
# df.write.mode("overwrite").parquet("gs://my-bucket/sales_data/")

print("\nIn a real cloud environment, only the path (and auth setup) changes —")
print("every DataFrame operation you've learned so far works exactly the same.")

spark.stop()
