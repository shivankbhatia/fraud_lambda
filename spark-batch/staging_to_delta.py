# spark-batch/staging_to_delta.py
#
# Reads finalized (checkpoint-committed) JSON files written by the Flink
# scoring job's FileSink, and commits them into a Delta Lake table.
#
# Uses Delta's MERGE capability (idempotent upsert on transaction_id) so that
# re-running this script - e.g. periodically, or after a crash - does not
# create duplicate records for files that were already committed.

import os
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip
from delta.tables import DeltaTable

builder = (
    SparkSession.builder
    .appName("StagingToDelta")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
)
spark = configure_spark_with_delta_pip(builder).getOrCreate()
spark.sparkContext.setLogLevel("WARN")

base_dir = os.path.dirname(os.path.abspath(__file__))
staging_path = os.path.join(base_dir, "..", "data", "staging")
delta_path = os.path.join(base_dir, "..", "data", "delta", "scored_transactions")

# recursiveFileLookup is required because Flink's FileSink writes into
# hourly bucket subdirectories (e.g. data/staging/2026-07-03--16/...),
# not directly into staging_path itself.
df = spark.read.option("recursiveFileLookup", "true").json(staging_path)
print(f"Read {df.count()} records from staging")

if DeltaTable.isDeltaTable(spark, delta_path):
    delta_table = DeltaTable.forPath(spark, delta_path)
    (
        delta_table.alias("target")
        .merge(df.alias("source"), "target.transaction_id = source.transaction_id")
        .whenNotMatchedInsertAll()
        .execute()
    )
    print("Merged into existing Delta table (duplicates skipped by transaction_id)")
else:
    df.write.format("delta").mode("append").save(delta_path)
    print("Created new Delta table")

# --- Verification ---
result = spark.read.format("delta").load(delta_path)
print(f"Delta table now has {result.count()} total records (deduped)")
result.show(5, truncate=False)

print("\n--- Flagged transactions sample ---")
result.select("dest_txn_count_so_far", "fraud_probability", "is_flagged") \
      .where("is_flagged = 1") \
      .show(20)

print("\n--- Delta transaction history ---")
spark.sql(f"DESCRIBE HISTORY delta.`{delta_path}`").show(truncate=False)