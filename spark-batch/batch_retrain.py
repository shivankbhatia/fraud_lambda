# spark-batch/batch_retrain.py
#
# Day 5 batch layer: reads the full labeled PaySim dataset (simulating
# "delayed fraud labels" becoming available), recomputes the same feature
# set used by the Flink speed layer, retrains XGBoost, and writes a
# curated, queryable table to Hive.
#
# NOTE ON JOIN KEY: Flink's live stream assigns each transaction a random
# UUID (transaction_id) at replay time, which has no relationship to the
# original CSV rows. To allow Day 6 reconciliation to compare the speed
# layer's predictions against this batch table's predictions for the
# "same" transaction, we derive a composite natural key from fields both
# pipelines have access to: nameOrig + nameDest + step + amount. This
# should be unique in practice for this dataset; documented as a known
# simplification rather than a guaranteed-unique surrogate key.

import os
import pickle

import pandas as pd
import xgboost as xgb
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, row_number, concat_ws
from pyspark.sql.window import Window
from sklearn.metrics import roc_auc_score, average_precision_score

base_dir = os.path.dirname(os.path.abspath(__file__))

spark = (
    SparkSession.builder
    .appName("BatchRetrain")
    .config("hive.metastore.uris", "thrift://localhost:9083")
    .config("spark.sql.warehouse.dir", os.path.join(base_dir, "..", "data", "hive-warehouse"))
    .enableHiveSupport()
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

# --- Load ---
csv_path = os.path.join(base_dir, "..", "data", "PS_20174392719_1491204439457_log.csv")
df = spark.read.csv(csv_path, header=True, inferSchema=True)

df_filtered = df.filter(col("type").isin("TRANSFER", "CASH_OUT"))
print(f"Filtered to {df_filtered.count()} TRANSFER/CASH_OUT transactions")

# --- Join key for later reconciliation with the Flink speed layer ---
df_filtered = df_filtered.withColumn(
    "txn_key",
    concat_ws("|", col("nameOrig"), col("nameDest"), col("step").cast("string"), col("amount").cast("string"))
)

# --- Feature 1: is_transfer_type (trivially 1, we already filtered) ---
df_feat = df_filtered.withColumn("is_transfer_type", lit(1))

# --- Feature 2: amount_to_balance_ratio ---
df_feat = df_feat.withColumn(
    "amount_to_balance_ratio",
    col("amount") / (col("oldbalanceOrg") + 1)
)

# --- Feature 3: dest_txn_count_so_far (Spark window-function equivalent of
# Flink's ValueState counter and pandas' cumcount()). A secondary sort key
# (nameOrig) is added to make row ordering deterministic among transactions
# that share the same (nameDest, step) - Spark's row_number() ordering among
# true ties is otherwise not guaranteed stable across runs.
dest_window = Window.partitionBy("nameDest").orderBy("step", "nameOrig")
df_feat = df_feat.withColumn(
    "dest_txn_count_so_far",
    row_number().over(dest_window) - 1
)

# --- Feature 4: amount (already present, no transform needed) ---

feature_cols = ["is_transfer_type", "amount_to_balance_ratio", "dest_txn_count_so_far", "amount"]
model_df = df_feat.select(*feature_cols, "isFraud", "step", "nameDest", "txn_key")

model_df.show(5, truncate=False)
print(f"Final modeling dataframe: {model_df.count()} rows")

# --- Convert to pandas for XGBoost training (fits comfortably in memory at this size) ---
pdf = model_df.toPandas()
print(f"Converted to pandas: {pdf.shape}")

# Time-respecting split, same approach as Day 1 (not random shuffle)
pdf_sorted = pdf.sort_values("step").reset_index(drop=True)
split_step = pdf_sorted["step"].quantile(0.8)

train_df = pdf_sorted[pdf_sorted["step"] <= split_step]
test_df = pdf_sorted[pdf_sorted["step"] > split_step].copy()

X_train, y_train = train_df[feature_cols], train_df["isFraud"]
X_test, y_test = test_df[feature_cols], test_df["isFraud"]

scale_pos_weight = (y_train == 0).sum() / (y_train == 1).sum()

batch_model = xgb.XGBClassifier(
    n_estimators=200, max_depth=6, learning_rate=0.1,
    scale_pos_weight=scale_pos_weight, eval_metric="aucpr", random_state=42
)
batch_model.fit(X_train, y_train)

probs = batch_model.predict_proba(X_test)[:, 1]
print(f"Batch model AUC: {roc_auc_score(y_test, probs):.4f}")
print(f"Batch model PR-AUC: {average_precision_score(y_test, probs):.4f}")

# Save as a distinct artifact from Day 1's original model - Day 6 reconciliation
# compares predictions from both models against each other.
batch_model_path = os.path.join(base_dir, "..", "models", "fraud_model_batch.pkl")
with open(batch_model_path, "wb") as f:
    pickle.dump(batch_model, f)
print(f"Batch model saved to {batch_model_path}")

# --- Write curated, scored test-set predictions to Hive ---
# Intentionally test-set-only (~20% most recent by step): this represents
# "the batch layer's corrected predictions for recent transactions",
# the meaningful comparison set for Day 6 reconciliation against the
# speed layer - not a full historical dump.
test_df["batch_fraud_probability"] = probs
test_df["batch_is_flagged"] = (probs >= 0.5).astype(int)

result_spark_df = spark.createDataFrame(test_df)

spark.sql("CREATE DATABASE IF NOT EXISTS fraud_detection")
result_spark_df.write.mode("overwrite").saveAsTable("fraud_detection.batch_scored_transactions")
print("Written to Hive table: fraud_detection.batch_scored_transactions")

# --- Verification ---
spark.sql(
    "SELECT COUNT(*) as total, SUM(isFraud) as fraud_count "
    "FROM fraud_detection.batch_scored_transactions"
).show()
spark.sql("SELECT * FROM fraud_detection.batch_scored_transactions LIMIT 5").show(truncate=False)