# reconciliation/reconcile.py
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from delta import configure_spark_with_delta_pip

base_dir = os.path.dirname(os.path.abspath(__file__))

builder = (
    SparkSession.builder
    .appName("Reconciliation")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.hadoop.hive.metastore.uris", "thrift://localhost:9083")
    .enableHiveSupport()
)
spark = configure_spark_with_delta_pip(builder).getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# --- Load speed layer predictions (Flink -> Delta, via Day 3-4 pipeline) ---
delta_path = os.path.join(base_dir, "..", "data", "delta", "scored_transactions")
speed_df = spark.read.format("delta").load(delta_path)
print(f"Speed layer (Delta): {speed_df.count()} records")

# --- Load batch layer predictions (Spark -> Hive, via Day 5 pipeline) ---
batch_df = spark.table("fraud_detection.batch_scored_transactions")
print(f"Batch layer (Hive): {batch_df.count()} records")

# --- Join on txn_key ---
joined = speed_df.alias("s").join(
    batch_df.alias("b"),
    col("s.txn_key") == col("b.txn_key"),
    "inner"
).select(
    col("s.txn_key"),
    col("b.isFraud").alias("true_label"),
    col("s.fraud_probability").alias("speed_probability"),
    col("s.is_flagged").alias("speed_flagged"),
    col("b.batch_fraud_probability").alias("batch_probability"),
    col("b.batch_is_flagged").alias("batch_flagged"),
    col("s.dest_txn_count_so_far").alias("speed_dest_count"),
    col("b.dest_txn_count_so_far").alias("batch_dest_count"),
)

joined_count = joined.count()
print(f"Joined records (matched on txn_key): {joined_count}")

if joined_count == 0:
    print("WARNING: zero matches. Check txn_key formatting consistency between pipelines.")
else:
    joined.show(10, truncate=False)

# Add to reconciliation/reconcile.py, after the join
from pyspark.sql.functions import abs as spark_abs, when as spark_when

# --- Agreement rate: do speed and batch layers agree on the flag? ---
agreement = joined.withColumn(
    "agree", (col("speed_flagged") == col("batch_flagged")).cast("int")
)
agreement_rate = agreement.selectExpr("avg(agree) as agreement_rate").collect()[0]["agreement_rate"]
print(f"\nAgreement rate (speed vs batch flag): {agreement_rate:.4f}")

# --- Confusion-style breakdown ---
print("\nFlag combination breakdown:")
joined.groupBy("speed_flagged", "batch_flagged").count().orderBy("speed_flagged", "batch_flagged").show()

# --- Compare each layer's predictions against ground truth ---
from pyspark.ml.evaluation import BinaryClassificationEvaluator

joined_pd = joined.toPandas()  # small enough (991 rows) to bring to pandas for sklearn metrics
from sklearn.metrics import roc_auc_score, average_precision_score

print("\n--- Speed layer vs ground truth ---")
print(f"AUC: {roc_auc_score(joined_pd['true_label'], joined_pd['speed_probability']):.4f}")
print(f"PR-AUC: {average_precision_score(joined_pd['true_label'], joined_pd['speed_probability']):.4f}")

print("\n--- Batch layer vs ground truth ---")
print(f"AUC: {roc_auc_score(joined_pd['true_label'], joined_pd['batch_probability']):.4f}")
print(f"PR-AUC: {average_precision_score(joined_pd['true_label'], joined_pd['batch_probability']):.4f}")

print(f"\nFraud cases in joined sample: {joined_pd['true_label'].sum()} out of {len(joined_pd)}")

print("\n--- Sample disagreement cases where speed flagged but batch didn't ---")
joined.filter((col("speed_flagged") == 1) & (col("batch_flagged") == 0)).show(15, truncate=False)

print("\n--- Cases where batch flagged but speed didn't ---")
joined.filter((col("speed_flagged") == 0) & (col("batch_flagged") == 1)).show(15, truncate=False)

print("\n--- Ground truth check: how many actual fraud cases does each layer miss? ---")
print("Speed layer false negatives (missed fraud):")
joined.filter((col("true_label") == 1) & (col("speed_flagged") == 0)).show(truncate=False)
print("Batch layer false negatives (missed fraud):")
joined.filter((col("true_label") == 1) & (col("batch_flagged") == 0)).show(truncate=False)


# Add to reconciliation/reconcile.py, at the end
import matplotlib.pyplot as plt

fig, axes = plt.subplots(1, 3, figsize=(18, 5))

# --- Chart 1: Score distribution comparison (speed vs batch) ---
axes[0].hist(joined_pd['speed_probability'], bins=50, alpha=0.5, label='Speed layer', color='steelblue')
axes[0].hist(joined_pd['batch_probability'], bins=50, alpha=0.5, label='Batch layer', color='crimson')
axes[0].set_yscale('log')  # fraud scores are heavily skewed toward 0
axes[0].set_title('Fraud Probability Distribution: Speed vs Batch')
axes[0].set_xlabel('Predicted fraud probability')
axes[0].set_ylabel('Count (log scale)')
axes[0].legend()

# --- Chart 2: Agreement scatter - where do the two layers diverge? ---
colors = joined_pd.apply(
    lambda r: 'green' if r['true_label'] == 1 else ('crimson' if r['speed_flagged'] != r['batch_flagged'] else 'lightgray'),
    axis=1
)
axes[1].scatter(joined_pd['batch_probability'], joined_pd['speed_probability'], c=colors, alpha=0.6, s=20)
axes[1].plot([0, 1], [0, 1], 'k--', alpha=0.3)  # diagonal = perfect agreement
axes[1].axhline(0.5, color='gray', linestyle=':', alpha=0.5)
axes[1].axvline(0.5, color='gray', linestyle=':', alpha=0.5)
axes[1].set_xlabel('Batch layer probability')
axes[1].set_ylabel('Speed layer probability')
axes[1].set_title('Speed vs Batch Agreement\n(green = actual fraud, red = disagreement)')

# --- Chart 3: dest_txn_count_so_far gap vs disagreement ---
joined_pd['dest_count_gap'] = joined_pd['batch_dest_count'] - joined_pd['speed_dest_count']
joined_pd['disagrees'] = (joined_pd['speed_flagged'] != joined_pd['batch_flagged']).astype(int)
axes[2].scatter(joined_pd['dest_count_gap'], joined_pd['disagrees'],
                alpha=0.3, s=15, c=joined_pd['disagrees'], cmap='coolwarm')
axes[2].set_xlabel('dest_txn_count_so_far gap (batch - speed)')
axes[2].set_ylabel('Disagreement (0=agree, 1=disagree)')
axes[2].set_title('State Completeness Gap vs Disagreement')

plt.tight_layout()
plt.savefig(os.path.join(base_dir, 'reconciliation_charts.png'), dpi=150)
print(f"\nCharts saved to {os.path.join(base_dir, 'reconciliation_charts.png')}")
plt.show()