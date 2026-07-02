# Real-Time Fraud Detection — Lambda Architecture

Kafka + Flink + Spark + Delta Lake + Hive fraud detection system combining
real-time transaction scoring with nightly batch retraining.


# Key Findings & Feature Investigation

## Class Imbalance
- Fraud represents **0.129%** of all transactions.
- Fraud is concentrated entirely in **TRANSFER** and **CASH_OUT** transaction types.
- **PAYMENT**, **CASH_IN**, and **DEBIT** contain **0% fraud**.
- The pipeline filters transactions to only **TRANSFER** and **CASH_OUT** before scoring, reducing the streaming volume by **~56%** while maintaining **100% fraud recall**.

## Leakage Investigation
Initial feature engineering included:
- `orig_balance_error`
- `dest_balance_error`

These features measured the deviation between expected and actual account balances after each transaction.

A single-feature AUC analysis showed that `orig_balance_error` alone achieved an **AUC of 0.947**, indicating an unusually strong predictive signal. Further investigation revealed this was caused by a **dataset artifact** rather than genuine fraud behavior.

### Investigation Results
- **32.9%** of legitimate transactions have untracked (zero-value) origin balances in the PaySim simulator.
- **99.5%** of fraudulent transactions exhibit mathematically perfect balance reconciliation.

This behavior is a **known characteristic of the PaySim simulator**, not a realistic fraud indicator. Consequently, both balance-error features were removed from the final feature set.

## Remaining Dominant Feature
After removing the balance-error features:

- `amount_to_balance_ratio` (transaction amount divided by origin balance) accounts for approximately **96%** of model decisions.
- Fraudulent transactions cluster around ratios of **0.9999–1.0**, representing near-total account drainage.

This pattern reflects PaySim's intended fraud simulation, where fraudulent behavior follows an **account-takeover** scenario:

1. Drain the victim's account.
2. Cash out the stolen funds.

While this represents a plausible fraud strategy, it is considerably more deterministic than real-world financial fraud, where transaction amounts are typically much more diverse. This is a limitation of the dataset's fraud-generation process rather than the modeling approach.

## Validation Methodology
Model evaluation uses **walk-forward (expanding-window) validation** across **5 temporal folds** instead of a single random or time-based split.

This decision was made after observing that:

- PaySim's transaction volume drops sharply after approximately **step 400**.
- The absolute number of fraudulent transactions remains roughly constant.
- As a result, the fraud rate varies by more than **20×** across different periods despite stable fraud counts.

Walk-forward validation provides a more reliable estimate of real-world performance by avoiding evaluation on a single potentially unrepresentative time window.

## Final Model Performance

**Model:** XGBoost

| Metric | Value |
|---------|------:|
| Features Used | 4 |
| ROC-AUC | **0.9995 ± 0.0003** |
| PR-AUC | **0.940 ± 0.049** |
| Validation | 5-fold Walk-Forward |

### Final Features
1. `amount_to_balance_ratio`
2. `amount`
3. `dest_txn_count_so_far`
4. `is_transfer_type`