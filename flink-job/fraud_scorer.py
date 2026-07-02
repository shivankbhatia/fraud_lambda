# flink-job/fraud_scorer.py — Phase 3.3: full scoring
import json
import os
import pickle
from urllib.parse import quote

from pyflink.datastream import StreamExecutionEnvironment, KeyedProcessFunction
from pyflink.common import WatermarkStrategy, Types
from pyflink.common.typeinfo import Types as TypeInfoTypes
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream.state import ValueStateDescriptor


class FraudScorer(KeyedProcessFunction):
    """
    Maintains per-destination transaction count (stateful) AND scores
    each transaction using the Day 1 trained XGBoost model.
    """

    def open(self, runtime_context):
        state_descriptor = ValueStateDescriptor("dest_txn_count", TypeInfoTypes.INT())
        self.txn_count_state = runtime_context.get_state(state_descriptor)

        # Load model once per task instance, not per record
        model_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "models", "fraud_model.pkl")
        with open(model_path, 'rb') as f:
            self.model = pickle.load(f)

        feature_list_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "models", "feature_list.pkl")
        with open(feature_list_path, 'rb') as f:
            self.feature_order = pickle.load(f)  # ensures we feed features in the exact order the model expects

    def process_element(self, value, ctx):
        try:
            msg = json.loads(value)
        except Exception as e:
            yield f"PARSE ERROR: {e}"
            return

        # --- stateful feature ---
        current_count = self.txn_count_state.value()
        if current_count is None:
            current_count = 0
        msg['dest_txn_count_so_far'] = current_count
        self.txn_count_state.update(current_count + 1)

        # --- stateless features ---
        msg['is_transfer_type'] = 1  # always true here, we pre-filtered
        msg['amount_to_balance_ratio'] = msg['amount'] / (msg['oldbalanceOrg'] + 1)

        # --- build feature vector in the exact order the model was trained on ---
        feature_row = [[msg[f] for f in self.feature_order]]

        # --- score ---
        fraud_prob = float(self.model.predict_proba(feature_row)[0][1])
        msg['fraud_probability'] = round(fraud_prob, 6)
        msg['is_flagged'] = int(fraud_prob >= 0.5)  # simple threshold for now

        yield json.dumps(msg)


def is_scorable_type(raw_json):
    try:
        msg = json.loads(raw_json)
        return msg['type'] in ('TRANSFER', 'CASH_OUT')
    except Exception:
        return False


def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)

    jar_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "lib")
    jar_dir_encoded = quote(jar_dir)
    env.add_jars(
        f"file://{jar_dir_encoded}/flink-connector-kafka-3.0.2-1.18.jar",
        f"file://{jar_dir_encoded}/kafka-clients-3.4.0.jar"
    )

    kafka_source = (
        KafkaSource.builder()
        .set_bootstrap_servers("localhost:9092")
        .set_topics("transactions")
        .set_group_id("fraud-scorer-group-v4")
        .set_starting_offsets(KafkaOffsetsInitializer.earliest())
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )

    stream = env.from_source(
        kafka_source,
        WatermarkStrategy.no_watermarks(),
        "kafka-source"
    )

    filtered_stream = stream.filter(is_scorable_type)

    def extract_dest_key(raw_json):
        try:
            return json.loads(raw_json)['nameDest']
        except Exception:
            return "UNKNOWN"

    keyed_stream = filtered_stream.key_by(extract_dest_key, key_type=Types.STRING())
    result_stream = keyed_stream.process(FraudScorer(), output_type=Types.STRING())

    result_stream.print()

    env.execute("fraud-scorer-full")


if __name__ == '__main__':
    main()