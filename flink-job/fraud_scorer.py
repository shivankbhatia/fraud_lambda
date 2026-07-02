import json
import os
import logging
import sys
from urllib.parse import quote

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.common import WatermarkStrategy, Types
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer
from pyflink.datastream.formats.json import JsonRowDeserializationSchema


def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)  # single parallelism for now - simplicity over performance at this stage

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
        .set_group_id("fraud-scorer-group")
        .set_starting_offsets(KafkaOffsetsInitializer.earliest())
        .set_value_only_deserializer(
            __import__('pyflink.common.serialization', fromlist=['SimpleStringSchema']).SimpleStringSchema()
        )
        .build()
    )

    stream = env.from_source(
        kafka_source,
        WatermarkStrategy.no_watermarks(),  # no event-time semantics, per our scoping decision
        "kafka-source"
    )

    # Trivial pass-through: parse JSON, print it
    def parse_and_print(raw_json):
        try:
            msg = json.loads(raw_json)
            return f"Received: {msg['transaction_id']} | type={msg['type']} | amount={msg['amount']}"
        except Exception as e:
            return f"PARSE ERROR: {e} | raw={raw_json[:100]}"

    stream.map(parse_and_print, output_type=Types.STRING()).print()

    env.execute("fraud-scorer-skeleton")

if __name__ == '__main__':
    main()