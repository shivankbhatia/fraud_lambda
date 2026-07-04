# producer/kafka_producer.py
import csv
import json
import time
import argparse
from datetime import datetime, timezone
from kafka import KafkaProducer
import uuid


def create_producer(bootstrap_servers='localhost:9092'):
    return KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        key_serializer=lambda k: k.encode('utf-8') if k else None
    )


def replay_transactions(csv_path, topic, speed_multiplier=1.0, limit=None, start_step=None):
    producer = create_producer()

    with open(csv_path, 'r') as f:
        reader = csv.DictReader(f)

        prev_step = None
        count = 0

        for row in reader:
            if limit and count >= limit:
                break

            current_step = int(row['step'])

            # Skip rows before the desired step range - lets us target the
            # batch layer's test-split window (step > 354, per Day 1's split)
            # so speed-layer and batch-layer predictions actually overlap
            # for Day 6 reconciliation.
            if start_step is not None and current_step < start_step:
                continue

            # Simulate time passing between steps (1 step = 1 hour in PaySim)
            if prev_step is not None and current_step != prev_step:
                step_delta = current_step - prev_step
                sleep_time = (step_delta * 3600) / (3600 * speed_multiplier)
                time.sleep(min(sleep_time, 2.0))

            message = {
                "transaction_id": str(uuid.uuid4()),
                "step": current_step,
                "type": row['type'],
                "amount": float(row['amount']),
                "nameOrig": row['nameOrig'],
                "oldbalanceOrg": float(row['oldbalanceOrg']),
                "newbalanceOrig": float(row['newbalanceOrig']),
                "nameDest": row['nameDest'],
                "oldbalanceDest": float(row['oldbalanceDest']),
                "newbalanceDest": float(row['newbalanceDest']),
                "event_timestamp": datetime.now(timezone.utc).isoformat()
            }

            producer.send(topic, key=row['nameDest'], value=message)

            prev_step = current_step
            count += 1

            if count % 1000 == 0:
                print(f"Sent {count} transactions... (step {current_step})")

        producer.flush()
        print(f"Done. Total sent: {count}")


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--csv', default='./data/PS_20174392719_1491204439457_log.csv')
    parser.add_argument('--topic', default='transactions')
    parser.add_argument('--speed', type=float, default=3600.0,
                         help='Speed multiplier. 3600 = 1 sim-hour per real second. Use higher for faster testing.')
    parser.add_argument('--limit', type=int, default=None,
                         help='Max transactions to send (for testing)')
    parser.add_argument('--start-step', type=int, default=None,
                         help='Only replay rows with step >= this value. '
                              'Use 355+ to overlap with the Day 5 batch test split.')
    args = parser.parse_args()

    replay_transactions(args.csv, args.topic, args.speed, args.limit, args.start_step)