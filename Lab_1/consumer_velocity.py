from kafka import KafkaConsumer
from collections import defaultdict
from datetime import datetime, timedelta
import json

consumer = KafkaConsumer(
    "transactions",
    bootstrap_servers="broker:9092",
    auto_offset_reset="earliest",
    group_id="velocity-group",
    value_deserializer=lambda x: json.loads(x.decode("utf-8"))
)

user_timestamps = defaultdict(list)

print("Listening for velocity anomalies...")

for message in consumer:
    transaction = message.value

    user_id = transaction["user_id"]
    tx_id = transaction["tx_id"]
    amount = transaction["amount"]
    timestamp = datetime.fromisoformat(transaction["timestamp"])

    user_timestamps[user_id].append(timestamp)

    cutoff_time = timestamp - timedelta(seconds=60)

    user_timestamps[user_id] = [
        t for t in user_timestamps[user_id]
        if t >= cutoff_time
    ]

    if len(user_timestamps[user_id]) > 3:
        print(
            f"VELOCITY ALERT: {user_id} made "
            f"{len(user_timestamps[user_id])} transactions within 60 seconds "
            f"| latest tx: {tx_id} | amount: {amount:.2f} PLN"
        )