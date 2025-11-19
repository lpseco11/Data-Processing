import json
from kafka import KafkaConsumer
from collections import defaultdict

# Initialize the Kafka consumer
consumer = KafkaConsumer(
    'purchase-topic',
    group_id='purchase-group',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    value_deserializer=lambda v: json.loads(v.decode('utf-8'))
)

print("Listening to purchase-topic...")

# Dictionary to store total amount spent per user
user_totals = defaultdict(float)

for message in consumer:
    data = message.value
    user_id = data['user_id']
    amount = data['amount']

    # Update running total
    user_totals[user_id] += amount

    # Print the running total for the user
    print(f"User: {user_id}, Total Spent: {user_totals[user_id]:.2f}")
