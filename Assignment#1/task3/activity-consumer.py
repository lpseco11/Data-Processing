import json
from kafka import KafkaConsumer
from collections import defaultdict

# Initialize the Kafka consumer
consumer = KafkaConsumer(
    'user-activity-topic',
    group_id='activity-group',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    value_deserializer=lambda v: json.loads(v.decode('utf-8'))
)

print("Listening to user-activity-topic...")

# Dictionary to store activity count per user
user_activity_count = defaultdict(int)

for message in consumer:
    data = message.value
    user_id = data['user_id']

    # Update activity count
    user_activity_count[user_id] += 1

    # Print the activity count for the user
    print(f"User: {user_id}, Activity Count: {user_activity_count[user_id]}")
