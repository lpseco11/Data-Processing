import json
from kafka import KafkaConsumer

# Initialize the Kafka consumer
consumer = KafkaConsumer(
    'task2-topic',
    group_id='activity-group',  # Consumer group name
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    value_deserializer=lambda v: json.loads(v.decode('utf-8'))
)

print("Listening to task2-topic as part of activity-group...")

for message in consumer:
    print(f"Received: {message.value}")
