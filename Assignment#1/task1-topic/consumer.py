import json
from kafka import KafkaConsumer

# Initialize the Kafka consumer
consumer = KafkaConsumer(
    'task1-topic',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    value_deserializer=lambda v: json.loads(v.decode('utf-8'))
)

print("Listening to task1-topic...")

for message in consumer:
    print(f"Received: {message.value}")
