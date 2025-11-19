import time
import random
import json
from kafka import KafkaProducer

# Initialize the Kafka producer
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

topic = 'task1-topic'

while True:
    # Generate a random temperature reading
    message = {
        "sensor_id": random.randint(1, 10),
        "temperature": round(random.uniform(15.0, 35.0), 2)
    }
    # Send the message to Kafka
    producer.send(topic, message)
    print(f"Sent: {message}")
    time.sleep(2)  # Wait for 2 seconds
