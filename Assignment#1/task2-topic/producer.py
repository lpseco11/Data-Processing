import time
import json
import random
from kafka import KafkaProducer

# Initialize the Kafka producer
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

topic = 'task2-topic'

id = 0
# Simulate user activity logs
activities = ["login", "logout", "purchase", "view_item", "add_to_cart"]

while True:
    # Generate a random user activity
    message = {
        "user_id": f"user{id}",
        "activity": random.choice(activities)
    }
    # Send the message to Kafka
    producer.send(topic, message)
    print(f"Sent: {message}")
    id +=1
    time.sleep(1)  # Send one message per second
    
