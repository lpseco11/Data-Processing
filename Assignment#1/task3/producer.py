import time
import json
import random
from kafka import KafkaProducer

# Initialize the Kafka producer
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Define topics
purchase_topic = 'purchase-topic'
activity_topic = 'user-activity-topic'

# Simulated data
items = ['book', 'laptop', 'phone', 'headphones', 'backpack']
activities = ['page_view', 'login', 'logout', 'add_to_cart', 'purchase']

while True:
    # Send a purchase transaction
    purchase_message = {
        "user_id": f"user{random.randint(1, 10)}",
        "amount": round(random.uniform(5.0, 100.0), 2),
        "item": random.choice(items)
    }
    producer.send(purchase_topic, purchase_message)
    print(f"Sent to {purchase_topic}: {purchase_message}")

    # Send a user activity
    activity_message = {
        "user_id": f"user{random.randint(1, 10)}",
        "activity": random.choice(activities)
    }
    producer.send(activity_topic, activity_message)
    print(f"Sent to {activity_topic}: {activity_message}")

    time.sleep(2)  # Wait for 2 seconds
