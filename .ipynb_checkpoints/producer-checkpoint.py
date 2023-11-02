from confluent_kafka import Producer
import requests
import json
import time

# Kafka producer configuration
producer_config = {
    'bootstrap.servers': 'kafka:9092',  # Your Kafka broker(s)
    'client.id': 'python-producer'
}

# Create a Kafka producer instance
producer = Producer(producer_config)

kafka_topic = 'user_data_topic'

def get_user_data():
    while True:
        try:
            # Get user data from the Random User Generator API
            response = requests.get("https://randomuser.me/api")
            response.raise_for_status()
            user_data = response.json()
            return user_data
        except requests.exceptions.RequestException as e:
            print(f"Request exception: {str(e)}. Retrying...")
            time.sleep(5)  # Wait for a few seconds before retrying

def delivery_report(err, msg):
    if err is not None:
        print(f'Message delivery failed: {err}')
    else:
        print(f'Message delivered to topic: {msg.topic()} [{msg.partition()}]')

# Produce user data to Kafka
while True:
    user_data = get_user_data()
    value = json.dumps(user_data)

    producer.produce(kafka_topic, value=value, callback=delivery_report)
    producer.poll(0)  # Trigger any message delivery reports (non-blocking)
    producer.flush()
    print(value)
    time.sleep(5)  # Wait for a few seconds before fetching and sending the next user data
