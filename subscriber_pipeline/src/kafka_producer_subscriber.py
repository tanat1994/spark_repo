import time
import json
from subscriber_gens import generate_subscriber_log_line
from kafka import KafkaProducer

kafka_producer = KafkaProducer(bootstrap_servers="localhost:9092")
TOPIC_NAME = "raw_subscriber_pipeline"


def fetch_subscriber_data():
    timestamp, user_id, subscribe_bool, subscribe_plan, country, credit_card_type, transaction_amount = generate_subscriber_log_line()
    data = {}
    data['timestamp'] = timestamp.strftime("%Y-%M-%d %H:%M:%S")
    data['user_id'] = user_id
    data['subscribe_bool'] = subscribe_bool
    data['subscribe_plan'] = subscribe_plan
    data['country'] = country
    data['credit_card_type'] = credit_card_type
    data['transaction_amount'] = transaction_amount
    kafka_message = json.dumps(data)
    kafka_producer.send(TOPIC_NAME, kafka_message.encode('utf-8'))
    print("Message to <TOPIC: {topic}> <Message: {message}>".format(topic=TOPIC_NAME, message=kafka_message))


if __name__ == "__main__":
    while True:
        fetch_subscriber_data()
        time.sleep(5)

