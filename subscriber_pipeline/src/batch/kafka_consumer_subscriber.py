import json
from sqlite_connection import db_insert_data
from kafka import KafkaConsumer

TOPIC = 'raw_subscriber_pipeline'
DATA_FILE = 'data_src/subscriber_data.csv'
CSV_LOG_LINE = """{user_id},{subscribe_bool},{subscribe_plan},{country},{credit_type},{transaction_amount},{timestamp}\n"""


def create_new_log_line(user_id, subscribe_bool, subscribe_plan, country, credit_type, transaction_amount, timestamp):
    return CSV_LOG_LINE.format(
        user_id=user_id,
        subscribe_bool=subscribe_bool,
        subscribe_plan=subscribe_plan,
        country=country,
        credit_type=credit_type,
        transaction_amount=transaction_amount,
        timestamp=timestamp
    )


def main():
    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=["localhost:9092"],
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )
    #value_deserializer=lambda x: json.loads(x.decode('utf-8'))

    print("Reading from Topic: <{}>".format(TOPIC))

    for message in consumer:
        message_val = message.value
        timestamp = message_val.get("timestamp")
        user_id = message_val.get("user_id")
        subscribe_bool = message_val.get("subscribe_bool")
        subscribe_plan = message_val.get("subscribe_plan")
        country = message_val.get("country")
        credit_type = message_val.get("credit_card_type")
        transaction_amount = message_val.get("transaction_amount")
        subscriber_log = create_new_log_line(user_id, subscribe_bool, subscribe_plan, country, credit_type, transaction_amount, timestamp)

        with open(DATA_FILE, "a") as file:
            print(subscriber_log)
            file.write(subscriber_log)


if __name__ == "__main__":
    main()

