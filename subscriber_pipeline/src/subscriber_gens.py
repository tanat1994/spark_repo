import random
from faker import Faker
from datetime import datetime


LOG_LINE = """
{timestamp},{user_id},{subscribe_bool},{subscribe_plan},{country},{credit_card_type},{transaction_amount}
"""


def generate_subscriber_log_line():
    fake = Faker()
    timestamp = datetime.now()
    user_id = fake.email()
    country = ['Thailand', 'Japan', 'USA', 'Korea', 'China', 'Vietnam', 'Singapore']
    subscribe_bool = fake.random_int(0, 1)
    subscribe_plan = ['p01', 'p02']
    credit_card_type = ['Visa', 'MasterCard', 'American Express', 'Maestro']

    subscribe_plan_rand = random.choice(subscribe_plan)
    if subscribe_plan_rand == "p01":
        transaction_amount = 5
    else:
        transaction_amount = 10

    subscriber_log_line = LOG_LINE.format(
        timestamp=timestamp,
        user_id=user_id,
        subscribe_bool=subscribe_bool,
        subscribe_plan=subscribe_plan_rand,
        country=random.choice(country),
        credit_card_type=random.choice(credit_card_type),
        transaction_amount=transaction_amount
    )

    return timestamp, user_id, subscribe_bool, subscribe_plan_rand, random.choice(country), random.choice(credit_card_type), transaction_amount
    # return subscriber_log_line


def write_subscriber_log(log_file, log_line):
    with open(log_file, "a") as file:
        print(log_line)
        file.write(log_line)


if __name__ == "__main__":
    for _ in range(10):
        LOG_FILE = "test_subscriber_log.csv"
        write_subscriber_log(LOG_FILE, generate_subscriber_log_line())