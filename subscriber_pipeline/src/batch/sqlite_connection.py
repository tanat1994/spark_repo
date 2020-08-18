import sqlite3
import os
import sys

p = os.path.abspath('../..')
if p not in sys.path:
    sys.path.append(p)

from subscriber_pipeline.src.subscriber_gens import generate_subscriber_log_line


def db_create_connection():
    try:
        conn = sqlite3.connect("../subscriber.db")
        return conn
    except sqlite3.Error as error:
        print("Error logs: ", error)


def db_create_table():
    conn = db_create_connection()
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS subscriber_data
    (user_email text, subscriber_bool boolean, subscriber_plan text, 
    country text, credit_card_type text, transaction_amount integer, created_at TIMESTAMP)''')


def db_insert_data(insert_data):
    try:
        conn = db_create_connection()
        c = conn.cursor()
        sql_insert_query = '''INSERT INTO 
        subscriber_data(user_email, subscriber_bool, subscriber_plan, country, credit_card_type, transaction_amount, created_at) 
        VALUES(?,?,?,?,?,?,?);'''
        c.execute(sql_insert_query, insert_data)
        conn.commit()
        c.close()
        conn.close()
    except sqlite3.Error as error:
        print("Error logs: ", error)


def gens_dummy_data():
    timestamp, user_id, subscribe_bool, subscribe_plan, country, credit_card_type, transaction_amount = generate_subscriber_log_line()
    data_tuple = [user_id, subscribe_bool, subscribe_plan, country, credit_card_type, transaction_amount, timestamp]
    return data_tuple


def db_test_select():
    # test_data = gens_dummy_data()
    # print("======= Print new dummy data =======")
    # print(test_data)
    # db_insert_data(test_data)
    conn = db_create_connection()
    c = conn.cursor()
    sql_query = "SELECT * FROM subscriber_data;"
    c.execute(sql_query)
    rows = c.fetchall()
    print("======= SELECT * FROM subscriber_data =======")
    for row in rows:
        print(row)


def test():
    return 'aa'


def main():
    db_create_table()
    db_test_select()


if __name__ == "__main__":
    main()


