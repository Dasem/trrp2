import socket
import sqlite3
import constants
from des import DesKey
from kafka import KafkaProducer

key = DesKey(constants.key)

sqlite = sqlite3.connect(constants.sqlite_db)

sqlite_cur = sqlite.cursor()

kafka_sender = KafkaProducer(bootstrap_servers=constants.kafka_ip_port, api_version=(1, 0, 0))
socket_sender = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

if constants.mode == 'SOCKET':
    socket_sender.connect((constants.socket_ip, constants.socket_port))


def send_data(data):
    print(f'sended data:"{data}"')
    if constants.mode == 'KAFKA':
        encrypted_data = key.encrypt(bytes(data, 'utf=8'), padding=True)
        print(f'encrypted data:"{encrypted_data}"')
        kafka_sender.send('TutorialTopic', encrypted_data)
    elif constants.mode == 'SOCKET':
        encrypted_data = key.encrypt(bytes(data + '\n', 'utf=8'), padding=True)
        print(f'encrypted data:"{encrypted_data}"')
        socket_sender.sendall(encrypted_data)


def extract_sellers(data):
    sellers = set()
    for row in data:
        sellers.add(row[constants.seller_name_col])
    sellers.discard(None)

    for name in sellers:
        send_data(f"insert into sellers (sellers_name) values ('{name}')")


def extract_customers(data):
    customers = set()
    for row in data:
        customers.add(row[constants.customer_name_col])
    customers.discard(None)

    for name in customers:
        send_data(f"insert into customers (customer_name) values ('{name}')")


def extract_countries(data):
    countries = set()
    for row in data:
        countries.add(row[constants.origin_country_col])
    countries.discard(None)

    for name in countries:
        send_data(f"insert into countries (country_name) values ('{name}')")


def extract_recepeits(data):
    sqlite_cur.execute(
        'select distinct seller_name, customer_name, sell_date, buy_rejected, payment_date from sellers;')
    data = sqlite_cur.fetchall()
    for row in data:
        if row[0] is None:
            continue
        send_data(
            f"insert into recepeit (sell_date, payment_date, customer_id, sellers_id, refused) values ('{row[2]}', '{row[4]}', \
        (select id from customers where customer_name='{row[1]}'), \
        (select id from sellers where sellers_name='{row[0]}'),\
        {row[3]})")


def extract_discounts(data):
    sqlite_cur.execute(
        'select distinct name_goods, discount_value, expiration_date from sellers;')
    data = sqlite_cur.fetchall()
    for row in data:
        if row[1] is None:
            continue
        send_data(
            f"insert into discounts (goods_id, value, expiration_date) values (\
        (select id from goods where goods_name='{row[0]}'), \
        {row[1]}, \
        '{row[2]}')")


def extract_goods(data):
    sqlite_cur.execute(
        'select distinct origin_country, model, name_goods, cost, count_in_stock from sellers;')
    data = sqlite_cur.fetchall()
    for row in data:
        if row[2] is None:
            continue
        send_data(
            f"insert into goods (country_id, model, goods_name, cost, stock_count) values (\
        (select id from countries where country_name='{row[0]}'), \
        '{row[1]}', \
        '{row[2]}',\
        {row[3]},\
        {row[4]})")


def extract_sells(data):
    for row in data:
        send_data(
            f"insert into sells (goods_id, recepeit_id) values (\
            (select id from goods where goods_name='{row[constants.name_goods_col]}'), \
            (select id from recepeit where sell_date='{row[constants.sell_date_col]}' and payment_date='{row[constants.payment_date_col]}'))")


def init_db():
    for line in open('tables3form.sql'):
        send_data(line)


def extract_all(data):
    extract_sellers(data)
    extract_customers(data)
    extract_countries(data)
    extract_recepeits(data)
    extract_goods(data)
    extract_discounts(data)
    extract_sells(data)


if __name__ == '__main__':
    sqlite_cur.execute(
        'select model, name_goods, origin_country, cost, count_in_stock, seller_name, customer_name, sell_date, discount_value, buy_rejected, payment_date, expiration_date from sellers;')
    data = sqlite_cur.fetchall()

    init_db()

    extract_all(data)

    if constants.mode == 'KAFKA':
        send_data('exit')
        kafka_sender.flush()

    sqlite.close()
