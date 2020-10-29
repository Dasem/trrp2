import socket
import pymysql
import constants
from des import DesKey
from kafka import KafkaConsumer
import string


mysql = pymysql.connect(constants.mysql_ip, constants.mysql_user, constants.mysql_pwd, constants.mysql_db)

mysql_cur = mysql.cursor()

key = DesKey(constants.key)


def execute_all():
    if constants.mode == 'KAFKA':
        kafka_receiver = KafkaConsumer('TutorialTopic', bootstrap_servers=constants.kafka_ip_port)
        for msg in kafka_receiver:
            item = key.decrypt(msg.value, padding=True).decode("utf-8")

            print(f'recieved data:"{item}"')
            if item == 'exit':
                return
            mysql_cur.execute(item)
            mysql.commit()
    elif constants.mode == 'SOCKET':
        socket_receiver = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        socket_receiver.bind(('', constants.socket_port))
        socket_receiver.listen()
        conn, addr = socket_receiver.accept()
        lines = linesplit(conn)
        for line in lines:
            if line.strip():
                print(f'recieved data:"{line}"')
                mysql_cur.execute(line)
                mysql.commit()


def linesplit(socket):
    printable = set(string.printable+'ёйцукенгшщзхъфывапролджэячсмитьбюЁЙЦУКЕНГШЩЗХЪФЫВАПРОЛДЖЭЯЧСМИТЬБЮ')
    buffer = key.decrypt(socket.recv(4096), padding=True).decode('utf-8', errors='ignore')
    buffer = ''.join(filter(lambda x: x in printable, buffer))
    buffering = True
    while buffering:
        if "\n" in buffer:
            (line, buffer) = buffer.split("\n", 1)
            yield line
        else:
            more = socket.recv(4096)
            if not more:
                buffering = False
            else:
                more = key.decrypt(more, padding=True).decode('utf-8')
                more = ''.join(filter(lambda x: x in printable, more))
                buffer += more
    if buffer:
        yield buffer


def init_db():
    for line in open('tables3form.sql'):
        mysql_cur.execute(line)


if __name__ == '__main__':
    init_db()

    execute_all()

    mysql.close()
