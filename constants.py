import configparser

config = configparser.ConfigParser()
config.read('default.properties')

mysql_ip = config.get('DefaultSection', 'mysql_ip')
mysql_db = config.get('DefaultSection', 'mysql_db')
mysql_user = config.get('DefaultSection', 'mysql_user')
mysql_pwd = config.get('DefaultSection', 'mysql_pwd')

mode = config.get('DefaultSection', 'mode')  # SOCKET, KAFKA

key = bytes(config.get('DefaultSection', 'key'), 'utf-8')

kafka_ip_port = config.get('DefaultSection', 'kafka_ip_port')
socket_ip = config.get('DefaultSection', 'socket_ip')
socket_port = int(config.get('DefaultSection', 'socket_port'))

sqlite_db = config.get('DefaultSection', 'sqlite_db')

model_col = 0
name_goods_col = 1
origin_country_col = 2
cost_col = 3
count_in_stock_col = 4
seller_name_col = 5
customer_name_col = 6
sell_date_col = 7
discount_value_col = 8
buy_rejected_col = 9
payment_date_col = 10
expiration_date_col = 11






