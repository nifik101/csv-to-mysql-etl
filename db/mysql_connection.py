import mysql.connector
from dev.config import MYSQL_CONFIG

def get_mysql_connection():
    return mysql.connector.connect(**MYSQL_CONFIG)