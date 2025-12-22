"""Modul för MySQL-databasanslutning."""

import os

import mysql.connector
from dotenv import load_dotenv

# Ladda miljövariabler från .env-fil
load_dotenv()


def get_mysql_connection():
    """
    Skapar och returnerar en MySQL-databasanslutning.

    Läser databasinställningar från miljövariabler:
    - MYSQL_HOST
    - MYSQL_USER
    - MYSQL_PASSWORD
    - MYSQL_DATABASE

    Returns:
        mysql.connector.connection.MySQLConnection: MySQL-anslutningsobjekt

    Raises:
        mysql.connector.Error: Om anslutningen misslyckas
    """
    config = {
        "host": os.getenv("MYSQL_HOST", "localhost"),
        "user": os.getenv("MYSQL_USER", "root"),
        "password": os.getenv("MYSQL_PASSWORD", ""),
        "database": os.getenv("MYSQL_DATABASE", "de_project"),
    }
    return mysql.connector.connect(**config)
