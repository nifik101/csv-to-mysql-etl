"""MySQL-anslutningsmodul med contextmanager-support."""
from contextlib import contextmanager
from typing import Optional
import mysql.connector
from mysql.connector.connection import MySQLConnection
from mysql.connector import Error
from utils.logger import logger
from dev.config import MYSQL_CONFIG


@contextmanager
def get_mysql_connection(config: Optional[dict] = None):
    """Contextmanager för MySQL-anslutning.
    
    Automatisk stängning av anslutning när context lämnas.
    
    Args:
        config: MySQL-konfiguration. Använder MYSQL_CONFIG från config om None.
        
    Yields:
        MySQL-anslutning
        
    Raises:
        mysql.connector.Error: Om anslutning misslyckas
        
    Example:
        with get_mysql_connection() as conn:
            # Använd anslutning här
            load_to_mysql(df, conn)
        # Anslutning stängs automatiskt
    """
    if config is None:
        config = MYSQL_CONFIG
    
    connection = None
    try:
        logger.info(f"Ansluter till MySQL-databas: {config.get('database')} på {config.get('host')}")
        connection = mysql.connector.connect(**config)
        logger.info("MySQL-anslutning etablerad")
        yield connection
    except Error as e:
        logger.error(f"Fel vid MySQL-anslutning: {e}")
        raise
    finally:
        if connection and connection.is_connected():
            logger.info("Stänger MySQL-anslutning")
            connection.close()