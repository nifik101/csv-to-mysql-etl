"""Load-modul för att ladda data till MySQL.

Denna modul har ett enda ansvar: att ladda DataFrame till MySQL-databas.
Alla transformationer och valideringar ska vara gjorda innan denna funktion anropas.
"""
from typing import Optional
import pandas as pd
import mysql.connector
from mysql.connector.connection import MySQLConnection
from utils.logger import logger
from dev.config import DB_TABLE_NAME, DB_COLUMNS
from validation.schemas import validate_load


def prepare_dataframe_for_mysql(df: pd.DataFrame) -> pd.DataFrame:
    """Förbereder DataFrame för MySQL-insert.
    
    Ersätter NaN med None och konverterar datetime till date.
    
    Args:
        df: DataFrame att förbereda
        
    Returns:
        Förberedd DataFrame
    """
    df = df.copy()
    # Ersätt NaN med None för MySQL
    df = df.where(pd.notnull(df), None)
    return df


def build_insert_query(table_name: str, columns: list[str]) -> str:
    """Bygger INSERT IGNORE query dynamiskt.
    
    Args:
        table_name: Tabellnamn
        columns: Lista med kolumnnamn
        
    Returns:
        SQL INSERT-query som sträng
    """
    columns_str = ", ".join(columns)
    placeholders = ", ".join(["%s"] * len(columns))
    return f"INSERT IGNORE INTO {table_name} ({columns_str}) VALUES ({placeholders})"


def prepare_row_data(row: pd.Series, columns: list[str]) -> tuple:
    """Förbereder en rad för MySQL-insert.
    
    Konverterar datetime-objekt till date och hanterar None-värden.
    
    Args:
        row: Pandas Series (en rad från DataFrame)
        columns: Lista med kolumnnamn att extrahera
        
    Returns:
        Tuple med värden i rätt ordning
    """
    row_data = []
    for col in columns:
        value = row[col]
        # Konvertera datetime till date om det är datetime-objekt
        if pd.api.types.is_datetime64_any_dtype(type(value)) and value is not None:
            value = value.date()
        row_data.append(value)
    return tuple(row_data)


def load_to_mysql(
    df: pd.DataFrame,
    connection: MySQLConnection,
    table_name: Optional[str] = None,
    columns: Optional[list[str]] = None,
    validate_before_load: bool = True,
) -> int:
    """Laddar DataFrame till MySQL-databas.
    
    Denna funktion har ett enda ansvar: att ladda data till databasen.
    Alla transformationer ska vara gjorda innan anrop.
    
    Args:
        df: DataFrame att ladda (måste innehålla alla nödvändiga kolumner)
        connection: Öppen MySQL-anslutning
        table_name: Tabellnamn. Använder DB_TABLE_NAME från config om None.
        columns: Lista med kolumnnamn att ladda. Använder DB_COLUMNS från config om None.
        validate_before_load: Om validering ska köras innan load (default: True)
        
    Returns:
        Antal rader som laddades
        
    Raises:
        ValueError: Om kolumner saknas i DataFrame
        mysql.connector.Error: Vid databasfel
        
    Example:
        with get_mysql_connection() as conn:
            rows_loaded = load_to_mysql(df, conn, table_name="my_table")
    """
    if table_name is None:
        table_name = DB_TABLE_NAME
    if columns is None:
        columns = DB_COLUMNS
    
    # Validera innan load (valfritt)
    if validate_before_load:
        try:
            logger.info("Validerar data innan load")
            df = validate_load(df)
        except Exception as e:
            logger.warning(f"Validering gav varning: {e}")
    
    # Kontrollera att alla kolumner finns
    missing_columns = [col for col in columns if col not in df.columns]
    if missing_columns:
        logger.error(f"Saknade kolumner i DataFrame: {missing_columns}")
        raise ValueError(f"Saknade kolumner: {missing_columns}")
    
    # Förbered data
    df = prepare_dataframe_for_mysql(df)
    
    cursor = None
    try:
        logger.info(f"Laddar {len(df)} rader till tabell {table_name}")
        cursor = connection.cursor()
        
        # Bygg query
        insert_query = build_insert_query(table_name, columns)
        
        # Förbered alla rader
        data = [prepare_row_data(row, columns) for _, row in df.iterrows()]
        
        # Kör insert
        cursor.executemany(insert_query, data)
        connection.commit()
        
        rows_loaded = cursor.rowcount
        logger.info(f"Laddade {rows_loaded} rader till {table_name}")
        return rows_loaded
        
    except mysql.connector.Error as e:
        logger.error(f"Databasfel vid load: {e}")
        connection.rollback()
        raise
    except Exception as e:
        logger.error(f"Fel vid load: {e}")
        connection.rollback()
        raise ValueError(f"Load misslyckades: {e}") from e
    finally:
        if cursor:
            cursor.close()