"""
Huvudmodul för ETL-pipeline.

Orkestrerar hela ETL-processen:
1. Extraherar data från CSV-fil
2. Transformerar data till databasformat (4 separata DataFrames)
3. Laddar data till MySQL-databas med UPSERT-logik
"""
import os
import sys
from pathlib import Path
from loguru import logger
from dotenv import load_dotenv

from etl_pipeline.extract import extract_csv
from etl_pipeline.transform import transform_csv
from etl_pipeline.load import load_to_mysql
from db.mysql_connection import get_mysql_connection

# Ladda miljövariabler
load_dotenv()

# Konfigurera loguru baserat på LOG_LEVEL miljövariabel
log_level = os.getenv("LOG_LEVEL", "INFO").upper()
logger.remove()  # Ta bort default handler
logger.add(
    sys.stderr,
    format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>",
    level=log_level,
    colorize=True,
)
logger.add(
    "logs/etl_{time:YYYY-MM-DD}.log",
    rotation="00:00",
    retention="30 days",
    level=log_level,
    format="{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {name}:{function}:{line} - {message}",
)

# Skapa logs-katalog om den inte finns
Path("logs").mkdir(exist_ok=True)


def main():
    """
    Huvudfunktion för ETL-pipeline.
    
    Läser CSV-fil, transformerar data och laddar till MySQL-databas.
    Hanterar fel och loggar hela processen.
    """
    logger.info("=" * 60)
    logger.info("Startar ETL-pipeline")
    logger.info("=" * 60)
    
    try:
        # Hämta konfiguration från miljövariabler
        data_directory = os.getenv("DATA_DIRECTORY", "data")
        raw_file = os.getenv("RAW_FILE", "raw/data.csv")
        
        # Bygg sökvägar
        full_path = os.path.join(data_directory, raw_file)
        processed_base_path = os.path.join(data_directory, "processed", "processed_data")
        
        logger.info(f"Data directory: {data_directory}")
        logger.info(f"Raw file: {full_path}")
        logger.info(f"Processed files will be saved to: {processed_base_path}")
        
        # Steg 1: Extrahera CSV
        logger.info("-" * 60)
        logger.info("STEG 1: Extraktion")
        logger.info("-" * 60)
        df = extract_csv(full_path)
        
        # Steg 2: Transformera data
        logger.info("-" * 60)
        logger.info("STEG 2: Transformation")
        logger.info("-" * 60)
        users_df, perf_df, retention_df, nps_df = transform_csv(df, processed_base_path)
        
        logger.info(f"Transformering klar:")
        logger.info(f"  - Users: {len(users_df)} rader")
        logger.info(f"  - Performance: {len(perf_df)} rader")
        logger.info(f"  - Retention: {len(retention_df)} rader")
        logger.info(f"  - NPS: {len(nps_df)} rader")
        
        # Steg 3: Ladda till databas
        logger.info("-" * 60)
        logger.info("STEG 3: Laddning till databas")
        logger.info("-" * 60)
        conn = get_mysql_connection()
        
        try:
            results = load_to_mysql(users_df, perf_df, retention_df, nps_df, conn)
            
            logger.info("-" * 60)
            logger.info("ETL-pipeline klar!")
            logger.info("-" * 60)
            logger.info("Sammanfattning:")
            for table, stats in results.items():
                logger.info(f"  {table}: {stats}")
            logger.info("=" * 60)
            
        except Exception as e:
            logger.error(f"Fel vid laddning till databas: {e}")
            raise
        finally:
            conn.close()
            logger.debug("Databasanslutning stängd")
    
    except FileNotFoundError as e:
        logger.error(f"Filen hittades inte: {e}")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Kritiskt fel i ETL-pipeline: {e}")
        logger.exception("Fullständig stack trace:")
        sys.exit(1)


if __name__ == "__main__":
    main()
