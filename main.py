"""Huvudmodul för ETL-pipeline.

Detta är en template som kan anpassas för olika datatyper.
Anpassa config och/eller skicka parametrar för att köra olika pipelines.

Exempel:
    # Grundläggande användning (använder config)
    main()
    
    # Med anpassade sökvägar
    main(raw_file="my_data.csv", processed_file="my_processed.csv")
    
    # Med anpassad MySQL-konfiguration
    main(mysql_config={"host": "other_host", ...})
"""
from pathlib import Path
from typing import Optional
from dev.config import RAW_DIR, PROCESSED_DIR, raw_file, processed_file
from etl_pipeline.extract import extract_csv
from etl_pipeline.transform import transform_csv
from etl_pipeline.load import load_to_mysql
from db.mysql_connection import get_mysql_connection
from utils.logger import logger


def run_extract_step(
    raw_file_path: str | Path,
    **extract_kwargs,
) -> "pd.DataFrame":
    """Kör extract-steg av ETL-pipeline.
    
    Args:
        raw_file_path: Sökväg till raw CSV-fil
        **extract_kwargs: Ytterligare parametrar för extract_csv()
        
    Returns:
        DataFrame med extraherad data
    """
    logger.info("-" * 50)
    logger.info("STEG 1: EXTRACT")
    logger.info("-" * 50)
    logger.info(f"Läser från: {raw_file_path}")
    
    df = extract_csv(raw_file_path, **extract_kwargs)
    logger.info(f"Extraherade {len(df)} rader")
    
    return df


def run_transform_step(
    df: "pd.DataFrame",
    processed_file_path: Optional[str | Path] = None,
    **transform_kwargs,
) -> "pd.DataFrame":
    """Kör transform-steg av ETL-pipeline.
    
    Args:
        df: DataFrame att transformera
        processed_file_path: Valfri sökväg för att spara processad data
        **transform_kwargs: Ytterligare parametrar för transform_csv()
        
    Returns:
        Transformerad DataFrame
    """
    logger.info("-" * 50)
    logger.info("STEG 2: TRANSFORM")
    logger.info("-" * 50)
    
    if processed_file_path:
        logger.info(f"Sparar till: {processed_file_path}")
    
    transformed_df = transform_csv(
        df,
        save_path=processed_file_path,
        **transform_kwargs,
    )
    logger.info(f"Transformerade {len(transformed_df)} rader")
    
    return transformed_df


def run_load_step(
    df: "pd.DataFrame",
    mysql_config: Optional[dict] = None,
    **load_kwargs,
) -> int:
    """Kör load-steg av ETL-pipeline.
    
    Args:
        df: DataFrame att ladda till databas
        mysql_config: Valfri MySQL-konfiguration
        **load_kwargs: Ytterligare parametrar för load_to_mysql()
        
    Returns:
        Antal rader som laddades
    """
    logger.info("-" * 50)
    logger.info("STEG 3: LOAD")
    logger.info("-" * 50)
    
    # Använd contextmanager för automatisk hantering
    with get_mysql_connection(config=mysql_config) as connection:
        rows_loaded = load_to_mysql(df, connection, **load_kwargs)
        logger.info(f"Laddade {rows_loaded} rader till databas")
    
    return rows_loaded


def main(
    raw_file_path: Optional[str | Path] = None,
    processed_file_path: Optional[str | Path] = None,
    mysql_config: Optional[dict] = None,
    extract_kwargs: Optional[dict] = None,
    transform_kwargs: Optional[dict] = None,
    load_kwargs: Optional[dict] = None,
) -> None:
    """Kör komplett ETL-pipeline: Extract, Transform, Load.
    
    Detta är en template-funktion som kan anpassas för olika datatyper.
    Alla parametrar är valfria och använder config-värden som default.
    
    Args:
        raw_file_path: Sökväg till raw CSV-fil. Använder config om None.
        processed_file_path: Sökväg för processad data. Använder config om None.
        mysql_config: MySQL-konfiguration. Använder config om None.
        extract_kwargs: Ytterligare parametrar för extract_csv()
        transform_kwargs: Ytterligare parametrar för transform_csv()
        load_kwargs: Ytterligare parametrar för load_to_mysql()
        
    Example:
        # Grundläggande användning (använder config)
        main()
        
        # Med anpassade sökvägar
        main(
            raw_file_path="data/my_file.csv",
            processed_file_path="data/processed/my_file.csv"
        )
        
        # Med anpassade extract-parametrar
        main(
            extract_kwargs={"encoding": "utf-8", "sep": ";"}
        )
    """
    try:
        logger.info("=" * 50)
        logger.info("Startar ETL-pipeline")
        logger.info("=" * 50)
        
        # Använd config-värden om inte specificerat
        if raw_file_path is None:
            raw_file_path = RAW_DIR / raw_file
        if processed_file_path is None:
            processed_file_path = PROCESSED_DIR / processed_file
        
        logger.info(f"Raw-fil: {raw_file_path}")
        logger.info(f"Processed-fil: {processed_file_path}")
        
        # Extract
        df = run_extract_step(
            raw_file_path,
            **(extract_kwargs or {})
        )
        
        # Transform
        transformed_df = run_transform_step(
            df,
            processed_file_path,
            **(transform_kwargs or {})
        )
        
        # Load
        rows_loaded = run_load_step(
            transformed_df,
            mysql_config,
            **(load_kwargs or {})
        )
        
        logger.info("=" * 50)
        logger.info(f"ETL-pipeline slutförd framgångsrikt! ({rows_loaded} rader laddade)")
        logger.info("=" * 50)
        
    except FileNotFoundError as e:
        logger.error(f"Filen hittades inte: {e}")
        raise
    except ValueError as e:
        logger.error(f"Valideringsfel: {e}")
        raise
    except Exception as e:
        logger.error(f"Oväntat fel i ETL-pipeline: {e}")
        raise


if __name__ == "__main__":
    main()