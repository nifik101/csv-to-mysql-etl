"""Modul för att extrahera data från CSV-filer."""
import pandas as pd
from loguru import logger


def extract_csv(path):
    """
    Läser in CSV-fil och returnerar DataFrame.
    
    Hanterar svenska tecken (åäö) genom att använda lämplig encoding.
    Försöker först med utf-8, sedan latin1 som fallback.
    
    Args:
        path (str): Sökväg till CSV-filen som ska läsas in
        
    Returns:
        pd.DataFrame: DataFrame med data från CSV-filen
        
    Raises:
        FileNotFoundError: Om filen inte hittas
        pd.errors.EmptyDataError: Om filen är tom
        UnicodeDecodeError: Om encoding-problem uppstår
    """
    logger.info(f"Startar extraktion av CSV-fil: {path}")
    
    try:
        # Försök först med utf-8 för svenska tecken
        try:
            df = pd.read_csv(path, encoding="utf-8")
            logger.debug(f"CSV-fil lästes in med utf-8 encoding")
        except UnicodeDecodeError:
            # Fallback till latin1 om utf-8 misslyckas
            logger.warning(f"utf-8 encoding misslyckades, försöker med latin1")
            df = pd.read_csv(path, encoding="latin1")
            logger.debug(f"CSV-fil lästes in med latin1 encoding")
        
        logger.info(f"Extraktion klar: {len(df)} rader lästes in")
        logger.debug(f"Kolumner i DataFrame: {list(df.columns)}")
        
        return df
        
    except FileNotFoundError:
        logger.error(f"Filen hittades inte: {path}")
        raise
    except pd.errors.EmptyDataError:
        logger.error(f"CSV-filen är tom: {path}")
        raise
    except Exception as e:
        logger.error(f"Oväntat fel vid extraktion: {e}")
        raise