"""Extract-modul för att läsa CSV-filer.

Detta är en generisk wrapper runt pandas.read_csv() som tillhandahåller
grundläggande funktionalitet med logging och felhantering.

Använd denna modul som byggsten och anpassa efter behov för olika CSV-format.
"""
from pathlib import Path
from typing import Optional, Any
import pandas as pd
from utils.logger import logger
from dev.config import CSV_ENCODING


def validate_file_exists(file_path: str | Path) -> Path:
    """Validerar att filen finns.
    
    Args:
        file_path: Sökväg till filen
        
    Returns:
        Path-objekt om filen finns
        
    Raises:
        FileNotFoundError: Om filen inte hittas
    """
    path = Path(file_path)
    if not path.exists():
        logger.error(f"Filen hittades inte: {path}")
        raise FileNotFoundError(f"Filen hittades inte: {path}")
    return path


def extract_csv(
    path: str | Path,
    encoding: Optional[str] = None,
    **pandas_kwargs: Any,
) -> pd.DataFrame:
    """Extraherar data från CSV-fil.
    
    Detta är en wrapper runt pandas.read_csv() som tillhandahåller
    grundläggande felhantering och logging. Alla extra parametrar
    skickas vidare till pandas.read_csv().
    
    Args:
        path: Sökväg till CSV-filen
        encoding: Teckenkodning för filen. Använder CSV_ENCODING från config om None.
        **pandas_kwargs: Ytterligare parametrar som skickas till pandas.read_csv()
                        (t.ex. sep, delimiter, skiprows, na_values, dtype, etc.)
    
    Returns:
        DataFrame med inläst data
        
    Raises:
        FileNotFoundError: Om filen inte hittas
        ValueError: Om filen inte kan läsas
        
    Example:
        # Grundläggande användning
        df = extract_csv("data.csv")
        
        # Med anpassade parametrar
        df = extract_csv(
            "data.csv",
            encoding="utf-8",
            sep=";",
            skiprows=1,
            na_values=["N/A", "NULL"]
        )
    """
    # Validera fil (separat ansvar)
    file_path = validate_file_exists(path)
    
    # Använd default encoding om inte specificerat
    if encoding is None:
        encoding = CSV_ENCODING
    
    # Förbered parametrar för pandas.read_csv()
    read_params = {
        "filepath_or_buffer": file_path,
        "encoding": encoding,
        **pandas_kwargs,  # Tillåt anpassade parametrar
    }
    
    try:
        logger.info(f"Läser CSV-fil: {file_path}")
        logger.debug(f"Parametrar: encoding={encoding}, extra={pandas_kwargs}")
        
        df = pd.read_csv(**read_params)
        
        logger.info(f"Läste {len(df)} rader och {len(df.columns)} kolumner från {file_path}")
        logger.debug(f"Kolumner: {list(df.columns)}")
        
        return df
        
    except pd.errors.EmptyDataError as e:
        logger.error(f"CSV-filen är tom: {file_path}")
        raise ValueError(f"CSV-filen är tom: {e}") from e
    except pd.errors.ParserError as e:
        logger.error(f"Parsing-fel i CSV-fil {file_path}: {e}")
        raise ValueError(f"Kunde inte parsa CSV-fil: {e}") from e
    except UnicodeDecodeError as e:
        logger.error(f"Encoding-fel i CSV-fil {file_path}: {e}")
        raise ValueError(
            f"Encoding-fel. Prova annan encoding (t.ex. 'utf-8', 'latin1'): {e}"
        ) from e
    except Exception as e:
        logger.error(f"Fel vid läsning av CSV-fil {file_path}: {e}")
        raise ValueError(f"Kunde inte läsa CSV-fil: {e}") from e