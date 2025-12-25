"""Transform-modul för datatransformationer.

Detta är en generisk wrapper som tillhandahåller grundläggande transformationer.
Använd dessa funktioner som byggstenar och skapa egna transformationer efter behov.
"""
from pathlib import Path
from typing import Optional, Callable
import pandas as pd
import pandera as pa
from utils.logger import logger
from dev.config import DATE_FORMAT
from validation.schemas import validate_transform


def standardize_column_names(df: pd.DataFrame) -> pd.DataFrame:
    """Standardiserar kolumnnamn till snake_case.
    
    Args:
        df: DataFrame att transformera
        
    Returns:
        DataFrame med standardiserade kolumnnamn
    """
    logger.debug("Standardiserar kolumnnamn till snake_case")
    df = df.copy()
    df.columns = df.columns.str.lower().str.replace(" ", "_").str.replace("-", "_")
    return df


def convert_to_datetime(
    df: pd.DataFrame,
    columns: list[str],
    date_format: Optional[str] = None,
) -> pd.DataFrame:
    """Konverterar specifierade kolumner till datetime.
    
    Args:
        df: DataFrame att transformera
        columns: Lista med kolumnnamn att konvertera
        date_format: Datumformat. Använder DATE_FORMAT från config om None.
        
    Returns:
        DataFrame med konverterade datum-kolumner
    """
    if date_format is None:
        from dev.config import DATE_FORMAT
        date_format = DATE_FORMAT
    
    df = df.copy()
    for col in columns:
        if col in df.columns:
            logger.debug(f"Konverterar {col} till datetime med format {date_format}")
            df[col] = pd.to_datetime(df[col], format=date_format, errors="coerce")
    return df


def transform_csv(
    df: pd.DataFrame,
    save_path: Optional[str | Path] = None,
    custom_transformations: Optional[list[Callable]] = None,
    date_columns: Optional[list[str]] = None,
    date_format: Optional[str] = None,
    validate: bool = True,
    schema: Optional[pa.DataFrameSchema] = None,
) -> pd.DataFrame:
    """Generisk transform-funktion som tillämpar standardtransformationer.
    
    Detta är en wrapper som tillhandahåller grundläggande transformationer.
    Använd custom_transformations för att lägga till egna transformationer.
    
    Args:
        df: DataFrame att transformera
        save_path: Valfri sökväg för att spara processad data
        custom_transformations: Lista med funktioner som tar df och returnerar df
        date_columns: Kolumner att konvertera till datetime
        date_format: Datumformat för konvertering
        validate: Om validering ska köras (default: True)
        schema: Anpassat pandera-schema. Använder default om None.
        
    Returns:
        Transformerad DataFrame
        
    Example:
        # Grundläggande användning
        df = transform_csv(df)
        
        # Med egna transformationer
        def multiply_columns(df):
            df['total'] = df['price'] * df['quantity']
            return df
            
        df = transform_csv(df, custom_transformations=[multiply_columns])
    """
    try:
        logger.info("Startar datatransformation")
        df = df.copy()
        
        # Standardisera kolumnnamn (alltid)
        df = standardize_column_names(df)
        
        # Konvertera datum om specificerat
        if date_columns:
            df = convert_to_datetime(df, date_columns, date_format)
        
        # Applicera anpassade transformationer
        if custom_transformations:
            for transform_func in custom_transformations:
                logger.debug(f"Applicerar anpassad transformation: {transform_func.__name__}")
                df = transform_func(df)
        
        # Validera om önskat
        if validate:
            logger.info("Validerar transformerad data")
            df = validate_transform(df, schema)
        
        # Spara om sökväg angiven
        if save_path:
            save_path_obj = Path(save_path)
            save_path_obj.parent.mkdir(parents=True, exist_ok=True)
            logger.info(f"Sparar processad data till {save_path_obj}")
            df.to_csv(save_path_obj, index=False)
        
        logger.info(f"Transformation klar: {len(df)} rader processade")
        return df
        
    except pa.errors.SchemaError as e:
        logger.error(f"Validering misslyckades: {e}")
        raise
    except Exception as e:
        logger.error(f"Fel vid transformation: {e}")
        raise ValueError(f"Transformation misslyckades: {e}") from e