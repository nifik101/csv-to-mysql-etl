"""Pandera-scheman för datavalidering.

DETTA ÄR EN TEMPLATE - ANPASSA FÖR DIN DATA
===========================================

Hur du använder detta:
1. Skapa ditt eget schema baserat på dina kolumner och valideringsregler
2. Använd validate_transform() och validate_load() med ditt schema
3. Eller skapa helt egna valideringsfunktioner

Schema-struktur:
- Column(datatype, checks=[...], nullable=True/False)
- DataFrameSchema({...}, strict=False)  # strict=False tillåter extra kolumner

Exempel på checks:
- Check.greater_than(0)
- Check.between(0, 100)
- Check.isin(['value1', 'value2'])
"""
import pandera as pa
from pandera import Column, DataFrameSchema, Check
from typing import Optional
import pandas as pd


def create_schema_from_dataframe(
    df: pd.DataFrame,
    nullable_columns: Optional[list[str]] = None,
    required_columns: Optional[list[str]] = None,
) -> DataFrameSchema:
    """Skapar ett grundläggande schema från DataFrame-struktur.
    
    Detta är en hjälpfunktion för att skapa scheman dynamiskt.
    Anpassa schemat efter dina behov.
    
    Args:
        df: DataFrame att skapa schema från
        nullable_columns: Kolumner som får vara None
        required_columns: Kolumner som måste finnas (valideras)
        
    Returns:
        Grundläggande DataFrameSchema
    """
    schema_dict = {}
    nullable_cols = nullable_columns or []
    
    for col in df.columns:
        dtype = df[col].dtype
        nullable = col in nullable_cols
        
        # Mappa pandas-dtyper till pandera-typer
        if pd.api.types.is_integer_dtype(dtype):
            schema_dict[col] = Column(int, nullable=nullable)
        elif pd.api.types.is_float_dtype(dtype):
            schema_dict[col] = Column(float, nullable=nullable)
        elif pd.api.types.is_datetime64_any_dtype(dtype):
            schema_dict[col] = Column(pa.DateTime, nullable=nullable)
        else:
            schema_dict[col] = Column(str, nullable=nullable)
    
    return DataFrameSchema(schema_dict, strict=False)


# EXEMPEL-SCHEMAN (ersätt med dina egna)
# ========================================

# Detta är ett exempel - skapa ditt eget schema för din data
transform_schema_example = DataFrameSchema(
    {
        # Lägg till dina kolumner här med valideringsregler
        # Exempel (ta bort och ersätt med dina):
        # "my_column": Column(float, checks=Check.greater_than(0), nullable=True),
    },
    strict=False,  # Tillåt extra kolumner
)

load_schema_example = DataFrameSchema(
    {
        # Lägg till valideringar som behövs innan databasinsert
        # Exempel (ta bort och ersätt med dina):
        # "required_column": Column(str, nullable=False),
    },
    strict=False,
)


def validate_transform(
    df: pd.DataFrame,
    schema: Optional[DataFrameSchema] = None,
) -> pd.DataFrame:
    """Validerar DataFrame efter transform-steg.
    
    Args:
        df: DataFrame att validera
        schema: Pandera-schema. Använder transform_schema_example om None.
                Skapa ditt eget schema för bättre validering!
        
    Returns:
        Validerad DataFrame
        
    Raises:
        pa.errors.SchemaError: Om validering misslyckas
    """
    if schema is None:
        schema = transform_schema_example
    
    try:
        return schema.validate(df, lazy=True)
    except pa.errors.SchemaError as e:
        logger.error(f"Validering misslyckades: {e}")
        raise


def validate_load(
    df: pd.DataFrame,
    schema: Optional[DataFrameSchema] = None,
) -> pd.DataFrame:
    """Validerar DataFrame innan load-steg.
    
    Args:
        df: DataFrame att validera
        schema: Pandera-schema. Använder load_schema_example om None.
                Skapa ditt eget schema för bättre validering!
        
    Returns:
        Validerad DataFrame
        
    Raises:
        pa.errors.SchemaError: Om validering misslyckas
    """
    if schema is None:
        schema = load_schema_example
    
    try:
        return schema.validate(df, lazy=True)
    except pa.errors.SchemaError as e:
        logger.error(f"Validering misslyckades: {e}")
        raise