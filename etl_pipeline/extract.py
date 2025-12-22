"""Modul för att extrahera data från CSV-filer."""

import sys
from pathlib import Path

import pandas as pd
from loguru import logger


def extract_csv(path):
    """
    Läser in CSV-fil och returnerar DataFrame.

    Hanterar svenska tecken (åäö) genom att använda lämplig encoding.
    Försöker först med utf-8, sedan latin1 som fallback.
    Validerar att filen finns och innehåller data.

    Args:
        path (str): Sökväg till CSV-filen som ska läsas in

    Returns:
        pd.DataFrame: DataFrame med data från CSV-filen

    Raises:
        FileNotFoundError: Om filen inte hittas
        pd.errors.EmptyDataError: Om filen är tom
        ValueError: Om filen är tom eller saknar data efter läsning
        UnicodeDecodeError: Om encoding-problem uppstår
        SystemExit: Om kritiska valideringar misslyckas
    """
    logger.info(f"Startar extraktion av CSV-fil: {path}")

    # Validera att filen finns innan vi försöker läsa den
    file_path = Path(path)
    if not file_path.exists():
        logger.error(f"Filen hittades inte: {path}")
        logger.error("Kritiskt fel: Kan inte fortsätta utan datafil")
        sys.exit(1)

    if not file_path.is_file():
        logger.error(f"Sökvägen är inte en fil: {path}")
        logger.error("Kritiskt fel: Ogiltig fil-sökväg")
        sys.exit(1)

    try:
        # Försök först med utf-8 och auto-detektera separator
        # Pandas kan automatiskt detektera komma eller semikolon
        try:
            df = pd.read_csv(path, encoding="utf-8", sep=None, engine="python")
            logger.debug("CSV-fil lästes in med utf-8 encoding och auto-detekterad separator")
        except UnicodeDecodeError:
            # Fallback till latin1 om utf-8 misslyckas
            logger.warning("utf-8 encoding misslyckades, försöker med latin1")
            try:
                df = pd.read_csv(path, encoding="latin1", sep=None, engine="python")
                logger.debug("CSV-fil lästes in med latin1 encoding och auto-detekterad separator")
            except UnicodeDecodeError as e2:
                logger.error("Kunde inte läsa filen med utf-8 eller latin1 encoding")
                logger.error(f"latin1 fel: {e2}")
                logger.error("Kritiskt fel: Encoding-problem - kan inte fortsätta")
                sys.exit(1)
        except Exception:
            # Om auto-detektering misslyckas, försök manuellt med semikolon och komma
            logger.debug("Auto-detektering misslyckades, försöker med semikolon och komma")
            try:
                # Försök först med semikolon (vanligt i svenska CSV-filer)
                df = pd.read_csv(path, encoding="utf-8", sep=";")
                logger.debug("CSV-fil lästes in med utf-8 encoding och semikolon-separator")
            except Exception:
                try:
                    # Fallback till komma
                    df = pd.read_csv(path, encoding="utf-8", sep=",")
                    logger.debug("CSV-fil lästes in med utf-8 encoding och komma-separator")
                except UnicodeDecodeError:
                    # Försök med latin1 och semikolon
                    try:
                        df = pd.read_csv(path, encoding="latin1", sep=";")
                        logger.debug("CSV-fil lästes in med latin1 encoding och semikolon-separator")
                    except Exception as e2:
                        logger.error("Kunde inte läsa CSV-filen med någon av metoderna")
                        logger.error(f"Fel: {e2}")
                        logger.error("Kritiskt fel: Kan inte fortsätta")
                        sys.exit(1)

        # Validera att DataFrame inte är tom efter läsning
        if df.empty:
            logger.error(f"CSV-filen är tom eller innehåller ingen data: {path}")
            logger.error("Kritiskt fel: Kan inte fortsätta med tom data")
            sys.exit(1)

        # Validera att vi har minst några kolumner
        if len(df.columns) == 0:
            logger.error(f"CSV-filen saknar kolumner: {path}")
            logger.error("Kritiskt fel: Ogiltig CSV-struktur")
            sys.exit(1)

        logger.info(f"Extraktion klar: {len(df)} rader lästes in")
        logger.debug(f"Kolumner i DataFrame: {list(df.columns)}")

        return df

    except FileNotFoundError:
        logger.error(f"Filen hittades inte: {path}")
        logger.error("Kritiskt fel: Kan inte fortsätta utan datafil")
        sys.exit(1)
    except pd.errors.EmptyDataError:
        logger.error(f"CSV-filen är tom: {path}")
        logger.error("Kritiskt fel: Kan inte fortsätta med tom data")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Oväntat fel vid extraktion: {e}")
        logger.exception("Fullständig stack trace:")
        logger.error("Kritiskt fel: Kan inte fortsätta")
        sys.exit(1)
