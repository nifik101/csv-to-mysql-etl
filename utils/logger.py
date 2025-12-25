"""Centraliserad logger-konfiguration med loguru.

Detta är en generisk logger-setup som kan anpassas för olika projekt.
Konfigurerar loguru för att logga till både konsol och fil.

Användning:
    from utils.logger import logger
    
    logger.info("Meddelande")
    logger.error("Fel")
    
För att anpassa konfiguration, ändra värdena i setup_logger() eller
använd configure_logger() för att skapa anpassad konfiguration.
"""
import sys
from pathlib import Path
from typing import Optional
from loguru import logger as loguru_logger

# Base directory
BASE_DIR = Path(__file__).parent.parent
LOGS_DIR = BASE_DIR / "logs"

# Default-konfiguration (kan anpassas)
DEFAULT_LOG_PREFIX = "etl"  # Prefix för log-filer
DEFAULT_CONSOLE_LEVEL = "INFO"  # Konsol log-nivå
DEFAULT_FILE_LEVEL = "DEBUG"  # Fil log-nivå
DEFAULT_RETENTION = "30 days"  # Behåll loggar i 30 dagar
DEFAULT_ROTATION = "00:00"  # Ny fil varje dag vid midnatt


def setup_console_logging(
    level: str = DEFAULT_CONSOLE_LEVEL,
    format_string: Optional[str] = None,
) -> None:
    """Konfigurerar konsol-logging.
    
    Args:
        level: Log-nivå för konsol (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        format_string: Anpassat format. Använder default om None.
    """
    if format_string is None:
        format_string = (
            "<green>{time:YYYY-MM-DD HH:mm:ss}</green> | "
            "<level>{level: <8}</level> | "
            "<cyan>{name}</cyan>:<cyan>{function}</cyan> - "
            "<level>{message}</level>"
        )
    
    loguru_logger.add(
        sys.stderr,
        format=format_string,
        level=level,
        colorize=True,
    )


def setup_file_logging(
    log_prefix: str = DEFAULT_LOG_PREFIX,
    level: str = DEFAULT_FILE_LEVEL,
    format_string: Optional[str] = None,
    retention: str = DEFAULT_RETENTION,
    rotation: str = DEFAULT_ROTATION,
    logs_dir: Optional[Path] = None,
) -> None:
    """Konfigurerar fil-logging.
    
    Args:
        log_prefix: Prefix för log-filnamn (t.ex. "etl", "pipeline", "my_app")
        level: Log-nivå för fil (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        format_string: Anpassat format. Använder default om None.
        retention: Hur länge loggar ska behållas (t.ex. "30 days", "1 week")
        rotation: När ny log-fil ska skapas (t.ex. "00:00", "500 MB", "1 week")
        logs_dir: Mapp för loggar. Använder default om None.
    """
    if logs_dir is None:
        logs_dir = LOGS_DIR
    
    # Skapa logs-mapp om den inte finns
    logs_dir.mkdir(parents=True, exist_ok=True)
    
    if format_string is None:
        format_string = (
            "{time:YYYY-MM-DD HH:mm:ss} | "
            "{level: <8} | "
            "{name}:{function} - "
            "{message}"
        )
    
    log_file = logs_dir / f"{log_prefix}_{{time:YYYY-MM-DD}}.log"
    
    loguru_logger.add(
        log_file,
        format=format_string,
        level=level,
        rotation=rotation,
        retention=retention,
        compression="zip",  # Komprimera gamla loggar
    )


def setup_logger(
    log_prefix: str = DEFAULT_LOG_PREFIX,
    console_level: str = DEFAULT_CONSOLE_LEVEL,
    file_level: str = DEFAULT_FILE_LEVEL,
    retention: str = DEFAULT_RETENTION,
    rotation: str = DEFAULT_ROTATION,
    logs_dir: Optional[Path] = None,
) -> None:
    """Konfigurerar logger med standardinställningar.
    
    Detta är huvudfunktionen som anropas automatiskt när modulen importeras.
    Anpassa default-värdena i denna funktion eller använd configure_logger()
    för mer kontroll.
    
    Args:
        log_prefix: Prefix för log-filnamn
        console_level: Log-nivå för konsol
        file_level: Log-nivå för fil
        retention: Hur länge loggar ska behållas
        rotation: När ny log-fil ska skapas
        logs_dir: Mapp för loggar
    """
    # Ta bort standard logger
    loguru_logger.remove()
    
    # Konfigurera konsol och fil logging
    setup_console_logging(level=console_level)
    setup_file_logging(
        log_prefix=log_prefix,
        level=file_level,
        retention=retention,
        rotation=rotation,
        logs_dir=logs_dir,
    )


def configure_logger(
    log_prefix: str = DEFAULT_LOG_PREFIX,
    console_level: str = DEFAULT_CONSOLE_LEVEL,
    file_level: str = DEFAULT_FILE_LEVEL,
    retention: str = DEFAULT_RETENTION,
    rotation: str = DEFAULT_ROTATION,
    logs_dir: Optional[Path] = None,
    console_format: Optional[str] = None,
    file_format: Optional[str] = None,
) -> None:
    """Konfigurerar logger med full kontroll över alla inställningar.
    
    Använd denna funktion om du behöver anpassa format eller andra inställningar.
    
    Args:
        log_prefix: Prefix för log-filnamn
        console_level: Log-nivå för konsol
        file_level: Log-nivå för fil
        retention: Hur länge loggar ska behållas
        rotation: När ny log-fil ska skapas
        logs_dir: Mapp för loggar
        console_format: Anpassat format för konsol
        file_format: Anpassat format för fil
    """
    loguru_logger.remove()
    
    setup_console_logging(level=console_level, format_string=console_format)
    setup_file_logging(
        log_prefix=log_prefix,
        level=file_level,
        format_string=file_format,
        retention=retention,
        rotation=rotation,
        logs_dir=logs_dir,
    )


# Konfigurera logger automatiskt när modulen importeras
# Ändra default-värdena här för att anpassa för ditt projekt
setup_logger(
    log_prefix=DEFAULT_LOG_PREFIX,  # Ändra till t.ex. "my_app", "pipeline", etc.
    console_level=DEFAULT_CONSOLE_LEVEL,
    file_level=DEFAULT_FILE_LEVEL,
    retention=DEFAULT_RETENTION,
    rotation=DEFAULT_ROTATION,
)

# Exportera logger för användning i andra moduler
logger = loguru_logger
__all__ = ["logger", "setup_logger", "configure_logger"]