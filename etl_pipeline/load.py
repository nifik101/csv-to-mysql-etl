"""Modul för att ladda data till MySQL-databas."""

import sys

import pandas as pd
from loguru import logger


def load_users(df, connection):
    """
    Laddar användardata till users-tabellen med UPSERT-logik.

    Använder INSERT ... ON DUPLICATE KEY UPDATE för att uppdatera
    befintliga användare eller skapa nya.

    Args:
        df (pd.DataFrame): DataFrame med kolumner user_id och namn
        connection: MySQL-anslutningsobjekt

    Returns:
        tuple: (rows_inserted, rows_updated) - antal rader som påverkades
    """
    if df.empty:
        logger.warning("Users DataFrame är tom, inget att ladda")
        return 0, 0

    logger.info(f"Laddar {len(df)} användare till users-tabellen")

    # Konvertera NaN till None för SQL
    df = df.where(pd.notnull(df), None)

    cursor = connection.cursor()

    insert_query = """
        INSERT INTO users (user_id, namn)
        VALUES (%s, %s)
        ON DUPLICATE KEY UPDATE
            namn = VALUES(namn),
            updated_at = CURRENT_TIMESTAMP
    """

    data = [(row["user_id"], row["namn"]) for _, row in df.iterrows()]

    try:
        cursor.executemany(insert_query, data)
        connection.commit()

        # För UPSERT kan vi inte enkelt få antal inserts vs updates
        # Så vi returnerar totalt antal rader som påverkades
        logger.info(f"Users: {len(data)} rader bearbetade")
        logger.debug(f"Exempel på användare som laddades: {df.head(3).to_dict('records')}")

        return len(data), 0

    except Exception as e:
        connection.rollback()
        logger.error(f"Fel vid laddning av users: {e}")
        raise
    finally:
        cursor.close()


def load_daily_performance(df, connection):
    """
    Laddar daglig prestationsdata till daily_performance-tabellen med UPSERT-logik.

    Använder INSERT ... ON DUPLICATE KEY UPDATE för att uppdatera
    befintliga dagliga poster eller skapa nya.

    Args:
        df (pd.DataFrame): DataFrame formaterad för daily_performance-tabellen
        connection: MySQL-anslutningsobjekt

    Returns:
        int: Antal rader som påverkades
    """
    if df.empty:
        logger.warning("Daily performance DataFrame är tom, inget att ladda")
        return 0

    logger.info(f"Laddar {len(df)} rader till daily_performance-tabellen")

    df = df.where(pd.notnull(df), None)

    cursor = connection.cursor()

    insert_query = """
        INSERT INTO daily_performance (
            user_id, datum, samtal, acd_seconds, acw_seconds, hold_seconds,
            koppling_pct, bb_antal, pp_antal, tv_antal, mbb_antal, other_antal,
            erbjud_pct, save_provis_kr, provis_kr, fmc_prov_kr, value_change_kr
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            samtal = VALUES(samtal),
            acd_seconds = VALUES(acd_seconds),
            acw_seconds = VALUES(acw_seconds),
            hold_seconds = VALUES(hold_seconds),
            koppling_pct = VALUES(koppling_pct),
            bb_antal = VALUES(bb_antal),
            pp_antal = VALUES(pp_antal),
            tv_antal = VALUES(tv_antal),
            mbb_antal = VALUES(mbb_antal),
            other_antal = VALUES(other_antal),
            erbjud_pct = VALUES(erbjud_pct),
            save_provis_kr = VALUES(save_provis_kr),
            provis_kr = VALUES(provis_kr),
            fmc_prov_kr = VALUES(fmc_prov_kr),
            value_change_kr = VALUES(value_change_kr)
    """

    def convert_nan_to_none(value):
        """Konverterar NaN/NaT till None för SQL."""
        if pd.isna(value):
            return None
        return value

    data = [
        (
            row["user_id"],
            row["datum"],
            convert_nan_to_none(row.get("samtal")),
            convert_nan_to_none(row.get("acd_seconds")),
            convert_nan_to_none(row.get("acw_seconds")),
            convert_nan_to_none(row.get("hold_seconds")),
            convert_nan_to_none(row.get("koppling_pct")),
            convert_nan_to_none(row.get("bb_antal")),
            convert_nan_to_none(row.get("pp_antal")),
            convert_nan_to_none(row.get("tv_antal")),
            convert_nan_to_none(row.get("mbb_antal")),
            convert_nan_to_none(row.get("other_antal")),
            convert_nan_to_none(row.get("erbjud_pct")),
            convert_nan_to_none(row.get("save_provis_kr")),
            convert_nan_to_none(row.get("provis_kr")),
            convert_nan_to_none(row.get("fmc_prov_kr")),
            convert_nan_to_none(row.get("value_change_kr")),
        )
        for _, row in df.iterrows()
    ]

    try:
        cursor.executemany(insert_query, data)
        connection.commit()

        logger.info(f"Daily performance: {len(data)} rader bearbetade")
        logger.debug(f"Exempel på performance-data: {df.head(2).to_dict('records')}")

        return len(data)

    except Exception as e:
        connection.rollback()
        logger.error(f"Fel vid laddning av daily_performance: {e}")
        raise
    finally:
        cursor.close()


def load_daily_retention(df, connection):
    """
    Laddar daglig retention-data till daily_retention-tabellen med UPSERT-logik.

    Använder INSERT ... ON DUPLICATE KEY UPDATE för att uppdatera
    befintliga dagliga poster eller skapa nya.

    Args:
        df (pd.DataFrame): DataFrame formaterad för daily_retention-tabellen
        connection: MySQL-anslutningsobjekt

    Returns:
        int: Antal rader som påverkades
    """
    if df.empty:
        logger.warning("Daily retention DataFrame är tom, inget att ladda")
        return 0

    logger.info(f"Laddar {len(df)} rader till daily_retention-tabellen")

    df = df.where(pd.notnull(df), None)

    cursor = connection.cursor()

    insert_query = """
        INSERT INTO daily_retention (
            user_id, datum, vand_tv_pct, vand_bb_pct, vand_pp_pct,
            vand_total_pct, vand_antal
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            vand_tv_pct = VALUES(vand_tv_pct),
            vand_bb_pct = VALUES(vand_bb_pct),
            vand_pp_pct = VALUES(vand_pp_pct),
            vand_total_pct = VALUES(vand_total_pct),
            vand_antal = VALUES(vand_antal)
    """

    def convert_nan_to_none(value):
        """Konverterar NaN/NaT till None för SQL."""
        if pd.isna(value):
            return None
        return value

    data = [
        (
            row["user_id"],
            row["datum"],
            convert_nan_to_none(row.get("vand_tv_pct")),
            convert_nan_to_none(row.get("vand_bb_pct")),
            convert_nan_to_none(row.get("vand_pp_pct")),
            convert_nan_to_none(row.get("vand_total_pct")),
            convert_nan_to_none(row.get("vand_antal")),
        )
        for _, row in df.iterrows()
    ]

    try:
        cursor.executemany(insert_query, data)
        connection.commit()

        logger.info(f"Daily retention: {len(data)} rader bearbetade")

        return len(data)

    except Exception as e:
        connection.rollback()
        logger.error(f"Fel vid laddning av daily_retention: {e}")
        raise
    finally:
        cursor.close()


def load_daily_nps(df, connection):
    """
    Laddar daglig NPS-data till daily_nps-tabellen med UPSERT-logik.

    Använder INSERT ... ON DUPLICATE KEY UPDATE för att uppdatera
    befintliga dagliga poster eller skapa nya.

    Args:
        df (pd.DataFrame): DataFrame formaterad för daily_nps-tabellen
        connection: MySQL-anslutningsobjekt

    Returns:
        int: Antal rader som påverkades
    """
    if df.empty:
        logger.warning("Daily NPS DataFrame är tom, inget att ladda")
        return 0

    logger.info(f"Laddar {len(df)} rader till daily_nps-tabellen")

    df = df.where(pd.notnull(df), None)

    cursor = connection.cursor()

    insert_query = """
        INSERT INTO daily_nps (
            user_id, datum, nps_antal_svar, nps_score, csat_pct, cb_pct
        )
        VALUES (%s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            nps_antal_svar = VALUES(nps_antal_svar),
            nps_score = VALUES(nps_score),
            csat_pct = VALUES(csat_pct),
            cb_pct = VALUES(cb_pct)
    """

    def convert_nan_to_none(value):
        """Konverterar NaN/NaT till None för SQL."""
        if pd.isna(value):
            return None
        return value

    data = [
        (
            row["user_id"],
            row["datum"],
            convert_nan_to_none(row.get("nps_antal_svar")),
            convert_nan_to_none(row.get("nps_score")),
            convert_nan_to_none(row.get("csat_pct")),
            convert_nan_to_none(row.get("cb_pct")),
        )
        for _, row in df.iterrows()
    ]

    try:
        cursor.executemany(insert_query, data)
        connection.commit()

        logger.info(f"Daily NPS: {len(data)} rader bearbetade")
        logger.debug(
            f"Exempel på NPS-data (kan innehålla negativa värden): {df[['nps_score']].describe() if 'nps_score' in df.columns else 'N/A'}"
        )

        return len(data)

    except Exception as e:
        connection.rollback()
        logger.error(f"Fel vid laddning av daily_nps: {e}")
        raise
    finally:
        cursor.close()


def load_to_mysql(users_df, perf_df, retention_df, nps_df, connection):
    """
    Huvudfunktion för att ladda alla DataFrames till MySQL-databasen.

    Orkestrerar laddningen i rätt ordning med transaktionshantering:
    1. Users först (för referentiell integritet)
    2. Sedan daily-tabellerna

    Använder transaktioner för att säkerställa atomisk laddning:
    - Om något steg misslyckas, rollbackas allt
    - Förhindrar korrupt data i databasen

    Args:
        users_df (pd.DataFrame): Users DataFrame
        perf_df (pd.DataFrame): Daily performance DataFrame
        retention_df (pd.DataFrame): Daily retention DataFrame
        nps_df (pd.DataFrame): Daily NPS DataFrame
        connection: MySQL-anslutningsobjekt

    Returns:
        dict: Dictionary med antal rader som laddades per tabell

    Raises:
        SystemExit: Om databasladdning misslyckas kritiskt
    """
    logger.info("Startar laddning till MySQL-databas")

    # Validera att connection är aktiv
    if not connection.is_connected():
        logger.error("Databasanslutning är inte aktiv")
        logger.error("Kritiskt fel: Kan inte ladda data")
        sys.exit(1)

    results = {}

    # Starta transaktion - alla steg måste lyckas eller inget sparas
    try:
        # Sätt autocommit till False för transaktionshantering
        connection.autocommit = False

        # Ladda users först (kritisk för foreign keys)
        logger.info("Steg 1/4: Laddar users")
        try:
            users_inserted, users_updated = load_users(users_df, connection)
            results["users"] = {"inserted": users_inserted, "updated": users_updated}
            logger.info(f"Users laddade: {users_inserted} inserts, {users_updated} updates")
        except Exception as e:
            logger.error(f"Fel vid laddning av users: {e}")
            logger.error("Kritiskt fel: Kan inte fortsätta utan users")
            connection.rollback()
            sys.exit(1)

        # Ladda daily-tabellerna
        logger.info("Steg 2/4: Laddar daily_performance")
        try:
            perf_rows = load_daily_performance(perf_df, connection)
            results["daily_performance"] = {"rows": perf_rows}
            logger.info(f"Daily performance laddade: {perf_rows} rader")
        except Exception as e:
            logger.error(f"Fel vid laddning av daily_performance: {e}")
            logger.error("Rollbackar hela transaktionen")
            connection.rollback()
            sys.exit(1)

        logger.info("Steg 3/4: Laddar daily_retention")
        try:
            retention_rows = load_daily_retention(retention_df, connection)
            results["daily_retention"] = {"rows": retention_rows}
            logger.info(f"Daily retention laddade: {retention_rows} rader")
        except Exception as e:
            logger.error(f"Fel vid laddning av daily_retention: {e}")
            logger.error("Rollbackar hela transaktionen")
            connection.rollback()
            sys.exit(1)

        logger.info("Steg 4/4: Laddar daily_nps")
        try:
            nps_rows = load_daily_nps(nps_df, connection)
            results["daily_nps"] = {"rows": nps_rows}
            logger.info(f"Daily NPS laddade: {nps_rows} rader")
        except Exception as e:
            logger.error(f"Fel vid laddning av daily_nps: {e}")
            logger.error("Rollbackar hela transaktionen")
            connection.rollback()
            sys.exit(1)

        # Alla steg lyckades - commit transaktionen
        connection.commit()
        logger.info("Alla tabeller laddade framgångsrikt - transaktion committad")
        logger.info(f"Sammanfattning: {results}")

        return results

    except Exception as e:
        logger.error(f"Kritiskt fel vid laddning till databas: {e}")
        logger.exception("Fullständig stack trace:")
        try:
            connection.rollback()
            logger.info("Transaktion rollbackad - inga ändringar sparades")
        except Exception as rollback_error:
            logger.error(f"Fel vid rollback: {rollback_error}")
        logger.error("Kritiskt fel: Kan inte fortsätta")
        sys.exit(1)
    finally:
        # Återställ autocommit till default
        try:
            connection.autocommit = True
        except Exception:
            pass
