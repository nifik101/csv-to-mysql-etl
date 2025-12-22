"""Modul för att transformera CSV-data till databasformat."""

import os
import re
import sys
from datetime import date

import pandas as pd
from loguru import logger


def clean_swedish_numeric_string(s):
    """
    Rensar en svensk numerisk sträng (t.ex. "1.234,56 kr") för att möjliggöra konvertering till float.
    - Tar bort "kr"
    - Tar bort mellanslag
    - Ersätter tusentalsavgränsare (punkt '.') med tom sträng
    - Ersätter decimaltecken (komma ',') med punkt '.'
    - Hanterar negativa tal.
    """
    if pd.isna(s) or not isinstance(s, str):
        return s
    
    # Remove 'kr' and any surrounding whitespace
    s = s.replace("kr", "").strip()
    
    # Check for negative sign and temporarily remove it
    is_negative = s.startswith('-')
    if is_negative:
        s = s[1:]

    # Remove thousands separator ('.')
    s = s.replace(".", "")
    
    # Replace decimal comma (',') with dot ('.')
    s = s.replace(",", ".")

    # Add negative sign back if it was there
    if is_negative:
        s = '-' + s

    return s


def parse_agent(agent_str):
    """
    Parsar Agent-kolumnen för att extrahera user_id och namn.

    Format: "Namn Efternamn (12345678)"
    Exempel: "Bajram Krushevci (10190035)"

    Args:
        agent_str (str): Agent-sträng i formatet "Namn (ID)"

    Returns:
        tuple: (namn, user_id) eller (None, None) om parsing misslyckas
    """
    if pd.isna(agent_str) or not isinstance(agent_str, str):
        logger.warning(f"Ogiltig Agent-värde: {agent_str}")
        return None, None

    pattern = r"^(.*)\s+\((\d+)\)\s*$"
    match = re.match(pattern, agent_str)

    if not match:
        logger.warning(f"Kunde inte parsa Agent-sträng: {agent_str}")
        return None, None

    namn = match.group(1).strip()
    user_id = int(match.group(2))

    logger.debug(f"Parsad Agent: namn='{namn}', user_id={user_id}")
    return namn, user_id


def transform_to_users(df):
    """
    Skapar users DataFrame från Agent-kolumnen.

    Extraherar user_id och namn från varje rad och skapar unika användare.

    Args:
        df (pd.DataFrame): DataFrame med Agent-kolumn

    Returns:
        pd.DataFrame: DataFrame med kolumner user_id och namn
    """
    logger.info("Transformering till users DataFrame")

    users_data = []
    for _, row in df.iterrows():
        namn, user_id = parse_agent(row.get("Agent", ""))
        if user_id is not None:
            users_data.append({"user_id": user_id, "namn": namn})

    if not users_data:
        logger.warning("Inga användare kunde extraheras från Agent-kolumnen")
        logger.warning("Returnerar tom DataFrame - kontrollera Agent-kolumnens format")
        return pd.DataFrame(columns=["user_id", "namn"])

    users_df = pd.DataFrame(users_data)
    # Ta bort duplicerade användare (behåll första förekomsten)
    users_df = users_df.drop_duplicates(subset=["user_id"], keep="first")

    logger.info(f"Skapade {len(users_df)} unika användare")
    logger.debug(f"Exempel på användare: {users_df.head().to_dict('records')}")

    return users_df


def transform_to_daily_performance(df, current_date):
    """
    Transformera CSV-data till daily_performance DataFrame.

    Mappar CSV-kolumner till daily_performance-tabellens kolumner
    och lägger till aktuellt datum.

    Args:
        df (pd.DataFrame): DataFrame med CSV-data
        current_date (date): Datum som ska användas för alla rader

    Returns:
        pd.DataFrame: DataFrame formaterad för daily_performance-tabellen
    """
    logger.info("Transformering till daily_performance DataFrame")

    # Skapa mapping från CSV-kolumner till databas-kolumner
    column_mapping = {
        "Samtal": "samtal",
        "ACD": "acd_seconds",
        "ACW": "acw_seconds",
        "Hold": "hold_seconds",
        "Koppling": "koppling_pct",
        "BB": "bb_antal",
        "PP": "pp_antal",
        "TV": "tv_antal",
        "MBB": "mbb_antal",
        "Other": "other_antal",
        "Erbjud %": "erbjud_pct",
        "Save provis": "save_provis_kr",
        "Provis": "provis_kr",
        "FMC prov": "fmc_prov_kr",
        "Value change": "value_change_kr",
    }

    performance_data = []

    for _, row in df.iterrows():
        namn, user_id = parse_agent(row.get("Agent", ""))
        if user_id is None:
            logger.warning(f"Hoppar över rad med ogiltig Agent: {row.get('Agent', 'N/A')}")
            continue

        perf_row = {"user_id": user_id, "datum": current_date}

        # Mappa kolumner
        for csv_col, db_col in column_mapping.items():
            value = row.get(csv_col)
            if pd.notna(value):
                perf_row[db_col] = value
            else:
                perf_row[db_col] = None

        performance_data.append(perf_row)

    perf_df = pd.DataFrame(performance_data)

    # Konvertera datatyper
    int_columns = [
        "samtal",
        "acd_seconds",
        "acw_seconds",
        "hold_seconds",
        "bb_antal",
        "pp_antal",
        "tv_antal",
        "mbb_antal",
        "other_antal",
    ]
    decimal_columns = [
        "koppling_pct",
        "erbjud_pct",
        "save_provis_kr",
        "provis_kr",
        "fmc_prov_kr",
        "value_change_kr",
    ]

    for col in int_columns:
        if col in perf_df.columns:
            perf_df[col] = pd.to_numeric(perf_df[col], errors="coerce").astype("Int64")

    for col in decimal_columns:
        if col in perf_df.columns:
            # Apply cleaning function before converting to numeric
            perf_df[col] = perf_df[col].apply(clean_swedish_numeric_string)
            perf_df[col] = pd.to_numeric(perf_df[col], errors="coerce")

    # Validering: samtal bör vara >= 0 (men tillåt None)
    if "samtal" in perf_df.columns:
        invalid_samtal = perf_df[(perf_df["samtal"].notna()) & (perf_df["samtal"] < 0)]
        if len(invalid_samtal) > 0:
            logger.warning(f"Hittade {len(invalid_samtal)} rader med negativa samtal-värden")

    logger.info(f"Skapade {len(perf_df)} rader för daily_performance")
    logger.debug(f"Kolumner i performance DataFrame: {list(perf_df.columns)}")

    return perf_df


def transform_to_daily_retention(df, current_date):
    """
    Transformera CSV-data till daily_retention DataFrame.

    Mappar CSV-kolumner till daily_retention-tabellens kolumner
    och lägger till aktuellt datum.

    Args:
        df (pd.DataFrame): DataFrame med CSV-data
        current_date (date): Datum som ska användas för alla rader

    Returns:
        pd.DataFrame: DataFrame formaterad för daily_retention-tabellen
    """
    logger.info("Transformering till daily_retention DataFrame")

    column_mapping = {
        "Vänd TV": "vand_tv_pct",
        "Vänd BB": "vand_bb_pct",
        "Vänd PP": "vand_pp_pct",
        "Vänd %": "vand_total_pct",
        "Antal Vänd": "vand_antal",
    }

    retention_data = []

    for _, row in df.iterrows():
        namn, user_id = parse_agent(row.get("Agent", ""))
        if user_id is None:
            continue

        retention_row = {"user_id": user_id, "datum": current_date}

        for csv_col, db_col in column_mapping.items():
            value = row.get(csv_col)
            if pd.notna(value):
                retention_row[db_col] = value
            else:
                retention_row[db_col] = None

        retention_data.append(retention_row)

    retention_df = pd.DataFrame(retention_data)

    # Konvertera datatyper
    decimal_columns = ["vand_tv_pct", "vand_bb_pct", "vand_pp_pct", "vand_total_pct"]
    int_columns = ["vand_antal"]

    for col in decimal_columns:
        if col in retention_df.columns:
            retention_df[col] = pd.to_numeric(retention_df[col], errors="coerce")

    for col in int_columns:
        if col in retention_df.columns:
            retention_df[col] = pd.to_numeric(retention_df[col], errors="coerce").astype("Int64")

    logger.info(f"Skapade {len(retention_df)} rader för daily_retention")

    return retention_df


def transform_to_daily_nps(df, current_date):
    """
    Transformera CSV-data till daily_nps DataFrame.

    Mappar CSV-kolumner till daily_nps-tabellens kolumner
    och lägger till aktuellt datum.

    Args:
        df (pd.DataFrame): DataFrame med CSV-data
        current_date (date): Datum som ska användas för alla rader

    Returns:
        pd.DataFrame: DataFrame formaterad för daily_nps-tabellen
    """
    logger.info("Transformering till daily_nps DataFrame")

    column_mapping = {
        "Antal": "nps_antal_svar",
        "NPS": "nps_score",
        "CSAT": "csat_pct",
        "CB": "cb_pct",
    }

    nps_data = []

    for _, row in df.iterrows():
        namn, user_id = parse_agent(row.get("Agent", ""))
        if user_id is None:
            continue

        nps_row = {"user_id": user_id, "datum": current_date}

        for csv_col, db_col in column_mapping.items():
            value = row.get(csv_col)
            if pd.notna(value):
                nps_row[db_col] = value
            else:
                nps_row[db_col] = None

        nps_data.append(nps_row)

    nps_df = pd.DataFrame(nps_data)

    # Konvertera datatyper
    # NPS kan vara negativ - inga min >= 0 valideringar
    int_columns = ["nps_antal_svar"]
    decimal_columns = ["nps_score", "csat_pct", "cb_pct"]

    for col in int_columns:
        if col in nps_df.columns:
            nps_df[col] = pd.to_numeric(nps_df[col], errors="coerce").astype("Int64")

    for col in decimal_columns:
        if col in nps_df.columns:
            nps_df[col] = pd.to_numeric(nps_df[col], errors="coerce")

    logger.info(f"Skapade {len(nps_df)} rader för daily_nps")
    logger.debug(
        f"Exempel på NPS-värden (kan vara negativa): {nps_df['nps_score'].describe() if 'nps_score' in nps_df.columns else 'N/A'}"
    )

    return nps_df


def transform_csv(df, save_path):
    """
    Huvudfunktion för att transformera CSV-data till databasformat.

    Orkestrerar alla transformationer:
    1. Parsar Agent-kolumnen
    2. Skapar users DataFrame
    3. Skapar daily_performance DataFrame
    4. Skapar daily_retention DataFrame
    5. Skapar daily_nps DataFrame
    6. Sparar processade dataframes till CSV för audit trail

    Args:
        df (pd.DataFrame): DataFrame med rå CSV-data
        save_path (str): Bas-sökväg för att spara processade filer

    Returns:
        tuple: (users_df, perf_df, retention_df, nps_df) - fyra DataFrames

    Raises:
        ValueError: Om kritiska kolumner saknas eller data är korrupt
        SystemExit: Om transformation misslyckas eller data är ogiltig
    """
    logger.info("Startar transform-process")

    # Validera att DataFrame inte är tom
    if df.empty:
        logger.error("DataFrame är tom - kan inte transformera")
        logger.error("Kritiskt fel: Inget data att transformera")
        sys.exit(1)

    # Standardisera kolumnnamn (hantera svenska tecken)
    original_columns = df.columns.tolist()
    df.columns = df.columns.str.strip()
    logger.debug(f"Originala kolumner: {original_columns}")

    # Validera att Agent-kolumnen finns (kritisk för alla transformationer)
    if "Agent" not in df.columns:
        logger.error("Saknar kritisk kolumn 'Agent' - kan inte transformera data")
        logger.error(f"Tillgängliga kolumner: {list(df.columns)}")
        logger.error("Kritiskt fel: Data-struktur är ogiltig")
        sys.exit(1)

    # Hämta dagens datum
    current_date = date.today()
    logger.info(f"Använder datum: {current_date}")

    try:
        # Skapa users DataFrame
        logger.info("Steg 1/4: Skapar users DataFrame")
        users_df = transform_to_users(df)

        # Validera att vi har minst några användare
        if users_df.empty:
            logger.warning("Inga användare kunde extraheras från Agent-kolumnen")
            logger.warning("Fortsätter men inga users kommer att laddas till databasen")

        # Skapa daily DataFrames
        logger.info("Steg 2/4: Skapar daily_performance DataFrame")
        perf_df = transform_to_daily_performance(df, current_date)

        logger.info("Steg 3/4: Skapar daily_retention DataFrame")
        retention_df = transform_to_daily_retention(df, current_date)

        logger.info("Steg 4/4: Skapar daily_nps DataFrame")
        nps_df = transform_to_daily_nps(df, current_date)

        # Validera att vi har minst något data att spara
        total_rows = len(perf_df) + len(retention_df) + len(nps_df)
        if total_rows == 0 and users_df.empty:
            logger.error("Ingen data kunde transformeras från CSV-filen")
            logger.error("Kritiskt fel: Kan inte fortsätta med tom data")
            sys.exit(1)

        # Spara processade dataframes för audit trail
        base_path = os.path.splitext(save_path)[0]
        processed_dir = os.path.dirname(base_path)

        # Skapa processed-katalog om den inte finns
        if processed_dir and not os.path.exists(processed_dir):
            try:
                os.makedirs(processed_dir, exist_ok=True)
                logger.debug(f"Skapade katalog: {processed_dir}")
            except OSError as e:
                logger.warning(f"Kunde inte skapa katalog {processed_dir}: {e}")
                logger.warning("Fortsätter utan att spara processade filer")

        users_path = f"{base_path}_users.csv"
        perf_path = f"{base_path}_performance.csv"
        retention_path = f"{base_path}_retention.csv"
        nps_path = f"{base_path}_nps.csv"

        try:
            users_df.to_csv(users_path, index=False)
            perf_df.to_csv(perf_path, index=False)
            retention_df.to_csv(retention_path, index=False)
            nps_df.to_csv(nps_path, index=False)

            logger.info("Sparade processade filer:")
            logger.info(f"  - {users_path}")
            logger.info(f"  - {perf_path}")
            logger.info(f"  - {retention_path}")
            logger.info(f"  - {nps_path}")
        except Exception as e:
            logger.error(f"Fel vid sparande av processade filer: {e}")
            logger.warning(
                "Fortsätter trots spar-fel - data kommer fortfarande laddas till databasen"
            )

        logger.info("Transform-process klar")
        logger.info(
            f"Sammanfattning: {len(users_df)} users, {len(perf_df)} performance, "
            f"{len(retention_df)} retention, {len(nps_df)} nps rader"
        )

        return users_df, perf_df, retention_df, nps_df

    except Exception as e:
        logger.error(f"Kritiskt fel vid transformation: {e}")
        logger.exception("Fullständig stack trace:")
        logger.error("Kritiskt fel: Kan inte fortsätta med korrupt data")
        sys.exit(1)
