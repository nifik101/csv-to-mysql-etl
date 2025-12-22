# CSV to MySQL ETL Pipeline - Daily Performance Data

## 📌 Projektöversikt

Detta projekt implementerar en **end-to-end ETL (Extract, Transform, Load) pipeline** som läser in CSV-data med daglig prestationsdata (sälj, samtal, NPS, användardata), transformerar data och laddar den till en MySQL-databas.

Pipeline hanterar:
- Användardata extraherad från Agent-kolumnen
- Daglig prestationsdata (samtal, ACD, ACW, provisioner, etc.)
- Daglig retention-data (vändningsprocent)
- Daglig NPS-data (NPS, CSAT, CB)

**Viktiga funktioner:**
- UPSERT-strategi för dagliga kumulativa uppdateringar
- Bevara historik - alla dagliga poster sparas
- Hantera negativa värden (NPS, value_change kan vara negativa)
- Omfattande logging med loguru (DEBUG och PRODUCTION-lägen)
- Säker konfiguration via .env-filer

---

## 🏗️ Arkitektur

```
CSV File (med Agent, Samtal, ACD, NPS, etc.)
   ↓
Extract (pandas, hantera svenska tecken)
   ↓
Transform (parsa Agent, skapa 4 DataFrames)
   ├── Users DataFrame
   ├── Daily Performance DataFrame
   ├── Daily Retention DataFrame
   └── Daily NPS DataFrame
   ↓
Load (MySQL UPSERT)
   ├── users (först, för referentiell integritet)
   ├── daily_performance
   ├── daily_retention
   └── daily_nps
```

---

## 🛠️ Tech Stack

- Python 3.8+
- Pandas
- MySQL
- mysql-connector-python
- loguru (logging)
- python-dotenv (konfiguration)
- ruff (kodkvalitet)

---

## 📂 Projektstruktur

```
csv-to-mysql-etl/
│
├── data/
│   ├── raw/
│   │   └── data.csv          # Rå CSV-fil med daglig data
│   └── processed/
│       ├── processed_data_users.csv
│       ├── processed_data_performance.csv
│       ├── processed_data_retention.csv
│       └── processed_data_nps.csv
│
├── etl_pipeline/
│   ├── extract.py            # Extraherar CSV-data
│   ├── transform.py          # Transformerar till databasformat
│   └── load.py               # Laddar till MySQL
│
├── db/
│   └── mysql_connection.py   # Databasanslutning
│
├── logs/                     # Loggfiler (skapas automatiskt)
│   └── etl_YYYY-MM-DD.log
│
├── main.py                   # Huvudscript
├── schema.sql                # SQL-schema för tabeller
├── requirements.txt          # Python-beroenden
├── pyproject.toml            # Ruff-konfiguration
├── .env.example              # Exempel på miljövariabler
└── README.md
```

---

## 🗄️ Databasdesign

### Tabeller

**users**
- `user_id` (PRIMARY KEY) - Extraherat från Agent-kolumnen
- `namn` - Användarens namn
- `created_at`, `updated_at` - Timestamps

**daily_performance**
- `user_id`, `datum` (PRIMARY KEY)
- `samtal`, `acd_seconds`, `acw_seconds`, `hold_seconds`
- `koppling_pct`, `bb_antal`, `pp_antal`, `tv_antal`, `mbb_antal`, `other_antal`
- `erbjud_pct`, `save_provis_kr`, `provis_kr`, `fmc_prov_kr`, `value_change_kr`

**daily_retention**
- `user_id`, `datum` (PRIMARY KEY)
- `vand_tv_pct`, `vand_bb_pct`, `vand_pp_pct`, `vand_total_pct`, `vand_antal`

**daily_nps**
- `user_id`, `datum` (PRIMARY KEY)
- `nps_antal_svar`, `nps_score`, `csat_pct`, `cb_pct`

**Viktigt:** Negativa värden tillåts för NPS, value_change och provisioner. Inga CHECK constraints begränsar värden till >= 0.

---

## ⚙️ Konfiguration

### 1. Skapa .env-fil

Kopiera `.env.example` till `.env`:

```bash
cp .env.example .env
```

### 2. Uppdatera .env med dina inställningar

```env
# MySQL Database Configuration
MYSQL_HOST=localhost
MYSQL_USER=root
MYSQL_PASSWORD=ditt_lösenord
MYSQL_DATABASE=de_project

# Data Directory
DATA_DIRECTORY=data
RAW_FILE=raw/data.csv

# Logging Configuration
# Options: DEBUG, INFO, WARNING, ERROR
# Default: INFO (för production)
LOG_LEVEL=INFO
```

> ⚠️ `.env` är ignorerad i `.gitignore` för att undvika exponering av känslig information.

### 3. Skapa databas och tabeller

Kör SQL-schemat:

```bash
mysql -u root -p de_project < schema.sql
```

---

## 📥 Extract Step

- Läser rå CSV med pandas
- Hanterar svenska tecken (åäö) - försöker utf-8, fallback till latin1
- Validerar filtillgänglighet
- Loggar extraktionsprocessen

---

## 🔄 Transform Step

1. **Parsar Agent-kolumnen** - Extraherar `user_id` och `namn` från formatet "Namn Efternamn (12345678)"
2. **Skapar 4 DataFrames:**
   - `users_df` - Unika användare
   - `perf_df` - Daglig prestationsdata
   - `retention_df` - Daglig retention-data
   - `nps_df` - Daglig NPS-data
3. **Mappar CSV-kolumner** till databas-kolumner enligt `my_structure.md`
4. **Lägger till dagens datum** till alla dagliga poster
5. **Konverterar datatyper** (INT för antal, DECIMAL för pengar/procent)
6. **Hanterar negativa värden** - Inga min >= 0 valideringar för NPS, value_change, provisioner
7. **Sparar processade filer** för audit trail

---

## 📤 Load Step

- **UPSERT-strategi** - Använder `INSERT ... ON DUPLICATE KEY UPDATE`
- Laddar users först (för referentiell integritet)
- Sedan laddar alla daily-tabeller
- Konverterar NaN till SQL NULL
- Använder parameteriserade SQL-queries
- Bulk inserts med `executemany()`
- Transaktionshantering med rollback vid fel

---

## 📊 Logging

Pipeline använder **loguru** för omfattande logging.

### Log-nivåer

Konfigureras via `LOG_LEVEL` i `.env`:

- **DEBUG**: Detaljerad information för debugging (data samples, mellanresultat)
- **INFO**: Allmän information om pipeline-framsteg (standard för production)
- **WARNING**: Data quality-problem som inte stoppar körningen
- **ERROR**: Fel som stoppar körningen

### Log-filer

Loggar sparas i `logs/etl_YYYY-MM-DD.log` med rotation varje dag och 30 dagars retention.

### Exempel på loggning

```python
logger.info("Startar ETL-pipeline")
logger.debug(f"Kolumner i DataFrame: {list(df.columns)}")
logger.warning(f"Hittade {count} rader med negativa samtal-värden")
logger.error(f"Fel vid laddning: {e}")
```

---

## ▶️ Hur man kör

### 1. Installera beroenden

Med `uv` (rekommenderat):

```bash
uv pip install -r requirements.txt
```

Eller med `pip`:

```bash
pip install -r requirements.txt
```

### 2. Konfigurera .env

Se [Konfiguration](#-konfiguration) ovan.

### 3. Skapa databas och tabeller

```bash
mysql -u root -p de_project < schema.sql
```

### 4. Placera CSV-fil

Placera din CSV-fil i `data/raw/data.csv` (eller uppdatera `RAW_FILE` i `.env`).

### 5. Kör pipeline

```bash
python main.py
```

### 6. Kontrollera loggar

```bash
# Senaste loggfilen
tail -f logs/etl_$(date +%Y-%m-%d).log

# Med DEBUG-nivå
LOG_LEVEL=DEBUG python main.py
```

---

## 📋 CSV-format

CSV-filen ska innehålla följande kolumner (exakta namn med svenska tecken):

```
Agent
Samtal, AHT, ACD, ACW, Hold, Koppling
Vänd TV, Vänd BB, Vänd PP, Vänd %, Antal Vänd, Value change
BB, PP, TV, MBB, Other, Nyteck, Erbjud %, Save provis, Provis, FMC prov, Total Provision
GI %, Tot%, Antal, NPS, CSAT, CB
```

**Agent-format:** `Namn Efternamn (12345678)`

Se `my_structure.md` för detaljerad mappning.

---

## 🔍 Kodkvalitet

Projektet använder **ruff** för linting och formatering:

```bash
# Kontrollera kodkvalitet
ruff check .

# Formatera kod
ruff format .
```

Konfiguration finns i `pyproject.toml`.

---

## 📈 Viktiga funktioner

- ✅ UPSERT-strategi för dagliga kumulativa uppdateringar
- ✅ Bevara historik - alla dagliga poster sparas
- ✅ Hantera negativa värden (NPS, value_change)
- ✅ Omfattande logging (DEBUG och PRODUCTION)
- ✅ Säker konfiguration via .env
- ✅ Svenska docstrings, engelsk kod
- ✅ Transaktionshantering och felhantering
- ✅ Audit trail (sparade processade filer)

---

## 🚀 Framtida förbättringar

- [ ] Stöd för flera CSV-filer i batch
- [ ] Validering av data quality metrics
- [ ] Airflow/Dagster för orchestration
- [ ] Docker-containerisering
- [ ] Unit tests
- [ ] Integration tests

---

## 📝 Noteringar

- **Negativa värden:** NPS, value_change och provisioner kan vara negativa. Inga CHECK constraints begränsar dessa.
- **Datum:** Alla dagliga poster får dagens datum när de importeras.
- **Agent-parsing:** Om Agent-kolumnen inte kan parsas hoppas raden över med en varning.
- **UPSERT:** Om samma `user_id` och `datum` redan finns uppdateras posten, annars skapas en ny.
