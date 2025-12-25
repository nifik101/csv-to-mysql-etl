# CSV to MySQL ETL Pipeline (Data Engineering Project)

## 📌 Project Overview
This project implements an **end-to-end ETL (Extract, Transform, Load) pipeline** that ingests a CSV dataset, applies data transformations and validations using Python, and loads the processed data into a MySQL database for analytical querying.

The goal of this project is to demonstrate **core Data Engineering fundamentals** such as schema enforcement, data validation, clean pipeline design, and reliable database loading.

---

## 🏗️ Architecture

```
CSV File
   ↓
Extract (pandas)
   ↓
Transform (schema enforcement, validation)
   ↓
Processed CSV
   ↓
Load (MySQL)
```

---

## 🛠️ Tech Stack
- Python
- Pandas
- MySQL
- mysql-connector-python
- SQL
- Ruff
- uv
- pandera
- loguru
- python-dotenv

---

## 📂 Project Structure

```
csv-to-mysql-etl/
│
├── data/
│   ├── raw/                    # Raw CSV input files
│   │   ├── Raw_Superstore.csv
│   │   └── data.csv
│   └── processed/              # Cleaned/transformed CSV files
│       └── clean_superstore.csv
│
├── etl_pipeline/               # ETL pipeline modules
│   ├── extract.py             # CSV extraction logic
│   ├── transform.py           # Data transformation logic
│   └── load.py                # MySQL loading logic
│
├── db/                        # Database utilities
│   └── mysql_connection.py    # MySQL connection manager
│
├── dev/                       # Configuration
│   ├── config.py              # Main config (create from .example)
│   └── config.py.example      # Config template
│
├── validation/                # Data validation
│   └── schemas.py             # Pandera validation schemas
│
├── utils/                     # Utility modules
│   └── logger.py              # Logging configuration
│
├── logs/                      # Log files (auto-generated)
│
├── main.py                    # Main pipeline entry point
├── schema.sql                 # MySQL database schema
├── requirements.txt           # Python dependencies
├── pyproject.toml             # Project metadata
└── README.md
```

---

## 📥 Extract Step

The extract module (`etl_pipeline/extract.py`) reads raw CSV files and loads them into pandas DataFrames.

**Features:**
- Validates file existence before reading
- Supports custom encoding (default: `latin1`)
- Flexible pandas parameters (separator, skiprows, na_values, etc.)
- Comprehensive error handling with logging
- Returns pandas DataFrame ready for transformation

**Example:**
```python
from etl_pipeline.extract import extract_csv

# Basic usage
df = extract_csv("data/raw/Raw_Superstore.csv")

# With custom parameters
df = extract_csv(
    "data/raw/data.csv",
    encoding="utf-8",
    sep=";",
    skiprows=1,
    na_values=["N/A", "NULL"]
)
```

---

## 🔄 Transform Step

The transform module (`etl_pipeline/transform.py`) applies data cleaning and transformation operations.

**Features:**
- **Column Standardization**: Converts column names to `snake_case`
- **Type Conversion**: Converts date columns to datetime format
- **Custom Transformations**: Supports custom transformation functions
- **Data Validation**: Uses Pandera schemas for schema enforcement
- **Error Handling**: Comprehensive validation with detailed error messages
- **Output**: Saves processed data to CSV (optional)

**Transformation Pipeline:**
1. Standardize column names (spaces/hyphens → underscores, lowercase)
2. Convert date columns to datetime (configurable format)
3. Apply custom transformation functions (if provided)
4. Validate data against Pandera schema
5. Save to processed directory (if path provided)

**Example:**
```python
from etl_pipeline.transform import transform_csv

# Basic transformation
df_transformed = transform_csv(df, save_path="data/processed/clean_data.csv")

# With custom transformations
def add_total_sales(df):
    df['total_sales'] = df['sales'] * (1 - df['discount'])
    return df

df_transformed = transform_csv(
    df,
    date_columns=['order_date', 'ship_date'],
    custom_transformations=[add_total_sales],
    save_path="data/processed/clean_data.csv"
)
```

---

## 🗄️ Database Design

The database schema is defined in `schema.sql` and creates a `sales` table optimized for analytical queries.

**Table: `sales`**

| Column | Type | Description |
|--------|------|-------------|
| `row_id` | INT PRIMARY KEY | Unique row identifier |
| `order_id` | VARCHAR(20) | Order identifier |
| `order_date` | DATE | Order placement date |
| `ship_date` | DATE | Shipment date |
| `ship_mode` | VARCHAR(20) | Shipping method |
| `customer_id` | VARCHAR(50) | Customer identifier |
| `customer_name` | VARCHAR(50) | Customer name |
| `segment` | VARCHAR(20) | Customer segment |
| `country` | VARCHAR(20) | Country |
| `city` | VARCHAR(20) | City |
| `state` | VARCHAR(20) | State/Province |
| `postal_code` | INT | Postal code |
| `region` | VARCHAR(20) | Region |
| `product_id` | VARCHAR(50) | Product identifier |
| `category` | VARCHAR(20) | Product category |
| `sub_category` | VARCHAR(30) | Product subcategory |
| `product_name` | VARCHAR(200) | Product name |
| `sales` | DECIMAL(10,2) | Sales amount |
| `quantity` | INT | Quantity ordered |
| `discount` | DECIMAL(10,2) | Discount percentage |
| `profit` | DECIMAL(10,2) | Profit amount |
| `total_sales` | DECIMAL(10,2) | Total sales after discount |

**Key Features:**
- Primary key on `row_id` for uniqueness
- `INSERT IGNORE` strategy prevents duplicate inserts
- Optimized data types for storage efficiency
- Date columns for time-based analysis
---

## 📤 Load Step

The load module (`etl_pipeline/load.py`) handles loading transformed data into MySQL database.

**Features:**
- **Idempotent Loading**: Uses `INSERT IGNORE` to prevent duplicates
- **Data Preparation**: Converts pandas types to MySQL-compatible types
- **Validation**: Optional pre-load validation
- **Transaction Management**: Automatic rollback on errors
- **Connection Management**: Uses context managers for safe connection handling
- **Error Handling**: Comprehensive logging and error reporting

**Load Process:**
1. Validate DataFrame columns match expected schema
2. Prepare data (convert NaN to None, datetime to date)
3. Build parameterized INSERT IGNORE query
4. Execute batch insert
5. Commit transaction
6. Return number of rows loaded

**Example:**
```python
from db.mysql_connection import get_mysql_connection
from etl_pipeline.load import load_to_mysql

with get_mysql_connection() as connection:
    rows_loaded = load_to_mysql(
        df_transformed,
        connection,
        table_name="sales",
        validate_before_load=True
    )
    print(f"Loaded {rows_loaded} rows")
```
---

## ⚙️ Configuration

This project uses a combination of configuration files and environment variables for flexible setup.

### Configuration Files

**1. `dev/config.py`** - Main configuration file
- Data directories (raw, processed)
- File paths
- CSV encoding settings
- Date format configuration
- Database table and column definitions

**2. `.env`** - Environment variables (create from template)
- MySQL connection credentials
- Database settings

### Setup Steps

1. **Copy config template:**
   ```bash
   cp dev/config.py.example dev/config.py
   ```

2. **Create `.env` file** in project root:
   ```env
   MYSQL_HOST=localhost
   MYSQL_USER=root
   MYSQL_PASSWORD=your_password
   MYSQL_DATABASE=de_project
   ```

3. **Customize `dev/config.py`** (if needed):
   - Update `raw_file` and `processed_file` paths
   - Adjust `CSV_ENCODING` if needed
   - Modify `DATE_FORMAT` for different date formats
   - Update `DB_TABLE_NAME` and `DB_COLUMNS` for different schemas

### Configuration Options

| Setting | Default | Description |
|---------|---------|-------------|
| `RAW_DIR` | `data/raw/` | Directory for raw CSV files |
| `PROCESSED_DIR` | `data/processed/` | Directory for processed CSV files |
| `CSV_ENCODING` | `latin1` | Default CSV file encoding |
| `DATE_FORMAT` | `%m/%d/%Y` | Date format for parsing |
| `DB_TABLE_NAME` | `sales` | Target MySQL table name |
| `MYSQL_CONFIG` | From `.env` | MySQL connection parameters |


---

## ▶️ How to Run

### Prerequisites

- Python 3.14+ (or compatible version)
- MySQL server installed and running
- `uv` package manager (or use `pip` with `requirements.txt`)

### Installation

1. **Clone the repository:**
   ```bash
   git clone <repository-url>
   cd csv-to-mysql-etl
   ```

2. **Install dependencies:**
   
   Using `uv` (recommended):
   ```bash
   uv sync
   ```
   
   Or using `pip`:
   ```bash
   pip install -r requirements.txt
   ```

3. **Set up configuration:**
   ```bash
   cp dev/config.py.example dev/config.py
   # Edit dev/config.py if needed
   ```

4. **Create `.env` file:**
   ```bash
   # Create .env file with MySQL credentials
   echo "MYSQL_HOST=localhost" >> .env
   echo "MYSQL_USER=root" >> .env
   echo "MYSQL_PASSWORD=your_password" >> .env
   echo "MYSQL_DATABASE=de_project" >> .env
   ```

5. **Create MySQL database and table:**
   ```bash
   mysql -u root -p < schema.sql
   ```
   Or manually:
   ```sql
   CREATE DATABASE de_project;
   USE de_project;
   SOURCE schema.sql;
   ```

### Running the Pipeline

**Basic execution:**
```bash
python main.py
```

This will:
1. Read the raw CSV file specified in `config.py`
2. Transform the data (standardize columns, convert types, validate)
3. Save processed data to `data/processed/`
4. Load data into MySQL database

**Custom execution (programmatic):**
```python
from main import main

# Use default config
main()

# Custom file paths
main(
    raw_file_path="data/raw/my_data.csv",
    processed_file_path="data/processed/my_clean_data.csv"
)

# Custom MySQL config
main(
    mysql_config={
        "host": "localhost",
        "user": "root",
        "password": "password",
        "database": "my_database"
    }
)

# Custom extract parameters
main(
    extract_kwargs={
        "encoding": "utf-8",
        "sep": ";"
    }
)
```

### Logging

Logs are automatically written to:
- **Console**: Real-time INFO level logs
- **File**: `logs/etl_YYYY-MM-DD.log` (DEBUG level, rotated daily)

Log files are automatically compressed and retained for 30 days.

### Verification

After running, verify the data:

```sql
-- Check row count
SELECT COUNT(*) FROM sales;

-- View sample data
SELECT * FROM sales LIMIT 10;

-- Check for duplicates
SELECT row_id, COUNT(*) as count 
FROM sales 
GROUP BY row_id 
HAVING count > 1;
```


---

## 📈 Key Learnings
- End-to-end ETL pipeline design
- Schema enforcement before database load
- Data validation and defensive programming
- Idempotent database loading
- Clean project structure and configuration management

---

## 🚀 Future Enhancements
- [x] Logging and error handling (implemented)
- [ ] Implement incremental loads
- [ ] Add data quality metrics and reporting
- [ ] Support for multiple data sources
- [ ] Airflow/Dagster integration for orchestration
- [ ] Unit and integration tests
- [ ] Docker containerization
- [ ] CI/CD pipeline setup

## 🐛 Troubleshooting

### Common Issues

**1. FileNotFoundError: CSV file not found**
- Verify the file path in `dev/config.py` matches your file location
- Check that the file exists in `data/raw/` directory

**2. MySQL Connection Error**
- Verify MySQL server is running: `mysql -u root -p`
- Check `.env` file has correct credentials
- Ensure database exists: `CREATE DATABASE de_project;`

**3. Encoding Errors**
- Try different encodings: `utf-8`, `latin1`, `cp1252`
- Update `CSV_ENCODING` in `dev/config.py`
- Or pass encoding directly: `extract_kwargs={"encoding": "utf-8"}`

**4. Schema Validation Errors**
- Check that required columns exist in your CSV
- Verify data types match expected schema
- Review validation errors in logs for specific issues

**5. Import Errors**
- Ensure all dependencies are installed: `uv sync` or `pip install -r requirements.txt`
- Verify Python version: `python --version` (requires 3.14+)

**6. Permission Errors**
- Ensure write permissions for `data/processed/` and `logs/` directories
- Check MySQL user has INSERT permissions on target table

### Getting Help

- Check log files in `logs/` directory for detailed error messages
- Review configuration in `dev/config.py`
- Verify database schema matches `schema.sql`
