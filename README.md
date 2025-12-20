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
Transform (cleaning, schema enforcement, validation)
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

---

## 📂 Project Structure

```
DE_Project/
│
├── data/
│   ├── raw/
│   │   └── Raw_Superstore.csv
│   └── processed/
│       └── clean_superstore.csv
│
├── etl_pipeline/
│   ├── extract.py
│   ├── transform.py
│   └── load.py
│
├── db/
│   └── mysql_connection.py
│
├── dev/
│   └── config.py
│
├── main.py
└── README.md
```

---

## 📥 Extract Step
- Reads raw CSV using pandas
- Validates file accessibility
- Loads data into a DataFrame

---

## 🔄 Transform Step
- Standardizes column names to snake_case
- Converts data types (dates, numeric fields)
- Adds derived column: `total_sales`
- Performs data validation checks
- Saves cleaned data to processed directory

---

## 🗄️ Database Design
- Database: `de_project`
- Table: `sales`
- Primary Key: `row_id`
- Uses appropriate SQL data types

---

## 📤 Load Step
- Converts NaN to NULL
- Uses parameterized queries
- Bulk inserts with `executemany()`
- Uses `INSERT IGNORE` for idempotency

---

## ▶️ How to Run

1. Create MySQL database and table
2. Update credentials in `config.py`
3. Run:
```bash
python main.py
```

---

## 📈 Key Learnings
- ETL pipeline design
- Schema enforcement
- Data validation
- MySQL loading strategies
- Clean project structure

---

## 🚀 Future Enhancements
- Logging and monitoring
- Incremental loads
- Spark-based processing
- Airflow orchestration
