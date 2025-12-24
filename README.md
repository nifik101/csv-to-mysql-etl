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
DE_Project/
│
├── data/
│   ├── raw/
│   │   └── raw_data_example.csv
│   └── processed/
│       └── clean_raw_data_example.csv
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
│   └── config.example.py
│
├── main.py
├── requirements.txt
└── README.md
```

---

## 📥 Extract Step
- Reads raw CSV using pandas
- Validates file accessibility
- Loads data into a DataFrame

---

## 🔄 Transform Step
- Standardizes column names to `snake_case`
- Converts data types (dates, numeric fields)
- Adds derived column: `total_sales`
- Performs data validation checks
- Saves cleaned data to processed directory

---

## 🗄️ Database Design


---

## 📤 Load Step

---

## ⚙️ Configuration

This project uses a configuration file for paths and database credentials.



---

## ▶️ How to Run


---

## 📈 Key Learnings
- End-to-end ETL pipeline design
- Schema enforcement before database load
- Data validation and defensive programming
- Idempotent database loading
- Clean project structure and configuration management

---

## 🚀 Future Enhancements
- Add logging and error handling
- Implement incremental loads
