# 📘 SQL Data Warehouse Project

This project demonstrates the design and implementation of a modern data warehouse using SQL Server.  
It includes building ETL pipelines, cleaning raw data, designing data models, and creating analytical queries for business insights.

The goal of this project is to showcase practical skills in data engineering, data modeling, and SQL analytics using a real-world style workflow.

---

## 🚀 Project Overview

In this project, I built a complete data warehouse pipeline from raw CSV files to business-ready analytical tables.

The project includes:

- Loading raw data from multiple sources
- Cleaning and transforming data
- Designing a layered data warehouse
- Creating fact and dimension tables
- Writing analytical SQL queries
- Generating insights from the data

This project follows industry-style data warehouse architecture.

---

## 🏗️ Data Architecture

![Architecture](docs/architecture.png)

This project uses the **Medallion Architecture** with three layers:

### Bronze Layer
- Stores raw data from source systems
- Data loaded from CSV files into SQL Server
- No transformations applied

### Silver Layer
- Data cleaning and standardization
- Removing duplicates and fixing errors
- Preparing data for modeling

### Gold Layer
- Business-ready tables
- Star schema design
- Optimized for analytics and reporting

---

## 🛠️ Technologies Used

- 💾 SQL Server
- 🧠 T-SQL
- 🗄️ Data Warehousing
- 🔄 ETL Pipelines
- 📊 Data Modeling
- 🧰 Git & GitHub
- 🖼️ Draw.io

---

## 📊 Skills Demonstrated

- 🏗️ Data Warehousing
- 🔄 ETL Development
- 🧹 Data Cleaning & Transformation
- ⭐ Star Schema Modeling
- 📈 SQL Analytics
- 🗄️ Database Design
- 🔧 Git Version Control
  
 ---


## 📦 Project Structure

```
sql-data-warehouse-project/
│
├── datasets/        # Raw data files
├── docs/            # Diagrams and documentation
├── scripts/
│   ├── bronze/      # Load raw data
│   ├── silver/      # Clean & transform data
│   └── gold/        # Analytical models
│
├── tests/           # Validation scripts
├── README.md
```

---

## 🎯 Project Goals

- Build a modern data warehouse using SQL Server
- Practice ETL pipeline development
- Learn Medallion Architecture (Bronze / Silver / Gold)
- Improve data modeling skills
- Create a portfolio-ready data engineering project

---

## 📈 Future Improvements

- Add incremental loading
- Add historical data tracking
- Connect to Power BI / Tableau
- Automate ETL process
- Add stored procedures

---

## 📝 Notes

This project is for learning purposes and follows real-world data engineering practices.
The goal is to simulate how data warehouses are built in industry environments.

---

## 👤 Author

Fiyin  
Computer Science Student  
Interested in Data Engineering, Data Analytics, and Backend Development

---

## 📜 License

This project is for educational use.

---

## 🙏 Acknowledgment

This project was inspired by a data engineering tutorial and was built as part of my learning process.  
All code, documentation, and structure in this repository were written by me for practice and portfolio purposes.
