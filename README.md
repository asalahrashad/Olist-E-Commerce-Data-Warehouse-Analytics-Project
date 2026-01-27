# 🛒 Olist Enterprise Data Warehouse (End-to-End Solution)

![SQL Server](https://img.shields.io/badge/Database-SQL%20Server-CC2927?style=for-the-badge&logo=microsoft-sql-server&logoColor=white)
![Architecture](https://img.shields.io/badge/Architecture-Medallion%20(Bronze%2FSilver%2FGold)-FFD700?style=for-the-badge)
![ETL](https://img.shields.io/badge/ETL-Stored%20Procedures-005C84?style=for-the-badge)
![Status](https://img.shields.io/badge/Status-Completed-success?style=for-the-badge)

## 📖 Description
This project is a complete **End-to-End Data Engineering solution** designed to process and analyze the **Brazilian E-Commerce Public Dataset (Olist)**. 

It transforms raw, unstructured data (CSVs) into a highly optimized, business-ready **Data Warehouse** using the **Medallion Architecture**. The system is built to answer complex business questions regarding sales performance, logistics efficiency, and customer behavior with sub-second query latency.

---

## 🚩 Problem Statement
The Olist dataset consists of **100k+ orders** scattered across **9 disconnected CSV files**. 
* **The Challenge:** The data contains duplicates, inconsistent data types, NULL values, and lacks a unified schema, making it impossible for business stakeholders to extract insights directly.
* **The Solution:** A robust ETL pipeline was built to ingest, clean, normalize, and model this data into a **Star Schema** optimized for BI reporting tools like Power BI.

---

## 🛠️ Tech Stack
* **Database Engine:** Microsoft SQL Server (T-SQL).
* **Architecture:** Medallion Architecture (Bronze, Silver, Gold Layers).
* **Data Modeling:** Dimensional Modeling (Kimball Star Schema).
* **ETL Orchestration:** Native Stored Procedures with Dynamic SQL.
* **Optimization:** Hybrid Indexing Strategy (Clustered Columnstore & B-Tree).
* **Data Quality:** Automated constraints and integrity checks.

---

## 📂 Dataset Overview
**Source:** [Brazilian E-Commerce Public Dataset by Olist (Kaggle)](https://www.kaggle.com/olistbr/brazilian-ecommerce)
**Volume:** ~100,000 Orders (2016–2018).

The dataset covers the entire order lifecycle:
* **Customers & Sellers:** Demographics and Geolocations.
* **Transactions:** Orders, Items, Payments.
* **Feedback:** Customer Reviews.

---

## ⚙️ Project Workflow
The pipeline follows a strict **ELT (Extract, Load, Transform)** process:

### 1️⃣ Bronze Layer (Raw Ingestion)
* **Goal:** High-performance data ingestion.
* **Method:** Used `BULK INSERT` with `TABLOCK` to minimize transaction logging.
* **Outcome:** Loaded 9 CSVs into Heap tables in seconds (Full Load strategy).

### 2️⃣ Silver Layer (Cleansing & Standardization)
* **Goal:** Data quality enforcement.
* **Method:** * **Views (`vw_`):** Handle logic (TRIM, NULL handling, Date casting).
    * **Tables:** Store physical data with **Surrogate Keys (SK)**.
    * **Maintenance:** Dynamic Stored Procedure to manage `DROP/CREATE` operations.

### 3️⃣ Gold Layer (Business Intelligence)
* **Goal:** Analytics and Reporting.
* **Method:** **Star Schema** design.
    * **Fact Tables:** `fact_sales`, `fact_order_reviews`.
    * **Dimension Tables:** `dim_customers`, `dim_products`, `dim_sellers`.
* **Logic:** Aggregated payment methods and denormalized location data for faster joins.

### 4️⃣ Optimization & Quality
* **Hybrid Indexing:** Implemented **Columnstore Indexes** on Fact tables for aggregation speed, and **Row-Store Indexes** on Dimensions for filtering.
* **Defensive Programming:** Scripts include environment checks (`DEV` vs `PROD`) and error logging.

---

## 📊 Results & Key Insights
The Data Warehouse now supports complex analytical queries, such as:
* **Logistics:** Identifying states with the highest late delivery rates.
* **Sales:** Month-over-Month revenue growth analysis.
* **Pareto Principle:** Identifying the top 10% of sellers driving 80% of revenue.
* **Performance:** Reduced query execution time for aggregations by **~90%** using Columnstore Indexing.

---

## 📷 Architecture & Modeling
### High-Level Architecture
![image alt](https://github.com/asalahrashad/Olist-E-Commerce-Data-Warehouse-Analytics-Project/blob/5cdec3ff9f053789128f836e1eec021874e7a62c/DOCs/Data%20Architecture.png)


### Data Model (Star Schema)
![ERD](https://github.com/asalahrashad/Olist-E-Commerce-Data-Warehouse-Analytics-Project/blob/5cdec3ff9f053789128f836e1eec021874e7a62c/DOCs/Data%20model.png)

---

## 📘 Project Structure & Detailed Documentation
Detailed technical design, ETL logic, data models, and implementation details are organized across the project folders.  
Please refer to the relevant directories (e.g., **DOCs/**, **Scripts/**, **Performance/**) for in-depth information.


### 👤 Author
**Ahmed Salah** Data Engineer | SQL • Data Warehousing • Analytics Engineering [https://www.linkedin.com/in/ahmed-sal4h/]
