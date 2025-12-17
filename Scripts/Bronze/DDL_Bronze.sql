/*
## 🏗️ 1. Bronze Layer Initialization (DDL)

This script is responsible for setting up the **Bronze Layer** (Raw Data Layer) of the Olist Data Warehouse. It establishes the database schema and creates the necessary tables to store raw data exactly as ingested from the source (CSVs/APIs).

### 📝 Script Overview
- **File:** `1_Setup_Bronze_Tables.sql`
- **Schema:** `Bronze`
- **Purpose:** Ingestion landing zone for raw Olist E-commerce data.

### ⚙️ Key Features
* **Schema Isolation:** Creates a dedicated `Bronze` schema to logically separate raw data from transformed data (Silver/Gold).
* **Idempotency:** Uses `IF OBJECT_ID ... DROP TABLE` logic, making the script safe to re-run multiple times (useful for resetting the environment).
* **Data Type Precision:** Utilizes appropriate SQL Server data types:
    * `NVARCHAR` for text fields to support special characters (Portuguese/English).
    * `DATETIME` for accurate timestamp storage.
    * `DECIMAL(10,2)` for financial values (prices, freight).
    * `MAX` for long text fields like review comments.
* **Source Fidelity:** Column names are kept exactly as they appear in the source files (including original typos like `product_name_lenght`) to ensure seamless bulk loading processes without mapping errors at this stage.

### 📂 Created Tables
The script initializes the following **9 datasets**:
1. `olist_orders_dataset`
2. `olist_order_items_dataset`
3. `olist_order_payments_dataset`
4. `olist_products_dataset`
5. `olist_customers_dataset`
6. `olist_sellers_dataset`
7. `olist_order_reviews_dataset`
8. `olist_geolocation_dataset`
9. `product_category_name_translation`
*/

Use Olist_DW;
GO

IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'Bronze')
BEGIN
    EXEC('CREATE SCHEMA [Bronze]')
END
GO

-----------------------------------------------------------
-- 1. Table: product_category_name_translation 
-----------------------------------------------------------
IF OBJECT_ID('Bronze.product_category_name_translation', 'U') IS NOT NULL
    DROP TABLE Bronze.product_category_name_translation;
GO

CREATE TABLE Bronze.product_category_name_translation (
    product_category_name NVARCHAR(100),
    product_category_name_english NVARCHAR(100)
);
GO

-----------------------------------------------------------
-- 2. Table: olist_customers_dataset
-----------------------------------------------------------
IF OBJECT_ID('Bronze.olist_customers', 'U') IS NOT NULL
    DROP TABLE Bronze.olist_customers;
GO

CREATE TABLE Bronze.olist_customers (
    customer_id VARCHAR(50),
    customer_unique_id VARCHAR(50),
    customer_zip_code_prefix VARCHAR(10),
    customer_city NVARCHAR(100),
    customer_state VARCHAR(5)
);
GO

-----------------------------------------------------------
-- 3. Table: olist_orders_dataset
-----------------------------------------------------------
IF OBJECT_ID('Bronze.olist_orders', 'U') IS NOT NULL
    DROP TABLE Bronze.olist_orders;
GO

CREATE TABLE Bronze.olist_orders (
    order_id VARCHAR(50),
    customer_id VARCHAR(50),
    order_status VARCHAR(20),
    order_purchase_timestamp DATETIME,
    order_approved_at DATETIME,
    order_delivered_carrier_date DATETIME,
    order_delivered_customer_date DATETIME,
    order_estimated_delivery_date DATETIME
);
GO

-----------------------------------------------------------
-- 4. Table: olist_order_items_dataset
-----------------------------------------------------------
IF OBJECT_ID('Bronze.olist_order_items', 'U') IS NOT NULL
    DROP TABLE Bronze.olist_order_items;
GO

CREATE TABLE Bronze.olist_order_items (
    order_id VARCHAR(50),
    order_item_id INT,
    product_id VARCHAR(50),
    seller_id VARCHAR(50),
    shipping_limit_date DATETIME,
    price DECIMAL(10, 2),
    freight_value DECIMAL(10, 2)
);
GO

-----------------------------------------------------------
-- 5. Table: olist_products_dataset
-----------------------------------------------------------
IF OBJECT_ID('Bronze.olist_products', 'U') IS NOT NULL
    DROP TABLE Bronze.olist_products;
GO

CREATE TABLE Bronze.olist_products (
    product_id VARCHAR(50),
    product_category_name NVARCHAR(100),
    product_name_lenght INT,        
    product_description_lenght INT, 
    product_photos_qty INT,
    product_weight_g INT,
    product_length_cm INT,
    product_height_cm INT,
    product_width_cm INT
);
GO

-----------------------------------------------------------
-- 6. Table: olist_order_payments_dataset
-----------------------------------------------------------
IF OBJECT_ID('Bronze.olist_order_payments', 'U') IS NOT NULL
    DROP TABLE Bronze.olist_order_payments;
GO

CREATE TABLE Bronze.olist_order_payments (
    order_id VARCHAR(50),
    payment_sequential INT,
    payment_type VARCHAR(20),
    payment_installments INT,
    payment_value DECIMAL(10, 2)
);
GO

-----------------------------------------------------------
-- 7. Table: olist_order_reviews_dataset
-----------------------------------------------------------
IF OBJECT_ID('Bronze.olist_order_reviews', 'U') IS NOT NULL
    DROP TABLE Bronze.olist_order_reviews;
GO

CREATE TABLE Bronze.olist_order_reviews (
    review_id VARCHAR(50),
    order_id VARCHAR(50),
    review_score INT,
    review_comment_title NVARCHAR(255),
    review_comment_message NVARCHAR(MAX), 
    review_creation_date DATETIME,
    review_answer_timestamp DATETIME
);
GO

-----------------------------------------------------------
-- 8. Table: olist_sellers_dataset
-----------------------------------------------------------
IF OBJECT_ID('Bronze.olist_sellers', 'U') IS NOT NULL
    DROP TABLE Bronze.olist_sellers;
GO

CREATE TABLE Bronze.olist_sellers (
    seller_id VARCHAR(50),
    seller_zip_code_prefix VARCHAR(10),
    seller_city NVARCHAR(100),
    seller_state VARCHAR(5)
);
GO

-----------------------------------------------------------
-- 9. Table: olist_geolocation_dataset
-----------------------------------------------------------
IF OBJECT_ID('Bronze.olist_geolocation', 'U') IS NOT NULL
    DROP TABLE Bronze.olist_geolocation;
GO

CREATE TABLE Bronze.olist_geolocation (
    geolocation_zip_code_prefix VARCHAR(10),
    geolocation_lat FLOAT,
    geolocation_lng FLOAT,
    geolocation_city NVARCHAR(100),
    geolocation_state VARCHAR(5)
);
GO
