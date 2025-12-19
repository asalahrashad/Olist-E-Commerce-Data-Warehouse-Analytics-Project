/*
## 🥈 2. Silver Layer Initialization (DDL)

This script establishes the schema structure for the **Silver Layer** (Cleansed & Standardized Data). 

In this layer, data structures are refined to be more user-friendly and business-oriented. The primary transformation applied at the DDL level is **Naming Convention Standardization**, where redundant suffixes (e.g., `_dataset`) are removed to ensure cleaner and more intuitive table names.

### 📝 Script Overview
- **File:** `2_Setup_Silver_Tables.sql`
- **Schema:** `Silver`
- **Purpose:** Structure definition for cleansed and deduplicated data.

### ⚙️ Key Architectural Decisions
* **Naming Standardization:** Renamed tables to remove technical suffixes (e.g., `olist_orders_dataset` → `olist_orders`) for better readability and querying experience.
* **Schema Isolation:** Dedicated `Silver` schema to separate cleansed data from raw inputs (Bronze).
* **Data Consistency:** Maintains compatible data types with the Bronze layer to ensure seamless data propagation while preparing the structure for subsequent cleaning transformations (handling nulls, date formatting, etc.).
* **Idempotency:** Includes `IF OBJECT_ID ... DROP TABLE` logic to allow safe re-execution of the script during development iterations.

### 📂 Created Tables
The script initializes the following **9 standardized tables**:
1. `olist_orders` (formerly `olist_orders_dataset`)
2. `olist_order_items`
3. `olist_order_payments`
4. `olist_products`
5. `olist_customers`
6. `olist_sellers`
7. `olist_order_reviews`
8. `olist_geolocation`
9. `product_category_name_translation`

*/
/*
===============================================================================
DDL Script: Silver Layer (Star Schema Design)
===============================================================================
Script Purpose:
    Creates the tables for the Silver layer using a Star Schema approach.
    - Dimensions: Equipped with Surrogate Keys (SK) and Business Keys (BK).
    - Facts: Use Foreign Surrogate Keys to link to Dimensions.
===============================================================================
*/

-----------------------------------------------------------
-- SECTION 1: DIMENSION TABLES (The Parents)
-- Must be created first so Fact tables can reference them.
-----------------------------------------------------------

-- 1. Table: Products Dimension
IF OBJECT_ID('Silver.olist_products', 'U') IS NOT NULL DROP TABLE Silver.olist_products;
GO
CREATE TABLE Silver.olist_products (
    product_sk INT IDENTITY(1,1) PRIMARY KEY,   -- Surrogate Key (Internal ID)
    product_id VARCHAR(32) NOT NULL,            -- Business Key (Original GUID)
    product_category_name NVARCHAR(100),        -- Translated or Original Category Name
    product_name_lenght INT,        
    product_description_lenght INT, 
    product_photos_qty INT,
    product_weight_g INT,
    product_length_cm INT,
    product_height_cm INT,
    product_width_cm INT,
    dwh_create_date DATETIME2 DEFAULT GETDATE() -- Audit: Load Date
);
-- Index on Business Key (Crucial for Merge/Upsert performance)
CREATE NONCLUSTERED INDEX IX_products_bk ON Silver.olist_products(product_id);
GO

-- 2. Table: Sellers Dimension
IF OBJECT_ID('Silver.olist_sellers', 'U') IS NOT NULL DROP TABLE Silver.olist_sellers;
GO
CREATE TABLE Silver.olist_sellers (
    seller_sk INT IDENTITY(1,1) PRIMARY KEY,    -- Surrogate Key
    seller_id VARCHAR(32) NOT NULL,             -- Business Key (Original GUID)
    seller_zip_code_prefix VARCHAR(10),
    seller_city NVARCHAR(100),
    seller_state VARCHAR(5),
    dwh_create_date DATETIME2 DEFAULT GETDATE()
);
-- Index on Business Key
CREATE NONCLUSTERED INDEX IX_sellers_bk ON Silver.olist_sellers(seller_id);
GO

-- 3. Table: Customers Dimension
IF OBJECT_ID('Silver.olist_customers', 'U') IS NOT NULL DROP TABLE Silver.olist_customers;
GO
CREATE TABLE Silver.olist_customers (
    customer_sk INT IDENTITY(1,1) PRIMARY KEY,  -- Surrogate Key
    customer_id VARCHAR(32) NOT NULL,           -- Business Key (Linked to specific Order)
    customer_unique_id VARCHAR(32),             -- Real unique identifier for a customer
    customer_zip_code_prefix VARCHAR(10),
    customer_city NVARCHAR(100),
    customer_state VARCHAR(5),
    dwh_create_date DATETIME2 DEFAULT GETDATE()
);
-- Index on Business Key
CREATE NONCLUSTERED INDEX IX_customers_bk ON Silver.olist_customers(customer_id);
GO

-- 4. Table: Geolocation Dimension
IF OBJECT_ID('Silver.olist_geolocation', 'U') IS NOT NULL DROP TABLE Silver.olist_geolocation;
GO
CREATE TABLE Silver.olist_geolocation (
    geo_sk INT IDENTITY(1,1) PRIMARY KEY,       -- Surrogate Key
    geolocation_zip_code_prefix VARCHAR(10),    -- Business Key logic
    geolocation_lat FLOAT,
    geolocation_lng FLOAT,
    geolocation_city NVARCHAR(100),
    geolocation_state VARCHAR(5),
    dwh_create_date DATETIME2 DEFAULT GETDATE()
);
-- Index on Zip Code
CREATE NONCLUSTERED INDEX IX_geo_zip ON Silver.olist_geolocation(geolocation_zip_code_prefix);
GO

-- 5. Table: Category Translation (Lookup Table)
-- Used for translating category names during the transformation phase
IF OBJECT_ID('Silver.product_category_name_translation', 'U') IS NOT NULL DROP TABLE Silver.product_category_name_translation;
GO
CREATE TABLE Silver.product_category_name_translation (
    product_category_name NVARCHAR(100),
    product_category_name_english NVARCHAR(100),
    dwh_create_date DATETIME2 DEFAULT GETDATE()
);
GO

-----------------------------------------------------------
-- SECTION 2: FACT TABLES (The Children)
-- These tables use the SKs generated in the Dimensions above.
-----------------------------------------------------------

-- 6. Table: Orders Fact (Header)
IF OBJECT_ID('Silver.olist_orders', 'U') IS NOT NULL DROP TABLE Silver.olist_orders;
GO
CREATE TABLE Silver.olist_orders (
    order_id VARCHAR(32) NOT NULL,              -- Natural Key (Maintained for Audit/Lineage)
    customer_sk INT DEFAULT -1,                 -- Foreign Key to Customer Dimension (SK)
    order_status VARCHAR(20),
    order_purchase_timestamp DATETIME2,
    order_approved_at DATETIME2,
    order_delivered_carrier_date DATETIME2,
    order_delivered_customer_date DATETIME2,
    order_estimated_delivery_date DATETIME2,
    dwh_create_date DATETIME2 DEFAULT GETDATE()
);
-- Index on FK for Reporting Performance
CREATE NONCLUSTERED INDEX IX_orders_cust_sk ON Silver.olist_orders(customer_sk);
GO

-- 7. Table: Order Items Fact (Details)
IF OBJECT_ID('Silver.olist_order_items', 'U') IS NOT NULL DROP TABLE Silver.olist_order_items;
GO
CREATE TABLE Silver.olist_order_items (
    order_id VARCHAR(32) NOT NULL,              -- Natural Key (Link to Header)
    order_item_id INT NOT NULL,                 -- Sequence Number within the order
    product_sk INT DEFAULT -1,                  -- Foreign Key to Product Dimension (SK)
    seller_sk INT DEFAULT -1,                   -- Foreign Key to Seller Dimension (SK)
    shipping_limit_date DATETIME2,
    price DECIMAL(10, 2),
    freight_value DECIMAL(10, 2),
    dwh_create_date DATETIME2 DEFAULT GETDATE()
);
-- Indexes on FKs for Join Performance
CREATE NONCLUSTERED INDEX IX_items_prod_sk ON Silver.olist_order_items(product_sk);
CREATE NONCLUSTERED INDEX IX_items_sel_sk ON Silver.olist_order_items(seller_sk);
GO

-- 8. Table: Order Payments Fact
IF OBJECT_ID('Silver.olist_order_payments', 'U') IS NOT NULL DROP TABLE Silver.olist_order_payments;
GO
CREATE TABLE Silver.olist_order_payments (
    order_id VARCHAR(32) NOT NULL,              -- Natural Key
    payment_sequential INT,
    payment_type VARCHAR(20),
    payment_installments INT,
    payment_value DECIMAL(10, 2),
    dwh_create_date DATETIME2 DEFAULT GETDATE()
);
GO

-- 9. Table: Order Reviews Fact
IF OBJECT_ID('Silver.olist_order_reviews', 'U') IS NOT NULL DROP TABLE Silver.olist_order_reviews;
GO
CREATE TABLE Silver.olist_order_reviews (
    review_id VARCHAR(32),
    order_id VARCHAR(32),                       -- Natural Key
    review_score INT,
    review_comment_title NVARCHAR(255),
    review_comment_message NVARCHAR(MAX), 
    review_creation_date DATETIME2,
    review_answer_timestamp DATETIME2,
    dwh_create_date DATETIME2 DEFAULT GETDATE()
);
GO
