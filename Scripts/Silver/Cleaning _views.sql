/*
===============================================================================
Silver Layer Views Creation Script
Objective: Data Cleaning & Transformation 
===============================================================================

Why use Views here instead of embedding logic in Stored Procedures?
-----------------------------------------------------------------------
We adopted the strategy of "Decoupling Cleaning from Loading":

1. Maintainability:
   If the cleaning logic changes (e.g., how we handle NULLs or standardize text),
   we only update the View, avoiding the risk of modifying complex Stored Procedures.

2. Abstraction:
   The Views act as a Clean Interface. The Stored Procedure will simply perform 
   "SELECT * FROM View" and "INSERT INTO Table" without worrying about casting or trimming.

3. Robustness:
   Using TRY_CAST within the View ensures that any "Garbage Data" is automatically 
   converted to NULL instead of causing the ETL pipeline to crash.

4. Consistency:
   Data Types in these Views are explicitly cast to match the Physical Silver Tables 
   exactly (e.g., VARCHAR(32), DECIMAL(10,2), DATETIME2).
===============================================================================
*/

-- Ensure Schema exists
IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'Silver')
BEGIN
    EXEC('CREATE SCHEMA [Silver]')
END
GO

-------------------------------------------------------------------------------
-- 1. Category Translation View (Lookup)
-- Purpose: Helper view for category translation, used inside the Products view.
-------------------------------------------------------------------------------
CREATE OR ALTER VIEW Silver.vw_product_category_name_translation AS
SELECT 
    CAST(TRIM(product_category_name) AS NVARCHAR(100)) AS product_category_name,
    CAST(TRIM(product_category_name_english) AS NVARCHAR(100)) AS product_category_name_english
FROM Bronze.product_category_name_translation;
GO

-------------------------------------------------------------------------------
-- 2. Geolocation View
-- Problem Solved: Deduplicates Zip Codes to prevent Row Explosion during joins.
-------------------------------------------------------------------------------
CREATE OR ALTER VIEW Silver.vw_geolocation AS
WITH RankedGeo AS (
    SELECT 
        CAST(geolocation_zip_code_prefix AS VARCHAR(10)) AS geolocation_zip_code_prefix,
        -- Using FLOAT as defined in the Physical Table
        TRY_CAST(geolocation_lat AS FLOAT) AS geolocation_lat,
        TRY_CAST(geolocation_lng AS FLOAT) AS geolocation_lng,
        CAST(UPPER(TRIM(geolocation_city)) AS NVARCHAR(100)) AS geolocation_city,
        CAST(UPPER(TRIM(geolocation_state)) AS VARCHAR(5)) AS geolocation_state,
        -- Take only one coordinate pair per zip code
        ROW_NUMBER() OVER (
            PARTITION BY geolocation_zip_code_prefix 
            ORDER BY geolocation_lat, geolocation_lng
        ) as rn
    FROM Bronze.geolocation
)
SELECT 
    geolocation_zip_code_prefix,
    geolocation_lat,
    geolocation_lng,
    geolocation_city,
    geolocation_state
FROM RankedGeo
WHERE rn = 1;
GO

-------------------------------------------------------------------------------
-- 3. Products View
-- Problem Solved: Category translation, NULL handling, and Dimension casting.
-------------------------------------------------------------------------------
CREATE OR ALTER VIEW Silver.vw_products AS
SELECT 
    CAST(p.product_id AS VARCHAR(32)) AS product_id,
    
    -- Priority: English Name -> Original Name -> 'Unknown'
    CAST(COALESCE(t.product_category_name_english, p.product_category_name, 'Unknown') AS NVARCHAR(100)) AS product_category_name,
    
    -- Casting dimensions to INT
    TRY_CAST(p.product_name_lenght AS INT) AS product_name_lenght,
    TRY_CAST(p.product_description_lenght AS INT) AS product_description_lenght,
    TRY_CAST(p.product_photos_qty AS INT) AS product_photos_qty,
    TRY_CAST(p.product_weight_g AS INT) AS product_weight_g,
    TRY_CAST(p.product_length_cm AS INT) AS product_length_cm,
    TRY_CAST(p.product_height_cm AS INT) AS product_height_cm,
    TRY_CAST(p.product_width_cm AS INT) AS product_width_cm

FROM Bronze.products p
LEFT JOIN Bronze.product_category_name_translation t 
    ON p.product_category_name = t.product_category_name;
GO

-------------------------------------------------------------------------------
-- 4. Sellers View
-- Problem Solved: Standardizing text case (Upper) and removing whitespace.
-------------------------------------------------------------------------------
CREATE OR ALTER VIEW Silver.vw_sellers AS
SELECT 
    CAST(seller_id AS VARCHAR(32)) AS seller_id,
    CAST(seller_zip_code_prefix AS VARCHAR(10)) AS seller_zip_code_prefix,
    CAST(UPPER(TRIM(seller_city)) AS NVARCHAR(100)) AS seller_city,
    CAST(UPPER(TRIM(seller_state)) AS VARCHAR(5)) AS seller_state
FROM Bronze.sellers;
GO

-------------------------------------------------------------------------------
-- 5. Customers View
-- Problem Solved: Standardizing text to ensure clean Joins later.
-------------------------------------------------------------------------------
CREATE OR ALTER VIEW Silver.vw_customers AS
SELECT 
    CAST(customer_id AS VARCHAR(32)) AS customer_id,
    CAST(customer_unique_id AS VARCHAR(32)) AS customer_unique_id,
    CAST(customer_zip_code_prefix AS VARCHAR(10)) AS customer_zip_code_prefix,
    CAST(UPPER(TRIM(customer_city)) AS NVARCHAR(100)) AS customer_city,
    CAST(UPPER(TRIM(customer_state)) AS VARCHAR(5)) AS customer_state
FROM Bronze.customers;
GO

-------------------------------------------------------------------------------
-- 6. Orders View (Fact Header)
-- Problem Solved: Converting string dates to correct DATETIME2 format.
-------------------------------------------------------------------------------
CREATE OR ALTER VIEW Silver.vw_orders AS
SELECT 
    CAST(order_id AS VARCHAR(32)) AS order_id,
    CAST(customer_id AS VARCHAR(32)) AS customer_id, 
    CAST(order_status AS VARCHAR(20)) AS order_status,
    
    -- Using TRY_CAST to handle potential invalid date formats safely
    TRY_CAST(order_purchase_timestamp AS DATETIME2) AS order_purchase_timestamp,
    TRY_CAST(order_approved_at AS DATETIME2) AS order_approved_at,
    TRY_CAST(order_delivered_carrier_date AS DATETIME2) AS order_delivered_carrier_date,
    TRY_CAST(order_delivered_customer_date AS DATETIME2) AS order_delivered_customer_date,
    TRY_CAST(order_estimated_delivery_date AS DATETIME2) AS order_estimated_delivery_date

FROM Bronze.orders;
GO

-------------------------------------------------------------------------------
-- 7. Order Items View (Fact Details)
-- Problem Solved: Enforcing DECIMAL(10,2) precision for financial fields.
-------------------------------------------------------------------------------
CREATE OR ALTER VIEW Silver.vw_order_items AS
SELECT 
    CAST(order_id AS VARCHAR(32)) AS order_id,
    TRY_CAST(order_item_id AS INT) AS order_item_id,
    CAST(product_id AS VARCHAR(32)) AS product_id, 
    CAST(seller_id AS VARCHAR(32)) AS seller_id,   
    TRY_CAST(shipping_limit_date AS DATETIME2) AS shipping_limit_date,
    
    -- Currency fields must be Decimal to ensure accuracy
    TRY_CAST(price AS DECIMAL(10, 2)) AS price,
    TRY_CAST(freight_value AS DECIMAL(10, 2)) AS freight_value
FROM Bronze.order_items;
GO

-------------------------------------------------------------------------------
-- 8. Order Payments View
-- Problem Solved: Matching types with the physical table definition.
-------------------------------------------------------------------------------
CREATE OR ALTER VIEW Silver.vw_order_payments AS
SELECT
    CAST(order_id AS VARCHAR(32)) AS order_id,
    TRY_CAST(payment_sequential AS INT) AS payment_sequential,
    CAST(payment_type AS VARCHAR(20)) AS payment_type,
    TRY_CAST(payment_installments AS INT) AS payment_installments,
    TRY_CAST(payment_value AS DECIMAL(10, 2)) AS payment_value
FROM Bronze.order_payments;
GO

-------------------------------------------------------------------------------
-- 9. Order Reviews View
-- Problem Solved: Cleaning text by removing Newlines/Enters that break reports.
-------------------------------------------------------------------------------
CREATE OR ALTER VIEW Silver.vw_reviews AS
SELECT 
    CAST(review_id AS VARCHAR(32)) AS review_id,
    CAST(order_id AS VARCHAR(32)) AS order_id,
    TRY_CAST(review_score AS INT) AS review_score,
    CAST(COALESCE(review_comment_title, 'No Title') AS NVARCHAR(255)) AS review_comment_title,
    
    -- Text Cleaning: Replace CHAR(13) & CHAR(10) with spaces
    CAST(REPLACE(REPLACE(ISNULL(NULLIF(review_comment_message, ''), 'No Review'), CHAR(13), ' '), CHAR(10), ' ') AS NVARCHAR(MAX)) AS review_comment_message,
    
    TRY_CAST(review_creation_date AS DATETIME2) AS review_creation_date,
    TRY_CAST(review_answer_timestamp AS DATETIME2) AS review_answer_timestamp
FROM Bronze.order_reviews;
GO

