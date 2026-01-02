/*
# 🏛️ Gold Layer (Star Schema)

## 📌 Overview
This module represents the **Gold Layer** of the Olist Data Warehouse. It transforms the cleaned "Silver" data into a polished, business-ready **Star Schema** optimized for high-performance reporting and analytics tools like **Power BI** and **Tableau**.

The logic is implemented using **SQL Views** to ensure data consistency without redundant storage, providing a semantic layer that abstracts complexity for end-users.

## 🏗️ Architecture & Design Decisions

### 1. Data Modeling (Star Schema)
The architecture follows the **Kimball Dimensional Modeling** approach:
- **Central Fact Tables:** capture business processes/events (Sales, Reviews).
- **Dimension Tables:** provide descriptive context (Who, What, Where, When).
  */
-------------------------------------------------------------------------------
-- 1. Create Fact Sales View
-- Granularity: One row per Order Item
-------------------------------------------------------------------------------
IF OBJECT_ID('Gold.vw_fact_sales', 'V') IS NOT NULL
    DROP VIEW Gold.vw_fact_sales;
GO

CREATE OR ALTER VIEW Gold.vw_fact_sales AS
WITH payments_agg AS (
    -- Aggregating payments to ensure 1:1 join with orders
    SELECT
        order_id,
        MAX(payment_installments) AS max_installments,
        MIN(payment_type)         AS payment_type -- Selecting primary payment type
    FROM Silver.order_payments
    GROUP BY order_id
)
SELECT
    -- Keys
    oi.order_id,
    oi.order_item_id,
    oi.product_sk               AS product_id,
    oi.seller_sk                AS seller_id,
    o.customer_sk               AS customer_id,

    -- Dimensions Attributes
    o.order_status              AS status,
    p.payment_type,
    
    -- Date Attributes
    CAST(o.order_purchase_timestamp AS DATE)      AS purchase_date,
    CAST(o.order_approved_at AS DATE)             AS approved_date,
    CAST(oi.shipping_limit_date AS DATE)          AS shipping_limit_date,
    CAST(o.order_delivered_carrier_date AS DATE)  AS delivered_carrier_date,
    CAST(o.order_delivered_customer_date AS DATE) AS delivered_customer_date,
    CAST(o.order_estimated_delivery_date AS DATE) AS estimated_delivery_date,

    -- Measures
    p.max_installments          AS installments,
    oi.price                    AS item_price,
    oi.freight_value            AS shipping_cost,
    -- Calculated Column: Total value for this specific item (Price + Freight)
    (oi.price + oi.freight_value) AS total_item_value 

FROM Silver.order_items oi
INNER JOIN Silver.orders o
    ON oi.order_id = o.order_id
LEFT JOIN payments_agg p
    ON oi.order_id = p.order_id;
GO

-------------------------------------------------------------------------------
-- 2. Create Fact Order Reviews View
-- Granularity: One row per Review
-------------------------------------------------------------------------------
IF OBJECT_ID('Gold.vw_fact_order_reviews', 'V') IS NOT NULL
    DROP VIEW Gold.vw_fact_order_reviews;
GO

CREATE OR ALTER VIEW Gold.vw_fact_order_reviews AS
SELECT 
    review_id,
    order_id,
    review_score,
    review_comment_title      AS review_title,
    review_comment_message    AS review_message,
    CAST(review_creation_date AS DATE) AS creation_date,
    CAST(review_answer_timestamp AS DATE) AS answer_date
FROM Silver.order_reviews;
GO

-------------------------------------------------------------------------------
-- 3. Create Dimension Customers View
-------------------------------------------------------------------------------
IF OBJECT_ID('Gold.vw_dim_customers', 'V') IS NOT NULL
    DROP VIEW Gold.vw_dim_customers;
GO

CREATE OR ALTER VIEW Gold.vw_dim_customers AS
SELECT 
    c.customer_sk             AS customer_id,
    c.customer_unique_id,
    c.customer_city           AS city,
    c.customer_state          AS state,
    g.geolocation_lat         AS latitude,
    g.geolocation_lng         AS longitude
FROM Silver.customers c
LEFT JOIN Silver.geolocation g
    ON c.customer_zip_code_prefix = g.geolocation_zip_code_prefix;
GO

-------------------------------------------------------------------------------
-- 4. Create Dimension Sellers View
-------------------------------------------------------------------------------
IF OBJECT_ID('Gold.vw_dim_sellers', 'V') IS NOT NULL
    DROP VIEW Gold.vw_dim_sellers;
GO

CREATE OR ALTER VIEW Gold.vw_dim_sellers AS
SELECT 
    s.seller_sk               AS seller_id,
    s.seller_city             AS city,
    s.seller_state            AS state,
    g.geolocation_lat         AS latitude,
    g.geolocation_lng         AS longitude
FROM Silver.sellers s
LEFT JOIN Silver.geolocation g
    ON s.seller_zip_code_prefix = g.geolocation_zip_code_prefix;
GO

-------------------------------------------------------------------------------
-- 5. Create Dimension Products View
-------------------------------------------------------------------------------
IF OBJECT_ID('Gold.vw_dim_products', 'V') IS NOT NULL
    DROP VIEW Gold.vw_dim_products;
GO

CREATE OR ALTER VIEW Gold.vw_dim_products AS
SELECT 
    product_sk                AS product_id,
    product_category_name     AS category,
    product_weight_g          AS weight_g,
    product_length_cm         AS length_cm,
    product_height_cm         AS height_cm,
    product_width_cm          AS width_cm
FROM Silver.products;
GO
