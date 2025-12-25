/*
===============================================================================
Data Quality Checks (DQC) - Olist E-commerce Dataset
===============================================================================
Schema:      Bronze (Staging/Raw Layer)
Description: 
    This script executes a comprehensive data quality audit on the Olist dataset
    post-ingestion (ELT process). It aims to identify data anomalies before 
    transforming data into the Silver/Core layer.

Key Check Categories:
    1. Completeness: Identifying NULLs in Primary/Foreign Keys and critical fields.
    2. Uniqueness: Detecting duplicates in Single and Composite Primary Keys.
    3. Consistency: Validating categorical values (e.g., State codes, Statuses).
    4. Validity: Ensuring logical constraints (e.g., Dates, Non-negative prices).
    5. Integrity: Verifying relationships between Orders, Items, and Payments.
===============================================================================
*/

-------------------------------------------------------------------------------
-- SECTION 1: DIMENSION TABLES CHECKS
-- Tables: Customers, Products, Sellers
-------------------------------------------------------------------------------

-- ============================================================================
-- 1.1 Customers Table
-- ============================================================================
-- Check for Nulls in Critical IDs
SELECT * FROM Bronze.customers 
WHERE customer_id IS NULL OR customer_unique_id IS NULL;

-- Check for Duplicate Primary Keys
SELECT customer_id, COUNT(*) AS duplicate_count
FROM Bronze.customers
GROUP BY customer_id
HAVING COUNT(*) > 1;

-- Check Consistency of 'customer_state' (Standard 2-char codes)
SELECT DISTINCT customer_state FROM Bronze.customers;


-- ============================================================================
-- 1.2 Products Table
-- ============================================================================
-- Check for Null Primary Keys
SELECT * FROM Bronze.products 
WHERE product_id IS NULL;

-- Check for Duplicate Primary Keys
SELECT product_id, COUNT(*) AS duplicate_count
FROM Bronze.products
GROUP BY product_id
HAVING COUNT(*) > 1;

-- Check Business Logic: Product photos quantity must be positive
SELECT * FROM Bronze.products 
WHERE product_photos_qty <= 0;

-- Check Consistency of Product Categories
SELECT DISTINCT product_category_name FROM Bronze.products;


-- ============================================================================
-- 1.3 Sellers Table
-- ============================================================================
-- Check for Null Primary Keys
SELECT * FROM Bronze.sellers 
WHERE seller_id IS NULL;

-- Check for Duplicate Primary Keys
SELECT seller_id, COUNT(*) AS duplicate_count
FROM Bronze.sellers
GROUP BY seller_id
HAVING COUNT(*) > 1;


-------------------------------------------------------------------------------
-- SECTION 2: FACT TABLES CHECKS
-- Tables: Orders, Order_Items, Payments, Reviews
-------------------------------------------------------------------------------

-- ============================================================================
-- 2.1 Orders Table
-- ============================================================================
-- Check for Null Primary Keys
SELECT * FROM Bronze.orders 
WHERE order_id IS NULL;

-- Check for Duplicate Primary Keys
SELECT order_id, COUNT(*) AS duplicate_count
FROM Bronze.orders
GROUP BY order_id
HAVING COUNT(*) > 1;

-- Check for Missing Timestamps
SELECT * FROM Bronze.orders 
WHERE order_purchase_timestamp IS NULL;

-- Check Date Logic: Delivery date cannot be before Purchase date
SELECT * FROM Bronze.orders
WHERE order_delivered_customer_date < order_purchase_timestamp;

-- Check Consistency of Order Status
SELECT DISTINCT order_status FROM Bronze.orders;


-- ============================================================================
-- 2.2 Order Items Table
-- ============================================================================
-- Check for Orphaned Records (Null Foreign Keys)
SELECT * FROM Bronze.order_items 
WHERE order_id IS NULL OR product_id IS NULL;

-- Check for Duplicates in Composite Key (order_id + order_item_id)
-- Note: An order can have multiple items, but the specific item sequence must be unique.
SELECT order_id, order_item_id, COUNT(*) AS duplicate_count
FROM Bronze.order_items
GROUP BY order_id, order_item_id
HAVING COUNT(*) > 1;

-- Check Data Validity: Prices must be positive, Freight non-negative
SELECT * FROM Bronze.order_items
WHERE price <= 0 
   OR freight_value < 0; 

-- Data Profiling: Price Outliers
SELECT 
    MIN(price) AS min_price,
    AVG(price) AS avg_price,
    MAX(price) AS max_price
FROM Bronze.order_items;


-- ============================================================================
-- 2.3 Order Payments Table
-- ============================================================================
-- Check for Null Foreign Keys
SELECT * FROM Bronze.order_payments 
WHERE order_id IS NULL;

-- Check for Duplicates in Composite Key (order_id + payment_sequential)
SELECT order_id, payment_sequential, COUNT(*) AS duplicate_count
FROM Bronze.order_payments
GROUP BY order_id, payment_sequential
HAVING COUNT(*) > 1;

-- Check Consistency of Payment Types
SELECT DISTINCT payment_type FROM Bronze.order_payments;


-- ============================================================================
-- 2.4 Order Reviews Table
-- ============================================================================
-- Check for Null Review IDs
SELECT * FROM Bronze.order_reviews 
WHERE review_id IS NULL;

-- Check for Duplicate Review IDs
SELECT review_id, COUNT(*) AS duplicate_count
FROM Bronze.order_reviews
GROUP BY review_id
HAVING COUNT(*) > 1;


-------------------------------------------------------------------------------
-- SECTION 3: ADVANCED INTEGRITY CHECKS (Cross-Table Logic)
-------------------------------------------------------------------------------

-- 3.1 Referential Integrity: Orphaned Order Items
-- Identify items that reference an Order ID that does not exist in the Orders table.
SELECT i.order_id AS orphaned_order_id
FROM Bronze.order_items i
LEFT JOIN Bronze.orders o ON i.order_id = o.order_id
WHERE o.order_id IS NULL;

-- 3.2 Financial Integrity: Total Payment vs. Order Value
-- Business Rule: The sum of payments should equal the sum of (Price + Freight).
-- We allow a small threshold (0.01) for floating-point rounding differences.
WITH OrderValue AS (
    SELECT order_id, SUM(price + freight_value) AS total_order_cost
    FROM Bronze.order_items
    GROUP BY order_id
),
PaymentValue AS (
    SELECT order_id, SUM(payment_value) AS total_paid
    FROM Bronze.order_payments
    GROUP BY order_id
)
SELECT 
    ov.order_id, 
    ov.total_order_cost, 
    pv.total_paid,
    (ov.total_order_cost - pv.total_paid) AS difference
FROM OrderValue ov
JOIN PaymentValue pv ON ov.order_id = pv.order_id
WHERE ABS(ov.total_order_cost - pv.total_paid) > 0.01;

