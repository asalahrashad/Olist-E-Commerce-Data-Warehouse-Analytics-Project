/* 
===============================================================================
Business Insights & Analytics Showcase
===============================================================================
Description:
    This script contains a collection of advanced Analytical SQL Queries designed 
    to extract actionable business insights from the Olist Gold Layer (Star Schema).

    It serves as a "Proof of Concept" for the Data Warehouse capabilities, 
    demonstrating how the Star Schema facilitates complex reporting.

Key Metrics Covered:
    1. Logistics Performance (Late Deliveries by State).
    2. Financial Trends (Month-over-Month Revenue Growth).
    3. Seller Analysis (Pareto Principle & Market Share).
    4. Product Quality (Categories with Negative Reviews).
    5. Customer Segmentation (RFM Analysis Preparation).
    6. Payment Behavior (Credit Card Installments vs. Cash).
    7. Operational Efficiency (Seller Processing Time).
    8. Temporal Patterns (Weekend vs. Weekday Sales).

Usage:
    Execute individual queries to generate specific reports.
===============================================================================
*/

USE Olist_DW;
GO

-------------------------------------------------------------------------------
-- 1. Logistics Performance: Late Delivery Percentage by State
-- Business Goal: Identify regions with significant shipping delays to optimize carrier selection.
-------------------------------------------------------------------------------
SELECT 
    c.state AS customer_state,
    COUNT(s.order_id) AS total_orders,
    SUM(CASE WHEN s.delivered_customer_date > s.estimated_delivery_date THEN 1 ELSE 0 END) AS late_orders,
    FORMAT(
        SUM(CASE WHEN s.delivered_customer_date > s.estimated_delivery_date THEN 1.0 ELSE 0.0 END) 
        / NULLIF(COUNT(s.order_id), 0), 
        'P'
    ) AS late_percentage
FROM Gold.vw_fact_sales s
JOIN Gold.vw_dim_customers c ON s.customer_id = c.customer_id
WHERE s.status = 'delivered'
GROUP BY c.state
ORDER BY late_orders DESC;
GO

-------------------------------------------------------------------------------
-- 2. Financial Trends: Month-over-Month (MoM) Revenue Growth
-- Business Goal: Analyze growth momentum and identify seasonal peaks/troughs.
-------------------------------------------------------------------------------
WITH MonthlySales AS (
    SELECT 
        FORMAT(purchase_date, 'yyyy-MM') AS order_month,
        SUM(total_item_value) AS revenue
    FROM Gold.vw_fact_sales
    GROUP BY FORMAT(purchase_date, 'yyyy-MM')
)
SELECT 
    order_month,
    revenue,
    LAG(revenue) OVER (ORDER BY order_month) AS previous_month_revenue,
    FORMAT(
        (revenue - LAG(revenue) OVER (ORDER BY order_month)) 
        / NULLIF(LAG(revenue) OVER (ORDER BY order_month), 0),
        'P'
    ) AS growth_rate
FROM MonthlySales;
GO

-------------------------------------------------------------------------------
-- 3. Seller Analysis: Top 10 Sellers & Market Share (Pareto Principle)
-- Business Goal: Identify key partners who drive the majority of the platform's revenue.
-------------------------------------------------------------------------------
WITH SellerRevenue AS (
    SELECT 
        s.seller_id,
        SUM(f.total_item_value) AS total_revenue
    FROM Gold.vw_fact_sales f
    JOIN Gold.vw_dim_sellers s ON f.seller_id = s.seller_id
    GROUP BY s.seller_id
)
SELECT TOP 10
    seller_id,
    total_revenue,
    FORMAT(total_revenue / SUM(total_revenue) OVER(), 'P') AS market_share_percentage
FROM SellerRevenue
ORDER BY total_revenue DESC;
GO

-------------------------------------------------------------------------------
-- 4. Product Quality: Categories with Lowest Average Review Scores
-- Business Goal: Spot product categories that negatively impact customer satisfaction.
-------------------------------------------------------------------------------
SELECT TOP 10
    p.category,
    AVG(CAST(r.review_score AS DECIMAL(10,2))) AS avg_score,
    COUNT(r.review_id) AS number_of_reviews
FROM Gold.vw_fact_order_reviews r
JOIN Gold.vw_fact_sales s ON r.order_id = s.order_id
JOIN Gold.vw_dim_products p ON s.product_id = p.product_id
WHERE p.category IS NOT NULL
GROUP BY p.category
HAVING COUNT(r.review_id) > 100 -- Filter for statistical significance
ORDER BY avg_score ASC;
GO

-------------------------------------------------------------------------------
-- 5. Customer Segmentation: RFM (Recency, Frequency, Monetary) Base
-- Business Goal: Prepare data for marketing campaigns (targeting high-value or churning customers).
-------------------------------------------------------------------------------
SELECT TOP 20
    c.customer_unique_id,
    MAX(s.purchase_date) AS last_purchase_date, -- Recency
    COUNT(DISTINCT s.order_id) AS frequency,    -- Frequency
    SUM(s.total_item_value) AS monetary_value   -- Monetary
FROM Gold.vw_fact_sales s
JOIN Gold.vw_dim_customers c ON s.customer_id = c.customer_id
GROUP BY c.customer_unique_id
ORDER BY monetary_value DESC;
GO

-------------------------------------------------------------------------------
-- 6. Payment Behavior: Average Order Value by Payment Type
-- Business Goal: Understand how payment methods correlate with spending power.
-------------------------------------------------------------------------------
SELECT 
    payment_type,
    COUNT(DISTINCT order_id) as total_orders,
    AVG(installments) as avg_installments,
    AVG(total_item_value) as avg_order_value
FROM Gold.vw_fact_sales
WHERE payment_type IS NOT NULL
GROUP BY payment_type
ORDER BY avg_order_value DESC;
GO

-------------------------------------------------------------------------------
-- 7. Operational Efficiency: Average Seller Processing Time
-- Business Goal: Measure how long it takes for sellers to hand over goods to carriers after approval.
-- Insight: Faster processing leads to faster delivery.
-------------------------------------------------------------------------------
SELECT TOP 10
    p.category,
    AVG(DATEDIFF(day, s.approved_date, s.delivered_carrier_date)) AS avg_shipping_delay_days,
    COUNT(s.order_id) AS total_orders
FROM Gold.vw_fact_sales s
JOIN Gold.vw_dim_products p ON s.product_id = p.product_id
WHERE s.delivered_carrier_date IS NOT NULL 
  AND s.approved_date IS NOT NULL
GROUP BY p.category
HAVING COUNT(s.order_id) > 50
ORDER BY avg_shipping_delay_days DESC; -- Showing slowest categories first
GO

-------------------------------------------------------------------------------
-- 8. Temporal Patterns: Sales Distribution (Weekday vs. Weekend)
-- Business Goal: Optimize ad spend by identifying peak purchasing days.
-------------------------------------------------------------------------------
SELECT 
    DATENAME(WEEKDAY, purchase_date) AS day_of_week,
    COUNT(DISTINCT order_id) AS total_orders,
    SUM(total_item_value) AS total_revenue,
    -- Simple index to order days correctly (Monday=1, Sunday=7) or depends on SQL settings
    CASE DATENAME(WEEKDAY, purchase_date)
        WHEN 'Monday' THEN 1 WHEN 'Tuesday' THEN 2 WHEN 'Wednesday' THEN 3
        WHEN 'Thursday' THEN 4 WHEN 'Friday' THEN 5 WHEN 'Saturday' THEN 6 WHEN 'Sunday' THEN 7
    END AS day_index
FROM Gold.vw_fact_sales
GROUP BY DATENAME(WEEKDAY, purchase_date)
ORDER BY day_index;
GO
