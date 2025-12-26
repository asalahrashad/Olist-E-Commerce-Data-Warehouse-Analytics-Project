/*
===============================================================================
Stored Procedure: Load Silver Layer (Olist Data Warehouse)
===============================================================================
Script Purpose:
    This stored procedure performs the ETL (Extract, Transform, Load) process.
    It loads data from the 'Silver Views' (which contain cleaning logic) 
    into the 'Silver Physical Tables' (which contain Surrogate Keys).

    Execution Flow:
    1. Truncate All Silver Tables (Reset Data & Identity Keys).
    2. Load Lookup & Dimension Tables (Generate new SKs).
    3. Load Fact Tables (Perform Lookups to retrieve SKs).

Parameters: None.
Usage Example: EXEC Silver.load_silver;
===============================================================================
*/

CREATE OR ALTER PROCEDURE Silver.load_silver AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME; 
    
    BEGIN TRY
        SET @batch_start_time = GETDATE();
        PRINT '================================================';
        PRINT ' LOADING SILVER LAYER (ETL START) ';
        PRINT '================================================';

        ---------------------------------------------------------------------------
        -- PHASE 1: LOADING DIMENSION TABLES (Independent Tables)
        ---------------------------------------------------------------------------
        PRINT '------------------------------------------------';
        PRINT ' Phase 1: Loading Dimensions & Lookups';
        PRINT '------------------------------------------------';

        -- 1. Load Category Translation Lookup
        SET @start_time = GETDATE();
        PRINT '>> Truncating & Loading: Silver.product_category_name_translation';
        TRUNCATE TABLE Silver.product_category_name_translation;
        
        INSERT INTO Silver.product_category_name_translation (
            product_category_name, 
            product_category_name_english
        )
        SELECT 
            product_category_name, 
            product_category_name_english
        FROM Silver.vw_translation;
        
        PRINT '>> Loaded Rows: ' + CAST(@@ROWCOUNT AS NVARCHAR);
        SET @end_time = GETDATE();
        PRINT '>> Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';

        -- 2. Load Geolocation Dimension
        SET @start_time = GETDATE();
        PRINT '>> Truncating & Loading: Silvergeolocation';
        TRUNCATE TABLE Silver.geolocation;
        
        INSERT INTO Silver.geolocation (
            geolocation_zip_code_prefix, 
            geolocation_lat, 
            geolocation_lng, 
            geolocation_city, 
            geolocation_state
        )
        SELECT 
            geolocation_zip_code_prefix, 
            geolocation_lat, 
            geolocation_lng, 
            geolocation_city, 
            geolocation_state
        FROM Silver.vw_geolocation;

        PRINT '>> Loaded Rows: ' + CAST(@@ROWCOUNT AS NVARCHAR);
        SET @end_time = GETDATE();
        PRINT '>> Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';

        -- 3. Load Sellers Dimension
        SET @start_time = GETDATE();
        PRINT '>> Truncating & Loading: Silver.sellers';
        TRUNCATE TABLE Silver.sellers;
        
        INSERT INTO Silver.sellers (
            seller_id, 
            seller_zip_code_prefix, 
            seller_city, 
            seller_state
        )
        SELECT 
            seller_id, 
            seller_zip_code_prefix, 
            seller_city, 
            seller_state
        FROM Silver.vw_sellers;

        PRINT '>> Loaded Rows: ' + CAST(@@ROWCOUNT AS NVARCHAR);
        SET @end_time = GETDATE();
        PRINT '>> Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';

        -- 4. Load Products Dimension
        SET @start_time = GETDATE();
        PRINT '>> Truncating & Loading: Silver.products';
        TRUNCATE TABLE Silver.products;
        
        INSERT INTO Silver.products (
            product_id, 
            product_category_name, 
            product_name_lenght, 
            product_description_lenght, 
            product_photos_qty, 
            product_weight_g, 
            product_length_cm, 
            product_height_cm, 
            product_width_cm
        )
        SELECT 
            product_id, 
            product_category_name, 
            product_name_lenght, 
            product_description_lenght, 
            product_photos_qty, 
            product_weight_g, 
            product_length_cm, 
            product_height_cm, 
            product_width_cm
        FROM Silver.vw_products;

        PRINT '>> Loaded Rows: ' + CAST(@@ROWCOUNT AS NVARCHAR);
        SET @end_time = GETDATE();
        PRINT '>> Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';

        -- 5. Load Customers Dimension
        SET @start_time = GETDATE();
        PRINT '>> Truncating & Loading: Silver.customers';
        TRUNCATE TABLE Silver.customers;
        
        INSERT INTO Silver.customers (
            customer_id, 
            customer_unique_id, 
            customer_zip_code_prefix, 
            customer_city, 
            customer_state
        )
        SELECT 
            customer_id, 
            customer_unique_id, 
            customer_zip_code_prefix, 
            customer_city, 
            customer_state
        FROM Silver.vw_customers;

        PRINT '>> Loaded Rows: ' + CAST(@@ROWCOUNT AS NVARCHAR);
        SET @end_time = GETDATE();
        PRINT '>> Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';


        ---------------------------------------------------------------------------
        -- PHASE 2: LOADING FACT TABLES (Dependent on Dimensions for SKs)
        ---------------------------------------------------------------------------
        PRINT '------------------------------------------------';
        PRINT ' Phase 2: Loading Facts (with SK Lookups)';
        PRINT '------------------------------------------------';

        -- 6. Load Orders Fact
        SET @start_time = GETDATE();
        PRINT '>> Truncating & Loading: Silver.orders';
        TRUNCATE TABLE Silver.orders;
        
        INSERT INTO Silver.orders (
            order_id, 
            customer_sk, -- LOOKUP COLUMN
            order_status, 
            order_purchase_timestamp, 
            order_approved_at, 
            order_delivered_carrier_date, 
            order_delivered_customer_date, 
            order_estimated_delivery_date
        )
        SELECT 
            src.order_id,
            ISNULL(cust.customer_sk, -1) AS customer_sk, -- Retrieve Generated SK
            src.order_status,
            src.order_purchase_timestamp,
            src.order_approved_at,
            src.order_delivered_carrier_date,
            src.order_delivered_customer_date,
            src.order_estimated_delivery_date
        FROM Silver.vw_orders src
        LEFT JOIN Silver.customers cust 
            ON src.customer_id = cust.customer_id; -- Join on Business Key

        PRINT '>> Loaded Rows: ' + CAST(@@ROWCOUNT AS NVARCHAR);
        SET @end_time = GETDATE();
        PRINT '>> Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';

        -- 7. Load Order Items Fact
        SET @start_time = GETDATE();
        PRINT '>> Truncating & Loading: Silver.order_items';
        TRUNCATE TABLE Silver.order_items;
        
        INSERT INTO Silver.order_items (
            order_id, 
            order_item_id, 
            product_sk, -- LOOKUP COLUMN
            seller_sk,  -- LOOKUP COLUMN
            shipping_limit_date, 
            price, 
            freight_value
        )
        SELECT 
            src.order_id,
            src.order_item_id,
            ISNULL(p.product_sk, -1) AS product_sk, -- Retrieve Product SK
            ISNULL(s.seller_sk, -1) AS seller_sk,   -- Retrieve Seller SK
            src.shipping_limit_date,
            src.price,
            src.freight_value
        FROM Silver.vw_order_items src
        LEFT JOIN Silver.products p 
            ON src.product_id = p.product_id -- Join on Business Key
        LEFT JOIN Silver.sellers s 
            ON src.seller_id = s.seller_id;  -- Join on Business Key

        PRINT '>> Loaded Rows: ' + CAST(@@ROWCOUNT AS NVARCHAR);
        SET @end_time = GETDATE();
        PRINT '>> Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';

        -- 8. Load Order Payments Fact
        SET @start_time = GETDATE();
        PRINT '>> Truncating & Loading: Silver.order_payments';
        TRUNCATE TABLE Silver.order_payments;
        
        INSERT INTO Silver.order_payments (
            order_id, 
            payment_sequential, 
            payment_type, 
            payment_installments, 
            payment_value
        )
        SELECT 
            order_id, 
            payment_sequential, 
            payment_type, 
            payment_installments, 
            payment_value
        FROM Silver.vw_order_payments;

        PRINT '>> Loaded Rows: ' + CAST(@@ROWCOUNT AS NVARCHAR);
        SET @end_time = GETDATE();
        PRINT '>> Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';

        -- 9. Load Order Reviews Fact
        SET @start_time = GETDATE();
        PRINT '>> Truncating & Loading: Silver.order_reviews';
        TRUNCATE TABLE Silver.order_reviews;
        
        INSERT INTO Silver.order_reviews (
            review_id, 
            order_id, 
            review_score, 
            review_comment_title, 
            review_comment_message, 
            review_creation_date, 
            review_answer_timestamp
        )
        SELECT 
            review_id, 
            order_id, 
            review_score, 
            review_comment_title, 
            review_comment_message, 
            review_creation_date, 
            review_answer_timestamp
        FROM Silver.vw_reviews;

        PRINT '>> Loaded Rows: ' + CAST(@@ROWCOUNT AS NVARCHAR);
        SET @end_time = GETDATE();
        PRINT '>> Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';


        ---------------------------------------------------------------------------
        -- COMPLETION
        ---------------------------------------------------------------------------
        SET @batch_end_time = GETDATE();
        PRINT '================================================';
        PRINT ' SILVER LAYER LOADING COMPLETED ';
        PRINT ' Total Duration: ' + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds';
        PRINT '================================================';

    END TRY
    BEGIN CATCH
        PRINT '================================================';
        PRINT ' ERROR OCCURED DURING LOADING SILVER LAYER ';
        PRINT ' Error Message: ' + ERROR_MESSAGE();
        PRINT ' Error Number: ' + CAST(ERROR_NUMBER() AS NVARCHAR);
        PRINT ' Error State: ' + CAST(ERROR_STATE() AS NVARCHAR);
        PRINT '================================================';
        -- Optional: Throw error to fail the job if running in SQL Agent
        THROW;
    END CATCH
END
GO
