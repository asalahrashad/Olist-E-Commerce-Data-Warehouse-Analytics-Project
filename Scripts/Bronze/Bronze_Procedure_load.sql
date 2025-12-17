/*
## 🔄 2. Bronze Layer Data Ingestion (Stored Procedure)

This script creates the stored procedure `Bronze.load_bronze`, which acts as the primary orchestration engine for ingesting raw data from local CSV files into the SQL Server Bronze tables.

### 📝 Script Overview
- **File:** `2_Load_Bronze_Data.sql`
- **Procedure Name:** `Bronze.load_bronze`
- **Operation Type:** Full Load (Truncate & Load)

### 🚀 Key Technical Features
* **High-Performance Bulk Loading:** Utilizes the `BULK INSERT` command combined with `TABLOCK` to minimize transaction logging and maximize insertion speed for large datasets.
* **Execution Monitoring:** Tracks and prints the duration of each individual table load as well as the total batch execution time, providing immediate feedback on performance bottlenecks.
* **Robust Error Handling:** Implements a `TRY...CATCH` block to gracefully handle runtime errors. If a failure occurs, it captures and reports the specific Error Number, State, and Message for easier debugging.
* **Cross-Platform Compatibility:** Configured with `ROWTERMINATOR = '0x0a'` to correctly parse CSV files generated in Linux/Unix environments (common in Kaggle datasets).
* **Data Freshness:** Performs a `TRUNCATE` operation before insertion to ensure the Bronze layer always reflects the exact state of the source files without duplication.

### 📋 Ingestion Workflow
The procedure iterates through the following 9 datasets:
1. `product_category_name_translation`
2. `olist_customers_dataset`
3. `olist_orders_dataset`
4. `olist_order_items_dataset`
5. `olist_products_dataset`
6. `olist_order_payments_dataset`
7. `olist_order_reviews_dataset`
8. `olist_sellers_dataset`
9. `olist_geolocation_dataset`

### ⚠️ Configuration Note
The script currently uses hardcoded file paths (e.g., `D:\Data Engineering Projects\...`). Ensure these paths are updated to match your local environment or container volume mounts before execution.

Usage Example:
    EXEC Bronze.load_bronze;
*/


CREATE OR ALTER PROCEDURE Bronze.load_bronze AS
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME; 
	BEGIN TRY
		SET @batch_start_time = GETDATE();
		PRINT '================================================';
		PRINT 'Loading Bronze Layer (Olist Dataset)';
		PRINT '================================================';

		PRINT '------------------------------------------------';
		PRINT 'Loading E-Commerce Tables';
		PRINT '------------------------------------------------';

        -- 1. Table: product_category_name_translation
		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: Bronze.product_category_name_translation';
		TRUNCATE TABLE Bronze.product_category_name_translation;
		PRINT '>> Inserting Data Into: Bronze.product_category_name_translation';
		BULK INSERT Bronze.product_category_name_translation
		FROM 'D:\Data Engineering Projects\Olist WareHouse & Dashbourd\Datasete\product_category_name_translation.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			ROWTERMINATOR = '0x0a', -- Fix for Linux/Kaggle style line endings
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -------------';

        -- 2. Table: olist_customers_dataset
        SET @start_time = GETDATE();
		PRINT '>> Truncating Table: Bronze.olist_customers';
		TRUNCATE TABLE Bronze.olist_customers;
		PRINT '>> Inserting Data Into: Bronze.olist_customers';
		BULK INSERT Bronze.olist_customers
		FROM 'D:\Data Engineering Projects\Olist WareHouse & Dashbourd\Datasete\olist_customers_dataset.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			ROWTERMINATOR = '0x0a',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -------------';

        -- 3. Table: olist_orders_dataset
        SET @start_time = GETDATE();
		PRINT '>> Truncating Table: Bronze.olist_orders';
		TRUNCATE TABLE Bronze.olist_orders;
		PRINT '>> Inserting Data Into: Bronze.olist_orders';
		BULK INSERT Bronze.olist_orders
		FROM 'D:\Data Engineering Projects\Olist WareHouse & Dashbourd\Datasete\olist_orders_dataset.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			ROWTERMINATOR = '0x0a',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -------------';

        -- 4. Table: olist_order_items_dataset
        SET @start_time = GETDATE();
		PRINT '>> Truncating Table: Bronze.olist_order_items';
		TRUNCATE TABLE Bronze.olist_order_items;
		PRINT '>> Inserting Data Into: Bronze.olist_order_items';
		BULK INSERT Bronze.olist_order_items
		FROM 'D:\Data Engineering Projects\Olist WareHouse & Dashbourd\Datasete\olist_order_items_dataset.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			ROWTERMINATOR = '0x0a',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -------------';

        -- 5. Table: olist_products
        SET @start_time = GETDATE();
		PRINT '>> Truncating Table: Bronze.olist_products';
		TRUNCATE TABLE Bronze.olist_products;
		PRINT '>> Inserting Data Into: Bronze.olist_products';
		BULK INSERT Bronze.olist_products
		FROM 'D:\Data Engineering Projects\Olist WareHouse & Dashbourd\Datasete\olist_products_dataset.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			ROWTERMINATOR = '0x0a',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -------------';

        -- 6. Table: olist_order_payments
        SET @start_time = GETDATE();
		PRINT '>> Truncating Table: Bronze.olist_order_payments';
		TRUNCATE TABLE Bronze.olist_order_payments;
		PRINT '>> Inserting Data Into: Bronze.olist_order_payments';
		BULK INSERT Bronze.olist_order_payments
		FROM 'D:\Data Engineering Projects\Olist WareHouse & Dashbourd\Datasete\olist_order_payments_dataset.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			ROWTERMINATOR = '0x0a',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -------------';

        -- 7. Table: olist_order_reviews_dataset
        SET @start_time = GETDATE();
		PRINT '>> Truncating Table: Bronze.olist_order_reviews';
		TRUNCATE TABLE Bronze.olist_order_reviews;
		PRINT '>> Inserting Data Into: Bronze.olist_order_reviews';
		BULK INSERT Bronze.olist_order_reviews
		FROM 'D:\Data Engineering Projects\Olist WareHouse & Dashbourd\Datasete\olist_order_reviews_dataset.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			ROWTERMINATOR = '0x0a',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -------------';

        -- 8. Table: olist_sellers_dataset
        SET @start_time = GETDATE();
		PRINT '>> Truncating Table: Bronze.olist_sellers';
		TRUNCATE TABLE Bronze.olist_sellers;
		PRINT '>> Inserting Data Into: Bronze.olist_sellers';
		BULK INSERT Bronze.olist_sellers
		FROM 'D:\Data Engineering Projects\Olist WareHouse & Dashbourd\Datasete\olist_sellers_dataset.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			ROWTERMINATOR = '0x0a',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -------------';

        -- 9. Table: olist_geolocation_dataset
        SET @start_time = GETDATE();
		PRINT '>> Truncating Table: Bronze.olist_geolocation';
		TRUNCATE TABLE Bronze.olist_geolocation;
		PRINT '>> Inserting Data Into: Bronze.olist_geolocation';
		BULK INSERT Bronze.olist_geolocation
		FROM 'D:\Data Engineering Projects\Olist WareHouse & Dashbourd\Datasete\olist_geolocation_dataset.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			ROWTERMINATOR = '0x0a',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -------------';

		SET @batch_end_time = GETDATE();
		PRINT '=========================================='
		PRINT 'Loading Bronze Layer is Completed';
        PRINT '   - Total Load Duration: ' + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds';
		PRINT '=========================================='
	END TRY
	BEGIN CATCH
		PRINT '=========================================='
		PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER'
		PRINT 'Error Message: ' + ERROR_MESSAGE();
		PRINT 'Error Number: ' + CAST (ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Error State: ' + CAST (ERROR_STATE() AS NVARCHAR);
		PRINT '=========================================='
	END CATCH
END
