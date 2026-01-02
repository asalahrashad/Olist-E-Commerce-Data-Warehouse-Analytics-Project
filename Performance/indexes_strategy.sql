/*
===============================================================================
Stored Procedure: Silver.sp_rebuild_indexes
===============================================================================
Description:
    This stored procedure implements a "Hybrid Indexing Strategy" for the Silver 
    Layer of the Olist Data Warehouse. It automates the maintenance of indexes 
    to ensure optimal performance for both ETL processes and Analytical Reporting (EDA).

    The procedure performs the following operations:
    1. cleanup: Dynamically identifies and drops all existing Primary Keys, 
       Unique Constraints, and Indexes (clustered/non-clustered) to ensure a clean slate.
    2. Dimension Table Indexing (Row-Store): Applies Clustered Indexes on Surrogate Keys 
       and Non-Clustered Indexes on Business Keys/Filter columns. This optimizes 
       Join performance and high-cardinality filtering.
    3. Fact Table Indexing (Column-Store): Applies Clustered Columnstore Indexes (CCI) 
       on Fact tables. This provides 10x data compression and high-performance 
       aggregations for analytical queries.

Usage:
    EXEC Silver.sp_rebuild_indexes;
===============================================================================
*/

USE Olist_DW;
GO

CREATE OR ALTER PROCEDURE Silver.sp_rebuild_indexes
AS
BEGIN
    SET NOCOUNT ON;
    
    -- Variables for Error Handling and Dynamic SQL Execution
    DECLARE @ErrorMessage NVARCHAR(4000);
    DECLARE @ErrorSeverity INT;
    DECLARE @ErrorState INT;
    DECLARE @sql NVARCHAR(MAX) = N'';
    DECLARE @StartTime DATETIME = GETDATE();

    BEGIN TRY
        PRINT '=========================================================';
        PRINT '>>> STARTING INDEX OPTIMIZATION PROCEDURE';
        PRINT '>>> 1. CLEANUP PHASE: Dropping all existing Keys & Indexes';
        PRINT '=========================================================';

        ---------------------------------------------------------------------------
        -- 1. Dynamically Drop all Constraints (Primary Keys / Unique Keys)
        ---------------------------------------------------------------------------
        -- We must use Dynamic SQL because PK names are system-generated and random.
        SELECT @sql += N'ALTER TABLE ' + QUOTENAME(CONSTRAINT_SCHEMA) + N'.' + QUOTENAME(TABLE_NAME) 
            + N' DROP CONSTRAINT ' + QUOTENAME(CONSTRAINT_NAME) + N'; '
        FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS
        WHERE CONSTRAINT_SCHEMA = 'Silver' 
          AND CONSTRAINT_TYPE IN ('PRIMARY KEY', 'UNIQUE');

        -- Execute the drop commands if any constraints exist
        IF @sql IS NOT NULL AND LEN(@sql) > 0
        BEGIN
            PRINT '   > Dropping Constraints (PKs/UKs)...';
            EXEC sp_executesql @sql;
        END
        
        -- Reset variable for the next step
        SET @sql = N'';

        ---------------------------------------------------------------------------
        -- 2. Drop all remaining Indexes (Non-Clustered / Columnstore)
        ---------------------------------------------------------------------------
        -- Query sys.indexes to find any index that isn't a constraint and drop it.
        SELECT @sql += N'DROP INDEX ' + QUOTENAME(name) + N' ON ' 
            + QUOTENAME(SCHEMA_NAME(schema_id)) + N'.' + QUOTENAME(OBJECT_NAME(object_id)) + N'; '
        FROM sys.indexes
        WHERE is_primary_key = 0      -- Already dropped in step 1
          AND is_unique_constraint = 0
          AND type_desc <> 'HEAP'     -- Do not drop the table itself
          AND SCHEMA_NAME(schema_id) = 'Silver';

        IF @sql IS NOT NULL AND LEN(@sql) > 0
        BEGIN
            PRINT '   > Dropping Remaining Indexes...';
            EXEC sp_executesql @sql;
        END

        PRINT '>>> CLEANUP COMPLETED. All Silver tables are now HEAPS.';
        PRINT '---------------------------------------------------------';
        PRINT '>>> 2. BUILDING PHASE: Applying Hybrid Strategy';
        PRINT '---------------------------------------------------------';

        ---------------------------------------------------------------------------
        -- 3. Indexing DIMENSION Tables (Row-Store Strategy)
        -- Purpose: Optimize lookup performance for Joins and Filters.
        ---------------------------------------------------------------------------
        PRINT '   > Indexing Dimensions (Row-Store)...';

        -- [Products]: Indexing for SK, BK, and Category filtering
        CREATE CLUSTERED INDEX CX_products_sk ON Silver.olist_products(product_sk);
        CREATE NONCLUSTERED INDEX IX_products_bk ON Silver.olist_products(product_id);
        CREATE NONCLUSTERED INDEX IX_products_category ON Silver.olist_products(product_category_name);

        -- [Customers]: Indexing for SK, BK, and Location (State/City) analysis
        CREATE CLUSTERED INDEX CX_customers_sk ON Silver.olist_customers(customer_sk);
        CREATE NONCLUSTERED INDEX IX_customers_bk ON Silver.olist_customers(customer_id);
        CREATE NONCLUSTERED INDEX IX_customers_location ON Silver.olist_customers(customer_state) INCLUDE (customer_city);

        -- [Sellers]: Indexing for SK, BK, and State distribution
        CREATE CLUSTERED INDEX CX_sellers_sk ON Silver.olist_sellers(seller_sk);
        CREATE NONCLUSTERED INDEX IX_sellers_bk ON Silver.olist_sellers(seller_id);
        CREATE NONCLUSTERED INDEX IX_sellers_state ON Silver.olist_sellers(seller_state);

        -- [Geolocation]: Indexing for SK and Zip Code (Primary Join Key)
        CREATE CLUSTERED INDEX CX_geo_sk ON Silver.olist_geolocation(geo_sk);
        CREATE NONCLUSTERED INDEX IX_geo_zip ON Silver.olist_geolocation(geolocation_zip_code_prefix);

        ---------------------------------------------------------------------------
        -- 4. Indexing FACT Tables (Column-Store Strategy)
        -- Purpose: Optimize Aggregations (SUM/AVG) and Compression for Reporting.
        ---------------------------------------------------------------------------
        PRINT '   > Indexing Facts (Column-Store)...';

        -- [Orders]: Optimized for Time-Series analysis and Status checking
        CREATE CLUSTERED COLUMNSTORE INDEX CCI_olist_orders ON Silver.olist_orders;

        -- [Order Items]: The largest table. CCI is critical here for performance.
        CREATE CLUSTERED COLUMNSTORE INDEX CCI_olist_order_items ON Silver.olist_order_items;

        -- [Payments]: Optimized for financial calculations
        CREATE CLUSTERED COLUMNSTORE INDEX CCI_olist_order_payments ON Silver.olist_order_payments;

        -- [Reviews]: Optimized for text storage and score analysis
        CREATE CLUSTERED COLUMNSTORE INDEX CCI_olist_order_reviews ON Silver.olist_order_reviews;

        PRINT '=========================================================';
        PRINT '>>> SUCCESS: All Indexes Rebuilt.';
        PRINT '>>> Duration: ' + CAST(DATEDIFF(SECOND, @StartTime, GETDATE()) AS NVARCHAR) + ' seconds.';
        PRINT '=========================================================';

    END TRY
    BEGIN CATCH
        -- Capture Error Details
        SELECT 
            @ErrorMessage = ERROR_MESSAGE(),
            @ErrorSeverity = ERROR_SEVERITY(),
            @ErrorState = ERROR_STATE();

        PRINT '!!! ERROR OCCURRED !!!';
        PRINT 'Error Msg: ' + @ErrorMessage;
        
        -- Raise the error again to ensure it is logged by the calling application/agent
        RAISERROR (@ErrorMessage, @ErrorSeverity, @ErrorState);
    END CATCH
END;
GO
