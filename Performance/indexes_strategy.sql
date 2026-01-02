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
        SELECT @sql += N'ALTER TABLE ' + QUOTENAME(CONSTRAINT_SCHEMA) + N'.' + QUOTENAME(TABLE_NAME) 
            + N' DROP CONSTRAINT ' + QUOTENAME(CONSTRAINT_NAME) + N'; '
        FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS
        WHERE CONSTRAINT_SCHEMA = 'Silver' 
          AND CONSTRAINT_TYPE IN ('PRIMARY KEY', 'UNIQUE');

        IF @sql IS NOT NULL AND LEN(@sql) > 0
        BEGIN
            PRINT '   > Dropping Constraints (PKs/UKs)...';
            EXEC sp_executesql @sql;
        END
        
        SET @sql = N'';

        ---------------------------------------------------------------------------
        -- 2. Drop all remaining Indexes (Non-Clustered / Columnstore) [FIXED HERE]
        ---------------------------------------------------------------------------
        -- Correction: Joined sys.indexes with sys.tables and sys.schemas to get schema_id correctly
        SELECT @sql += N'DROP INDEX ' + QUOTENAME(i.name) + N' ON ' 
            + QUOTENAME(s.name) + N'.' + QUOTENAME(t.name) + N'; '
        FROM sys.indexes i
        INNER JOIN sys.tables t ON i.object_id = t.object_id
        INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
        WHERE i.is_primary_key = 0      
          AND i.is_unique_constraint = 0
          AND i.type_desc <> 'HEAP'     
          AND s.name = 'Silver';        -- Filter by Schema Name properly

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
        ---------------------------------------------------------------------------
        PRINT '   > Indexing Dimensions (Row-Store)...';

        -- [Products]
        CREATE CLUSTERED INDEX CX_products_sk ON Silver.products(product_sk);
        CREATE NONCLUSTERED INDEX IX_products_bk ON Silver.products(product_id);
        CREATE NONCLUSTERED INDEX IX_products_category ON Silver.products(product_category_name);

        -- [Customers]
        CREATE CLUSTERED INDEX CX_customers_sk ON Silver.customers(customer_sk);
        CREATE NONCLUSTERED INDEX IX_customers_bk ON Silver.customers(customer_id);
        CREATE NONCLUSTERED INDEX IX_customers_location ON Silver.customers(customer_state) INCLUDE (customer_city);

        -- [Sellers]
        CREATE CLUSTERED INDEX CX_sellers_sk ON Silver.sellers(seller_sk);
        CREATE NONCLUSTERED INDEX IX_sellers_bk ON Silver.sellers(seller_id);
        CREATE NONCLUSTERED INDEX IX_sellers_state ON Silver.sellers(seller_state);

        -- [Geolocation]
        CREATE CLUSTERED INDEX CX_geo_sk ON Silver.geolocation(geo_sk);
        CREATE NONCLUSTERED INDEX IX_geo_zip ON Silver.geolocation(geolocation_zip_code_prefix);

        ---------------------------------------------------------------------------
        -- 4. Indexing FACT Tables (Column-Store Strategy)
        ---------------------------------------------------------------------------
        PRINT '   > Indexing Facts (Column-Store)...';

        -- [Orders]
        CREATE CLUSTERED COLUMNSTORE INDEX CCI_orders ON Silver.orders;

        -- [Order Items]
        CREATE CLUSTERED COLUMNSTORE INDEX CCI_order_items ON Silver.order_items;

        -- [Payments]
        CREATE CLUSTERED COLUMNSTORE INDEX CCI_order_payments ON Silver.order_payments;

        -- [Reviews]
        CREATE CLUSTERED COLUMNSTORE INDEX CCI_order_reviews ON Silver.order_reviews;

        PRINT '=========================================================';
        PRINT '>>> SUCCESS: All Indexes Rebuilt.';
        PRINT '>>> Duration: ' + CAST(DATEDIFF(SECOND, @StartTime, GETDATE()) AS NVARCHAR) + ' seconds.';
        PRINT '=========================================================';

    END TRY
    BEGIN CATCH
        SELECT 
            @ErrorMessage = ERROR_MESSAGE(),
            @ErrorSeverity = ERROR_SEVERITY(),
            @ErrorState = ERROR_STATE();

        PRINT '!!! ERROR OCCURRED !!!';
        PRINT 'Error Msg: ' + @ErrorMessage;
        
        RAISERROR (@ErrorMessage, @ErrorSeverity, @ErrorState);
    END CATCH
END;
GO
