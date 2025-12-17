/*
# Olist Data Warehouse Automation Utility 🛠️

A robust, environment-aware SQL Server Stored Procedure designed to automate the maintenance, cleaning, and preparation of the **Olist Data Warehouse** layers (Bronze, Silver, and Gold). 

This utility acts as a centralized control unit for Data Engineers to manage the ETL lifecycle safely across different environments (DEV, TEST, PROD).

## 🚀 Key Features

* **🌍 Environment Awareness:** Automatically detects the server environment (`DEV`, `TEST`, `PROD`) and adjusts behavior accordingly.
* **🛡️ Defensive Programming (Safety First):** Implements strict safety guardrails. Destructive operations in `PROD` (like resetting the Gold layer) are blocked unless explicitly authorized via a `@Force` flag.
* **🔄 Dynamic SQL & Metadata Driven:** Utilizes `INFORMATION_SCHEMA` and `Cursors` to dynamically identify and clean tables. The script does not require modification when new tables are added to the schema.
* **📝 Audit Logging:** Every operation (Success or Failure) is logged into `DW_Operations_Log` with timestamps, the user identity (`SYSTEM_USER`), and execution details.
* **⚡ Optimized Performance:**
    * Uses `TRUNCATE` for the **Bronze** layer (fast reset for raw data).
    * Uses `DELETE` for **Silver/Gold** layers (preserves table constraints and relationships).

## 🏗️ Architecture

The solution follows a modular design pattern:
1.  **Context Detection:** Identifies the user and environment.
2.  **Validation:** Checks permissions and safety flags.
3.  **Orchestration:** Iterates through the requested schema (Bronze, Silver, or Gold).
4.  **Execution:** Generates and executes Dynamic SQL commands.
5.  **Logging:** Records the transaction history.

## 💻 Usage

To reset the **Bronze Layer** (Safe Mode):
```sql
EXEC dbo.Manage_Olist_DW 
    @ResetBronze = 1;
    */

  USE master;
GO

CREATE OR ALTER PROCEDURE dbo.Manage_Olist_DW
(
    @ResetBronze BIT = 0,
    @ResetSilver BIT = 0,
    @ResetGold   BIT = 0,
    @Force       BIT = 0
)
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE 
        @Env VARCHAR(10),
        @DbName SYSNAME = 'Olist_DW',
        @TableName NVARCHAR(300),
        @Sql NVARCHAR(MAX);

    /* =============================================
       Auto-Detect User 
    ============================================= */
    -- Automatically capture the current system user
    DECLARE @WhoDidIt NVARCHAR(100) = SYSTEM_USER; 

    /* =============================================
       1️⃣ Detect Environment
    ============================================= */
    IF @@SERVERNAME LIKE '%DEV%' SET @Env = 'DEV';
    ELSE IF @@SERVERNAME LIKE '%TEST%' OR @@SERVERNAME LIKE '%UAT%' SET @Env = 'TEST';
    ELSE IF @@SERVERNAME LIKE '%PROD%' SET @Env = 'PROD';
    ELSE SET @Env = 'DEV'; -- Default safe mode

    /* =============================================
       2️⃣ Create Database (If needed)
    ============================================= */
    IF NOT EXISTS (SELECT 1 FROM sys.databases WHERE name = @DbName)
    BEGIN
        PRINT 'Creating Database...';
        SET @Sql = N'CREATE DATABASE ' + QUOTENAME(@DbName);
        EXEC sp_executesql @Sql;
    END;

    /* =============================================
       🆕 Create Log Table (For auditing)
    ============================================= */
    -- Create the log table inside Olist_DW to track operations
    SET @Sql = N'USE ' + QUOTENAME(@DbName) + N';
    IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = ''DW_Operations_Log'')
    BEGIN
        CREATE TABLE dbo.DW_Operations_Log (
            LogID INT IDENTITY(1,1) PRIMARY KEY,
            LogDate DATETIME DEFAULT GETDATE(),
            Environment VARCHAR(10),
            OperationType NVARCHAR(50),
            TargetLayer NVARCHAR(50),
            ExecutedBy NVARCHAR(100), -- This column stores the @WhoDidIt value
            Details NVARCHAR(MAX)
        );
    END';
    EXEC sp_executesql @Sql;

    /* =============================================
       3️⃣ Create Schemas
    ============================================= */
    DECLARE @Schemas TABLE (SchemaName SYSNAME);
    INSERT INTO @Schemas(SchemaName) VALUES ('Bronze'), ('Silver'), ('Gold');
    
    DECLARE @s SYSNAME;
    DECLARE SchemaCursor CURSOR LOCAL FAST_FORWARD FOR SELECT SchemaName FROM @Schemas;
    
    OPEN SchemaCursor;
    FETCH NEXT FROM SchemaCursor INTO @s;
    WHILE @@FETCH_STATUS = 0
    BEGIN
        SET @Sql = N'USE ' + QUOTENAME(@DbName) + N'; 
                     IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = @s)
                         EXEC(N''CREATE SCHEMA ' + QUOTENAME(@s) + N''');';
        EXEC sp_executesql @Sql, N'@s SYSNAME', @s = @s;
        FETCH NEXT FROM SchemaCursor INTO @s;
    END
    CLOSE SchemaCursor; DEALLOCATE SchemaCursor;

    /* =============================================
       4️⃣ Safety Rules
    ============================================= */
    IF @Env = 'PROD' AND @ResetGold = 1 AND @Force = 0
    BEGIN
        RAISERROR ('Gold reset blocked in PROD without FORCE. User: %s', 16, 1, @WhoDidIt);
        RETURN;
    END;

    IF @Env = 'PROD' AND @ResetSilver = 1 AND @Force = 0
    BEGIN
        RAISERROR ('Silver reset requires FORCE in PROD. User: %s', 16, 1, @WhoDidIt);
        RETURN;
    END;

    /* =============================================
       5️⃣ Reset Logic & Logging
    ============================================= */
    BEGIN TRY
        
        /* -------- Bronze -------- */
        IF @ResetBronze = 1
        BEGIN
            -- 1. Clean/Truncate Data
            SET @Sql = N'
            DECLARE c CURSOR LOCAL FAST_FORWARD FOR 
            SELECT QUOTENAME(TABLE_SCHEMA) + ''.'' + QUOTENAME(TABLE_NAME)
            FROM ' + QUOTENAME(@DbName) + N'.INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = ''Bronze'';
            DECLARE @T NVARCHAR(300); OPEN c; FETCH NEXT FROM c INTO @T;
            WHILE @@FETCH_STATUS = 0 BEGIN EXEC(N''TRUNCATE TABLE ' + QUOTENAME(@DbName) + N'.'' + @T); FETCH NEXT FROM c INTO @T; END
            CLOSE c; DEALLOCATE c;';
            EXEC sp_executesql @Sql;

            -- 2. Log operation (Using @WhoDidIt variable)
            SET @Sql = N'INSERT INTO ' + QUOTENAME(@DbName) + N'.dbo.DW_Operations_Log 
                         (Environment, OperationType, TargetLayer, ExecutedBy, Details)
                         VALUES (@Env, ''RESET'', ''Bronze'', @User, ''Truncated all tables in Bronze layer'');';
            
            EXEC sp_executesql @Sql, N'@Env VARCHAR(10), @User NVARCHAR(100)', @Env=@Env, @User=@WhoDidIt;
            
            PRINT 'Bronze Reset & Logged by: ' + @WhoDidIt;
        END;

        /* -------- Silver -------- */
        IF @ResetSilver = 1
        BEGIN
            -- 1. Clean/Delete Data
            SET @Sql = N'
            DECLARE c CURSOR LOCAL FAST_FORWARD FOR 
            SELECT QUOTENAME(TABLE_SCHEMA) + ''.'' + QUOTENAME(TABLE_NAME)
            FROM ' + QUOTENAME(@DbName) + N'.INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = ''Silver'';
            DECLARE @T NVARCHAR(300); OPEN c; FETCH NEXT FROM c INTO @T;
            WHILE @@FETCH_STATUS = 0 BEGIN EXEC(N''DELETE FROM ' + QUOTENAME(@DbName) + N'.'' + @T); FETCH NEXT FROM c INTO @T; END
            CLOSE c; DEALLOCATE c;';
            EXEC sp_executesql @Sql;

             -- 2. Log operation
            SET @Sql = N'INSERT INTO ' + QUOTENAME(@DbName) + N'.dbo.DW_Operations_Log 
                         (Environment, OperationType, TargetLayer, ExecutedBy, Details)
                         VALUES (@Env, ''RESET'', ''Silver'', @User, ''Deleted data from Silver layer'');';
            
            EXEC sp_executesql @Sql, N'@Env VARCHAR(10), @User NVARCHAR(100)', @Env=@Env, @User=@WhoDidIt;

            PRINT 'Silver Reset & Logged by: ' + @WhoDidIt;
        END;

        /* -------- Gold -------- */
        IF @ResetGold = 1
        BEGIN
            -- 1. Clean/Delete Data
            SET @Sql = N'
            DECLARE c CURSOR LOCAL FAST_FORWARD FOR 
            SELECT QUOTENAME(TABLE_SCHEMA) + ''.'' + QUOTENAME(TABLE_NAME)
            FROM ' + QUOTENAME(@DbName) + N'.INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = ''Gold'';
            DECLARE @T NVARCHAR(300); OPEN c; FETCH NEXT FROM c INTO @T;
            WHILE @@FETCH_STATUS = 0 BEGIN EXEC(N''DELETE FROM ' + QUOTENAME(@DbName) + N'.'' + @T); FETCH NEXT FROM c INTO @T; END
            CLOSE c; DEALLOCATE c;';
            EXEC sp_executesql @Sql;

             -- 2. Log operation
            SET @Sql = N'INSERT INTO ' + QUOTENAME(@DbName) + N'.dbo.DW_Operations_Log 
                         (Environment, OperationType, TargetLayer, ExecutedBy, Details)
                         VALUES (@Env, ''RESET'', ''Gold'', @User, ''Deleted data from Gold layer'');';
            
            EXEC sp_executesql @Sql, N'@Env VARCHAR(10), @User NVARCHAR(100)', @Env=@Env, @User=@WhoDidIt;

            PRINT 'Gold Reset & Logged by: ' + @WhoDidIt;
        END;

    END TRY
    BEGIN CATCH
        -- Log the error details into the table
        DECLARE @ErrMsg NVARCHAR(MAX) = ERROR_MESSAGE();
        
        -- Attempt to log the error (Using Dynamic SQL to ensure correct context)
        SET @Sql = N'INSERT INTO ' + QUOTENAME(@DbName) + N'.dbo.DW_Operations_Log 
                     (Environment, OperationType, TargetLayer, ExecutedBy, Details)
                     VALUES (@Env, ''ERROR'', ''ALL'', @User, @Msg);';
        
        -- Try to execute logging; if logging fails, ignore it to ensure the original error is thrown
        BEGIN TRY
            EXEC sp_executesql @Sql, N'@Env VARCHAR(10), @User NVARCHAR(100), @Msg NVARCHAR(MAX)', 
                               @Env=@Env, @User=@WhoDidIt, @Msg=@ErrMsg;
        END TRY
        BEGIN CATCH END CATCH;

        THROW;
    END CATCH
END;
GO
    
