/*
-- ===============================================
-- Procedure: bronze.load
-- Purpose: Load data into Bronze layer tables from
--          source CSV files extracted from CRM and ERP
--          systems. Each table is truncated before load,
--          and execution time per table and total load
--          time are printed.
-- Notes:
--   - CRM source files located in 'source_crm' folder.
--   - ERP source files located in 'source_erp' folder.
--   - Uses BULK INSERT with TABLOCK for performance.
--   
-- How to execute:
--   EXEC bronze.load;
--   
-- Parameters:
--   None
-- ===============================================
*/

EXEC bronze.load;
GO

CREATE OR ALTER PROCEDURE bronze.load
AS
BEGIN 
    DECLARE @globalStartTime DATETIME = GETDATE();
    DECLARE @startTime DATETIME, @endTime DATETIME;

    PRINT 'Starting data load into Bronze layer...';

    BEGIN TRY
        -- CRM Data Load Section
        PRINT '--- Loading CRM Source Data ---';

        -- Load bronze.crm_cust_info (Source: CRM)
        PRINT 'Inserting into bronze.crm_cust_info from cust_info.csv';
        SET @startTime = GETDATE();
        TRUNCATE TABLE bronze.crm_cust_info;

        BULK INSERT bronze.crm_cust_info
        FROM 'C:\Users\bovil\OneDrive\Documents\SQL\Files\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
        WITH
        (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            ROWTERMINATOR = '\n',
            TABLOCK
        );
        SET @endTime = GETDATE();
        PRINT 'Loaded crm_cust_info in ' + CAST(DATEDIFF(SECOND, @startTime, @endTime) AS VARCHAR) + ' seconds.';

        -- Load bronze.crm_prd_info (Source: CRM)
        PRINT 'Inserting into bronze.crm_prd_info from prd_info.csv';
        SET @startTime = GETDATE();
        TRUNCATE TABLE bronze.crm_prd_info;

        BULK INSERT bronze.crm_prd_info
        FROM 'C:\Users\bovil\OneDrive\Documents\SQL\Files\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
        WITH
        (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            ROWTERMINATOR = '\n',
            TABLOCK
        );
        SET @endTime = GETDATE();
        PRINT 'Loaded crm_prd_info in ' + CAST(DATEDIFF(SECOND, @startTime, @endTime) AS VARCHAR) + ' seconds.';

        -- Load bronze.crm_sales_details (Source: CRM)
        PRINT 'Inserting into bronze.crm_sales_details from sales_details.csv';
        SET @startTime = GETDATE();
        TRUNCATE TABLE bronze.crm_sales_details;

        BULK INSERT bronze.crm_sales_details
        FROM 'C:\Users\bovil\OneDrive\Documents\SQL\Files\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
        WITH
        (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            ROWTERMINATOR = '\n',
            TABLOCK
        );
        SET @endTime = GETDATE();
        PRINT 'Loaded crm_sales_details in ' + CAST(DATEDIFF(SECOND, @startTime, @endTime) AS VARCHAR) + ' seconds.';

        -- ERP Data Load Section
        PRINT '--- Loading ERP Source Data ---';

        -- Load bronze.erp_cust_az12 (Source: ERP)
        PRINT 'Inserting into bronze.erp_cust_az12 from cust_az12.csv';
        SET @startTime = GETDATE();
        TRUNCATE TABLE bronze.erp_cust_az12;

        BULK INSERT bronze.erp_cust_az12
        FROM 'C:\Users\bovil\OneDrive\Documents\SQL\Files\sql-data-warehouse-project\datasets\source_erp\cust_az12.csv'
        WITH
        (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            ROWTERMINATOR = '\n',
            TABLOCK
        );
        SET @endTime = GETDATE();
        PRINT 'Loaded erp_cust_az12 in ' + CAST(DATEDIFF(SECOND, @startTime, @endTime) AS VARCHAR) + ' seconds.';

        -- Load bronze.erp_loc_a101 (Source: ERP)
        PRINT 'Inserting into bronze.erp_loc_a101 from loc_a101.csv';
        SET @startTime = GETDATE();
        TRUNCATE TABLE bronze.erp_loc_a101;

        BULK INSERT bronze.erp_loc_a101
        FROM 'C:\Users\bovil\OneDrive\Documents\SQL\Files\sql-data-warehouse-project\datasets\source_erp\loc_a101.csv'
        WITH
        (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            ROWTERMINATOR = '\n',
            TABLOCK
        );
        SET @endTime = GETDATE();
        PRINT 'Loaded erp_loc_a101 in ' + CAST(DATEDIFF(SECOND, @startTime, @endTime) AS VARCHAR) + ' seconds.';

        -- Load bronze.erp_px_cat_g1v2 (Source: ERP)
        PRINT 'Inserting into bronze.erp_px_cat_g1v2 from px_cat_g1v2.csv';
        SET @startTime = GETDATE();
        TRUNCATE TABLE bronze.erp_px_cat_g1v2;

        BULK INSERT bronze.erp_px_cat_g1v2
        FROM 'C:\Users\bovil\OneDrive\Documents\SQL\Files\sql-data-warehouse-project\datasets\source_erp\px_cat_g1v2.csv'
        WITH
        (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            ROWTERMINATOR = '\n',
            TABLOCK
        );
        SET @endTime = GETDATE();
        PRINT 'Loaded erp_px_cat_g1v2 in ' + CAST(DATEDIFF(SECOND, @startTime, @endTime) AS VARCHAR) + ' seconds.';

    END TRY
    BEGIN CATCH
        PRINT '*** ERROR OCCURRED DURING DATA LOAD ***';
        PRINT 'Error Message: ' + ERROR_MESSAGE();
        PRINT 'Error Number: ' + CAST(ERROR_NUMBER() AS VARCHAR(10));
        PRINT 'Error Severity: ' + CAST(ERROR_SEVERITY() AS VARCHAR(10));
        PRINT 'Error State: ' + CAST(ERROR_STATE() AS VARCHAR(10));
        PRINT 'Error Line: ' + CAST(ERROR_LINE() AS VARCHAR(10));
        PRINT 'Procedure: ' + ISNULL(ERROR_PROCEDURE(), 'N/A');
    END CATCH

    DECLARE @globalEndTime DATETIME = GETDATE();
    PRINT 'Total execution time: ' + CAST(DATEDIFF(SECOND, @globalStartTime, @globalEndTime) AS VARCHAR) + ' seconds.';
END;
GO
