/*
-- ===============================================
-- Procedure: silver.load
-- Purpose: Load and transform data from Bronze layer
--          into Silver layer tables for analytics.
--
-- Notes:
--   - Data is loaded from bronze schema tables.
--   - Includes CRM and ERP domain data.
--   - Prints execution time per table and total load time.
--
-- How to execute:
--   EXEC silver.load;
--
-- Parameters:
--   None
-- ===============================================
*/
--
CREATE OR ALTER PROCEDURE silver.load
AS
BEGIN
    SET NOCOUNT ON;

    BEGIN TRY
        DECLARE 
            @overall_start DATETIME = GETDATE(),
            @section_start DATETIME,
            @section_end DATETIME;

        PRINT '=== Starting Silver Layer Data Load: ' + CONVERT(VARCHAR, @overall_start, 120) + ' ===';

        -- ============================
        -- Load crm_cust_info Table
        -- ============================
        PRINT 'Loading silver.crm_cust_info...';
        SET @section_start = GETDATE();

        IF OBJECT_ID('silver.crm_cust_info', 'U') IS NOT NULL
            TRUNCATE TABLE silver.crm_cust_info;

        INSERT INTO silver.crm_cust_info (
            cst_id,
            cst_key,
            cst_firstname,
            cst_lastname,
            cst_maritial_status,
            cst_gender,
            cst_create_date
        )
        SELECT 
            cst_id,
            cst_key,
            TRIM(cst_firstname),
            TRIM(cst_lastname),
            CASE UPPER(TRIM(cst_maritial_status))
                WHEN 'S' THEN 'Single'
                WHEN 'M' THEN 'Married'
                ELSE 'n/a'
            END,
            CASE UPPER(TRIM(cst_gender))
                WHEN 'F' THEN 'Female'
                WHEN 'M' THEN 'Male'
                ELSE 'n/a'
            END,
            cst_create_date
        FROM (
            SELECT *,
                RANK() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
            FROM bronze.crm_cust_info
            WHERE cst_id IS NOT NULL
        ) AS rk
        WHERE flag_last = 1;

        SET @section_end = GETDATE();
        PRINT 'silver.crm_cust_info loaded. Duration (sec): ' + CAST(DATEDIFF(SECOND, @section_start, @section_end) AS VARCHAR);

        -- ============================
        -- Load crm_prd_info Table
        -- ============================
        PRINT 'Loading silver.crm_prd_info...';
        SET @section_start = GETDATE();

        IF OBJECT_ID('silver.crm_prd_info', 'U') IS NOT NULL
            TRUNCATE TABLE silver.crm_prd_info;

        INSERT INTO silver.crm_prd_info (
            prd_id,
            cat_id,
            prd_key,
            prd_nm,
            prd_cost,
            prd_line,
            prd_start_dt,
            prd_end_dt
        )
        SELECT 
            prd_id,
            REPLACE(LEFT(TRIM(prd_key), 5), '-', '_'),
            RIGHT(TRIM(prd_key), LEN(prd_key) - 6),
            prd_nm,
            COALESCE(prd_cost, 0),
            CASE UPPER(TRIM(prd_line))
                WHEN 'R' THEN 'Road'
                WHEN 'M' THEN 'Mountain'
                WHEN 'T' THEN 'Touring'
                WHEN 'S' THEN 'Other Sales'
                ELSE 'n/a'
            END,
            prd_start_dt,
            DATEADD(day, -1, LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt))
        FROM bronze.crm_prd_info;

        SET @section_end = GETDATE();
        PRINT 'silver.crm_prd_info loaded. Duration (sec): ' + CAST(DATEDIFF(SECOND, @section_start, @section_end) AS VARCHAR);

        -- ============================
        -- Load crm_sales_details Table
        -- ============================
        PRINT 'Loading silver.crm_sales_details...';
        SET @section_start = GETDATE();

        IF OBJECT_ID('silver.crm_sales_details', 'U') IS NOT NULL
            TRUNCATE TABLE silver.crm_sales_details;

        INSERT INTO silver.crm_sales_details (
            sls_ord_num,
            sls_prd_key,
            sls_cust_id,
            sls_order_dt,
            sls_ship_dt,
            sls_due_dt,
            sls_sales,
            sls_quantity,
            sls_price
        )
        SELECT 
            sls_ord_num,
            sls_prd_key,
            sls_cust_id,
            CASE 
                WHEN LEN(sls_order_dt) != 8 OR sls_order_dt <= 0 THEN NULL
                WHEN sls_order_dt > sls_ship_dt THEN CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
                ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
            END,
            CASE 
                WHEN LEN(sls_ship_dt) != 8 OR sls_ship_dt <= 0 THEN NULL
                WHEN sls_order_dt > sls_ship_dt THEN CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
                ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
            END,
            CASE 
                WHEN LEN(sls_due_dt) != 8 OR sls_due_dt <= 0 THEN NULL
                ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
            END,
            CASE
                WHEN sls_sales <= 0 OR sls_sales IS NULL OR sls_sales != sls_quantity * ABS(sls_price)
                    THEN ISNULL(sls_quantity * sls_price, 0)
                ELSE sls_sales
            END,
            CASE
                WHEN sls_quantity <= 0 OR sls_quantity IS NULL THEN sls_sales / NULLIF(sls_price, 0)
                ELSE sls_quantity
            END,
            CASE
                WHEN sls_price = 0 OR sls_price IS NULL THEN sls_sales / NULLIF(sls_quantity, 0)
                WHEN sls_price < 0 THEN ABS(sls_price)
                ELSE sls_price
            END
        FROM bronze.crm_sales_details;

        SET @section_end = GETDATE();
        PRINT 'silver.crm_sales_details loaded. Duration (sec): ' + CAST(DATEDIFF(SECOND, @section_start, @section_end) AS VARCHAR);

        -- ============================
        -- Load erp_cust_az12 Table
        -- ============================
        PRINT 'Loading silver.erp_cust_az12...';
        SET @section_start = GETDATE();

        IF OBJECT_ID('silver.erp_cust_az12', 'U') IS NOT NULL
            TRUNCATE TABLE silver.erp_cust_az12;

        INSERT INTO silver.erp_cust_az12 (
            cid,
            bdate,
            gen
        )
        SELECT
            CASE 
                WHEN cid LIKE 'NAS%' THEN RIGHT(cid, LEN(cid) - 3)
                ELSE cid
            END,
            CASE
                WHEN bdate > GETDATE() OR DATEDIFF(YEAR, bdate, GETDATE()) > 100 THEN NULL
                ELSE bdate
            END,
            CASE
                WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
                WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
                ELSE 'n/a'
            END
        FROM bronze.erp_cust_az12;

        SET @section_end = GETDATE();
        PRINT 'silver.erp_cust_az12 loaded. Duration (sec): ' + CAST(DATEDIFF(SECOND, @section_start, @section_end) AS VARCHAR);

        -- ============================
        -- Load erp_loc_a101 Table
        -- ============================
        PRINT 'Loading silver.erp_loc_a101...';
        SET @section_start = GETDATE();

        IF OBJECT_ID('silver.erp_loc_a101', 'U') IS NOT NULL
            TRUNCATE TABLE silver.erp_loc_a101;

        INSERT INTO silver.erp_loc_a101 (
            cid,
            cntry
        )
        SELECT 
            REPLACE(TRIM(cid), '-', ''),
            CASE 
                WHEN UPPER(TRIM(cntry)) = 'DE' THEN 'Germany'
                WHEN UPPER(TRIM(cntry)) IN ('US', 'USA') THEN 'United States'
                WHEN TRIM(cntry) = '' THEN NULL
                ELSE TRIM(cntry)
            END
        FROM bronze.erp_loc_a101;

        SET @section_end = GETDATE();
        PRINT 'silver.erp_loc_a101 loaded. Duration (sec): ' + CAST(DATEDIFF(SECOND, @section_start, @section_end) AS VARCHAR);

        -- ============================
        -- Load erp_px_cat_g1v2 Table
        -- ============================
        PRINT 'Loading silver.erp_px_cat_g1v2...';
        SET @section_start = GETDATE();

        IF OBJECT_ID('silver.erp_px_cat_g1v2', 'U') IS NOT NULL
            TRUNCATE TABLE silver.erp_px_cat_g1v2;

        INSERT INTO silver.erp_px_cat_g1v2 (
            id,
            cat,
            subcat,
            maintenance
        )
        SELECT *
        FROM bronze.erp_px_cat_g1v2;

        SET @section_end = GETDATE();
        PRINT 'silver.erp_px_cat_g1v2 loaded. Duration (sec): ' + CAST(DATEDIFF(SECOND, @section_start, @section_end) AS VARCHAR);

        -- ============================
        -- Overall duration
        -- ============================
        DECLARE @overall_end DATETIME = GETDATE();
        PRINT '=== Silver Layer Data Load Completed: ' + CONVERT(VARCHAR, @overall_end, 120) + ' ===';
        PRINT 'Total Duration (sec): ' + CAST(DATEDIFF(SECOND, @overall_start, @overall_end) AS VARCHAR);

    END TRY
    BEGIN CATCH
        PRINT '*** ERROR OCCURRED DURING SILVER LAYER LOAD ***';
        PRINT 'Error Message: ' + ERROR_MESSAGE();
        PRINT 'Error Number: ' + CAST(ERROR_NUMBER() AS VARCHAR(10));
        PRINT 'Error Severity: ' + CAST(ERROR_SEVERITY() AS VARCHAR(10));
        PRINT 'Error State: ' + CAST(ERROR_STATE() AS VARCHAR(10));
        PRINT 'Error Line: ' + CAST(ERROR_LINE() AS VARCHAR(10));
        PRINT 'Procedure: ' + ISNULL(ERROR_PROCEDURE(), 'N/A');
    END CATCH
END;


EXEC silver.load;
