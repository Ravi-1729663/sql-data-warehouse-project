/*
===============================================================================
    SQL SCRIPT: Data Quality Check – Silver Layer (CRM & ERP)
    ---------------------------------------------------------------------------
    PURPOSE:
        Perform detailed data quality validations across various Silver Layer 
        tables from CRM and ERP systems.

    NOTE:
        - These checks help in identifying data anomalies such as:
            • Nulls in key fields
            • Duplicates
            • Inconsistent formatting
            • Invalid value ranges
            • Referential logic violations
        - This is not a stored procedure. This script is for manual or 
          scheduled validation during ETL QA cycles.
===============================================================================
*/
-- Check for duplicate customer IDs
SELECT cst_id, COUNT(*) AS duplicate_count
FROM silver.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1;

-- Check for trimmed first names
SELECT cst_firstname
FROM silver.crm_cust_info
WHERE cst_firstname <> TRIM(cst_firstname);

-- Check for trimmed last names
SELECT cst_lastname
FROM silver.crm_cust_info
WHERE cst_lastname <> TRIM(cst_lastname);

-- Check distinct values in marital status
SELECT DISTINCT cst_maritial_status
FROM silver.crm_cust_info;

-- Check distinct values in gender
SELECT DISTINCT cst_gender
FROM silver.crm_cust_info;


-- CRM Product Info: silver.crm_prd_info
-- --------------------------------------------------

-- Check for duplicate product IDs
SELECT prd_id, COUNT(*) AS duplicate_count
FROM silver.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*) > 1;

-- Check for trimmed product key
SELECT prd_key
FROM silver.crm_prd_info
WHERE prd_key <> TRIM(prd_key);

-- Check category ID formatting and length
SELECT cat_id
FROM silver.crm_prd_info
WHERE cat_id <> TRIM(cat_id) OR LEN(cat_id) > 5;

-- Check for null or untrimmed product names
SELECT prd_nm
FROM silver.crm_prd_info
WHERE prd_nm IS NULL OR prd_nm <> TRIM(prd_nm);

-- Check for null or negative product cost
SELECT prd_cost
FROM silver.crm_prd_info
WHERE prd_cost IS NULL OR prd_cost < 0;

-- Check for trimmed product line
SELECT prd_line
FROM silver.crm_prd_info
WHERE prd_line <> TRIM(prd_line);

-- List distinct product lines
SELECT DISTINCT prd_line
FROM silver.crm_prd_info;

-- Check invalid date logic (start date > end date or null)
SELECT *
FROM bronze.crm_prd_info
WHERE prd_start_dt > prd_end_dt OR prd_start_dt IS NULL;


-- CRM Sales Details: silver.crm_sales_details
-- --------------------------------------------------

-- Preview records
SELECT TOP 200 *
FROM silver.crm_sales_details;

-- Validate sales date logic
SELECT sls_order_dt
FROM silver.crm_sales_details
WHERE sls_order_dt > sls_ship_dt OR sls_order_dt > sls_due_dt;

SELECT sls_ship_dt
FROM silver.crm_sales_details
WHERE sls_ship_dt > sls_due_dt;

-- Validate sales calculation and values
SELECT sls_sales, sls_quantity, sls_price
FROM silver.crm_sales_details
WHERE 
    sls_sales IS NULL 
    OR sls_quantity IS NULL 
    OR sls_price IS NULL 
    OR sls_sales < 0
    OR sls_quantity < 0
    OR sls_price < 0
    OR sls_sales <> sls_quantity * sls_price;


-- ERP Customer Master: silver.erp_cust_az12
-- --------------------------------------------------

-- Check for invalid customer IDs with 'NAS' prefix
SELECT *
FROM silver.erp_cust_az12
WHERE cid LIKE 'NAS%';

-- List distinct gender values
SELECT DISTINCT gen
FROM silver.erp_cust_az12;

-- Check for invalid birthdates
SELECT bdate
FROM silver.erp_cust_az12
WHERE bdate > GETDATE() OR DATEDIFF(YEAR, bdate, GETDATE()) > 100;


-- ERP Location Master: silver.erp_loc_a101
-- --------------------------------------------------

-- Check for untrimmed customer ID
SELECT cid
FROM silver.erp_loc_a101
WHERE TRIM(cid) <> cid;

-- Remove dashes in CID and validate
SELECT cid
FROM silver.erp_loc_a101
WHERE cid <> REPLACE(TRIM(cid), '-', '');

-- Check trimmed country values
SELECT DISTINCT cntry
FROM silver.erp_loc_a101
WHERE TRIM(cntry) <> cntry;

-- List all distinct countries
SELECT DISTINCT cntry
FROM silver.erp_loc_a101
ORDER BY cntry;
