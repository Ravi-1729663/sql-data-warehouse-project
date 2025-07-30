USE DataWarehouse;
GO

/*
-- ===============================================
-- Table: silver.crm_cust_info
-- Purpose: Store latest customer information with cleaned and normalized data
-- Columns:
--   - cst_id: Customer ID (integer)
--   - cst_key: Customer key (string)
--   - cst_firstname, cst_lastname: Customer names (trimmed)
--   - cst_maritial_status: Marital status as descriptive text
--   - cst_gender: Gender as descriptive text
--   - cst_create_date: Date record was created
--   - dwh_create_date: Timestamp when inserted into data warehouse (default: current time)
-- ===============================================
*/
IF OBJECT_ID('silver.crm_cust_info', 'U') IS NOT NULL
    DROP TABLE silver.crm_cust_info;
GO

CREATE TABLE silver.crm_cust_info
(
    cst_id INT,
    cst_key NVARCHAR(50),
    cst_firstname NVARCHAR(50),
    cst_lastname NVARCHAR(50),
    cst_maritial_status NVARCHAR(50),
    cst_gender NVARCHAR(50),
    cst_create_date DATE,
    dwh_create_date DATETIME2 DEFAULT GETDATE()
);
GO

SELECT * FROM silver.crm_cust_info;
GO

/*
-- ===============================================
-- Table: silver.crm_prd_info
-- Purpose: Store product information with category extraction,
--          line normalization, and product date range
-- ===============================================
*/
IF OBJECT_ID('silver.crm_prd_info', 'U') IS NOT NULL
    DROP TABLE silver.crm_prd_info;
GO

CREATE TABLE silver.crm_prd_info
(
    prd_id INT,
    cat_id NVARCHAR(50),
    prd_key NVARCHAR(50),
    prd_nm NVARCHAR(50),
    prd_cost INT,
    prd_line NVARCHAR(50),
    prd_start_dt DATE,
    prd_end_dt DATE,
    dwh_create_date DATETIME2 DEFAULT GETDATE()
);
GO

SELECT * FROM silver.crm_prd_info;
GO

/*
-- ===============================================
-- Table: silver.crm_sales_details
-- Purpose: Store cleaned sales transactions data
-- Includes validated dates, sales amounts, quantities, and prices
-- ===============================================
*/
IF OBJECT_ID('silver.crm_sales_details', 'U') IS NOT NULL
    DROP TABLE silver.crm_sales_details;
GO

CREATE TABLE silver.crm_sales_details
(
    sls_ord_num NVARCHAR(50),
    sls_prd_key NVARCHAR(50),
    sls_cust_id INT,
    sls_order_dt DATE,
    sls_ship_dt DATE,
    sls_due_dt DATE,
    sls_sales INT,
    sls_quantity INT,
    sls_price INT,
    dwh_create_date DATETIME2 DEFAULT GETDATE()
);
GO

SELECT * FROM silver.crm_sales_details;
GO

/*
-- ===============================================
-- Table: silver.erp_px_cat_g1v2
-- Purpose: Store ERP price category data as-is from Bronze layer
-- ===============================================
*/
IF OBJECT_ID('silver.erp_px_cat_g1v2', 'U') IS NOT NULL
    DROP TABLE silver.erp_px_cat_g1v2;
GO

CREATE TABLE silver.erp_px_cat_g1v2
(
    id NVARCHAR(50),
    cat NVARCHAR(50),
    subcat NVARCHAR(50),
    maintenance NVARCHAR(50),
    dwh_create_date DATETIME2 DEFAULT GETDATE()
);
GO

SELECT * FROM silver.erp_px_cat_g1v2;
GO

/*
-- ===============================================
-- Table: silver.erp_cust_az12
-- Purpose: Store ERP customer data with cleaned customer IDs,
--          validated birthdates, and normalized gender values
-- ===============================================
*/
IF OBJECT_ID('silver.erp_cust_az12', 'U') IS NOT NULL
    DROP TABLE silver.erp_cust_az12;
GO

CREATE TABLE silver.erp_cust_az12
(
    cid NVARCHAR(50),
    bdate DATE,
    gen NVARCHAR(50),
    dwh_create_date DATETIME2 DEFAULT GETDATE()
);
GO

SELECT * FROM silver.erp_cust_az12;
GO

/*
-- ===============================================
-- Table: silver.erp_loc_a101
-- Purpose: Store ERP location data with cleaned customer IDs
--          and normalized country names
-- ===============================================
*/
IF OBJECT_ID('silver.erp_loc_a101', 'U') IS NOT NULL
    DROP TABLE silver.erp_loc_a101;
GO

CREATE TABLE silver.erp_loc_a101
(
    cid NVARCHAR(50),
    cntry NVARCHAR(50),
    dwh_create_date DATETIME2 DEFAULT GETDATE()
);
GO

SELECT * FROM silver.erp_loc_a101;
GO
