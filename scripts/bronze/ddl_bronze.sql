USE DataWarehouse;
GO

/* ============================================================================
   DDL SCRIPT: Create Bronze Layer Tables (Raw Staging)
   ---------------------------------------------------------------------------
   - Purpose: Define structure for raw CRM and ERP data ingestion
   - Layer:   Bronze (Staging Layer in Data Warehouse Architecture)
   - Type:    DDL (Data Definition Language)
   - Note:    Tables are dropped and recreated — existing data will be lost
============================================================================ */

/* CRM: Customer Information */
IF OBJECT_ID('bronze.crm_cust_info', 'U') IS NOT NULL
	DROP TABLE bronze.crm_cust_info;
GO

CREATE TABLE bronze.crm_cust_info (
	cst_id INT,
	cst_key NVARCHAR(50),
	cst_firstname NVARCHAR(50),
	cst_lastname NVARCHAR(50),
	cst_maritial_status NVARCHAR(50),
	cst_gender NVARCHAR(50),
	cst_create_date DATE
);
GO

SELECT * FROM bronze.crm_cust_info;
GO

/* CRM: Product Information */
IF OBJECT_ID('bronze.crm_prd_info', 'U') IS NOT NULL
	DROP TABLE bronze.crm_prd_info;
GO

CREATE TABLE bronze.crm_prd_info (
	prd_id INT,
	prd_key NVARCHAR(50),
	prd_nm NVARCHAR(50),
	prd_cost INT,
	prd_line NVARCHAR(50),
	prd_start_dt DATE,
	prd_end_dt DATE
);
GO

SELECT * FROM bronze.crm_prd_info;
GO

/* CRM: Sales Details
   ⚠ Note: Date fields are stored as INT. Consider using DATE if applicable.
*/
IF OBJECT_ID('bronze.crm_sales_details', 'U') IS NOT NULL
	DROP TABLE bronze.crm_sales_details;
GO

CREATE TABLE bronze.crm_sales_details (
	sls_ord_num NVARCHAR(50),
	sls_prd_key NVARCHAR(50),
	sls_cust_id INT,
	sls_order_dt INT,
	sls_ship_dt INT,
	sls_due_dt  INT,
	sls_sales INT,
	sls_quantity INT,
	sls_price INT
);
GO

SELECT * FROM bronze.crm_sales_details;
GO

/* ERP: Product Category */
IF OBJECT_ID('bronze.erp_px_cat_g1v2', 'U') IS NOT NULL
	DROP TABLE bronze.erp_px_cat_g1v2;
GO

CREATE TABLE bronze.erp_px_cat_g1v2 (
	id NVARCHAR(50),
	cat NVARCHAR(50),
	subcat NVARCHAR(50),
	maintenance NVARCHAR(50)
);
GO

SELECT * FROM bronze.erp_px_cat_g1v2;
GO

/* ERP: Customer Basic Info */
IF OBJECT_ID('bronze.erp_cust_az12', 'U') IS NOT NULL
	DROP TABLE bronze.erp_cust_az12;
GO

CREATE TABLE bronze.erp_cust_az12 (
	cid NVARCHAR(50),
	bdate DATE,
	gen NVARCHAR(50)
);
GO

SELECT * FROM bronze.erp_cust_az12;
GO

/* ERP: Customer Location */
IF OBJECT_ID('bronze.erp_loc_a101', 'U') IS NOT NULL
	DROP TABLE bronze.erp_loc_a101;
GO

CREATE TABLE bronze.erp_loc_a101 (
	cid NVARCHAR(50),
	cntry NVARCHAR(50)
);
GO

SELECT * FROM bronze.erp_loc_a101;
