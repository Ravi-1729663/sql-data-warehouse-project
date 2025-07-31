-- ========================================
-- View: gold.dim_customers
-- Purpose: Create a dimension table for customer details
-- Combines CRM and ERP source data to form a unified customer dimension
-- ========================================
CREATE VIEW gold.dim_customers AS
SELECT 
    ROW_NUMBER() OVER (ORDER BY ci.cst_id) AS customer_key,       -- Surrogate key
    ci.cst_id AS customer_id,                                     -- Natural key from CRM
    ci.cst_key AS customer_number,                                -- External ID
    ci.cst_firstname AS first_name,
    ci.cst_lastname AS last_name,
    cl.cntry AS country,
    ci.cst_maritial_status AS maritial_status,
    CASE 
        WHEN ci.cst_gender = 'n/a' THEN ISNULL(gen, 'n/a')        -- CRM is the master; fill missing gender from ERP if available
        ELSE ci.cst_gender
    END AS gender,
    cu.bdate AS birthday,
    ci.cst_create_date AS create_date
FROM silver.crm_cust_info AS ci
LEFT JOIN silver.erp_cust_az12 AS cu ON ci.cst_key = cu.cid
LEFT JOIN silver.erp_loc_a101 AS cl ON ci.cst_key = cl.cid;



-- ========================================
-- View: gold.dim_products
-- Purpose: Create a dimension table for products
-- Combines CRM product info with ERP product categories
-- ========================================
CREATE VIEW gold.dim_products AS
SELECT 
    ROW_NUMBER() OVER (ORDER BY pd.prd_start_dt, pd.prd_key) AS product_key, -- Surrogate key
    pd.prd_id AS product_id,
    pd.prd_key AS product_number,
    pd.prd_nm AS product_name,
    pd.cat_id AS category_id,
    pc.cat AS category,
    pc.subcat AS subcategory,
    pc.maintenance AS maintenance,
    pd.prd_cost AS cost,
    pd.prd_line AS product_line,
    pd.prd_start_dt AS start_date
FROM silver.crm_prd_info AS pd
LEFT JOIN silver.erp_px_cat_g1v2 AS pc ON pd.cat_id = pc.id
WHERE pd.prd_end_dt IS NULL;  -- Filter to include only current (active) products



-- ========================================
-- View: gold.fact_sales
-- Purpose: Create the fact table for sales transactions
-- Includes links to dimension tables for products and customers
-- ========================================
CREATE VIEW gold.fact_sales AS
SELECT
    sd.sls_ord_num AS order_number,
    dp.product_key,
    dc.customer_key,
    sd.sls_order_dt AS order_date,
    sd.sls_ship_dt AS shipping_date,
    sd.sls_due_dt AS due_date,
    sd.sls_sales AS sales,
    sd.sls_quantity AS quantity,
    sd.sls_price AS price
FROM silver.crm_sales_details AS sd
LEFT JOIN gold.dim_products AS dp ON sd.sls_prd_key = dp.product_number
LEFT JOIN gold.dim_customers AS dc ON sd.sls_cust_id = dc.customer_id;
