-- ========================================
-- 1. Check full customer data (exploration)
-- ========================================
SELECT *
FROM gold.dim_customers;



-- ========================================
-- 2. Check for duplicate customer_id (DATA QUALITY CHECK)
-- If this returns rows, you have duplicate natural keys
-- ========================================
SELECT customer_id, COUNT(*) AS duplicate_count
FROM gold.dim_customers
GROUP BY customer_id
HAVING COUNT(*) > 1;



-- ========================================
-- 3. List all distinct gender values (DATA PROFILING)
-- Helps check for invalid or unexpected values like 'n/a', 'null', etc.
-- ========================================
SELECT DISTINCT gender
FROM gold.dim_customers;



-- ========================================
-- 4. List all distinct country values (DATA PROFILING)
-- Helps validate expected country codes or names
-- ========================================
SELECT DISTINCT country
FROM gold.dim_customers;



-- ========================================
-- 5. Check full product data (exploration)
-- ========================================
SELECT *
FROM gold.dim_products;



-- ========================================
-- 6. Check for duplicate product_key (DATA QUALITY CHECK)
-- If this returns rows, surrogate keys might not be unique
-- ========================================
SELECT product_key, COUNT(*) AS duplicate_count
FROM gold.dim_products
GROUP BY product_key
HAVING COUNT(*) > 1;



-- ========================================
-- 7. Check full sales fact data (exploration)
-- ========================================
SELECT *
FROM gold.fact_sales;
