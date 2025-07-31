# 📘 Data Catalog – Data Warehouse Project (CRM + ERP)

This document provides a complete catalog of all tables and columns in the Bronze, Silver, and Gold layers of the data warehouse. The goal is to make the data pipeline transparent, maintainable, and auditable.

---

## 🥉 Bronze Layer – Raw Ingested Data

### CRM Source Tables

| Table Name                | Description                         |
|---------------------------|-------------------------------------|
| `bronze.crm_cust_info`    | Raw customer information from CRM   |
| `bronze.crm_prd_info`     | Raw product master from CRM         |
| `bronze.crm_sales_details`| Raw sales transactions from CRM     |

#### Table: `bronze.crm_cust_info`

| Column Name         | Type         | Description                   |
|---------------------|--------------|-------------------------------|
| `cst_id`            | INT          | Customer ID                   |
| `cst_key`           | NVARCHAR(50) | Source CRM customer key       |
| `cst_firstname`     | NVARCHAR(50) | First Name                    |
| `cst_lastname`      | NVARCHAR(50) | Last Name                     |
| `cst_maritial_status`| NVARCHAR(50)| Marital Status                |
| `cst_gender`        | NVARCHAR(10) | Gender                        |
| `cst_create_date`   | DATE         | Customer creation date        |

---

### ERP Source Tables

| Table Name               | Description                         |
|--------------------------|-------------------------------------|
| `bronze.erp_px_cat_g1v2` | ERP product category master         |
| `bronze.erp_cust_az12`   | ERP customer master                 |
| `bronze.erp_loc_a101`    | ERP customer location info          |

---

## 🥈 Silver Layer – Cleaned/Validated Data

| Table Name               | Description                               |
|--------------------------|-------------------------------------------|
| `silver.crm_cust_info`   | Cleaned CRM customer info                 |
| `silver.crm_prd_info`    | Product table with cleaned end dates      |
| `silver.crm_sales_details`| Standardized sales data                  |
| `silver.erp_px_cat_g1v2` | Cleaned ERP product category              |
| `silver.erp_cust_az12`   | ERP customers with calculated age         |
| `silver.erp_loc_a101`    | ERP customer location                     |

🛠 **Transformations in Silver Layer**:
- Removed leading/trailing spaces
- Standardized gender/marital status
- Calculated age from DOB
- Used `LEAD()` to infer missing end dates

---

## 🥇 Gold Layer – Analytical Data Models

| Table Name             | Description                            |
|------------------------|----------------------------------------|
| `gold.fact_sales_summary` | Final sales fact table              |
| `gold.dim_customer`       | Master customer dimension           |
| `gold.dim_product`        | Master product dimension            |
| `gold.dim_date`           | Time-based reference dimension      |

#### Table: `gold.fact_sales_summary`

| Column Name     | Description                            |
|------------------|----------------------------------------|
| `order_id`       | Unique order ID                        |
| `customer_id`    | FK to `dim_customer`                   |
| `product_id`     | FK to `dim_product`                    |
| `order_date`     | Order date                             |
| `quantity`       | Quantity sold                          |
| `total_sales`    | Revenue generated                      |

---

## ✅ Example: Understanding a Data Flow from Bronze to Gold

🧾 Suppose you're analyzing **Product Performance by Customer** in the `gold.fact_sales_summary` table.

You want to know **where the product info and customer info came from**:

- `customer_id`: Comes from `silver.crm_cust_info`, which is cleaned from `bronze.crm_cust_info`.
- `product_id`: Comes from `silver.crm_prd_info`, derived from `bronze.crm_prd_info`.
- `total_sales`: Comes from `silver.crm_sales_details`, which sums and standardizes sales from `bronze.crm_sales_details`.

All transformations (like trimming names, inferring end dates, and standardizing keys) are described in the Silver layer section.

➡️ With this catalog, you now know **the origin, format, and quality process** for every column in your report!

---

📝 *Keep this file versioned and up to date with schema changes. For large changes, maintain a changelog section or version tags.*

