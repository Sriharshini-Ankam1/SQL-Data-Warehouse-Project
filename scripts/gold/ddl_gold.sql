/*
===============================================================================
DDL Script: Create Gold Views
===============================================================================
Script Purpose:
    This script creates views for the Gold layer in the data warehouse. 
    The Gold layer represents the final dimension and fact tables (Star Schema)

    Each view performs transformations and combines data from the Silver layer 
    to produce a clean, enriched, and business-ready dataset.

Usage:
    - These views can be queried directly for analytics and reporting.
===============================================================================
*/

-- ==========================================================
-- Create Dim Table : gold.dim_customers
-- ==========================================================
IF OBJECT_ID('gold.dim_customers', 'V') IS NOT NULL
	DROP VIEW gold.dim_customers;
GO

CREATE VIEW gold.dim_customers AS
SELECT 
	ROW_NUMBER() OVER(ORDER BY cst_id ) AS customer_key, -- adding a surrogate key since it a dimension table and we need a unique column
	ci.cst_id AS customer_id,
	ci.cst_key AS customer_number,
	ci.cst_firstname AS first_name,
	ci.cst_lastname AS last_name,
	ea.cntry as country,
	ci.cst_marital_status AS marital_status,
	CASE
		WHEN cst_gndr != 'N/A' THEN ci.cst_gndr
		ELSE COALESCE(ca.gen, 'N/A')
	END AS gender,
	ca.bdate as birthdate,
	ci.cst_create_date AS create_date
FROM silver.crm_cust_info ci
LEFT JOIN silver.erp_cust_az12 ca
ON ci.cst_key =ca.cid
LEFT JOIN silver.erp_loc_a101 ea
ON ci.cst_key= ea.cid;
GO
-- ==========================================================
-- Create Dim Table : gold.dim_products
-- ==========================================================
IF OBJECT_ID('gold.dim_products', 'V') IS NOT NULL
	DROP VIEW gold.dim_products;
GO

CREATE VIEW gold.dim_products AS 
SELECT 
	ROW_NUMBER() OVER(ORDER BY p.prd_start_dt , prd_key) AS product_key,  -- adding a surrogate key since it a dimension table and we need a unique column
	p.prd_id AS prodcut_id,
	p.prd_key AS product_number,
	p.prd_nm AS product_name,
	p.cat_id AS category_id,
	c.cat AS category,
	c.sbcat AS subcategory,
	c.maintenance,
	p.prd_cost AS cost,
	p.prd_line AS product_line,
	p.prd_start_dt AS start_date
	-- p.prd_end_dt, -- since it is NULL after removing the histrical data
FROM silver.crm_prd_info p
LEFT JOIN silver.erp_px_cat_g1v2 c
ON p.cat_id = c.id
WHERE p.prd_end_dt IS NULL;  -- FILTER OUT ALL HISTORICAL DATA
GO
-- ==========================================================
-- Create Fact Table : gold.fact_sales
-- ==========================================================
IF OBJECT_ID('gold.fact_sales', 'V') IS NOT NULL
	DROP VIEW gold.fact_sales;
GO

CREATE VIEW gold.fact_sales AS
SELECT 
	s.sls_ord_num AS order_number, 
--	s.sls_prd_key,
--	s.sls_cust_id,
	p.product_key,
	c.customer_key,
	s.sls_order_dt AS order_date,
	s.sls_ship_dt  AS ship_date,
	s.sls_due_dt AS due_date,
	s.sls_Sales AS sales_amount,
	s.sls_quantity AS quantity,
	s.sls_price AS price
FROM silver.crm_sales_details s
LEFT JOIN gold.dim_customers c
ON s.sls_cust_id = c.customer_id
LEFT JOIN gold.dim_products p
ON s.sls_prd_key = p.product_number
;

