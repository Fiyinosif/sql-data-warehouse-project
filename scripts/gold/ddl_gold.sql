/*
===============================================================================
DDL Script: Gold Layer Views
===============================================================================
Script Purpose:
    This script creates the business-level views for the Gold layer of the
    data warehouse. The Gold layer represents the final, analytics-ready
    data model structured as a star schema.

    The script defines dimension and fact views used for reporting,
    dashboards, and analytical queries.

Objects Created:
    - gold.dim_customers
    - gold.dim_products
    - gold.fact_sales

Schema Design:
    Star schema

        dim_customers
              |
              |
        fact_sales
              |
              |
        dim_products

Description:
    dim_customers
        Customer dimension containing demographic and geographic attributes.

    dim_products
        Product dimension containing product, category, and cost attributes.

    fact_sales
        Fact table containing sales transactions linked to customers
        and products using surrogate keys.

Source Layer:
    All objects are built from the Silver layer tables.

    silver.crm_cust_info
    silver.erp_cust_az12
    silver.erp_loc_a101
    silver.crm_prd_info
    silver.erp_px_cat_g1v2
    silver.crm_sales_details

Notes:
    - Surrogate keys are generated using ROW_NUMBER()
    - Only current products are included in dim_products
    - Gender is derived using CRM as master, ERP as fallback
    - LEFT JOIN is used to preserve all sales records

===============================================================================
*/

CREATE OR ALTER VIEW gold.dim_customers AS 
SELECT
	ROW_NUMBER() OVER(ORDER BY ci.cst_id ) AS customer_key,
	ci.cst_id AS customer_id,
	ci.cst_key AS customer_number,
	ci.cst_firstname AS first_name,
	ci.cst_lastname AS last_name,
	la.cntry AS country,
	ci.cst_marital_status AS marital_status,
	CASE 
		WHEN ci.cst_gndr != 'Unknown' THEN ci.cst_gndr
		ELSE COALESCE (ca.gen,'n/a')
	END AS gender,
	ca.bdate AS birthdate,
	ci.cst_create_date AS create_date
	
FROM silver.crm_cust_info ci
LEFT JOIN silver.erp_cust_az12 ca
	ON ci.cst_key = ca.cid
LEFT JOIN silver.erp_loc_a101 la
	ON ci.cst_key = la.cid 

CREATE OR ALTER VIEW gold.dim_products AS 
SELECT
	ROW_NUMBER() OVER(ORDER BY p.prd_start_dt,p.prd_key) AS product_key,
	p.prd_id AS product_id,
	p.prd_key AS product_number,
	p.prd_nm AS product_name,
	p.cat_id AS category_id,
	pc.cat AS category,
	pc.subcat AS Subcategory,
	pc.maintenance,
	p.prd_cost AS cost,
	p.prd_line AS product_line,
	p.prd_start_dt AS start_date
FROM silver.crm_prd_info p
LEFT JOIN silver.erp_px_cat_g1v2 pc
	ON p.cat_id = pc.id
WHERE p.prd_end_dt IS NULL  -- Filter out historical data

CREATE OR ALTER VIEW gold.fact_sales AS
SELECT 
	sd.sls_ord_num AS order_number,
	p.product_key,
	c.customer_key,
	sd.sls_order_dt AS order_date,
	sd.sls_ship_dt AS ship_date,
	sd.sls_due_dt AS due_date,
	sd.sls_sales AS sales_amount,
	sd.sls_quantity AS quantity,
	sd.sls_price AS price
FROM silver.crm_sales_details sd
LEFT JOIN gold.dim_customers c
ON sd.sls_cust_id = c.customer_id
LEFT JOIN gold.dim_products p 
ON sd.sls_prd_key = p.product_number



