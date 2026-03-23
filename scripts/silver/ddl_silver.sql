/*
=============================================================
Create Silver Layer Tables
=============================================================
Script Purpose:
    This script creates tables in the 'silver' schema for the DataWarehouse database.

    The silver layer stores cleaned and standardized data derived from the bronze layer.
    Data in this layer has been processed to improve quality and consistency before
    being used in the gold layer.

    Transformations in the silver layer may include:
        - Data cleansing
        - Data standardization
        - Data normalization
        - Data enrichment
        - Derived columns
        - Data type corrections

    Each table is dropped if it already exists and recreated to ensure
    a clean structure before loading transformed data.

Silver Layer Rules:
    - Data comes from bronze layer
    - Raw source data is not loaded directly
    - Data is cleaned and validated
    - No business aggregations yet
    - Tables are prepared for gold layer modeling

Additional Columns:
    dwh_create_date:
        Timestamp indicating when the record was inserted into
        the data warehouse.

WARNING:
    Running this script will drop existing silver tables if they exist.
    All data in those tables will be permanently deleted.
*/

-- Creating silver layer tables

-- From CRM Source : 

IF OBJECT_ID ('silver.crm_cust_info','U') IS NOT NULL 
	DROP TABLE silver.crm_cust_info
CREATE TABLE silver.crm_cust_info (
	cst_id INT,
	cst_key NVARCHAR(50),
	cst_firstname NVARCHAR(50),
	cst_lastname NVARCHAR(50),
	cst_marital_status NVARCHAR(50),
	cst_gndr NVARCHAR(50),
	cst_create_date DATE,
	dwh_create_date DATETIME2 DEFAULT GETDATE()

);
GO

IF OBJECT_ID ('silver.crm_prd_info','U') IS NOT NULL 
	DROP TABLE silver.crm_prd_info
CREATE TABLE silver.crm_prd_info (
	prd_id INT,
	cat_id NVARCHAR(50),
	prd_key NVARCHAR(50),
	prd_nm NVARCHAR(50),
	prd_cost INT,
	prd_line NVARCHAR(50),
	prd_start_dt DATE ,
	prd_end_dt DATE,
	dwh_create_date DATETIME2 DEFAULT GETDATE()
);
GO

IF OBJECT_ID ('silver.crm_sales_details','U') IS NOT NULL 
	DROP TABLE silver.crm_sales_details
CREATE TABLE silver.crm_sales_details (
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

-- From ERP Source : 

IF OBJECT_ID ('silver.erp_cust_az12','U') IS NOT NULL 
	DROP TABLE silver.erp_cust_az12
CREATE TABLE silver.erp_cust_az12 (
	cid NVARCHAR(50),
	bdate NVARCHAR(50),
	gen VARCHAR(50),
	dwh_create_date DATETIME2 DEFAULT GETDATE()
);
GO

IF OBJECT_ID ('silver.erp_loc_a101','U') IS NOT NULL 
	DROP TABLE silver.erp_loc_a101
CREATE TABLE silver.erp_loc_a101 (
	cid NVARCHAR(50),
	cntry NVARCHAR(50),
	dwh_create_date DATETIME2 DEFAULT GETDATE()
);
GO

IF OBJECT_ID ('silver.erp_px_cat_g1v2','U') IS NOT NULL 
	DROP TABLE silver.erp_px_cat_g1v2
CREATE TABLE silver.erp_px_cat_g1v2 (
	id NVARCHAR(50),
	cat VARCHAR(50),
	subcat VARCHAR(50),
	maintenance VARCHAR(50),
	dwh_create_date DATETIME2 DEFAULT GETDATE()
);
GO
