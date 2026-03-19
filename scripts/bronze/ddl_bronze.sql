/*
=============================================================
Create Bronze Layer Tables
=============================================================
Script Purpose:
    This script creates tables in the 'bronze' schema for the DataWarehouse database.
    The bronze layer stores raw data exactly as received from the source systems
    without applying any transformations.

    The tables created in this script represent data from two source systems:
        - CRM (Customer Relationship Management)
        - ERP (Enterprise Resource Planning)

    Each table is dropped if it already exists and then recreated to ensure
    a clean load of raw data.

Bronze Layer Rules:
    - No data cleaning
    - No transformations
    - No business logic
    - Store raw data as-is from source files

WARNING:
    Running this script will drop existing bronze tables if they exist.
    All data in those tables will be permanently deleted.
    Make sure this is intended before running the script.
*/

-- Creating bronze layer tables

-- From CRM Source : 

IF OBJECT_ID ('bronze.crm_cust_info','U') IS NOT NULL 
	DROP TABLE bronze.crm_cust_info
CREATE TABLE bronze.crm_cust_info (
	cst_id INT,
	cst_key NVARCHAR(50),
	cst_firstname NVARCHAR(50),
	cst_lastname NVARCHAR(50),
	cst_marital_status NVARCHAR(50),
	cst_gndr NVARCHAR(50),
	cst_create_date DATE
);
GO

IF OBJECT_ID ('bronze.crm_prd_info','U') IS NOT NULL 
	DROP TABLE bronze.crm_prd_info
CREATE TABLE bronze.crm_prd_info (
	prd_id INT,
	prd_key NVARCHAR(50),
	prd_nm NVARCHAR(50),
	prd_cost INT,
	prd_line NVARCHAR(50),
	prd_start_dt DATE ,
	prd_end_dt DATE
);
GO

IF OBJECT_ID ('bronze.crm_sales_details','U') IS NOT NULL 
	DROP TABLE bronze.crm_sales_details
CREATE TABLE bronze.crm_sales_details (
	sls_ord_num NVARCHAR(50),
	sls_prd_key NVARCHAR(50),
	sls_cust_id INT,
	sls_order_dt INT,
	sls_ship_dt INT,
	sls_due_dt INT,
	sls_sales INT,
	sls_quantity INT,
	sls_price INT
);
GO

-- From ERP Source : 

IF OBJECT_ID ('bronze.erp_cust_az12','U') IS NOT NULL 
	DROP TABLE bronze.erp_cust_az12
CREATE TABLE bronze.erp_cust_az12 (
	cid NVARCHAR(50),
	bdate NVARCHAR(50),
	gen VARCHAR(50)
);
GO

IF OBJECT_ID ('bronze.erp_loc_a101','U') IS NOT NULL 
	DROP TABLE bronze.erp_loc_a101
CREATE TABLE bronze.erp_loc_a101 (
	cid NVARCHAR(50),
	cntry NVARCHAR(50)
);
GO

IF OBJECT_ID ('bronze.erp_px_cat_g1v2','U') IS NOT NULL 
	DROP TABLE bronze.erp_px_cat_g1v2
CREATE TABLE bronze.erp_px_cat_g1v2 (
	id NVARCHAR(50),
	cat VARCHAR(50),
	subcat VARCHAR(50),
	maintenance VARCHAR(50)
);
GO
