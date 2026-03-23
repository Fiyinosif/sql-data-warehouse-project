/*
=============================================================
Load Data Into Silver Layer (Stored Procedure)
=============================================================
Script Purpose:
    This script creates or updates the stored procedure
    'silver.load_silver', which loads transformed data from the bronze
    layer into the silver layer tables of the DataWarehouse database.

    The procedure performs the following tasks:
        - Truncates existing silver tables
        - Inserts cleansed and standardized data from bronze tables
        - Applies normalization, enrichment, and derived columns
        - Fixes invalid or inconsistent values
        - Logs load start time and duration
        - Handles errors using TRY...CATCH

    This procedure is part of the ETL process and is responsible for
    preparing clean, consistent, and validated data for the gold layer.

Silver Layer Transformations:
    - Data cleansing
    - Data standardization
    - Data normalization
    - Data enrichment
    - Derived columns
    - Data type corrections
    - Duplicate handling

Source:
    - Bronze layer tables

Load Method:
    - Full reload using TRUNCATE + INSERT

Usage Example:
EXEC silver.load_silver

WARNING:
    This procedure truncates silver tables before loading.
    All existing data in the silver layer will be deleted
    before new transformed data is inserted.
*/


CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN
BEGIN TRY 
DECLARE @start_time DATETIME,@end_time DATETIME, @batch_start_time DATETIME , @batch_end_time DATETIME
    SET @batch_start_time = GETDATE();
    Print '===========================================';
    Print 'Silver layer loading started';
    Print '===========================================';


    Print'--------------------------------------------';
    Print'Loading CRM Tables';
    Print'--------------------------------------------';
    SET @start_time = GETDATE();
    Print '>>Truncating Table: silver.crm_cust_info';
    TRUNCATE TABLE silver.crm_cust_info;
    Print '>>Inserting Data Into: silver.crm_cust_info';
    INSERT INTO silver.crm_cust_info ( 
	    cst_id,
	    cst_key,
	    cst_firstname,
	    cst_lastname,
	    cst_marital_status,
	    cst_gndr,
	    cst_create_date
    )
    SELECT
	    cst_id,
	    cst_key,
	    TRIM(cst_firstname) AS cst_firstname,
	    TRIM(cst_lastname) AS cst_lastname,
	    CASE 
		    WHEN UPPER(TRIM(cst_marital_status)) = 's' THEN 'Single'
		    WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
		    ELSE 'Unknown'
	    END AS cst_marital_status,
	    CASE 
		    WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
		    WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
		    ELSE 'Unknown'
	    END AS cst_gndr ,
	    cst_create_date
    FROM (
		    SELECT 
		    * 
		    FROM (
				    SELECT 
						    *,
						    ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) AS Rk
				    FROM bronze.crm_cust_info
				    WHERE cst_id IS NOT null ) t
		    WHERE Rk = 1 ) t1
    SET @end_time = GETDATE();
    Print '>> Load Duration: '+ CAST(DATEDIFF(SECOND,@start_time,@end_time) AS VARCHAR )+ ' second(s)';
    Print'-----------------';


    SET @start_time = GETDATE();
    Print '>>Truncating Table: silver.crm_prd_info';
    TRUNCATE TABLE silver.crm_prd_info;
    Print '>>Inserting Data Into: silver.crm_prd_info';
    INSERT INTO silver.crm_prd_info(prd_id,cat_id,prd_key,prd_nm,prd_cost,prd_line,prd_start_dt,prd_end_dt)
    SELECT
	    prd_id,
	    REPLACE(SUBSTRING(prd_key,1,5),'-','_' )AS cat_id,   -- Extraced category id 
	    SUBSTRING(prd_key,7,LEN(prd_key)) AS prd_key,		-- Extraced product key
	    prd_nm,
	    COALESCE(prd_cost,0) AS prd_cost,   -- Handled nulls
	    CASE UPPER(TRIM(prd_line))
		    WHEN 'M' THEN 'Mountain'
		    WHEN 'R' THEN 'Road'
		    WHEN 'S' THEN 'Other Sales'
		    WHEN 'T' THEN 'Touring'
		    ELSE 'n/a'
	    END AS prd_line,  -- Map product line codes to descriptive values
	    prd_start_dt,
	    DATEADD(DAY,-1,LEAD(prd_start_dt) OVER(PARTITION BY prd_key ORDER BY prd_start_dt ASC)) AS prd_end_dt
    FROM bronze.crm_prd_info
    SET @end_time = GETDATE();
    Print '>> Load Duration: '+ CAST(DATEDIFF(SECOND,@start_time,@end_time) AS VARCHAR )+ ' second(s)';
    Print'-----------------';


    SET @start_time = GETDATE();
    Print '>>Truncating Table: silver.crm_sales_details';
    TRUNCATE TABLE silver.crm_sales_details;
    Print '>>Inserting Data Into: silver.crm_sales_details';
    INSERT INTO silver.crm_sales_details([sls_ord_num]
            ,[sls_prd_key]
            ,[sls_cust_id]
            ,[sls_order_dt]
            ,[sls_ship_dt]
            ,[sls_due_dt]
            ,[sls_sales]
            ,[sls_quantity]
            ,[sls_price]   )
    SELECT 
            sls_ord_num
            ,sls_prd_key
            ,sls_cust_id
            ,CASE 
                WHEN sls_order_dt = 0 OR LEN(sls_order_dt) != 8 THEN NULL
                ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
            END AS sls_order_dt
            ,CASE 
                WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) != 8 THEN NULL
                ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
            END AS sls_ship_dt
            ,CASE 
                WHEN sls_due_dt = 0 OR LEN(sls_ship_dt) != 8 THEN NULL
                ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
            END AS sls_due_dt
            , CASE
                WHEN  sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != (ABS(sls_price) * NULLIF(sls_quantity,0) ) THEN ( ABS(sls_price) * sls_quantity )
                ELSE sls_sales
            END AS sls_sales
            ,sls_quantity
            ,CASE 
                WHEN sls_price IS NULL OR sls_price <= 0 THEN sls_sales / NULLIF(sls_quantity,0)
                ELSE sls_price
            END AS sls_price
        FROM DataWarehouse.bronze.crm_sales_details 
    SET @end_time = GETDATE();
    Print '>> Load Duration: '+ CAST(DATEDIFF(SECOND,@start_time,@end_time) AS VARCHAR )+ ' second(s)';
    Print'-----------------';


    Print'--------------------------------------------';
    Print'Loading ERP Tables';
    Print'--------------------------------------------';

    SET @start_time = GETDATE();
    Print '>>Truncating Table: silver.erp_cust_az12';
    TRUNCATE TABLE silver.erp_cust_az12;
    Print '>>Inserting Data Into: silver.erp_cust_az12';
    INSERT INTO silver.erp_cust_az12 (cid, bdate,gen ) 
    SELECT  
                CASE 
                WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid,4,LEN(cid))         -- removed NAS to get correct customer id 
                ELSE cid 
            END AS cid
            ,CASE 
                WHEN bdate > GETDATE() THEN NULL
                ELSE bdate
            END AS bdate           -- Hnadled invalid dates
            ,CASE 
                WHEN TRIM(gen) = 'F' THEN 'Female'
                WHEN TRIM(gen) = 'M' THEN 'Male'
                WHEN TRIM(gen) = '' OR gen IS NULL  THEN 'n/a'
                ELSE gen
            END AS gen              -- Data Normalization , mapped gender roles to their abbreviations and dealt with unknown.
        FROM [DataWarehouse].[bronze].[erp_cust_az12] 
    SET @end_time = GETDATE();
    Print '>> Load Duration: '+ CAST(DATEDIFF(SECOND,@start_time,@end_time) AS VARCHAR )+ ' second(s)';
    Print'-----------------';



    SET @start_time = GETDATE();
    Print '>>Truncating Table: silver.erp_loc_a101';
    TRUNCATE TABLE silver.erp_loc_a101;
    Print '>>Inserting Data Into: silver.erp_loc_a101';
    INSERT INTO silver.erp_loc_a101(cid,cntry)
    SELECT 
            REPLACE(cid,'-','') AS cid,
            CASE 
                WHEN TRIM(cntry) = 'DE' THEN 'Germany'
                WHEN TRIM(cntry) IN ('USA','US') THEN 'United States'
                WHEN TRIM(cntry) IS NULL or TRIM(cntry) = '' THEN 'n/a'
                ELSE cntry
            END AS cntry
        FROM [DataWarehouse].[bronze].[erp_loc_a101]
     SET @end_time = GETDATE();
     Print '>> Load Duration: '+ CAST(DATEDIFF(SECOND,@start_time,@end_time) AS VARCHAR )+ ' second(s)';
     Print'-----------------';


    SET @start_time = GETDATE();
    Print '>>Truncating Table: silver.erp_px_cat_g1v2';
    TRUNCATE TABLE silver.erp_px_cat_g1v2;
    Print '>>Inserting Data Into: silver.erp_px_cat_g1v2';
    INSERT INTO silver.erp_px_cat_g1v2(id,cat,subcat,maintenance)

    SELECT  [id]
            ,[cat]
            ,[subcat]
            ,[maintenance]
    FROM [DataWarehouse].[bronze].[erp_px_cat_g1v2]

    SET @end_time = GETDATE();
    Print '>> Load Duration: '+ CAST(DATEDIFF(SECOND,@start_time,@end_time) AS VARCHAR )+ ' second(s)';

    SET @batch_end_time = GETDATE()
    Print '===========================================';
    Print 'Silver layer loading completed';
    Print 'Total time taken: '+ CAST(DATEDIFF(SECOND,@batch_start_time,@batch_end_time) AS VARCHAR )+ ' second(s)';
    Print '===========================================';

END TRY
BEGIN CATCH
    Print '===========================================';
    Print 'Error During Silver layer loading';
    Print 'Error message: '+ ERROR_MESSAGE();
    Print 'Error message: '+ CAST(ERROR_NUMBER() AS VARCHAR);
    Print '===========================================';
END CATCH
END
