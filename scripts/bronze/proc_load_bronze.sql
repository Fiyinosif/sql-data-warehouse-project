/*
=============================================================
Load Data Into Bronze Layer (Stored Procedure)
=============================================================
Script Purpose:
    This script creates or updates the stored procedure
    'bronze.load_bronze', which loads raw data into the bronze layer
    tables of the DataWarehouse database.

    The procedure performs the following tasks:
        - Truncates existing bronze tables
        - Loads raw data from CSV files using BULK INSERT
        - Loads data from CRM and ERP source systems
        - Logs load start time and duration
        - Handles errors using TRY...CATCH

    This procedure is part of the ETL process and is responsible for
    populating the bronze layer with raw, untransformed data.

Bronze Layer Rules:
    - Data is loaded exactly as received from source files
    - No transformations or cleaning applied
    - Tables are fully reloaded each time

Sources:
    - CRM source files
    - ERP source files

Usage Example : 
    EXEC bronze.load_bronze

WARNING:
    This procedure truncates bronze tables before loading.
    All existing data in the bronze layer will be deleted
    before new data is inserted.
*/

CREATE OR ALTER PROCEDURE bronze.load_bronze AS 
	BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME , @batch_start_time DATETIME, @batch_end_time DATETIME  
	BEGIN TRY
		SET @batch_start_time = GETDATE();
		print('========================================================');
		print('Loading Bronze Layer');
		print('========================================================');

		print('--------------------------------------------------------');
		print('Loading CRM Tables');
		print('--------------------------------------------------------');

		SET @start_time = GETDATE();
		print('>> Truncating Table: bronze.crm_cust_info');
		TRUNCATE TABLE bronze.crm_cust_info;

		print('>> Inserting Data Into: bronze.crm_cust_info');
		BULK INSERT bronze.crm_cust_info
		FROM 'C:\Users\osifa\OneDrive\Documents\AN_eng\data-warehouse\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		WITH  (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		print('>> Load Duration: '+ CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' second(s)');
		print('--------------------------------------------------------');


		SET @start_time = GETDATE();
		print('>> Truncating Table: bronze.crm_prd_info');
		TRUNCATE TABLE bronze.crm_prd_info;

		print('>> Inserting Data Into:bronze.crm_prd_info');
		BULK INSERT bronze.crm_prd_info
		FROM 'C:\Users\osifa\OneDrive\Documents\AN_eng\data-warehouse\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		WITH  (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		print('>> Load Duration: '+ CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' second(s)');
		print('--------------------------------------------------------');


		SET @start_time = GETDATE();
		print('>> Truncating Table: bronze.crm_sales_details');
		TRUNCATE TABLE bronze.crm_sales_details;

		print('>> Inserting Data Into: bronze.crm_sales_details');
		BULK INSERT bronze.crm_sales_details
		FROM 'C:\Users\osifa\OneDrive\Documents\AN_eng\data-warehouse\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		WITH  (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		print('>> Load Duration: '+ CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' second(s)');
		print('--------------------------------------------------------');


		print('--------------------------------------------------------');
		print('Loading ERP Tables');
		print('--------------------------------------------------------');

		SET @start_time = GETDATE();
		print('>> Truncating Table: bronze.erp_cust_az12');
		TRUNCATE TABLE bronze.erp_cust_az12;

		print('>> Inserting Data Into: bronze.erp_cust_az12');
		BULK INSERT bronze.erp_cust_az12
		FROM 'C:\Users\osifa\OneDrive\Documents\AN_eng\data-warehouse\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
		WITH  (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		print('>> Load Duration: '+ CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' second(s)');
		print('--------------------------------------------------------');


		SET @start_time = GETDATE();
		print('>> Truncating Table: bronze.erp_loc_a101');
		TRUNCATE TABLE bronze.erp_loc_a101;

		print('>> Inserting Data Into: bronze.erp_loc_a101');
		BULK INSERT bronze.erp_loc_a101
		FROM 'C:\Users\osifa\OneDrive\Documents\AN_eng\data-warehouse\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
		WITH  (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		print('>> Load Duration: '+ CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' second(s)');
		print('--------------------------------------------------------');


		SET @start_time = GETDATE();
		print('>> Truncating Table: bronze.erp_px_cat_g1v2');
		TRUNCATE TABLE bronze.erp_px_cat_g1v2;

		print('>> Inserting Data Into: bronze.erp_px_cat_g1v2');
		BULK INSERT bronze.erp_px_cat_g1v2
		FROM 'C:\Users\osifa\OneDrive\Documents\AN_eng\data-warehouse\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
		WITH  (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		print('>> Load Duration: '+ CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' second(s)');
		print('--------------------------------------------------------');

		SET @batch_end_time = GETDATE();
		print('========================================================');
		print('Loading Bronze Layer is completed')
		print('  -- Total Duration for Bronze layer: '+ CAST(DATEDIFF(second, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' second(s)');
		print('========================================================');

	END TRY
	BEGIN CATCH
		Print('===========================================');
		Print('ERROR OCCURED DURING LOADING BRONZE LAYER');
		Print('Error Message' + ERROR_MESSAGE());
		Print('Error Message' + CAST(ERROR_NUMBER() AS INT));
		Print('============================================');
	END CATCH
END
