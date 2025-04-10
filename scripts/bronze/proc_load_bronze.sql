/*
====================================================================================================
Stored Procedure : Load Bronze Layer (Source - > Bronze)
====================================================================================================
Script purpose:
  this stored procedure loads data into the 'bronze' schema from external CSV Files.
  It performs the following actions:
    - Truncates the bronze tables before loading data.
    - uses the 'BULK INSERT' command to load data from CSV Files to bronze tables.

Parameters:
  None.
  this stored procedure does not accept any parameters or return any values.

Usage example:
  EXEC bronze.load_bronze;
  
====================================================================================================

*/

CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN

	BEGIN TRY
		PRINT '*************************************************************************';
		PRINT 'Loading Bronze Layer';
		PRINT '*************************************************************************';
		PRINT '-------------------------------------------------------------------------';
		PRINT 'Loading CRM Tables';
		PRINT '-------------------------------------------------------------------------';
		-- BULK INSERT text/cav to direct to tables, all data in one go instaed of row by row
		DECLARE @start_time DATETIME, @end_time DATETIME , @start_time_total DATETIME, @end_time_total DATETIME;
		SET @start_time_total = GETDATE();
		SET @start_time = GETDATE();
		PRINT ' >> Truncating Table :bronze.crm_cust_info'; 
		TRUNCATE TABLE bronze.crm_cust_info;
		-- if the below code runs again, the loads twice and rows are doubles ; so delete and load the data
		-- Full load ; since loading in bulk at once
		PRINT '>> Loading data into : bronze.crm_cust_info';
		BULK INSERT bronze.crm_cust_info
		FROM 'C:\Users\Sandeep Swargam\Desktop\Data WAREHOUSE Project\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		WITH (
			-- First row in the source csv tables is the headers and not data
			FIRSTROW = 2,
			-- The csv files are comma seperated values or some time with # or ; so sepecify the delimitor
			FIELDTERMINATOR = ',',
			-- lock the table while loading into this table ; to improve perfrormance we lock the csv file
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load time of crm_cust_info : ' + CAST(DATEDIFF(millisecond, @start_time, @end_time) AS NVARCHAR) + ' milliseconds';
		PRINT '*********************************************************************'
----------------------------------------------------------------------------------------------------------------------------------------------------------
	
		
		PRINT ' >> Truncating Table :bronze.crm_prd_info'; 
		SET @start_time = GETDATE();
		TRUNCATE TABLE bronze.crm_prd_info;
		PRINT '>> Loading data into : bronze.crm_prd_info';
		BULK INSERT bronze.crm_prd_info
		FROM 'C:\Users\Sandeep Swargam\Desktop\Data WAREHOUSE Project\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		WITH(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load time of crm_prd_info : ' + CAST(DATEDIFF(millisecond, @start_time, @end_time) AS NVARCHAR) + ' milliseconds';
		PRINT '*********************************************************************'

---------------------------------------------------------------------------------------------------------------------------------------------
		PRINT ' >> Truncating Table :bronze.crm_sales_details';
		SET @start_time = GETDATE();
		TRUNCATE TABLE bronze.crm_sales_details;
		PRINT '>> Loading data into : bronze.crm_sales_details';
		BULK INSERT bronze.crm_sales_details
		FROM 'C:\Users\Sandeep Swargam\Desktop\Data WAREHOUSE Project\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		WITH(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load time of crm_sales_details : ' + CAST(DATEDIFF(millisecond, @start_time, @end_time) AS NVARCHAR) + ' milliseconds';
		PRINT '*********************************************************************'

----------------------------------------------------------------------------------------------------------------------------------------------------------
		SET @start_time = GETDATE();
		PRINT '-------------------------------------------------------------------------';
		PRINT 'Loading ERP Tables';
		PRINT '-------------------------------------------------------------------------';
		PRINT ' >> Truncating Table :bronze.erp_cust_az12';
		TRUNCATE TABLE bronze.erp_cust_az12;
		PRINT '>> Loading data into : bronze.erp_cust_az12';
		BULK INSERT bronze.erp_cust_az12
		FROM 'C:\Users\Sandeep Swargam\Desktop\Data WAREHOUSE Project\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
		WITH(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load time of erp_cust_az12 : ' + CAST(DATEDIFF(millisecond, @start_time, @end_time) AS NVARCHAR) + ' milliseconds';
		PRINT '*********************************************************************'

----------------------------------------------------------------------------------------------------------------------------------------------------------
			SET @start_time = GETDATE();
		PRINT ' >> Truncating Table :bronze.erp_loc_a101';
		TRUNCATE TABLE bronze.erp_loc_a101;
		PRINT '>> Loading data into : bronze.erp_loc_a101';
		BULK INSERT bronze.erp_loc_a101
		FROM 'C:\Users\Sandeep Swargam\Desktop\Data WAREHOUSE Project\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
		WITH(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load time of erp_loc_a101 : ' + CAST(DATEDIFF(millisecond, @start_time, @end_time) AS NVARCHAR) + ' milliseconds';
		PRINT '*********************************************************************'

----------------------------------------------------------------------------------------------------------------------------------------------------------
		SET @start_time = GETDATE();
		PRINT ' >> Truncating Table :bronze.erp_px_cat_g1v2';
		TRUNCATE TABLE bronze.erp_px_cat_g1v2;
		PRINT '>> Loading data into : bronze.erp_px_cat_g1v2';
		BULK INSERT bronze.erp_px_cat_g1v2
		FROM 'C:\Users\Sandeep Swargam\Desktop\Data WAREHOUSE Project\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
		WITH(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
			SET @end_time = GETDATE();
		PRINT '>> Load time of erp_px_cat_g1v2 : ' + CAST(DATEDIFF(millisecond, @start_time, @end_time) AS NVARCHAR) + ' milliseconds';
		PRINT '*********************************************************************'
		SET @end_time_total = GETDATE();
		PRINT ' ******* BRONZE LAYER LOADED *************************************'
		PRINT '>> Total Load time for the Bronze Layer is : ' + CAST(DATEDIFF(millisecond, @start_time_total, @end_time_total) AS NVARCHAR) + ' milliseconds';
		PRINT '*********************************************************************'
----------------------------------------------------------------------------------------------------------------------------------------------------------
	END TRY

	BEGIN CATCH 
		PRINT '========================================================================';
		PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER';
		PRINT ' ERROR MESSAGE' +ERROR_MESSAGE();
		PRINT ' ERROR MESSAGE' +CAST(ERROR_NUMBER() AS NVARCHAR);
		PRINT ' ERROR MESSAGE' +CAST(ERROR_STATE() AS NVARCHAR);
		PRINT '========================================================================';
	END CATCH
END

-- EXEC bronze.load_bronze;
