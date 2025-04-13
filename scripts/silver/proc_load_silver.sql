/*
===============================================================================
Stored Procedure: Load Silver Layer (Bronze -> Silver)
===============================================================================
Script Purpose:
    This stored procedure performs the ETL (Extract, Transform, Load) process to 
    populate the 'silver' schema tables from the 'bronze' schema.
	Actions Performed:
		- Truncates Silver tables.
		- Inserts transformed and cleansed data from Bronze into Silver tables.
		
Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC Silver.load_silver;
===============================================================================
*/
CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN
	BEGIN TRY 
		PRINT ' ****************************************************************************';
		PRINT ' Loading Silver Layer';
		PRINT ' ****************************************************************************';
		PRINT ' Loading CRM Tables ';
		PRINT ' ---------------------------------------------------------------------------';
		DECLARE @start_time DATETIME, @end_time DATETIME, @Start_total_time DATETIME , @end_total_time DATETIME 
		SET @Start_total_time = GETDATE();
		SET @start_time = GETDATE();
		PRINT '>> Truncating Table : silver.crm_cust_info ';
		TRUNCATE TABLE silver.crm_cust_info;
		PRINT '>> Loading Data into : silver.crm_cust_info ';
		INSERT INTO silver.crm_cust_info(
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
				WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married' 
				WHEN UPPER(TRIM(cst_marital_status))  = 'S' THEN 'Single' 
				ELSE 'N/A' 
			END AS cst_marital_status,
			CASE 
				WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
   				WHEN UPPER(TRIM(cst_gndr))  = 'F' THEN  'Female' 
				ELSE 'N/A' 
			END AS cst_gndr, 
			cst_create_date 
		FROM 
		(
			SELECT 
				*, 
				ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date desc) as flag_latest
			FROM bronze.crm_cust_info
			WHERE cst_id IS NOT NULL
		) AS T
		WHERE flag_latest = 1;  -- Select the most recent record per customer
		SET @end_time = GETDATE();
		PRINT '>> Load time of crm_cust_info is : ' + CAST( DATEDIFF(millisecond, @start_time, @end_time) AS NVARCHAR) + ' Milliseconds';
		PRINT ' ****************************************************************************';
		-------------------------------------------------------------------------------------------------------------------------
		SET @start_time = GETDATE();
		PRINT '>> Truncating Table : silver.crm_prd_info '
		TRUNCATE TABLE silver.crm_prd_info ;
		PRINT '>> Loading Data into : silver.crm_prd_info ';
		INSERT INTO silver.crm_prd_info (
			prd_id,
			cat_id,
			prd_key,
			prd_nm,
			prd_cost,
			prd_line,
			prd_start_dt,
			prd_end_dt
		)

		SELECT 
			prd_id,
			REPLACE(SUBSTRING(prd_key, 1,5), '-','_') as cat_id,  -- for joining with erp table
			SUBSTRING(prd_key, 7, LEN(prd_key)) as prd_key,   -- for joining with sales deatils table
			prd_nm,                                           -- TRIM not needed
			ISNULL(prd_cost,0) as prd_cost,                   -- Nulls should not be present in cost for aggregations
			CASE UPPER(TRIM(prd_line)) 
				WHEN 'M' THEN 'Mountain'
				WHEN 'R' THEN 'Road'
				WHEN 'S' THEN 'Others'
				WHEN 'T' THEN 'Touring' 
				ELSE 'N/A'                                  -- DATA NORMALIZATION
			END AS prd_line,
			CAST(prd_start_dt AS DATE) AS prd_start_dt ,
			CAST(DATEADD(DAY, -1, LEAD(prd_start_dt) OVER(PARTITION BY prd_key ORDER BY prd_start_dt)) AS DATE) AS prd_end_dt
			 -- Data enrichment ; calculate end date as one day before the next start date since the data has start date > end date
		FROM bronze.crm_prd_info;
		SET @end_time = GETDATE();
		PRINT '>> Load time of crm_cust_info is : ' + CAST( DATEDIFF(millisecond, @start_time, @end_time) AS NVARCHAR) + ' Milliseconds';
		PRINT ' ****************************************************************************';

		----------------------------------------------------------------------------------------------------------------------------
		SET @start_time = GETDATE();
		PRINT '>> Truncating Table : silver.crm_sales_details ';
		TRUNCATE TABLE silver.crm_sales_details ;
		PRINT '>> Loading Data into : silver.crm_sales_details ';
		INSERT INTO silver.crm_sales_details (
			sls_ord_num,
			sls_prd_key,
			sls_cust_id,
			sls_order_dt,
			sls_ship_dt,
			sls_due_dt,
			sls_Sales,
			sls_quantity,
			sls_price

		)

		SELECT 
			sls_ord_num,
			sls_prd_key,
			sls_cust_id,
			CAST(CAST(
			CASE 
				WHEN LEN(sls_order_dt) != 8 THEN NULL 
				ELSE sls_order_dt 
				END  AS NVARCHAR) AS DATE) AS sls_order_dt,  -- converting to date 
			CAST(CAST(sls_ship_dt AS NVARCHAR) AS DATE) AS sls_ship_dt,  -- converting to date 
			CAST(CAST(sls_due_dt AS NVARCHAR) AS DATE) AS sls_due_dt,  -- converting to date 
			CASE 
				WHEN sls_Sales IS NULL OR sls_Sales <=0 OR sls_Sales != sls_quantity * ABS(sls_price) AND sls_price IS NOT NULL THEN sls_quantity * sls_price 
				ELSE sls_Sales 
			END AS sls_Sales,
			sls_quantity,  -- data validation for numeric values should not be NULL, <0 and should match the calculation sales = quantity*price
			CASE 
				WHEN sls_price IS NULL OR sls_price <=0 THEN sls_Sales/NULLIF(sls_quantity,0)
				ELSE sls_price 
			END AS sls_price
		FROM bronze.crm_sales_details;
		SET @end_time = GETDATE();
		PRINT '>> Load time of crm_cust_info is : ' + CAST( DATEDIFF(millisecond, @start_time, @end_time) AS NVARCHAR) + ' Milliseconds';
		PRINT ' ****************************************************************************';
		---------------------------------------------------------------------------------------------------------------------------------
		PRINT ' ****************************************************************************';
		PRINT ' Loading ERP Tables ';
		PRINT ' ---------------------------------------------------------------------------';
		SET @start_time = GETDATE();
		PRINT '>> Truncating Table : silver.erp_cust_az12 ';
		TRUNCATE TABLE silver.erp_cust_az12;
		PRINT '>> Loading Data into : silver.erp_cust_az12 ';
		INSERT INTO silver.erp_cust_az12 (
			cid,
			bdate,
			gen
		)

		SELECT 
			CASE 
				WHEN cid LIKE 'NAS%' THEN SUBSTRING (cid, 4, LEN(cid))
				ELSE cid
			END AS cid,
			CASE 
				WHEN bdate > GETDATE() THEN GETDATE()
				ELSE bdate 
			END AS bdate,
			CASE 
				WHEN UPPER(gen) IN ('M', 'MALE') THEN 'Male'
				WHEN UPPER(gen) IN ('F', 'FEMALE') THEN 'Female'
				ELSE 'n/a' 
			END AS gen
		

		FROM bronze.erp_cust_az12;
		SET @end_time = GETDATE();
		PRINT '>> Load time of crm_cust_info is : ' + CAST( DATEDIFF(millisecond, @start_time, @end_time) AS NVARCHAR) + ' Milliseconds';
		PRINT ' ****************************************************************************';
		-------------------------------------------------------------------------------------------------------------------------
		SET @start_time = GETDATE();
		PRINT '>> Truncating Table : silver.erp_loc_a101 ';
		TRUNCATE TABLE silver.erp_loc_a101;
		PRINT '>> Loading Data into : silver.erp_loc_a101 ';
		insert into silver.erp_loc_a101(
		cid,
		cntry
		)
		SELECT 
			REPLACE(cid,'-','') AS cid,
			CASE 
				WHEN UPPER(cntry) IN('DE' ,'GERMANY') THEN 'Germany'
				WHEN UPPER(cntry) IN('US' ,'UNITED STATES') THEN 'USA'
				WHEN UPPER(cntry) IN('AUS' ,'AUSTRALIA') THEN 'Austarlia'
				WHEN UPPER(cntry) IN('FR' ,'FRANCE')  THEN 'France'
				WHEN UPPER(cntry) IN('CAN' ,'CANADA')  THEN 'Canada'
				ELSE 'n/a'
			END AS cntry
		FROM bronze.erp_loc_a101;
		SET @end_time = GETDATE();
		PRINT '>> Load time of crm_cust_info is : ' + CAST( DATEDIFF(millisecond, @start_time, @end_time) AS NVARCHAR) + ' Milliseconds';
		PRINT ' ****************************************************************************';

		----------------------------------------------------------------------------------------------------------------------------------
		SET @start_time = GETDATE();
		PRINT '>> Truncating Table : silver.erp_px_cat_g1v2 ';
		TRUNCATE TABLE silver.erp_px_cat_g1v2;
		PRINT '>> Loading Data into : silver.erp_px_cat_g1v2 ';
		INSERT INTO silver.erp_px_cat_g1v2 (
			id,
			cat,
			sbcat,
			maintenance
		)
		SELECT 
			id,
			cat,
			sbcat,
			maintenance
		FROM bronze.erp_px_cat_g1v2
		SET @end_time = GETDATE();
		SET @end_total_time = GETDATE();
		PRINT '>> Load time of crm_cust_info is : ' + CAST( DATEDIFF(millisecond, @start_time, @end_time) AS NVARCHAR) + ' Milliseconds';
		PRINT '>> ****** Load time of Silver Layer is  : ' + CAST( DATEDIFF(millisecond, @start_total_time, @end_total_time) AS NVARCHAR) + ' Milliseconds **********';
		PRINT ' ****************************************************************************';
	END TRY

	BEGIN CATCH 
		PRINT '========================================================================';
		PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER';
		PRINT ' ERROR MESSAGE' +ERROR_MESSAGE();
		PRINT ' ERROR MESSAGE' +CAST(ERROR_NUMBER() AS NVARCHAR);
		PRINT ' ERROR MESSAGE' +CAST(ERROR_STATE() AS NVARCHAR);
		PRINT '========================================================================';
	END CATCH
END;

