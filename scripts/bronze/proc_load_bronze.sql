/*
=============================================================
Stored Procedure : Load Bronze Layer (Source -> Bronze)
=============================================================

Script Purpose:
    This Stored procedure loads data into the 'bronze' schema from external CSV files.
    It performs the following actions:
    - Truncates the bronze tables before loading data.
    - Uses the 'BULK INSERT' command to load data from csv files to bronze tables.

Parameters:
  None.
  This stored procedure does not accept any parameters or return any values.

Usage Example.
    EXEC. bronze.load_bronze;
======================================================================
*/


CREATE OR ALTER PROCEDURE bronze.load_bronze AS
	BEGIN

		DECLARE @start_time DATETIME, @end_time DATETIME,@batch_start_time DATETIME, @batch_end_time DATETIME;
		BEGIN TRY


		-- LOADING CRM DATA
		--- 1.Loading CRM CUST DATA

			PRINT '=================================';
			PRINT'LOADING INTO BRONZE LAYER';
			PRINT '=================================';

			PRINT '----------------------------------';
			PRINT 'LOADING CRM DATA';
			PRINT '----------------------------------';

			SET  @start_time = GETDATE();
			PRINT 'TRUNCATING TABLE : bronze.crm_cust_info ';
			TRUNCATE TABLE bronze.crm_cust_info;
	
			PRINT 'INSERTING DATA INTO: bronze.crm_cust_info'; 
			BULK INSERT bronze.crm_cust_info
			FROM 'C:\Users\Abdul.s\Desktop\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
			WITH (
				FIRSTROW = 2,
				FIELDTERMINATOR = ',',
				TABLOCK 
			);
			SET @end_time = GETDATE();
			PRINT '>> Load Duration: '+CAST(DATEDIFF(second, @start_time,@end_time) AS NVARCHAR)+ 'seconds';
			PRINT '-----------------------------------------' ;




			---2.Loading CRM PRD Data 

			SET  @start_time = GETDATE();
			PRINT 'TRUNCATING TABLE : bronze.crm_prd_info' ;
			TRUNCATE TABLE bronze.crm_prd_info;

			PRINT 'INSERTING DATA INTO : bronze.crm_prd_info'; 
			BULK INSERT bronze.crm_prd_info
			FROM 'C:\Users\Abdul.s\Desktop\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
			WITH(
				FIRSTROW = 2,
				FIELDTERMINATOR = ',',
				TABLOCK
			);
			SET @end_time = GETDATE();
			PRINT '>> Load Duration: '+CAST(DATEDIFF(second, @start_time,@end_time) AS NVARCHAR)+ 'seconds';
			PRINT '-----------------------------------------' ;

			--- 3.Loading CRM Sales Details

			SET  @start_time = GETDATE();
			PRINT 'TRUNCATING TABLE : bronze.crm_sales_details ' ;
			TRUNCATE TABLE bronze.crm_sales_details;

			PRINT 'INSERTING INTO TABLE : bronze.crm_sales_details'; 
			BULK INSERT bronze.crm_sales_details
			FROM 'C:\Users\Abdul.s\Desktop\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
			WITH (
				FIRSTROW = 2,
				FIELDTERMINATOR = ',',
				TABLOCK
			);
			SET @end_time = GETDATE();
			PRINT '>> Load Duration: '+CAST(DATEDIFF(second, @start_time,@end_time) AS NVARCHAR)+ 'seconds';
			PRINT '-----------------------------------------' ;


			---LOADING ERP DATA
			---- 4.LOADING ERP CUSTAZ

			PRINT '=========================================';
			PRINT 'LOADING ERP DATA ';
			PRINT '=========================================';

			SET  @start_time = GETDATE();
			PRINT 'TRUNCATING TABLE  : bronze.erp_custaz12'; 
			TRUNCATE TABLE bronze.erp_custaz12;

			PRINT 'INSERTING DATA INTO : bronze.erp_custaz12 ' ;
			BULK INSERT bronze.erp_custaz12
			FROM 'C:\Users\Abdul.s\Desktop\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.CSV'
			WITH(
				FIRSTROW = 2,
				FIELDTERMINATOR = ',',
				TABLOCK

			);
			SET @end_time = GETDATE();
			PRINT '>> Load Duration: '+CAST(DATEDIFF(second, @start_time,@end_time) AS NVARCHAR)+ 'seconds';
			PRINT '-----------------------------------------' ;


			---5.LOADING ERP LOC A 101
			SET  @start_time = GETDATE();
			PRINT 'TRUNCATING TABLE : bronze.erp_loc_a101';
			TRUNCATE TABLE  bronze.erp_loc_a101;

			PRINT 'INSERTING INTO TABLE : bronze.erp_loc_a101'; 
			BULK INSERT bronze.erp_loc_a101
			FROM 'C:\Users\Abdul.s\Desktop\sql-data-warehouse-project\datasets\source_erp\LOC_A101.CSV'
			WITH(	
				FIRSTROW = 2,
				FIELDTERMINATOR = ',',
				TABLOCK

			);
			SET @end_time = GETDATE();
			PRINT '>> Load Duration: '+CAST(DATEDIFF(second, @start_time,@end_time) AS NVARCHAR)+ 'seconds';
			PRINT '-----------------------------------------' ;

			--- 6. LOADING ERP PX CATG1V2
			SET  @start_time = GETDATE();
			PRINT 'TRUNCATE TABLE : bronze.erp_px_cat_g1v2 ' ;
			TRUNCATE TABLE bronze.erp_px_cat_g1v2;

			PRINT 'INSERITNG INTO TABLE : bronze.erp_px_cat_g1v2';
			BULK INSERT bronze.erp_px_cat_g1v2
			FROM 'C:\Users\Abdul.s\Desktop\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
			WITH (
				FIRSTROW = 2,
				FIELDTERMINATOR = ',',
				TABLOCK
				);
			SET @end_time = GETDATE();
			PRINT '>> Load Duration: '+CAST(DATEDIFF(second, @start_time,@end_time) AS NVARCHAR)+ 'seconds';
			PRINT '-----------------------------------------' ;	

			SET @batch_end_time = GETDATE();
			PRINT '==========================================';
			PRINT 'LOADING BRONZE LAYER IS COMPLETED';
			PRINT ' -TOTAL LOAD DURATION : ' + CAST(DATEDIFF(SECOND, @batch_start_time,@batch_end_time)AS NVARCHAR) + 'seconds';

		END TRY
		BEGIN CATCH
			PRINT '=========================================';
			PRINT 'ERROR OCCURRED DURING BRONZE LOAD';

			PRINT 'Error Message: ' + ERROR_MESSAGE();
			PRINT 'Error Number: ' + CAST(ERROR_NUMBER() AS NVARCHAR);
			PRINT 'Error State: ' + CAST(ERROR_STATE() AS NVARCHAR);

			THROW;  -- VERY IMPORTANT
		END CATCH
		

END
