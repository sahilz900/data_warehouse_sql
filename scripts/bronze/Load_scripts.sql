create or alter procedure bronze.load_bronze as
begin

	declare @start_time datetime, @end_time datetime, @batch_start_time datetime, @batch_end_time datetime;;
	begin try
		
		set @batch_start_time = getdate();
		print '=====================================';
		print 'Loading the Bronze Layer';
		print '=====================================';

		print '-------------------------------------';
		print 'Loading CRM table';
		print '-------------------------------------';

		set @start_time = getdate();
		print '>>Truncating Table: bronze.crm_cust_info';
		truncate table bronze.crm_cust_info;
		print '>>Inserting data into: bronze.crm_cust_info';
		bulk insert bronze.crm_cust_info
		from 'C:\Users\sahil\OneDrive\Desktop\Sahil\Projects\Data_Warehouse\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		with(
			firstrow = 2,
			fieldterminator = ',',
			tablock
		);
		set @end_time = getdate();
		print '>> Load Duration: ' + cast(datediff(second, @start_time, @end_time) as nvarchar)+ ' seconds';
		print '--------------------------------';


		set @start_time = getdate();
		print '>>Truncating Table: bronze.crm_pred_info';
		truncate table bronze.crm_prd_info;
		print '>>Inserting data into: bronze.crm_pred_info';
		bulk insert bronze.crm_prd_info
		from 'C:\Users\sahil\OneDrive\Desktop\Sahil\Projects\Data_Warehouse\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		with(
			firstrow = 2,
			fieldterminator = ',',
			tablock
		);
		set @end_time = getdate();
		print '>> Load Duration: ' + cast(datediff(second, @start_time, @end_time) as nvarchar)+ ' seconds';
		print '--------------------------------';


		set @start_time = getdate();
		print '>>Truncating Table: bronze.crm_sales_details';
		truncate table bronze.crm_sales_details;
		print '>>Inserting data into: bronze.crm_sales_details';
		bulk insert bronze.crm_sales_details
		from 'C:\Users\sahil\OneDrive\Desktop\Sahil\Projects\Data_Warehouse\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		with(
			firstrow = 2,
			fieldterminator = ',',
			tablock
		);
		set @end_time = getdate();
		print'>> Load Duration: ' + cast(datediff(second, @start_time, @end_time) as nvarchar)+ ' seconds';
		print '--------------------------------';



		
		print '-------------------------------------';
		print 'Loading ERP table';
		print '-------------------------------------';


		set @start_time = getdate();
		print '>>Truncating Table: bronze.erp_cust_az12';
		truncate table bronze.erp_cust_az12;
		print '>>Inserting data into: bronze.erp_cust_az12';
		bulk insert bronze.erp_cust_az12
		from 'C:\Users\sahil\OneDrive\Desktop\Sahil\Projects\Data_Warehouse\sql-data-warehouse-project\datasets\source_erp\cust_az12.csv'
		with(
			firstrow = 2,
			fieldterminator = ',',
			tablock
		);
		set @end_time = getdate();
		print'>> Load Duration: ' + cast(datediff(second, @start_time, @end_time) as nvarchar)+ ' seconds';
		print '--------------------------------';


		set @start_time = getdate();
		print '>>Truncating table: bronze.erp_loc_a101';
		truncate table bronze.erp_loc_a101;
		print '>>Inserting data into: bronze.erp_loc_a101';
		bulk insert bronze.erp_loc_a101
		from 'C:\Users\sahil\OneDrive\Desktop\Sahil\Projects\Data_Warehouse\sql-data-warehouse-project\datasets\source_erp\loc_a101.csv'
		with(
		firstrow = 2,
		fieldterminator = ',',
		tablock
		);
		set @end_time = getdate();
		print '>> Load Duration: ' + cast(datediff(second, @start_time, @end_time) as nvarchar) + 'seconds';
		print '--------------------------------';



		set @start_time = getdate();
		print '>>Truncating Table: bronze.erp_px_cat_g1v2';
		truncate table bronze.erp_px_cat_g1v2;
		print '>>Inserting data into: bronze.erp_px_cat_g1v2';
		bulk insert bronze.erp_px_cat_g1v2
		from 'C:\Users\sahil\OneDrive\Desktop\Sahil\Projects\Data_Warehouse\sql-data-warehouse-project\datasets\source_erp\px_cat_g1v2.csv'
		with(
			firstrow = 2,
			fieldterminator = ',',
			tablock
		);
		set @end_time = getdate();
		print '>> Load Duration: ' + cast(datediff(second, @start_time, @end_time) as nvarchar)+ ' seconds';
		print '--------------------------------';

		set @batch_end_time = getdate();
		print '=====================================';
		print 'Loading Bronze layer is Completed';
		print '	-Total Load Duration: ' + cast(datediff(second, @batch_start_time, @batch_end_time) as nvarchar) + ' seconds';
		print '=====================================';

	end try


	begin catch
		print '=====================================';
		print 'Error occured during loading';
		print 'Error message' + error_message();
		print 'Error message' + cast(error_state() as nvarchar);
		print '=====================================' ;
	end catch

	
end