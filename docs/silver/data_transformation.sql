create or alter procedure silver.load_silver as
begin
	
	declare @start_time datetime, @end_time datetime, @batch_start_time datetime, @batch_end_time datetime
	begin try
		
		set @batch_start_time = getdate();

		print '=====================================';
		print 'Loading the Silver Layer';
		print '=====================================';


		print '-------------------------------------';
		print 'Loading CRM table';
		print '-------------------------------------';

		------------[silver].[crm_cust_info]---------------

		set @start_time = getdate();

		print '>>Truncating Table: silver.crm_cust_info';
		truncate table [silver].[crm_cust_info];

		print '>>Inserting data into: silver.crm_cust_info';
		insert into [silver].[crm_cust_info] (
		[cust_id],
		[cust_key],
		[cst_firstname],
		[cst_lastname],
		[cst_material_status],
		[cst_gender],
		[cst_create_date])

		select
		[cust_id],
		[cust_key],
		trim([cst_firstname]) as [cst_firstname],
		trim([cst_lastname]) as [cst_lastname],

		case
			when cst_material_status = 'S' then 'Single'
			when cst_material_status = 'M' then 'Married'
			else 'n/a'
		end as [cst_material_status],

		case
			when cst_gender = 'F' then 'Female'
			when cst_gender = 'M' then 'Male'
			else 'others'
		end as [cst_gender],

		[cst_create_date]

		from(
		select *,
		row_number() over (partition by cust_id order by cst_create_date desc) as recent
		from [bronze].[crm_cust_info] where cust_id is not null) as latest_record
		where recent = 1;


		set @end_time = getdate();

		print '>>Load Duration:' + cast(datediff(second, @start_time, @end_time) as nvarchar) + 'seconds'
		print '-----------------------------------------------------------'

		-----------------------------------------------------------

		--------------[silver].[crm_prd_info]----------------

		set @start_time = getdate();

		print '>>Truncating Table: silver.crm_prd_info';
		truncate table [silver].[crm_prd_info];

		print '>>Inserting data into: silver.crm_prd_info';
		insert into [silver].[crm_prd_info](
		[prd_id],
		[prd_key],
		[cat_id],
		[sls_prd_key],
		[prd_nm],
		[prd_cost],
		[prd_line],
		[prd_start_dt],
		[prd_end_dt])

		select
		[prd_id],
		[prd_key],
		replace(SUBSTRING([prd_key], 1, 5), '-', '_') as cat_id,
		SUBSTRING([prd_key], 7, len([prd_key])) as sls_prd_key,
		[prd_nm],
		isnull([prd_cost], 0) as prd_cost,

		case upper(trim(prd_line))
			when 'M' then 'Mountain'
			when 'R' then 'Road'
			when 'S' then 'Other Sales'
			when 'T' then 'Touring'
		end as prd_line,

		[prd_start_dt],

		dateadd(day, -1, lead(prd_start_dt) over (partition by prd_key order by prd_start_dt)) as prd_end_dt
		from [bronze].[crm_prd_info];


		set @end_time = getdate();

		print '>>Load Duration:' + cast(datediff(second, @start_time, @end_time) as nvarchar) + 'seconds'
		print '-----------------------------------------------------------'

		-----------------------------------------------------------

		--------------[silver].[crm_sales_details]----------------

		set @start_time = getdate();

		print '>>Truncating Table: silver.crm_sales_details';
		truncate table [silver].[crm_sales_details];

		print '>>Inserting data into: silver.crm_sales_details';
		insert into [silver].[crm_sales_details](
		sls_ord_num, 
		sls_pred_key, 
		sls_cust_id, 
		sls_order_dt, 
		sls_ship_dt, 
		sls_due_dt, 
		sls_sales, 
		sls_quantity, 
		sls_price)

		select
		sls_ord_num, 
		sls_pred_key, 
		sls_cust_id,

		case
			when len([sls_order_dt]) != 8 or isdate(cast(sls_order_dt as char(8)))=0 then null
			else cast(cast(sls_order_dt as char(8)) as date)
		end as sls_order_dt,

		case
			when len([sls_ship_dt]) != 8 or isdate(cast(sls_ship_dt as char(8)))=0 then null
			else cast(cast([sls_ship_dt] as char(8)) as date)
		end as [sls_ship_dt],

		case
			when len([sls_due_dt]) != 8 or isdate(cast(sls_due_dt as char(8)))=0 then null
			else cast(cast([sls_due_dt] as char(8)) as date)
		end as sls_due_dt, 

		case
			when sls_sales <= 0 then 0 
			else sls_sales
		end as sls_sales,

		sls_quantity, 

		case
			when sls_price < 0 then sls_price*(-1)
			else sls_sales * sls_quantity
		end as sls_price

		from [bronze].[crm_sales_details];


		set @end_time = getdate();

		print '>>Load Duration:' + cast(datediff(second, @start_time, @end_time) as nvarchar) + 'seconds'
		print '-----------------------------------------------------------'



		print '-------------------------------------';
		print 'Loading ERP table';
		print '-------------------------------------';


		-----------------------------------------------------------

		--------------[silver].[erp_cust_az12]----------------

		set @start_time = getdate();

		print '>>Truncating Table: silver.erp_cust_az12';
		truncate table [silver].[erp_cust_az12];

		print '>>Inserting data into: silver.erp_cust_az12';
		insert into [silver].[erp_cust_az12](
		[cid],
		[cst_key],
		[bdate],
		[gen])

		select
		[cid],
		
		case 
			when cid like 'NAS%' then  substring([cid], 4, len(cid))
			else cid
		end as cst_key,

		case 
			when bdate < dateadd(year, -100, getdate()) or bdate > getdate() then null
			else bdate
		end as bdate,

		case 
			when upper(trim(gen)) in ('F', 'Female') then 'Female'
			when upper(trim(gen)) in ('M', 'Male') then 'Male'
			else 'n/a'
		end as gen

		from [bronze].[erp_cust_az12];


		set @end_time = getdate();

		print '>>Load Duration:' + cast(datediff(second, @start_time, @end_time) as nvarchar) + 'seconds'
		print '-----------------------------------------------------------'


		-----------------------------------------------------------

		--------------[silver].[erp_loc_a101]----------------

		set @start_time = getdate();

		print '>>Truncating Table: silver.erp_loc_a101';
		truncate table [silver].[erp_loc_a101];

		print '>>Inserting data into: silver.erp_loc_a101';
		insert into [silver].[erp_loc_a101](
		[cid],
		[cntry])

		select
		replace(cid, '-', '') as cid,

		case
			when trim(cntry) = 'DE' then 'Germany'
			when trim(cntry) in ('US', 'USA') then 'United States'
			when trim(cntry) = '' or trim(cntry) is null then 'n/a'
			else trim(cntry)
		end as cntry

		from [bronze].[erp_loc_a101];


		set @end_time = getdate();

		print '>>Load Duration:' + cast(datediff(second, @start_time, @end_time) as nvarchar) + 'seconds'
		print '-----------------------------------------------------------'


		-----------------------------------------------------------

		--------------[silver].[erp_px_cat_g1v2]----------------

		set @start_time = getdate();

		print '>>Truncating Table: silver.erp_px_cat_g1v2';
		truncate table [silver].[erp_px_cat_g1v2];

		print '>>Inserting data into: silver.erp_px_cat_g1v2';
		insert into [silver].[erp_px_cat_g1v2](
		[id],
		[cat],
		[subcat],
		[maintenance])

		select 
		[id],
		[cat],
		[subcat],
		[maintenance]
		from [bronze].[erp_px_cat_g1v2];


		set @end_time = getdate();

		print '>>Load Duration:' + cast(datediff(second, @start_time, @end_time) as nvarchar) + 'seconds'
		print '-----------------------------------------------------------'



		--------------x------------------x------------------
		--------------x------------------x------------------

		set @batch_end_time = getdate();
		print '=====================================';
		print 'Loading Silver layer is Completed';
		print '-Total Load Duration:' + cast(datediff(second, @batch_start_time, @batch_end_time) as nvarchar) + 'seconds';
		print '=====================================';

	end try


	begin catch
		print '=====================================';
		print 'Error occured during loading';
		print 'Error message' + error_message();
		print 'Error message' + cast(error_state() as nvarchar)
		print '=====================================';
	end catch

end
