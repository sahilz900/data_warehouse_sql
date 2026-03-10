if OBJECT_ID('silver.crm_cust_info', 'u') is not null
	drop table silver.crm_cust_info
create table silver.crm_cust_info(
	cust_id int,
	cust_key nvarchar(20),
	cst_firstname nvarchar(50),
	cst_lastname nvarchar(50),
	cst_material_status nvarchar(10),
	cst_gender nvarchar(10),
	cst_create_date date,
	dwh_create_date datetime2 default getdate()
);


if OBJECT_ID('silver.crm_prd_info', 'u') is not null
	drop table silver.crm_prd_info
create table silver.crm_prd_info(
	prd_id int,
	prd_key nvarchar(50),
	cat_id nvarchar(50),
	sls_prd_key nvarchar(50),
	prd_nm nvarchar(50),
	prd_cost int,
	prd_line nvarchar(50),
	prd_start_dt date,
	prd_end_dt date,
	dwh_create_date datetime2 default getdate()
);


if OBJECT_ID('silver.crm_sales_details', 'u') is not null
	drop table silver.crm_sales_details
create table silver.crm_sales_details(
	sls_ord_num nvarchar(50),
	sls_pred_key nvarchar(50),
	sls_cust_id int,
	sls_order_dt date,
	sls_ship_dt date,
	sls_due_dt date,
	sls_sales int,
	sls_quantity int,
	sls_price int,
	dwh_create_date datetime2 default getdate()
);



if OBJECT_ID('silver.erp_cust_az12', 'u') is not null
	drop table silver.erp_cust_az12
create table silver.erp_cust_az12(
	cid nvarchar(50),
	cst_key varchar(50),
	bdate date,
	gen varchar(50),
	dwh_create_date datetime2 default getdate()
);

IF OBJECT_ID('silver.erp_loc_a101', 'U') is not null
    drop table silver.erp_loc_a101;
create table silver.erp_loc_a101 (
    cid nvarchar(50),
    cntry nvarchar(50),
	dwh_create_date datetime2 default getdate()
);

if OBJECT_ID('silver.erp_px_cat_g1v2', 'u') is not null
	drop table silver.erp_px_cat_g1v2
create table silver.erp_px_cat_g1v2(
	id nvarchar(50),
	cat nvarchar(50),
	subcat nvarchar(50),
	maintenance nvarchar(50),
	dwh_create_date datetime2 default getdate()
);

