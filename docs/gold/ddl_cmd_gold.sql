IF OBJECT_ID('gold.dim_customers', 'V') IS NOT NULL
    DROP VIEW gold.dim_customers;
go 

create view gold.dim_customers as
select 
row_number() over(order by c.cust_id) as customer_id,
c.[cust_id] customer_key,
c.[cust_key] customer_number,
c.[cst_firstname] first_name,
c.[cst_lastname] last_name,

case
	when c.cst_gender != 'others' then c.cst_gender
	else coalesce(a.gen, 'n/a')
end as gender,

c.[cst_material_status] marital_status,
e.[cntry] country,
datediff(year, a.[bdate], getdate()) as age,
a.[bdate] birthdate,
c.[cst_create_date] create_date

from [silver].[crm_cust_info] c
left join [silver].[erp_cust_az12] a
on c.cust_key = a.cst_key
left join [silver].[erp_loc_a101] e
on c.cust_key = e.cid;


---------------------------------------------

IF OBJECT_ID('gold.dim_products', 'V') IS NOT NULL
    DROP VIEW gold.dim_products;
GO

create view gold.dim_products as
select
row_number() over(order by prd_id) as product_number,
p.[prd_id] as product_id,
p.[sls_prd_key] as product_key,
p.[prd_nm] as product_name,
p.[prd_cost] as product_cost,
p.[prd_line] as product_line,
p.[cat_id] as category_id,
g.[cat] as category,
g.[subcat] as subcategory,
g.[maintenance],
p.[prd_start_dt] as start_date
from [silver].[crm_prd_info] p 
left join [silver].[erp_px_cat_g1v2] g
on p.cat_id = g.id;


------------------------------------------------
IF OBJECT_ID('gold.fact_sales', 'V') IS NOT NULL
    DROP VIEW gold.fact_sales;
GO

create view gold.fact_sales as

select
s.[sls_ord_num] as order_number,
p.[sls_prd_key] as product_key,
c.[cust_id] as customer_key,
s.[sls_order_dt] as order_date,
s.[sls_ship_dt] as shipping_date,
s.[sls_due_dt] as due_date,
s.[sls_sales] as sales

from [silver].[crm_sales_details] s
left join [silver].[crm_cust_info] c
on s.sls_cust_id = c.cust_id
left join [silver].[crm_prd_info] p
on s.[sls_pred_key] = p.[sls_prd_key];

select * from [gold].[fact_sales]
select * from [gold].[dim_customers]