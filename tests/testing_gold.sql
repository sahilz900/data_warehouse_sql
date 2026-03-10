select 
c.[cust_id],
c.[cust_key],
c.[cst_firstname],
c.[cst_lastname],
c.[cst_material_status],
c.[cst_gender],
c.[cst_create_date],
a.[gen],
a.[bdate],
e.[cntry]
from [silver].[crm_cust_info] c
left join [silver].[erp_cust_az12] a
on c.cust_key = a.cst_key
left join [silver].[erp_loc_a101] e
on c.cust_key = e.cid;


select 
--distinct(c.[cst_gender])
--distinct(a.[gen])
c.cst_gender, a.gen
from [silver].[crm_cust_info] c
left join [silver].[erp_cust_az12] a
on c.cust_key = a.cst_key;



select count(*), cst_gender
from [silver].[crm_cust_info]
group by cst_gender;

select count(*), gen
from [silver].[erp_cust_az12]
group by gen;



select 
case 
	when c.cst_gender = 'n/a' then gen
	when c.cst_gender = 'n/a' and a.gen is null then 'n/a'
	else c.cst_gender
end as gender
from [silver].[crm_cust_info] c
left join [silver].[erp_cust_az12] a
on c.cust_key = a.cst_key;



select * from [silver].[crm_cust_info];
select * from [silver].[erp_cust_az12];
select * from [silver].[erp_loc_a101];



select * from [gold].[dim_customers];
select distinct(gender) from [gold].[dim_customers];

select gender, count(*)
from [gold].[dim_customers]
group by gender
order by count(*) desc;

-------------------------------------------------------

select prd_id, count(*) from (
select
row_number() over(order by prd_id) as serial_number,
p.[prd_id],
p.[cat_id],
p.[sls_prd_key],
p.[prd_nm],
p.[prd_cost],
p.[prd_line],
p.[prd_start_dt],
g.[cat],
g.[subcat],
g.[maintenance]
from [silver].[crm_prd_info] p 
left join [silver].[erp_px_cat_g1v2] g
on p.cat_id = g.id
where prd_end_dt is null --handling current data only
)t
group by prd_id
having count(*) >1;


select * from [silver].[crm_prd_info];
select * from [silver].[erp_px_cat_g1v2];

select * from gold.dim_products;


-----------------------------------------------

select * from [silver].[crm_prd_info]
where sls_prd_key= 'CA-1098';

select * from [gold].[fact_sales];

select prd_id , count(*)
from [bronze].[crm_prd_info]
group by prd_id having count(*)>1;

select prd_key , count(*)
from [bronze].[crm_prd_info]
group by prd_key having count(*)>1;


select * from [silver].[crm_prd_info];
select * from [silver].[crm_sales_details];