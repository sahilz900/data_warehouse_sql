
-------[silver].[crm_cust_info]--------

--remove duplicates
select cust_id, count(*) occurance
from [silver].[crm_cust_info]
group by cust_id
having count(*) > 1 or cust_id is null;



--remove spaces
select [cst_firstname]
from [silver].[crm_cust_info]
where cst_firstname != trim(cst_firstname);

select [cst_lastname]
from [silver].[crm_cust_info]
where cst_lastname != trim(cst_lastname);

select [cst_gender]
from [silver].[crm_cust_info]
where [cst_gender] != trim([cst_gender]);


--remove abbreviation
select distinct([cst_gender])
from [silver].[crm_cust_info];

select distinct([cst_material_status])
from [silver].[crm_cust_info];

select [cst_gender], count([cst_gender]) total
from silver.crm_cust_info
group by cst_gender
order by total desc;

select * from [silver].[crm_cust_info];

update [silver].[crm_cust_info]
set cst_gender = 'n/a'
where cst_gender = 'others';

---------------------------------------------------

--------[silver].[crm_prd_info]----------

select * from [bronze].[crm_prd_info];

select prd_id , count(prd_id)
from [bronze].[crm_prd_info]
group by prd_id
having count(prd_id) > 1;


select
[prd_id],
[prd_key],
replace(SUBSTRING([prd_key], 1, 5), '-', '_') as cat_id
from [bronze].[crm_prd_info]
where replace(SUBSTRING([prd_key], 1, 5), '-', '_') not in 
(select [id] from [bronze].[erp_px_cat_g1v2]);

select
[prd_id],
[prd_key],
replace(SUBSTRING([prd_key], 7, len(prd_key)), '-', '_') as cat_id
from [bronze].[crm_prd_info]
where replace(SUBSTRING([prd_key], 7, len(prd_key)), '-', '_') not in
(select sls_pred_key from [bronze].[crm_sales_details]);

select count(sls_pred_key) from [bronze].[crm_sales_details]
where sls_pred_key = 'FR_R92B_58';

--unwanted spaces in prd_nm
select prd_nm
from [bronze].[crm_prd_info]
where prd_nm != trim(prd_nm);

--checking Null values
select *
from [bronze].[crm_prd_info]
where prd_cost is null;

--remove abbreviations
select distinct(prd_line)
from [bronze].[crm_prd_info];

--invalid date
select *
from [bronze].[crm_prd_info]
where prd_start_dt < prd_end_dt;

select *
from [bronze].[crm_prd_info]
where prd_start_dt < prd_end_dt;

select DISTINCT(prd_key) , count(prd_key)
from [bronze].[crm_prd_info]
group by prd_key
having count(prd_key)>2;

select *, 
dateadd(day, -1, lead(prd_start_dt) over (partition by prd_key order by prd_start_dt)) as end_dt
from [bronze].[crm_prd_info]
where prd_key in('AC-HE-HL-U509','CL-CA-CA-1098');


---------------------------------------------------

-----------[silver].[crm_sales_details]------------

select * from [bronze].[crm_sales_details];

--checking correct format of date
select *
from [bronze].[crm_sales_details]
where [sls_order_dt] = 0 or  len([sls_order_dt])<8;

select
sls_ord_num, 
sls_pred_key, 
sls_cust_id
from [bronze].[crm_sales_details]
where sls_due_dt > 20500101 and sls_due_dt<19000101;


--handling dates
select [sls_ord_num],
[sls_pred_key],
[sls_cust_id],
case
	when len([sls_order_dt]) < 8 then null
	else cast(cast(sls_order_dt as char(8)) as date)
end as sls_order_dt,

case
	when len([sls_ship_dt]) < 8 then null
	else cast(cast([sls_ship_dt] as char(8)) as date)
end as [sls_ship_dt],

case
	when len([sls_due_dt]) < 8 then null
	else cast(cast([sls_due_dt] as char(8)) as date)
end as sls_due_date,
[sls_sales],
[sls_quantity],
[sls_price]
from [bronze].[crm_sales_details]
where sls_order_dt = 0 or len(sls_order_dt)<8;


--checking sls_ord_num
select [sls_ord_num],sls_cust_id 
from [bronze].[crm_sales_details]
where sls_cust_id not in 
(select cust_id
from [bronze].[crm_cust_info]);

select
sls_ord_num, 
sls_pred_key, 
sls_cust_id, 
sls_order_dt, 
sls_ship_dt, 
sls_due_dt, 
sls_sales, 
sls_quantity, 
sls_price
from [bronze].[crm_sales_details]
where [sls_cust_id] not in 
(select [cust_id] from [silver].[crm_cust_info])

select
sls_ord_num, 
sls_pred_key, 
sls_cust_id,
sls_order_dt,
case
	when len([sls_order_dt]) < 8 then null
	else cast(cast(sls_order_dt as char(8)) as date)
end as sls_order_date
from [bronze].[crm_sales_details]
where len(sls_order_dt) != 8;

--sequence of order, shipment and due date
select *
from [bronze].[crm_sales_details]
where 
[sls_order_dt]>[sls_ship_dt] or 
[sls_ship_dt]>[sls_due_dt] ;

--check sales details
select *
from [bronze].[crm_sales_details]
where sls_sales*sls_quantity != sls_price;

select
sls_ord_num, 
sls_pred_key, 
sls_cust_id,
sls_order_dt,
sls_sales,
sls_quantity,
case
	when sls_price < 0 then sls_price*(-1)
	else sls_sales * sls_quantity
end as sls_price
from(
select * from
[bronze].[crm_sales_details]
where sls_pred_key ='BK-M38S-40') as sub;

--checking
select *
from [silver].[crm_sales_details]
where [sls_quantity]*[sls_sales] != [sls_price];

select *
from [silver].[crm_sales_details]
where sls_sales is null or sls_quantity is null or sls_price is null;

select * from [bronze].[crm_sales_details]
where sls_sales*sls_quantity != sls_price;

select
sls_ord_num, 
sls_pred_key, 
sls_cust_id, 
sls_order_dt, 
sls_ship_dt, 
sls_due_dt,
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



---------------------------------------------------

-----------[silver].[erp_cust_az12]------------

select * from [bronze].[erp_cust_az12];
select * from [silver].[crm_cust_info];

--linking [cid] and [prd_key] from cust_info
select
cid,
case 
	when cid like 'NAS%' then  substring([cid], 4, len(cid))
	else cid
end as cst_key,
bdate,
gen
from [bronze].[erp_cust_az12]
where
(case 
	when cid like 'NAS%' then  substring([cid], 4, len(cid))
	else cid
end) not in
(select [cust_key] from [silver].[crm_cust_info]);


--bdate column
select 
[cid],
case 
	when bdate < dateadd(year, -100, getdate()) or bdate > getdate() then null
	else bdate
end as bdate,
[gen]
from [bronze].[erp_cust_az12]
where bdate < dateadd(year, -100, getdate()) or bdate > getdate();

--gender column
select distinct(gen)
from [bronze].[erp_cust_az12];

select
case 
	when upper(trim(gen)) in ('F', 'Female') then 'Female'
	when upper(trim(gen)) in ('M', 'Male') then 'Male'
	else 'n/a'
end as gen,
count(*) over(partition by 
case 
	when upper(trim(gen)) in ('F', 'Female') then 'Female'
	when upper(trim(gen)) in ('M', 'Male') then 'Male'
	else 'n/a'
end 
) as gender_count
from [bronze].[erp_cust_az12];


select
case 
	when upper(trim(gen)) in ('F', 'Female') then 'Female'
	when upper(trim(gen)) in ('M', 'Male') then 'Male'
	else 'n/a'
end as gen,
count(*)
from [bronze].[erp_cust_az12]
group by
case 
	when upper(trim(gen)) in ('F', 'Female') then 'Female'
	when upper(trim(gen)) in ('M', 'Male') then 'Male'
	else 'n/a'
end;


-----------------------------------------------------------

--------------[silver].[erp_loc_a101]----------------

select * from [bronze].[erp_loc_a101];

--replacing '-'
select
replace(cid, '-', '') as cid,
cntry
from [bronze].[erp_loc_a101]
where replace(cid, '-', '') not in
(select cust_key from [silver].[crm_cust_info]);


--handling cntry
select distinct(cntry)
from [bronze].[erp_loc_a101];

select 
distinct(cntry),
case
	when trim(cntry) = 'DE' then 'Germany'
	when trim(cntry) in ('US', 'USA') then 'United States'
	when trim(cntry) = '' or trim(cntry) is null then 'n/a'
	else trim(cntry)
end as cntr
from [bronze].[erp_loc_a101]
order by cntr;


-----------------------------------------------------------

--------------[silver].[erp_px_cat_g1v2]----------------


select * from [bronze].[erp_px_cat_g1v2];

select 
[id],
[cat],
[subcat],
[maintenance]
from [bronze].[erp_px_cat_g1v2];

--id check
select 
id
from [bronze].[erp_px_cat_g1v2] 
where id not in
(select cat_id from [silver].[crm_prd_info]);

select cat, count(id)
from [bronze].[erp_px_cat_g1v2]
group by cat;

select *
from [bronze].[erp_px_cat_g1v2]
where id = 'CO_PD';

select *
from [silver].[crm_prd_info]
where cat_id = 'CO_PD';


--check fo empty spaces
select *
from [bronze].[erp_px_cat_g1v2]
where cat != trim(cat);

select *
from [bronze].[erp_px_cat_g1v2]
where subcat != trim(subcat);

select *
from [bronze].[erp_px_cat_g1v2]
where maintenance != trim(maintenance);

--check for cat
select distinct(cat), count(*) total
from [bronze].[erp_px_cat_g1v2]
group by cat;

select *
from [bronze].[erp_px_cat_g1v2]
where cat = 'Components';

delete from [bronze].[erp_px_cat_g1v2]
where id = 'CO_PD';


--subcat
select subcat
from [bronze].[erp_px_cat_g1v2]
where id not in
(select cat_id from [silver].[crm_prd_info]);


--maintenance
select maintenance, count(*) cnt
from [bronze].[erp_px_cat_g1v2]
group by maintenance;

--------------x------------------x------------------
--------------x------------------x------------------

exec bronze.load_bronze;

exec silver.load_silver;