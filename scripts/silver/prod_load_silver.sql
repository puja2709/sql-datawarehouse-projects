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
    call Silver.load_silver();
===============================================================================
*/

CREATE OR REPLACE PROCEDURE silver.load_silver()
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN

TRUNCATE table SILVER.CRM_CUST_INFO;
INSERT INTO SILVER.CRM_CUST_INFO(cst_id,cst_key,cst_firstname,cst_lastname,cst_marital_status,cst_gndr,cst_create_date)
select cst_id,
cst_key,
trim(cst_firstname) as cst_firstname,
trim(cst_lastname) as cst_lastname,
case 
when upper(trim(cst_marital_status))='M' then 'Married'
when upper(trim(cst_marital_status)) ='S' then 'Single'
else 'n/a'
end cst_marital_status,
case 
when upper(trim(cst_gndr))='F' then 'Female'
when upper(trim(cst_gndr)) ='M' then 'Male'
else 'n/a'
end cst_gndr,
cst_create_date 
from(
select *,
row_number() over(partition by cst_id order by cst_create_date desc) flag_last
from bronze.crm_cust_info
where cst_id is not null
)t
where flag_last =1;

TRUNCATE table SILVER.CRM_PRD_INFO;
INSERT INTO SILVER.CRM_PRD_INFO(prd_id,cat_id,prd_key,prd_nm,prd_cost,prd_line,prd_start_dt,prd_end_dt)
select prd_id,
replace(left(prd_key,5),'-','_') as cat_id,
replace(substr(prd_key,7,length(prd_key)),'-','_') as prd_key,
prd_nm,
coalesce(prd_cost,0) as prd_cost,
case when upper(trim(prd_line)) = 'M' then 'Mountain'
when upper(trim(prd_line)) = 'R' then 'Road'
when upper(trim(prd_line)) = 'S' then 'other Sales'
when upper(trim(prd_line)) = 'T' then 'Touring'
else 'n/a'
end prd_line,
cast(prd_start_dt as date) as prd_start_dt,
cast(dateadd(day,-1,lead(prd_start_dt) over(partition by prd_key order by prd_start_dt))as date) as prd_end_dt
from bronze.crm_prd_info;



TRUNCATE table SILVER.CRM_SALES_DETAILS;
INSERT INTO SILVER.CRM_SALES_DETAILS(sls_ord_num,sls_prd_key,sls_cust_id,sls_order_dt,sls_ship_dt,sls_due_dt,sls_sales,sls_quantity,sls_price)
select 
sls_ord_num,
replace(sls_prd_key,'-','_') as sls_prd_key,
sls_cust_id,
case when sls_order_dt =0 or length(sls_order_dt)!=8 then Null
else cast(cast(sls_order_dt as string) as date)
end as sls_order_dt,
case when sls_ship_dt =0 or length(sls_ship_dt)!=8 then Null
else cast(cast(sls_ship_dt as string) as date)
end as sls_ship_dt,
case when sls_due_dt =0 or length(sls_due_dt)!=8 then Null
else cast(cast(sls_due_dt as string) as date)
end as sls_due_dt,
case when sls_sales is null or sls_sales <=0 or sls_sales != sls_quantity * abs(sls_price) then sls_quantity * abs(sls_price)
else sls_sales
end sls_sales,
sls_quantity,
case when sls_price is null or sls_price <=0 then sls_sales / nullif(sls_quantity,0)
else sls_price
end sls_price
from bronze.crm_sales_details;


TRUNCATE table SILVER.ERP_CUST_AZ12;
INSERT INTO SILVER.ERP_CUST_AZ12(CID,BDATE,GEN)
select
case when cid like 'NAS%' then substr(cid,4,length(cid))
else cid
end cid,
case when bdate >current_date() then null
else bdate
end as bdate,
case when upper(trim(gen))='F' then 'Female'
when upper(trim(gen)) = 'M' then 'Male'
else 'n/a'
end as gen
from bronze.erp_cust_az12;

TRUNCATE table SILVER.ERP_LOC_A101;
INSERT INTO SILVER.ERP_LOC_A101(CID,CNTRY)
select
replace(cid,'-','') as cid,
case when trim(cntry) = 'DE' then 'Germany'
when trim(cntry) in ('US','USA') then 'United States'
when trim(cntry) = '' or cntry is null then 'n/a'
else trim(cntry)
end as cntry
from bronze.erp_loc_a101;


TRUNCATE table SILVER.ERP_PX_CAT_G1V2;
INSERT INTO SILVER.ERP_PX_CAT_G1V2(ID,CAT,SUBCAT,MAINTENANCE)
select 
ID,
CAT,
SUBCAT,
MAINTENANCE
from bronze.erp_px_cat_g1v2;

    RETURN 'Load completed';
END;
$$;
