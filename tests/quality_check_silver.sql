/*
===============================================================================
Quality Checks
===============================================================================
Script Purpose:
    This script performs various quality checks for data consistency, accuracy, 
    and standardization across the 'silver' layer. It includes checks for:
    - Null or duplicate primary keys.
    - Unwanted spaces in string fields.
    - Data standardization and consistency.
    - Invalid date ranges and orders.
    - Data consistency between related fields.

Usage Notes:
    - Run these checks after data loading Silver Layer.
    - Investigate and resolve any discrepancies found during the checks.
===============================================================================
*/
--Check for Nulls or duplicates in Primary key
-- Expectation: No result
select cst_id,count(*) from crm_cust_info
group by cst_id
having count(*)>1 or cst_id is null;

--Check Unwanted spaces in columns
--expectations: No results

select cst_firstname from crm_cust_info
where length(cst_firstname)!=length(trim(cst_firstname));

select cst_lastname from crm_cust_info
where length(cst_lastname)!=length(trim(cst_lastname));

select cst_gndr from crm_cust_info
where length(cst_gndr)!=length(trim(cst_gndr));

--Data Standardization & Consistency

select distinct cst_gndr
from crm_cust_info;

select distinct cst_marital_status
from crm_cust_info;




--check for nulls or duplicates in primary key
--expectations: No result
select prd_id, count(*) from crm_prd_info
group by prd_id
having count(*)>1 or prd_id is null;

select * from crm_prd_info;

-- Unwanted spaces
--Expectations: No Results
select prd_nm from crm_prd_info
where prd_nm != trim(prd_nm);

--nulls or negative numbers
--expectations: No result
select prd_cost
from crm_prd_info 
where prd_cost is null or prd_cost<0;

--standardization and consistency
select distinct prd_line from crm_prd_info;

--check invalid date orders
select * from crm_prd_info
where prd_end_dt<prd_start_dt;

select prd_id, prd_key,prd_nm,prd_start_dt,prd_end_dt,
dateadd(day,-1,lead(prd_start_dt) over(partition by prd_key order by prd_start_dt)) as prd_end_dt_test
from crm_prd_info
where prd_key in ('AC-HE-HL-U509-R','AC-HE-HL-U509');



select * from crm_sales_details
where sls_ord_num != trim(sls_ord_num);

select * from silver.crm_sales_details
where sls_prd_key not in (select prd_key from silver.crm_prd_info);


select * from silver.crm_sales_details
where sls_cust_id not in (select cst_id from silver.crm_cust_info);

--invalid dates
select * from silver.crm_sales_details
where sls_order_dt<=0 or length(sls_order_dt)!=8 or sls_order_dt > '20500101';

--check for invalid date orders
select  * from silver.crm_sales_details
where sls_order_dt > sls_ship_dt or sls_order_dt > sls_due_dt;

select distinct
sls_sales as sls_sales_old,
sls_quantity,
sls_price as sls_price_old
from silver.crm_sales_details
where sls_sales != sls_quantity*sls_price 
or sls_sales is null or sls_quantity is null or sls_price is null
or sls_sales <=0 or sls_quantity <=0 or sls_price <=0
order by sls_sales,sls_quantity,sls_price;


--check the primary key substr
select cid,
bdate,
gen
from silver.erp_cust_az12
where cid like 'NAS%' ;

select * from silver.crm_cust_info;

--Identify out of range dates

select bdate from silver.erp_cust_az12
where bdate >current_date();

select distinct gen from silver.erp_cust_az12;

select * from silver.erp_px_cat_g1v2;
select * from silver.erp_loc_a101;
