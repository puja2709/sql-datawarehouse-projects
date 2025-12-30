/*
===============================================================================
DDL Script: Create Silver Tables
===============================================================================
Script Purpose:
    This script creates tables in the 'silver' schema, dropping existing tables 
    if they already exist.
	  Run this script to re-define the DDL structure of 'bronze' Tables
===============================================================================
*/

CREATE OR REPLACE TABLE silver.crm_cust_info(
cst_id int,
cst_key STRING,
cst_firstname STRING,
cst_lastname STRING,
cst_marital_status STRING,
cst_gndr STRING,
cst_create_date date,
dwh_create_date TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE OR REPLACE TABLE silver.crm_prd_info(
prd_id int,
cat_id string,
prd_key STRING,
prd_nm STRING,
prd_cost int,
prd_line STRING,
prd_start_dt date,
prd_end_dt date,
dwh_create_date TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE OR REPLACE TABLE silver.crm_sales_details(
sls_ord_num STRING,
sls_prd_key STRING,
sls_cust_id int,
sls_order_dt date,
sls_ship_dt date,
sls_due_dt date,
sls_sales int,
sls_quantity int,
sls_price int,
dwh_create_date TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE OR REPLACE TABLE silver.erp_cust_az12(
CID STRING,
BDATE date,
GEN STRING,
dwh_create_date TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE OR REPLACE TABLE silver.erp_loc_a101(
CID STRING,
CNTRY STRING,
dwh_create_date TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE OR REPLACE TABLE silver.erp_px_cat_g1v2(
ID STRING,
CAT STRING,
SUBCAT STRING,
MAINTENANCE STRING,
dwh_create_date TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);
