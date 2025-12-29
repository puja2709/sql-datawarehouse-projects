/*
===============================================================================
DDL Script: Create Bronze Tables
===============================================================================
Script Purpose:
    This script creates tables in the 'bronze' schema, dropping existing tables 
    if they already exist.
	  Run this script to re-define the DDL structure of 'bronze' Tables
===============================================================================
*/


CREATE OR REPLACE TABLE bronze.crm_cust_info(
cst_id int,
cst_key STRING,
cst_firstname STRING,
cst_lastname STRING,
cst_marital_status STRING,
cst_gndr STRING,
cst_create_date date
);

CREATE OR REPLACE TABLE bronze.crm_prd_info(
prd_id int,
prd_key STRING,
prd_nm STRING,
prd_cost int,
prd_line STRING,
prd_start_dt TIMESTAMP_NTZ,
prd_end_dt TIMESTAMP_NTZ
);

CREATE OR REPLACE TABLE bronze.crm_sales_details(
sls_ord_num STRING,
sls_prd_key STRING,
sls_cust_id int,
sls_order_dt int,
sls_ship_dt int,
sls_due_dt int,
sls_sales int,
sls_quantity int,
sls_price int
);

CREATE OR REPLACE TABLE bronze.erp_cust_az12(
CID STRING,
BDATE date,
GEN STRING
);

CREATE OR REPLACE TABLE bronze.erp_loc_a101(
CID STRING,
CNTRY STRING
);

CREATE OR REPLACE TABLE bronze.erp_px_cat_g1v2(
ID STRING,
CAT STRING,
SUBCAT STRING,
MAINTENANCE STRING
);
