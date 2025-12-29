
/*
===============================================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
===============================================================================
Script Purpose:
    This stored procedure loads data into the 'bronze' schema from external CSV files. 
    It performs the following actions:
    - Truncates the bronze tables before loading data.
    - Uses the `copy into` command to load data from csv Files to bronze tables.
    - Created internal stages in Snowflake to ingest flat files from the local system. 
    - CSV files are uploaded to the stage using Snowflake UI and loaded into Bronze tables using COPY INTO.

Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC bronze.load_bronze;
===============================================================================
*/
CREATE OR REPLACE PROCEDURE bronze.load_bronze()
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN
TRUNCATE TABLE bronze.crm_cust_info;
COPY INTO bronze.crm_cust_info
FROM @bronze.stg_crm_cust_info/cust_info.csv
FILE_FORMAT = (
    TYPE = 'CSV'
    SKIP_HEADER = 1
);
TRUNCATE TABLE bronze.crm_prd_info;
COPY INTO bronze.crm_prd_info
FROM @bronze.stg_crm_prd_info/prd_info.csv
FILE_FORMAT = (
    TYPE = 'CSV'
    SKIP_HEADER = 1
);
TRUNCATE TABLE bronze.crm_sales_details;
COPY INTO bronze.crm_sales_details
FROM @bronze.stg_crm_sales_details/sales_details.csv
FILE_FORMAT = (
    TYPE = 'CSV'
    SKIP_HEADER = 1
);
TRUNCATE TABLE bronze.erp_cust_az12;
COPY INTO bronze.erp_cust_az12
FROM @bronze.stg_erp_cust_az12/CUST_AZ12.csv
FILE_FORMAT = (
    TYPE = 'CSV'
    SKIP_HEADER = 1
);
TRUNCATE TABLE bronze.ERP_LOC_A101;
COPY INTO bronze.ERP_LOC_A101
FROM @bronze.stg_erp_loc_a101/LOC_A101.csv
FILE_FORMAT = (
    TYPE = 'CSV'
    SKIP_HEADER = 1
);

TRUNCATE TABLE bronze.erp_px_cat_g1v2;
COPY INTO bronze.erp_px_cat_g1v2
FROM @bronze.stg_erp_px_cat_g1v2/PX_CAT_G1V2.csv
FILE_FORMAT = (
    TYPE = 'CSV'
    SKIP_HEADER = 1
);
    RETURN 'Load completed';
END;
$$;
