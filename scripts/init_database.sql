/*
=============================================================
Create Database and Schemas
=============================================================
Script Purpose:
    This script creates a new database named 'DataWarehouse' after checking if it already exists. 
    If the database exists, it is dropped and recreated. Additionally, the script sets up three schemas 
    within the database: 'bronze', 'silver', and 'gold'.
	
WARNING:
    Running this script will drop the entire 'DataWarehouse' database if it exists. 
    All data in the database will be permanently deleted. Proceed with caution 
    and ensure you have proper backups before running this script.
### Option 1: Using a Graphical Interface (e.g., pgAdmin)

If you are using a GUI tool like [pgAdmin](www.pgadmin.org), you can create the necessary database manually:

1.  **Connect** to your PostgreSQL server instance within pgAdmin.
2.  **Right-click** on the "Databases" node in the object browser tree.
3.  Select **Create** > **Database...**
4.  In the "Create - Database" dialog, enter the following details:
    *   **Database:** `DataWarehouse`
    *   **Owner:** `postgres`
5.  Click **Save** to create the database.
*/


-- Create Schemas
CREATE schema bronze;
CREATE SCHEMA silver;
CREATE SCHEMA gold;





