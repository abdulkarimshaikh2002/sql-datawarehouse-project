/*
==============================================
Create Database and Schemas
==============================================

Script Purpose:
 This Script Creates a new Database named 'DataWarehouse' after checking if it already exists.
  If the database exists, it is dropped and recreated .Additionally, the script sets up three schemas
  within the database: 'bronze','silver', and 'gold'.

Warining : 
  Running this script will drop the entire "DataWarehouse" database if it exists.
  All data in the database will be permanently deleted.Proceed with caution
  and ensure you have proper backups before running this script/

*/

USE master;
GO

--Drop and recreate the 'DataWarehouse' database
IF Exists (Select 1 FROM sys.databases WHERE name = 'DataWarehouse')
BEGIN
  ALTER DATABASE DataWarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
  DROP DATABASE DataWarehouse;
END;
GO




--- Creating new Database(DATAWAREHOUSE) and using Datawarehouse.
Create Database DataWarehouse;
GO

USE DataWarehouse;
GO
--- Creating Schema.
CREATE SCHEMA bronze;
GO

CREATE SCHEMA silver;
GO

CREATE SCHEMA gold;
GO
