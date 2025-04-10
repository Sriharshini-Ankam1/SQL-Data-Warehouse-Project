/*
=========================================================
Create Database and schemas
 ========================================================
 script purpose :
	this script creates a new database names 'DataWarehouse' after checking if it already exists.
	if the database exists, it is dropped and recreated. additionally the script sets up three schemas within the database 'bronze',
	'silver', 'gold'.

Warning:
	Runnig this script will drop the entire 'DataWarehouse' if it exists.
	all data in the database will be permanently deleted. proceed with caution.
	and ensure you have proper backups before running this script.

*/

USE Master;
GO

-- drop and recreate the 'DataWarehouse' database
IF EXISTS (SELECT 1 FROM sys.databases WHERE name = 'DataWarehouse')
BEGIN
	ALTER DATABASE DataWarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
	DROP DATABASE DataWarehouse;
END;
GO

-- Create database DataWarehouse
CREATE DATABASE DataWarehouse;
GO
USE DataWarehouse;
GO

-- create schema for each layer - bronze, silver, gold

CREATE SCHEMA bronze;
GO
CREATE SCHEMA silver;
GO
CREATE SCHEMA gold;
GO
