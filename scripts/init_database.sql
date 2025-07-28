/***********************************************************************
   Script: DataWarehouse Setup Script
   Purpose: 
       - Drops the existing 'DataWarehouse' database if it exists.
       - Creates a fresh 'DataWarehouse' database with three schemas:
         bronze, silver, and gold (representing data layers in a DW).
   Warning:
       - This script forcefully disconnects all active connections to the 
         database 'DataWarehouse' and rolls back any open transactions.
       - Existing data in the database will be permanently lost.
************************************************************************/

USE master;
GO

-- Drop the database if it already exists (with forceful disconnect)
IF EXISTS (SELECT 1 FROM sys.databases WHERE name = 'DataWarehouse')
BEGIN
    ALTER DATABASE DataWarehouse 
    SET SINGLE_USER WITH ROLLBACK IMMEDIATE;

    DROP DATABASE DataWarehouse;
END
GO

-- Create a new empty DataWarehouse database
CREATE DATABASE DataWarehouse;
GO

-- Switch to the newly created DataWarehouse
USE DataWarehouse;
GO

-- Create data warehouse schemas for layered architecture
CREATE SCHEMA bronze;
GO

CREATE SCHEMA silver;
GO

CREATE SCHEMA gold;
GO
