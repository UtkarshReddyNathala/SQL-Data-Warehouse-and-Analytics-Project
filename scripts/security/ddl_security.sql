/* ============================================================
   ENTERPRISE SECURITY FOR GOLD LAYER
   - Row-Level Security Based on Customer Country
   - Column Masking on sales_amount
   ============================================================ */

---------------------------------------------------------------
-- 1️ CREATE SECURITY SCHEMA
---------------------------------------------------------------
IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'Security')
    EXEC('CREATE SCHEMA Security');
GO

---------------------------------------------------------------
-- 2️ USER-COUNTRY MAPPING TABLE
---------------------------------------------------------------
IF OBJECT_ID('Security.UserCountryMapping') IS NOT NULL
    DROP TABLE Security.UserCountryMapping;
GO

CREATE TABLE Security.UserCountryMapping (
    UserName SYSNAME NOT NULL,
    Country NVARCHAR(50) NOT NULL,
    CONSTRAINT PK_UserCountry PRIMARY KEY (UserName, Country)
);
GO

CREATE INDEX IX_UserCountry_UserName
ON Security.UserCountryMapping(UserName);
GO

---------------------------------------------------------------
-- 3️ SAMPLE USER ACCESS (FOR TESTING)
---------------------------------------------------------------
INSERT INTO Security.UserCountryMapping VALUES
('IndiaUser', 'India'),
('USUser', 'United States'),
('GlobalManager', 'India'),
('GlobalManager', 'United States');
GO

---------------------------------------------------------------
-- 4️ CREATE RLS FUNCTION (JOINING DIMENSION)
---------------------------------------------------------------
IF OBJECT_ID('Security.fn_FilterFactSalesByCountry') IS NOT NULL
    DROP FUNCTION Security.fn_FilterFactSalesByCountry;
GO

CREATE FUNCTION Security.fn_FilterFactSalesByCountry(@customer_key INT)
RETURNS TABLE
WITH SCHEMABINDING
AS
RETURN
(
    SELECT 1 AS fn_result
    FROM gold.dim_customers dc
    JOIN Security.UserCountryMapping ucm
        ON dc.country = ucm.Country
    WHERE dc.customer_key = @customer_key
      AND ucm.UserName = USER_NAME()
);
GO

---------------------------------------------------------------
-- 5️ DROP OLD POLICY IF EXISTS
---------------------------------------------------------------
IF EXISTS (SELECT * FROM sys.security_policies WHERE name = 'FactSalesCountryPolicy')
    DROP SECURITY POLICY Security.FactSalesCountryPolicy;
GO

---------------------------------------------------------------
-- 6️ APPLY SECURITY POLICY TO FACT TABLE
---------------------------------------------------------------
CREATE SECURITY POLICY Security.FactSalesCountryPolicy
ADD FILTER PREDICATE 
Security.fn_FilterFactSalesByCountry(customer_key)
ON gold.fact_sales
WITH (STATE = ON);
GO

---------------------------------------------------------------
-- 7️ COLUMN MASKING ON SALES AMOUNT
---------------------------------------------------------------
BEGIN TRY
    ALTER TABLE gold.fact_sales
    ALTER COLUMN sales_amount 
    ADD MASKED WITH (FUNCTION = 'default()');
END TRY
BEGIN CATCH
END CATCH;
GO

---------------------------------------------------------------
-- 8️ CREATE TEST USERS (OPTIONAL)
---------------------------------------------------------------
IF NOT EXISTS (SELECT * FROM sys.database_principals WHERE name = 'IndiaUser')
    CREATE USER IndiaUser WITHOUT LOGIN;

IF NOT EXISTS (SELECT * FROM sys.database_principals WHERE name = 'USUser')
    CREATE USER USUser WITHOUT LOGIN;

IF NOT EXISTS (SELECT * FROM sys.database_principals WHERE name = 'GlobalManager')
    CREATE USER GlobalManager WITHOUT LOGIN;
GO

GRANT SELECT ON gold.fact_sales TO IndiaUser, USUser, GlobalManager;
GO

GRANT UNMASK TO GlobalManager;
GO

/* ============================================================
   END OF GOLD SECURITY SCRIPT
   ============================================================ */
