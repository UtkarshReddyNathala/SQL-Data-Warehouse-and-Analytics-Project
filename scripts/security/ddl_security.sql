/* ============================================================
   ENTERPRISE SECURITY FOR GOLD LAYER
   - Role-Based Access Control (RBAC)
   - Row-Level Security (RLS)
   - Column-Level Security (Dynamic Data Masking)
   ============================================================ */

---------------------------------------------------------------
-- 1️ CREATE SECURITY SCHEMA
---------------------------------------------------------------
IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'Security')
    EXEC('CREATE SCHEMA Security');
GO

---------------------------------------------------------------
-- 2️ CREATE ROLES (RBAC)
---------------------------------------------------------------
IF NOT EXISTS (SELECT * FROM sys.database_principals WHERE name = 'gold_analyst')
    CREATE ROLE gold_analyst;

IF NOT EXISTS (SELECT * FROM sys.database_principals WHERE name = 'gold_manager')
    CREATE ROLE gold_manager;
GO

---------------------------------------------------------------
-- 3️ USER–COUNTRY ACCESS MAPPING TABLE
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
-- 4️ SAMPLE USER ACCESS (FOR TESTING)
---------------------------------------------------------------
INSERT INTO Security.UserCountryMapping VALUES
('IndiaUser', 'India'),
('USUser', 'United States'),
('GlobalManager', 'India'),
('GlobalManager', 'United States');
GO

---------------------------------------------------------------
-- 5️ CREATE RLS FUNCTION
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
-- 6️ DROP OLD POLICY IF EXISTS
---------------------------------------------------------------
IF EXISTS (SELECT * FROM sys.security_policies WHERE name = 'FactSalesCountryPolicy')
    DROP SECURITY POLICY Security.FactSalesCountryPolicy;
GO

---------------------------------------------------------------
-- 7️ APPLY SECURITY POLICY
---------------------------------------------------------------
CREATE SECURITY POLICY Security.FactSalesCountryPolicy
ADD FILTER PREDICATE 
Security.fn_FilterFactSalesByCountry(customer_key)
ON gold.fact_sales
WITH (STATE = ON);
GO

---------------------------------------------------------------
-- 8️ COLUMN-LEVEL SECURITY (DATA MASKING)
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
-- 9️ CREATE TEST USERS (NO LOGIN - DEMO PURPOSE)
---------------------------------------------------------------
IF NOT EXISTS (SELECT * FROM sys.database_principals WHERE name = 'IndiaUser')
    CREATE USER IndiaUser WITHOUT LOGIN;

IF NOT EXISTS (SELECT * FROM sys.database_principals WHERE name = 'USUser')
    CREATE USER USUser WITHOUT LOGIN;

IF NOT EXISTS (SELECT * FROM sys.database_principals WHERE name = 'GlobalManager')
    CREATE USER GlobalManager WITHOUT LOGIN;
GO

---------------------------------------------------------------
-- 10 GRANT PERMISSIONS TO ROLES (NOT USERS)
---------------------------------------------------------------
GRANT SELECT ON gold.fact_sales TO gold_analyst;
GRANT SELECT ON gold.dim_customers TO gold_analyst;

GRANT SELECT ON SCHEMA::gold TO gold_manager;
GRANT UNMASK TO gold_manager;
GO

---------------------------------------------------------------
-- 11 ADD USERS TO ROLES
---------------------------------------------------------------
ALTER ROLE gold_analyst ADD MEMBER IndiaUser;
ALTER ROLE gold_analyst ADD MEMBER USUser;

ALTER ROLE gold_manager ADD MEMBER GlobalManager;
GO

/* ============================================================
   END OF ENTERPRISE SECURITY SCRIPT (RBAC + RLS + MASKING)
   ============================================================ */
