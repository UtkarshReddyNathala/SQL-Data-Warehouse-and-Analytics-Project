/*
===============================================================================
DDL Script: Create Audit Schema and Logging Tables 
===============================================================================
Script Purpose:
1. **audit.etl_log** – Records when the system runs and what data it handled.
2. **audit.data_quality_issues** – Keeps track of data problems or errors found in the data.
3. **audit.etl_config** – Stores setup details that help the system move and process data automatically.
4. **audit.watermark_thresholds** – NEW! Remembers the last data processed so only new data is added next time.

===============================================================================
*/

-- Create the Audit Schema if it doesn't exist
IF SCHEMA_ID('audit') IS NULL 
    EXEC ('CREATE SCHEMA audit');
GO

-- =============================================================================
-- 1. Create the Process Logging Table (Execution Tracking)
-- =============================================================================
IF OBJECT_ID('audit.etl_log', 'U') IS NOT NULL
    DROP TABLE audit.etl_log;
GO

CREATE TABLE audit.etl_log (
    log_id        INT IDENTITY(1,1) PRIMARY KEY,
    batch_id      INT,                           -- Links all table loads in one run
    table_name    NVARCHAR(100),                 -- Name of the table or process
    start_time    DATETIME DEFAULT GETDATE(),    -- Execution start
    end_time      DATETIME,                      -- Execution end
    row_count     BIGINT,                        -- Upgraded to BIGINT for large datasets
    status        NVARCHAR(20),                  -- 'Success' or 'Failed'
    error_message NVARCHAR(MAX)                  -- System error details
);
GO

-- =============================================================================
-- 2. Create the Data Quality Table (Validation Tracking)
-- =============================================================================
IF OBJECT_ID('audit.data_quality_issues', 'U') IS NOT NULL
    DROP TABLE audit.data_quality_issues;
GO

CREATE TABLE audit.data_quality_issues (
    issue_id          INT IDENTITY(1,1) PRIMARY KEY,
    batch_id          INT,                         -- Groups errors by pipeline run
    table_name        NVARCHAR(100),               -- Table being validated
    check_name        NVARCHAR(100),               -- e.g., 'Row Count Match'
    expected_value    NVARCHAR(MAX),               -- Source Value (Bronze)
    actual_value      NVARCHAR(MAX),               -- Target Value (Silver)
    issue_description NVARCHAR(MAX),               -- MODIFIED: Upgraded to MAX to prevent truncation crashes
    check_layer        NVARCHAR(20),                -- 'Silver' or 'Gold'
    check_date        DATETIME DEFAULT GETDATE()   -- Timestamp of validation
);
GO

-- =============================================================================
-- 3. Create the ETL Configuration Table (Metadata Framework)
-- =============================================================================
IF OBJECT_ID('audit.etl_config', 'U') IS NOT NULL
    DROP TABLE audit.etl_config;
GO

CREATE TABLE audit.etl_config (
    config_id    INT IDENTITY(1,1) PRIMARY KEY,
    source_table NVARCHAR(255),  -- e.g., 'bronze.erp_loc_a101'
    target_table NVARCHAR(255) UNIQUE,  -- Added UNIQUE to prevent duplicate processing
    load_type    NVARCHAR(50),   -- 'FULL' or 'INCREMENTAL'
    is_active    BIT DEFAULT 1,  -- 1 = Active, 0 = Skip this table
    priority     INT DEFAULT 10  -- Order of execution (lower numbers first)
);
GO

-- =============================================================================
-- 4. Create the Watermark Tracking Table (Delta Load Framework)
-- =============================================================================
IF OBJECT_ID('audit.watermark_thresholds', 'U') IS NOT NULL
    DROP TABLE audit.watermark_thresholds;
GO

CREATE TABLE audit.watermark_thresholds (
    table_name       NVARCHAR(100) PRIMARY KEY,
    last_load_date   DATETIME,
    watermark_column NVARCHAR(50)  -- The column name used for filtering
);
GO

-- Seed the initial thresholds for CRM tables to enable Delta Loading
INSERT INTO audit.watermark_thresholds (table_name, last_load_date, watermark_column)
VALUES 
('silver.crm_cust_info', '1900-01-01', 'cst_create_date'),
('silver.crm_sales_details', '1900-01-01', 'sls_order_dt');

-- Seed the metadata configuration for ERP tables (Required for Metadata-Driven Engine)
INSERT INTO audit.etl_config (source_table, target_table, load_type, is_active, priority)
VALUES 
('bronze.erp_loc_a101', 'silver.erp_loc_a101', 'FULL', 1, 10),
('bronze.erp_cust_az12', 'silver.erp_cust_az12', 'FULL', 1, 10),
('bronze.erp_px_cat_g1v2', 'silver.erp_px_cat_g1v2', 'FULL', 1, 10);
GO
