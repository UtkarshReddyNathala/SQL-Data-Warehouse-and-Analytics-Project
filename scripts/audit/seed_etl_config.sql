/*
===============================================================================
Seed Script: Populate ETL Configuration (Metadata)
===============================================================================
*/

-- Clear existing config to avoid duplicates during testing
TRUNCATE TABLE audit.etl_config;

INSERT INTO audit.etl_config (source_table, target_table, load_type, priority)
VALUES 
('bronze.erp_loc_a101',    'silver.erp_loc_a101',    'FULL', 10),
('bronze.erp_cust_az12',   'silver.erp_cust_az12',   'FULL', 20),
('bronze.erp_px_cat_g1v2', 'silver.erp_px_cat_g1v2', 'FULL', 30);

PRINT '>> ETL Configuration Seeded Successfully.';
