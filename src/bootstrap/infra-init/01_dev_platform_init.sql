-- ==============================================================================
-- DEV PLATFORM INITIALIZATION: MAX PRIVILEGE CONFIGURATION
-- ==============================================================================

-- 1. Create the External Locations
CREATE EXTERNAL LOCATION IF NOT EXISTS ext_loc_ecom_managed_dev
URL 'abfss://managed-zone@staecomdltdev001.dfs.core.windows.net/'
WITH (STORAGE CREDENTIAL cred_ecom_landing_dev);

CREATE EXTERNAL LOCATION IF NOT EXISTS ext_loc_ecom_landing_dev
URL 'abfss://landing-zone@staecomdltdev001.dfs.core.windows.net/'
WITH (STORAGE CREDENTIAL cred_ecom_landing_dev);

-- 2. Create the Dev Catalog
CREATE CATALOG IF NOT EXISTS cat_ecom_dev
MANAGED LOCATION 'abfss://managed-zone@staecomdltdev001.dfs.core.windows.net/';


-- ==============================================================================
-- SPN EXECUTION BOUNDARY (spn-ecom-cicd-dev)
-- ==============================================================================
GRANT USE CATALOG, CREATE SCHEMA ON CATALOG cat_ecom_dev TO `563a4545-bb88-4c4d-89b8-8714ec7e2232`;

-- Landing Zone: Required for Autoloader reads and Bronze Checkpoint writes
GRANT CREATE EXTERNAL VOLUME, READ FILES, WRITE FILES ON EXTERNAL LOCATION ext_loc_ecom_landing_dev TO `563a4545-bb88-4c4d-89b8-8714ec7e2232`;

-- Managed Zone: Required for DLT to instantiate hidden pipeline state directories
GRANT CREATE MANAGED STORAGE, READ FILES, WRITE FILES ON EXTERNAL LOCATION ext_loc_ecom_managed_dev TO `563a4545-bb88-4c4d-89b8-8714ec7e2232`;


-- ==============================================================================
-- HUMAN OVERRIDE BOUNDARY (aamir.mscse@gmail.com)
-- ==============================================================================
-- Grants absolute maximum authority over the underlying credential
GRANT ALL PRIVILEGES ON STORAGE CREDENTIAL cred_ecom_landing_dev TO `aamir.mscse@gmail.com`;

-- Grants absolute maximum authority over physical files
GRANT ALL PRIVILEGES ON EXTERNAL LOCATION ext_loc_ecom_landing_dev TO `aamir.mscse@gmail.com`;
GRANT ALL PRIVILEGES ON EXTERNAL LOCATION ext_loc_ecom_managed_dev TO `aamir.mscse@gmail.com`;

-- Grants absolute maximum authority over logical metadata
GRANT ALL PRIVILEGES ON CATALOG cat_ecom_dev TO `aamir.mscse@gmail.com`;