-- drop catalog cat_ecom_dev cascade;
-- ==============================================================================
-- DEV PLATFORM INITIALIZATION: MAX PRIVILEGE CONFIGURATION
-- ==============================================================================

GRANT CREATE CATALOG ON METASTORE TO `563a4545-bb88-4c4d-89b8-8714ec7e2232`;
GRANT CREATE CATALOG ON METASTORE TO `aamir.***@gmail.com`;

-- 1. Create the External Locations
CREATE EXTERNAL LOCATION IF NOT EXISTS ext_loc_ecom_managed_dev
URL 'abfss://managed-zone@staecomdltdev001.dfs.core.windows.net/'
WITH (STORAGE CREDENTIAL cred_ecom_landing_dev);

CREATE EXTERNAL LOCATION IF NOT EXISTS ext_loc_ecom_landing_dev
URL 'abfss://landing-zone@staecomdltdev001.dfs.core.windows.net/'
WITH (STORAGE CREDENTIAL cred_ecom_landing_dev);

CREATE EXTERNAL LOCATION IF NOT EXISTS ext_loc_ecom_system_dev
URL 'abfss://system-zone@staecomdltdev001.dfs.core.windows.net/'
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

-- System Zone: Checkpoint vol location, etc.
GRANT CREATE EXTERNAL VOLUME, READ FILES, WRITE FILES ON EXTERNAL LOCATION ext_loc_ecom_system_dev TO `563a4545-bb88-4c4d-89b8-8714ec7e2232`;


-- ==============================================================================
-- HUMAN OVERRIDE BOUNDARY (aamir.***@gmail.com)
-- ==============================================================================
-- Grants absolute maximum authority over the underlying credential
GRANT ALL PRIVILEGES ON STORAGE CREDENTIAL cred_ecom_landing_dev TO `aamir.***@gmail.com`;

-- Grants absolute maximum authority over physical files
GRANT ALL PRIVILEGES ON EXTERNAL LOCATION ext_loc_ecom_landing_dev TO `aamir.***@gmail.com`;
GRANT ALL PRIVILEGES ON EXTERNAL LOCATION ext_loc_ecom_managed_dev TO `aamir.***@gmail.com`;
GRANT ALL PRIVILEGES ON EXTERNAL LOCATION ext_loc_ecom_system_dev TO `aamir.***@gmail.com`;

-- Grants absolute maximum authority over logical metadata
GRANT ALL PRIVILEGES ON CATALOG cat_ecom_dev TO `aamir.***@gmail.com`;