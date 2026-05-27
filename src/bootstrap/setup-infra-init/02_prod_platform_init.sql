-- ==============================================================================
-- MODULE: PLATFORM INITIALIZATION (PRODUCTION TIER 1)
-- EXECUTION: MANUAL VIA DATABRICKS SQL EDITOR (METASTORE ADMIN ONLY)
-- TARGET WORKSPACE: dbw-ecom-dlt-prod-001
-- ==============================================================================

GRANT CREATE CATALOG ON METASTORE TO `563a4545-bb88-4c4d-89b8-8714ec7e2232`;
GRANT CREATE CATALOG ON METASTORE TO `aamir.***@gmail.com`;

-- 1. Create the Managed Storage Boundary (For Delta Tables & DLT State)
CREATE EXTERNAL LOCATION IF NOT EXISTS ext_loc_ecom_managed_prod
URL 'abfss://managed-zone@staecomdltprod001.dfs.core.windows.net/'
WITH (STORAGE CREDENTIAL cred_ecom_landing_prod)
COMMENT 'Physical isolation boundary for Prod managed tables';

-- 2. Create the Raw Landing Storage Boundary (For Autoloader Ingestion)
CREATE EXTERNAL LOCATION IF NOT EXISTS ext_loc_ecom_landing_prod
URL 'abfss://landing-zone@staecomdltprod001.dfs.core.windows.net/'
WITH (STORAGE CREDENTIAL cred_ecom_landing_prod)
COMMENT 'Physical isolation boundary for inbound Prod JSON/CSV data';

CREATE EXTERNAL LOCATION IF NOT EXISTS ext_loc_ecom_system_prod
URL 'abfss://system-zone@staecomdltprod001.dfs.core.windows.net/'
WITH (STORAGE CREDENTIAL cred_ecom_landing_prod);

-- 3. Create the Prod Catalog
CREATE CATALOG IF NOT EXISTS cat_ecom_prod
MANAGED LOCATION 'abfss://managed-zone@staecomdltprod001.dfs.core.windows.net/'
COMMENT 'Production E-Commerce Data Catalog';

-- ==============================================================================
-- PRODUCTION SERVICE PRINCIPAL SANDBOX (spn-ecom-cicd-prod)
-- ==============================================================================
-- Grant schema manipulation rights within the catalog shell
GRANT USE CATALOG, CREATE SCHEMA ON CATALOG cat_ecom_prod TO `6f4ab106-75f1-406b-8276-18009e2f879d`;

-- Landing Zone: Required for Autoloader file streaming
GRANT CREATE EXTERNAL VOLUME, READ FILES, WRITE FILES ON EXTERNAL LOCATION ext_loc_ecom_landing_prod TO `6f4ab106-75f1-406b-8276-18009e2f879d`;

-- Managed Zone: Required for DLT cluster state tracking and metadata logging
GRANT CREATE MANAGED STORAGE, READ FILES, WRITE FILES ON EXTERNAL LOCATION ext_loc_ecom_managed_prod TO `6f4ab106-75f1-406b-8276-18009e2f879d`;

-- System Zone: Checkpoint location volume.
GRANT CREATE EXTERNAL VOLUME, READ FILES, WRITE FILES ON EXTERNAL LOCATION ext_loc_ecom_system_prod TO `6f4ab106-75f1-406b-8276-18009e2f879d`;

-- ==============================================================================
-- PRODUCTION HUMAN ACCESS to PROD (spn-ecom-cicd-prod)
-- ==============================================================================
-- 1. Grant visibility to the Catalog
GRANT USE CATALOG ON CATALOG cat_ecom_prod TO `aamir.***@gmail.com`;

-- 2. Grant visibility to the Target Schema(s)
GRANT USE SCHEMA ON SCHEMA cat_ecom_prod.bronze TO `aamir.***@gmail.com`;
GRANT USE SCHEMA ON SCHEMA cat_ecom_prod.silver TO `aamir.***@gmail.com`;
GRANT USE SCHEMA ON SCHEMA cat_ecom_prod.gold TO `aamir.***@gmail.com`;

-- 3. Grant Read Access to the underlying tables (Read-Only)
GRANT SELECT ON SCHEMA cat_ecom_prod.gold TO `aamir.***@gmail.com`;
GRANT SELECT ON SCHEMA cat_ecom_prod.bronze TO `aamir.***@gmail.com`;
GRANT SELECT ON SCHEMA cat_ecom_prod.silver TO `aamir.***@gmail.com`;