# Databricks notebook source
# MAGIC %sql
# MAGIC GRANT CREATE EXTERNAL LOCATION ON STORAGE CREDENTIAL cred_ecom_landing_dev TO `metastore_admins`;

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER STORAGE CREDENTIAL cred_ecom_landing_dev SET OWNER TO `metastore_admins`;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE EXTERNAL LOCATION IF NOT EXISTS ext_loc_ecom_landing_dev
# MAGIC URL 'abfss://landing-zone@staecomdltdev001.dfs.core.windows.net/'
# MAGIC WITH (STORAGE CREDENTIAL cred_ecom_landing_dev)
# MAGIC COMMENT 'E-commerce Raw Ingestion Landing Zone';

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE EXTERNAL LOCATION IF NOT EXISTS ext_loc_ecom_managed_dev
# MAGIC   URL 'abfss://managed-zone@staecomdltdev001.dfs.core.windows.net/'
# MAGIC   WITH (STORAGE CREDENTIAL cred_ecom_landing_dev);

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER EXTERNAL LOCATION ext_loc_ecom_managed_dev OWNER TO `metastore_admins`;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Enforces physical separation. All managed tables write to the dev managed-zone.
# MAGIC CREATE CATALOG IF NOT EXISTS cat_ecom_dev
# MAGIC   MANAGED LOCATION 'abfss://managed-zone@staecomdltdev001.dfs.core.windows.net/';
# MAGIC ALTER CATALOG cat_ecom_dev OWNER TO `metastore_admins`;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 1. Create the Medallion Processing Schemas
# MAGIC CREATE SCHEMA IF NOT EXISTS cat_ecom_dev.raw;
# MAGIC ALTER SCHEMA cat_ecom_dev.raw OWNER TO `metastore_admins`;
# MAGIC
# MAGIC CREATE SCHEMA IF NOT EXISTS cat_ecom_dev.bronze;
# MAGIC ALTER SCHEMA cat_ecom_dev.bronze OWNER TO `metastore_admins`;
# MAGIC
# MAGIC CREATE SCHEMA IF NOT EXISTS cat_ecom_dev.silver;
# MAGIC ALTER SCHEMA cat_ecom_dev.silver OWNER TO `metastore_admins`;
# MAGIC
# MAGIC CREATE SCHEMA IF NOT EXISTS cat_ecom_dev.gold;
# MAGIC ALTER SCHEMA cat_ecom_dev.gold OWNER TO `metastore_admins`;
# MAGIC
# MAGIC -- 2. Mount the ADLS Gen2 Landing Zone to the Raw Schema
# MAGIC CREATE EXTERNAL VOLUME IF NOT EXISTS cat_ecom_dev.raw.vol_landing_zone
# MAGIC   LOCATION 'abfss://landing-zone@staecomdltdev001.dfs.core.windows.net/'
# MAGIC   COMMENT 'External volume pointing to the 9 Olist raw CSV directories';
# MAGIC
# MAGIC ALTER VOLUME cat_ecom_dev.raw.vol_landing_zone OWNER TO `metastore_admins`;