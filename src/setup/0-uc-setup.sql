-- Databricks notebook source
create catalog if not exists serverless_sidekick;

-- COMMAND ----------

-- DBTITLE 1,Grant Catalog Permissions to Serverless Sidekick SPN Group
-- Note:  change to group instead of users
GRANT ALL PRIVILEGES ON CATALOG serverless_sidekick TO `emma.yamada@databricks.com`;
GRANT ALL PRIVILEGES ON CATALOG serverless_sidekick TO `matthew.giglia@databricks.com`;

-- COMMAND ----------

use serverless_sidekick.default;

-- COMMAND ----------

create volume if not exists original_yamls;

-- COMMAND ----------

create volume if not exists updated_yamls;
