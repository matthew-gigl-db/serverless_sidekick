-- Databricks notebook source
create catalog if not exists serverless_sidekick;

-- COMMAND ----------

GRANT ALL PRIVILEGES ON CATALOG serverless_sidekick TO `emma.yamada@databricks.com`, `matthew.giglia@databricks.com`;

-- COMMAND ----------

use serverless_sidekick.default;

-- COMMAND ----------

create volume if not exists original_yamls;

-- COMMAND ----------

create volume if not exists updated_yamls;
