-- Databricks notebook source
create catalog if not exists serverless_sidekick;

-- COMMAND ----------

use serverless_sidekick.default;

-- COMMAND ----------

create volume if not exists updated_yamls;
