-- Databricks notebook source
-- DBTITLE 1,Declare SQL Notebook Parameters
DECLARE OR REPLACE VARIABLE workspace_src_path STRING DEFAULT '/Workspace/Users/' || current_user() || '/serverless_sidekick';
DECLARE OR REPLACE VARIABLE host STRING DEFAULT 'https://e2-demo-field-eng.cloud.databricks.com/';
DECLARE OR REPLACE VARIABLE secret_scope STRING DEFAULT replace(substring_index(current_user(), "@", 1), ".", "-");
DECLARE OR REPLACE VARIABLE secret_databricks_pat STRING DEFAULT 'databricks_pat';

-- COMMAND ----------

-- DBTITLE 1,Display SQL Notebook Parameter Defaults
SELECT
  workspace_src_path
  ,host
  ,secret_scope
  ,secret_databricks_pat
  ,secret(secret_scope
  ,secret_databricks_pat) AS databricks_pat
;

-- COMMAND ----------

-- DBTITLE 1,Set SQL Notebook Parameters
SET VAR workspace_src_path = :`bundle.workspace.file_path` || '/src/';
SET VAR host = :`bundle.workspace.host`;
SET VAR secret_scope = :`bundle.secret_scope`;
SET VAR secret_databricks_pat = :`bundle.secret_databricks_pat`;

SELECT
  workspace_src_path
  ,host
  ,secret_scope
  ,secret_databricks_pat
  ,secret(secret_scope
  ,secret_databricks_pat) AS databricks_pat
;

-- COMMAND ----------

-- DBTITLE 1,Use Serverless Sidekick Catalog  with Default Schema
USE serverless_sidekick.default;

-- COMMAND ----------

-- DBTITLE 1,Define run_notebook SQL UDF
CREATE OR REPLACE FUNCTION run_notebook (notebook_path STRING, base_parameters MAP<STRING, STRING>, host STRING, token STRING)
RETURNS STRING
LANGUAGE python
AS $$ 

    from databricks.sdk import WorkspaceClient
    from databricks.sdk.service import jobs
    import time

    w = WorkspaceClient(
        host = host
        ,token = token
    )

    run = w.jobs.submit(run_name=f'serverless-sidekick-{time.time_ns()}',
                        tasks=[
                            jobs.SubmitTask(
                                notebook_task=jobs.NotebookTask(notebook_path=notebook_path, base_parameters=base_parameters),
                                task_key=f'sdk-{time.time_ns()}')
                        ]).result()

    return run.as_dict()

$$

-- COMMAND ----------

-- DBTITLE 1,Test the run_notebook SQL UDF
SELECT 
  run_notebook(
    workspace_src_path || "generate-yamls"
    ,map("existing_job_ids", "1, 2, 3")
    ,host
    ,secret(secret_scope, secret_databricks_pat)
  )
