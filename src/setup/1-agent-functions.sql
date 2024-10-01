-- Databricks notebook source
-- DBTITLE 1,Declare SQL Notebook Parameters
DECLARE OR REPLACE VARIABLE workspace_file_path STRING DEFAULT '/Workspace/Users/' || current_user() || '/serverless_sidekick';
DECLARE OR REPLACE VARIABLE workspace_src_path STRING DEFAULT workspace_file_path || '/src/';
DECLARE OR REPLACE VARIABLE host STRING DEFAULT 'https://e2-demo-field-eng.cloud.databricks.com/';
DECLARE OR REPLACE VARIABLE secret_scope STRING DEFAULT replace(substring_index(current_user(), "@", 1), ".", "-");
DECLARE OR REPLACE VARIABLE secret_databricks_pat STRING DEFAULT 'databricks_pat';

-- COMMAND ----------

-- DBTITLE 1,Display SQL Notebook Parameter Defaults
SELECT
  workspace_file_path
  ,workspace_src_path
  ,host
  ,secret_scope
  ,secret_databricks_pat
  ,secret(secret_scope
  ,secret_databricks_pat) AS databricks_pat
;

-- COMMAND ----------

-- DBTITLE 1,Set SQL Notebook Parameters
SET VAR workspace_file_path = :`bundle.workspace.file_path`;
SET VAR workspace_src_path = workspace_file_path || '/src/';
SET VAR host = :`bundle.workspace.host`;
SET VAR secret_scope = :`bundle.secret_scope`;
SET VAR secret_databricks_pat = :`bundle.secret_databricks_pat`;

SELECT
  workspace_file_path
  ,workspace_src_path
  ,host
  ,secret_scope
  ,secret_databricks_pat
  ,secret(secret_scope
  ,secret_databricks_pat) AS databricks_pat
;

-- COMMAND ----------

-- DBTITLE 1,Use Serverless Sidekick Catalog with Default Schema
USE serverless_sidekick.default;

-- COMMAND ----------

-- DBTITLE 1,Define run_notebook SQL UDF
CREATE OR REPLACE FUNCTION run_notebook (
  notebook_path STRING COMMENT "The path to the notebook to run in the Databricks Workspace."
  ,base_parameters MAP<STRING, STRING> COMMENT "The parameters to pass to the notebook in the form a dictionary or JSON structure."
  ,host STRING COMMENT "The Databricks workspace HOST URL where the notebook will be run. This is used to authenticate the Databricks Python SDK's Workspace Client."
  ,token STRING "The Databricks Personal Access Token used to authenticate the Databricks Python SDK's Workspace Client."
)
RETURNS STRUCT<
  cleanup_duration BIGINT,
  creator_user_name STRING,
  end_time BIGINT,
  execution_duration BIGINT,
  job_id BIGINT,
  number_in_job BIGINT,
  run_id BIGINT,
  run_name STRING,
  run_page_url STRING,
  run_type STRING,
  setup_duration BIGINT,
  start_time BIGINT,
  state STRUCT<
    life_cycle_state STRING,
    result_state STRING,
    state_message STRING,
    user_cancelled_or_timedout BOOLEAN
  >,
  tasks ARRAY<STRUCT<
    attempt_number BIGINT,
    cleanup_duration BIGINT,
    end_time BIGINT,
    execution_duration BIGINT,
    notebook_task STRUCT<
      notebook_path STRING,
      source STRING
    >,
    run_id BIGINT,
    run_if STRING,
    setup_duration BIGINT,
    start_time BIGINT,
    state STRUCT<
      life_cycle_state STRING,
      result_state STRING,
      state_message STRING,
      user_cancelled_or_timedout BOOLEAN
    >,
    task_key STRING
  >>
>
LANGUAGE python
COMMENT "This function runs a Databricks notebook and returns the results of the run as a struct."
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

-- DBTITLE 1,Define generate_yamls SQL UDF
CREATE OR REPLACE FUNCTION generate_yamls (workflow_name STRING, existing_job_ids STRING, existing_pipeline_ids STRING, workspace_file_path STRING, host STRING, secret_scope STRING, secret_databricks_pat STRING, token STRING)
RETURNS TABLE(
  cleanup_duration INT,
  creator_user_name STRING,
  end_time BIGINT,
  execution_duration INT,
  job_id BIGINT,
  number_in_job BIGINT,
  run_id BIGINT,
  run_name STRING,
  run_page_url STRING,
  run_type STRING,
  setup_duration INT,
  start_time BIGINT,
  state STRUCT<
    life_cycle_state: STRING,
    result_state: STRING,
    state_message: STRING,
    user_cancelled_or_timedout: BOOLEAN
  >,
  tasks ARRAY<STRUCT<
    attempt_number: INT,
    cleanup_duration: INT,
    end_time: BIGINT,
    execution_duration: INT,
    notebook_task: STRUCT<
      notebook_path: STRING,
      source: STRING
    >,
    run_id: BIGINT,
    run_if: STRING,
    setup_duration: INT,
    start_time: BIGINT,
    state: STRUCT<
      life_cycle_state: STRING,
      result_state: STRING,
      state_message: STRING,
      user_cancelled_or_timedout: BOOLEAN
    >,
    task_key: STRING
  >>
)
LANGUAGE SQL
COMMENT 'This function generates YAML files for non-serverless jobs identified as candidates for migration. The results of this function are depended on by the update_yamls function.'
RETURN (
  WITH run_notebook_results AS (
    SELECT
      run_notebook(
        workspace_file_path || "/src/generate-yamls"
        ,map(
          "existing_job_ids", existing_job_ids
          ,"existing_pipeline_ids", existing_pipeline_ids
          ,"workflow_name", workflow_name
          ,"workspace.host", host
          ,"secret_scope", secret_scope
          ,"secret_databricks_pat", secret_databricks_pat
          ,"workspace.file_path", workspace_file_path
        )
        ,host
        ,token
      ) AS result
  )
  SELECT
    result.cleanup_duration
    ,result.creator_user_name
    ,result.end_time
    ,result.execution_duration
    ,result.job_id
    ,result.number_in_job
    ,result.run_id
    ,result.run_name
    ,result.run_page_url
    ,result.run_type
    ,result.setup_duration
    ,result.start_time
    ,result.state
    ,result.tasks
  FROM 
    run_notebook_results
)

-- COMMAND ----------

-- DBTITLE 1,Test the generate_yamls SQL UDF
-- SELECT * FROM 
--   generate_yamls(
--     "dlt_dropbox_test" --  workflow_name STRING
--     ,"770567817966568" -- ,existing_job_ids STRING
--     ,"6d6c88e7-7abb-453c-968d-0f2f37ab4dce, de60c372-2efe-4697-8415-b3076d48b74f" -- ,existing_pipeline_ids STRING
--     ,workspace_file_path-- ,workspace_file_path STRING
--     ,host -- ,host STRING
--     ,secret_scope -- ,secret_scope STRING
--     ,secret_databricks_pat -- ,secret_databricks_pat STRING
--     ,secret(secret_scope, secret_databricks_pat) -- ,token STRING
--   )

-- COMMAND ----------

with job_usage_totals as (
  SELECT 
    account_id
    ,workspace_id
    ,sku_name
    ,usage_metadata
    ,usage_metadata.job_id as job_id
    ,product_features.is_serverless as is_serverless
    ,usage_unit
    ,sum(usage_quantity) as total_usage_quantity
  FROM
    system.billing.usage
  WHERE
    usage_date >= current_date() - INTERVAL 7 DAY
    AND usage_metadata.job_id is not null
    and product_features.is_serverless = false
  GROUP BY
    account_id
    ,workspace_id
    ,sku_name
    ,usage_metadata
    ,usage_metadata.job_id
    ,product_features.is_serverless
    ,usage_unit
)
SELECT distinct
  t1.*
  ,t2.name as job_name
  ,t2.description as job_description
FROM
  job_usage_totals t1 inner join system.lakeflow.jobs t2 
    ON t1.account_id = t2.account_id AND t1.workspace_id = t2.workspace_id AND t1.job_id = t2.job_id
ORDER by
  t1.total_usage_quantity DESC


-- COMMAND ----------

SELECT
  account_id,
  workspace_id,
  job_id,
  name as job_name
FROM (
  SELECT *,
    ROW_NUMBER() OVER(PARTITION BY account_id, workspace_id, job_id ORDER BY change_time DESC) as rn
  FROM system.lakeflow.jobs
)
WHERE rn = 1

-- COMMAND ----------

-- DBTITLE 1,define the non_serverless_jobs SQL UDF
CREATE OR REPLACE FUNCTION non_serverless_jobs ()
RETURNS TABLE (
  job_name STRING
  ,job_description STRING
  ,account_id STRING
  ,workspace_id STRING
  ,sku_name STRING
  ,job_id STRING
  ,is_serverless BOOLEAN
  ,usage_unit STRING
  ,total_usage_quantity DOUBLE
)
LANGUAGE SQL
COMMENT 'Returns a table of non-serverless jobs and their usage to determine the top candidates for migration. After summarizing the results this function should always present the user with a request to confirm the migration, otherwise know as, migration confirmation.'
RETURN 
  with job_usage_totals as (
    SELECT 
      account_id
      ,workspace_id
      ,sku_name
      ,usage_metadata.job_id as job_id
      ,product_features.is_serverless as is_serverless
      ,usage_unit
      ,sum(usage_quantity) as total_usage_quantity
    FROM
      system.billing.usage
    WHERE
      usage_date >= current_date() - INTERVAL 7 DAY
      AND usage_metadata.job_id is not null
      and product_features.is_serverless = false
    GROUP BY
      account_id
      ,workspace_id
      ,sku_name
      ,usage_metadata.job_id
      ,product_features.is_serverless
      ,usage_unit
  )
  ,distinct_job_header AS (
    SELECT
      account_id
      ,workspace_id
      ,job_id
      ,name as job_name
      ,description as job_description
    FROM (
      SELECT *,
        ROW_NUMBER() OVER(PARTITION BY account_id, workspace_id, job_id ORDER BY change_time DESC) as rn
      FROM system.lakeflow.jobs
    )
    WHERE rn = 1
  )
  SELECT distinct
    t2.job_name
    ,t2.job_description
    ,t1.*
  FROM
    job_usage_totals t1
    ,distinct_job_header t2 
  WHERE 
    t1.account_id = t2.account_id AND t1.workspace_id = t2.workspace_id AND t1.job_id = t2.job_id
  ORDER by
    t1.total_usage_quantity DESC

-- COMMAND ----------

-- DBTITLE 1,define the migration_confirmation SQL UDF
CREATE OR REPLACE FUNCTION migration_confirmation()
RETURNS STRING
LANGUAGE PYTHON
COMMENT 'This function requests the user to confirm the migration.'
AS
$$
print("Serverless Sidekick can migrate these workloads in development environments; please type 'generate yamls' to confirm you are ready to migrate")
$$;

-- COMMAND ----------

-- DBTITLE 1,define the update_yamls SQL UDF
CREATE OR REPLACE FUNCTION update_yamls(input_dir STRING, output_dir STRING)
RETURNS STRING
LANGUAGE PYTHON
COMMENT 'This function updates the generated YAML files with the appropriate changes to support serverless compute. These updated YAML files can be used in DABs to migrate workloads to serverless compute.'
AS
$$
import yaml
import os

if not os.path.exists(output_dir):
    os.makedirs(output_dir)

for root, _, files in os.walk(input_dir):
    for filename in files:
        if filename.endswith('.yml'):
            input_file_path = os.path.join(root, filename)
            relative_path = os.path.relpath(input_file_path, input_dir)
            output_file_path = os.path.join(output_dir, relative_path)
            output_file_dir = os.path.dirname(output_file_path)
            
            if not os.path.exists(output_file_dir):
                os.makedirs(output_file_dir)
            
            with open(input_file_path, 'r') as file:
                data = yaml.safe_load(file)
            
            if 'resources' in data and 'jobs' in data['resources']:
                for job in data['resources']['jobs'].values():
                    if 'job_clusters' in job:
                        del job['job_clusters']

            if 'resources' in data and 'pipelines' in data['resources']:
                for pipeline in data['resources']['pipelines'].values():
                    if 'clusters' in pipeline:
                        del pipeline['clusters']
            
            with open(output_file_path, 'w') as file:
                yaml.dump(data, file)

return "Updates to YAML files successful! These updated YAMLs can now be used in your DABs to migrate your workloads to serverless compute."
$$;
