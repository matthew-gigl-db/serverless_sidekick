# Databricks notebook source
# MAGIC %sql
# MAGIC use serverless_sidekick.default;

# COMMAND ----------

dbutils.widgets.text("bundle.workspace.file_path", "..")

# COMMAND ----------

# MAGIC %sql
# MAGIC DECLARE OR REPLACE VARIABLE work

# COMMAND ----------

from databricks.sdk import WorkspaceClient
from databricks.sdk.service import jobs

w = WorkspaceClient()

notebook_path = f'/Users/{w.current_user.me().user_name}/sdk-{time.time_ns()}'

cluster_id = w.clusters.ensure_cluster_is_running(
    os.environ["DATABRICKS_CLUSTER_ID"]) and os.environ["DATABRICKS_CLUSTER_ID"]

run = w.jobs.submit(run_name=f'sdk-{time.time_ns()}',
                    tasks=[
                        jobs.SubmitTask(existing_cluster_id=cluster_id,
                                        notebook_task=jobs.NotebookTask(notebook_path=notebook_path),
                                        task_key=f'sdk-{time.time_ns()}')
                    ]).result()

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace function run_notebook (notebook_path string)
# MAGIC returns table ()
