# Databricks notebook source
# MAGIC %sql
# MAGIC use serverless_sidekick.default;

# COMMAND ----------

dbutils.widgets.text("bundle.workspace.file_path", "..")

# COMMAND ----------

from databricks.sdk import WorkspaceClient
from databricks.sdk.service import jobs
import time
import os

# COMMAND ----------

help(WorkspaceClient)

# COMMAND ----------



w = WorkspaceClient(
    host="https://e2-demo-field-eng.cloud.databricks.com",
    token=dbutils.secrets.get("matthew-giglia", "databricks_pat")
)

notebook_path = os.path.abspath("../src/generate-yamls")

run = w.jobs.submit(run_name=f'serverless-sidekick-{time.time_ns()}',
                    tasks=[
                        jobs.SubmitTask(
                            notebook_task=jobs.NotebookTask(notebook_path=notebook_path),
                            task_key=f'sdk-{time.time_ns()}')
                    ]).result()

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType

# COMMAND ----------

run.as_dict()

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, LongType, BooleanType, ArrayType

run_schema = StructType([
    StructField("cleanup_duration", IntegerType(), True),
    StructField("creator_user_name", StringType(), True),
    StructField("end_time", LongType(), True),
    StructField("execution_duration", IntegerType(), True),
    StructField("job_id", LongType(), True),
    StructField("number_in_job", LongType(), True),
    StructField("run_id", LongType(), True),
    StructField("run_name", StringType(), True),
    StructField("run_page_url", StringType(), True),
    StructField("run_type", StringType(), True),
    StructField("setup_duration", IntegerType(), True),
    StructField("start_time", LongType(), True),
    StructField("state", StructType([
        StructField("life_cycle_state", StringType(), True),
        StructField("result_state", StringType(), True),
        StructField("state_message", StringType(), True),
        StructField("user_cancelled_or_timedout", BooleanType(), True)
    ]), True),
    StructField("tasks", ArrayType(StructType([
        StructField("attempt_number", IntegerType(), True),
        StructField("cleanup_duration", IntegerType(), True),
        StructField("end_time", LongType(), True),
        StructField("execution_duration", IntegerType(), True),
        StructField("notebook_task", StructType([
            StructField("notebook_path", StringType(), True),
            StructField("source", StringType(), True)
        ]), True),
        StructField("run_id", LongType(), True),
        StructField("run_if", StringType(), True),
        StructField("setup_duration", IntegerType(), True),
        StructField("start_time", LongType(), True),
        StructField("state", StructType([
            StructField("life_cycle_state", StringType(), True),
            StructField("result_state", StringType(), True),
            StructField("state_message", StringType(), True),
            StructField("user_cancelled_or_timedout", BooleanType(), True)
        ]), True),
        StructField("task_key", StringType(), True)
    ]), True))
])

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW run_schema (
# MAGIC   cleanup_duration INT,
# MAGIC   creator_user_name STRING,
# MAGIC   end_time BIGINT,
# MAGIC   execution_duration INT,
# MAGIC   job_id BIGINT,
# MAGIC   number_in_job BIGINT,
# MAGIC   run_id BIGINT,
# MAGIC   run_name STRING,
# MAGIC   run_page_url STRING,
# MAGIC   run_type STRING,
# MAGIC   setup_duration INT,
# MAGIC   start_time BIGINT,
# MAGIC   state STRUCT<
# MAGIC     life_cycle_state: STRING,
# MAGIC     result_state: STRING,
# MAGIC     state_message: STRING,
# MAGIC     user_cancelled_or_timedout: BOOLEAN
# MAGIC   >,
# MAGIC   tasks ARRAY<STRUCT<
# MAGIC     attempt_number: INT,
# MAGIC     cleanup_duration: INT,
# MAGIC     end_time: BIGINT,
# MAGIC     execution_duration: INT,
# MAGIC     notebook_task: STRUCT<
# MAGIC       notebook_path: STRING,
# MAGIC       source: STRING
# MAGIC     >,
# MAGIC     run_id: BIGINT,
# MAGIC     run_if: STRING,
# MAGIC     setup_duration: INT,
# MAGIC     start_time: BIGINT,
# MAGIC     state: STRUCT<
# MAGIC       life_cycle_state: STRING,
# MAGIC       result_state: STRING,
# MAGIC       state_message: STRING,
# MAGIC       user_cancelled_or_timedout: BOOLEAN
# MAGIC     >,
# MAGIC     task_key: STRING
# MAGIC   >>
# MAGIC )

# COMMAND ----------

df = spark.createDataFrame([run.as_dict()], schema=run_schema)
display(df)

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType

# Define the schema explicitly
schema = StructType([
    StructField("notebook_path", StringType(), True),
    StructField("source", StringType(), True)
])

# Create the DataFrame using the defined schema
df = spark.createDataFrame([run.as_dict()], schema=schema)

# COMMAND ----------

display(df)

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace function run_notebook (notebook_path string)
# MAGIC returns table ()
