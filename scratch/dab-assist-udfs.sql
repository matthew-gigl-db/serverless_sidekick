-- Databricks notebook source
-- DBTITLE 1,Use serverless_sidekick Catalog and default Schmea
use serverless_sidekick.default;

-- COMMAND ----------

CREATE OR REPLACE FUNCTION temp_directory ()
RETURNS STRING
LANGUAGE PYTHON
AS $$ 
  from tempfile import TemporaryDirectory

  Dir = TemporaryDirectory()
  return Dir.name
$$

-- COMMAND ----------

select temp_directory();

-- COMMAND ----------

CREATE OR REPLACE FUNCTION touch_file (file_path STRING, file_name STRING)
RETURNS STRING
LANGUAGE PYTHON
AS $$ 
  import subprocess

  cmd = f"mkdir -p {file_path}; touch {file_path}/{file_name}"
  result = subprocess.run(cmd, shell=True, capture_output=True)
  response = result.stdout.decode("utf-8") + "\n" + result.stderr.decode("utf-8")
  return subprocess.run(f"ls {file_path}", shell=True, capture_output=True).stdout.decode("utf-8")
$$

-- COMMAND ----------

select touch_file(temp_directory(), "test.txt");

-- COMMAND ----------

CREATE OR REPLACE FUNCTION home_directory ()
RETURNS STRING
LANGUAGE PYTHON
AS $$ 
  from pathlib import Path

  return str(Path.home())
$$

-- COMMAND ----------

select home_directory();

-- COMMAND ----------

CREATE OR REPLACE FUNCTION databricks_cli (workspace_url STRING, pat STRING)
RETURNS STRING
LANGUAGE PYTHON
AS $$
  import subprocess
  
  cmd = f"curl -fsSL https://raw.githubusercontent.com/databricks/setup-cli/main/install.sh | sh"
  #cmd = f"curl --create-dirs -O --output-dir /tmp/serverless-sidekick/databricks -fsSL https://raw.githubusercontent.com/databricks/setup-cli/main/install.sh; ls -alt /tmp/serverless-sidekick/databricks;"
  result = subprocess.run(cmd, shell=True, capture_output=True)
  response = result.stdout.decode("utf-8") + "\n" + result.stderr.decode("utf-8")
  #if response[0:4] == "Inst":
  #  cli_path = response.split("at ")[1].replace(".", "").strip("\n")
  #else:
  #  cli_path = response.split(" ")[2].strip("\n")
  #version = subprocess.run(f"{cli_path} --version", shell=True, capture_output=True)
  #print(f"databricks-cli installed at {cli_path} version {version.stdout.decode('utf-8').strip()}")
  #self.cli_path = cli_path
  #return cli_path
  return response
$$;

-- COMMAND ----------

select databricks_cli("https://e2-demo-field-eng.cloud.databricks.com/", secret('matthew-giglia', 'databricks_pat'));
