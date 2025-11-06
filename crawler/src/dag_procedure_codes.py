import boto3
from botocore.exceptions import ClientError

import awswrangler as wr
from datetime import datetime, timedelta

from airflow.models.dag import DAG
from airflow.models import Variable
from utils.utils_dags import athena_query_execution_wr
from airflow.operators.empty import EmptyOperator
from airflow.providers.docker.operators.docker import DockerOperator
from utils.meditech_dags import ContainerOperator_Single_Server
from airflow.operators.python_operator import PythonOperator
import json
from utils.secret_manager import get_secret
from configs.ecr_images import ECR_CRAWLER_IMAGE

from docker.types import Mount

RUN_IN_EKS = True
AWS_CREDENTIALS_ABSOLUTE_PATH = ''

AAPC_SECRET_ID = 'xxxxxxxxxxxxxxxxxxxxxx'
ATHENA_QUERY_OUTPUT_LOCATION = 'xxxxxxxxxxxxxxxxxxxxxxxxx'

ATHENA_OUTPUT_PROCEDURE_CODES_TABLE_SCHEMA = 'xxxxxxxx'
ATHENA_OUTPUT_PROCEDURE_CODES_TABLE_NAME = 'xxxxxxxxxxxx'
ATHENA_OUTPUT_PROCEDURE_CODES_TABLE_LOCATION = f'xxxxxxxxxxxxxxxxxxxxxxxxx{ATHENA_OUTPUT_PROCEDURE_CODES_TABLE_SCHEMA}/{ATHENA_OUTPUT_PROCEDURE_CODES_TABLE_NAME}/'

ATHENA_OUTPUT_PROCEDURE_MODIFIERS_TABLE_SCHEMA = 'xxxxxxxxx'
ATHENA_OUTPUT_PROCEDURE_MODIFIERS_TABLE_NAME = 'xxxxxxxxxxx'
ATHENA_OUTPUT_PROCEDURE_MODIFIERS_TABLE_LOCATION = f'xxxxxxxxxxxxxxxxxxxxxxxxxxx{ATHENA_OUTPUT_PROCEDURE_MODIFIERS_TABLE_SCHEMA}/{ATHENA_OUTPUT_PROCEDURE_MODIFIERS_TABLE_NAME}/'

ATHENA_OUTPUT_PROCEDURE_NDC_TABLE_SCHEMA = 'xxxxxxxxxxxxxxxxxxx'
ATHENA_OUTPUT_PROCEDURE_NDC_TABLE_NAME = 'xxxxxxxxxxxxxx'
ATHENA_OUTPUT_PROCEDURE_NDC_TABLE_LOCATION = f'xxxxxxxxxxxxxxxxxxxxxxxxxxxxx{ATHENA_OUTPUT_PROCEDURE_NDC_TABLE_SCHEMA}/{ATHENA_OUTPUT_PROCEDURE_NDC_TABLE_NAME}/'

ATHENA_PROCEDURE_CODES_DDL = f"""
  CREATE EXTERNAL TABLE IF NOT EXISTS `{ATHENA_OUTPUT_PROCEDURE_CODES_TABLE_SCHEMA}.{ATHENA_OUTPUT_PROCEDURE_CODES_TABLE_NAME}`(
    `code` string, 
    `code_type` string, 
    `main_interval` string, 
    `main_interval_name` string, 
    `modifiers` array<string>, 
    `short_description` string, 
    `long_description` string, 
    `description` string,
    `summary` string, 
    `date_deleted` string, 
    `betos_code` string, 
    `betos_description` string, 
    `guidelines` string, 
    `advice` string, 
    `lay_term` string, 
    `report` string, 
    `revenue_lookup` array<string>, 
    `icd10_cm` array<string>, 
    `ndc_alternate_id` array<string>, 
    `icd_10_pcs_x` string, 
    `cpt_code_symbols` array<string>
  )
  ROW FORMAT SERDE 
    'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
  STORED AS INPUTFORMAT 
    'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 
  OUTPUTFORMAT 
    'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
  LOCATION
    '{ATHENA_OUTPUT_PROCEDURE_CODES_TABLE_LOCATION}'
  TBLPROPERTIES (
    'classification'='parquet', 
    'compressionType'='snappy', 
    'projection.enabled'='false', 
    'typeOfData'='file'
  )
"""

ATHENA_PROCEDURE_CODES_MODIFIERS_DDL = f"""
  CREATE EXTERNAL TABLE IF NOT EXISTS `{ATHENA_OUTPUT_PROCEDURE_MODIFIERS_TABLE_SCHEMA}.{ATHENA_OUTPUT_PROCEDURE_MODIFIERS_TABLE_NAME}`(
    `modifier` string,
    `description` string
  )
  ROW FORMAT SERDE 
    'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
  STORED AS INPUTFORMAT 
    'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 
  OUTPUTFORMAT 
    'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
  LOCATION
    '{ATHENA_OUTPUT_PROCEDURE_MODIFIERS_TABLE_LOCATION}'
  TBLPROPERTIES (
    'classification'='parquet', 
    'compressionType'='snappy', 
    'projection.enabled'='false', 
    'typeOfData'='file'
  )
"""
ATHENA_PROCEDURE_NDC_DDL = f"""
CREATE EXTERNAL TABLE IF NOT EXISTS `{ATHENA_OUTPUT_PROCEDURE_NDC_TABLE_SCHEMA}.{ATHENA_OUTPUT_PROCEDURE_NDC_TABLE_NAME}`(
    `ndc_alternate_id` string, 
    `drug_name` string, 
    `labeler_name` string, 
    `hcpcs_dosage` string, 
    `bill_unit` string
  )
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  '{ATHENA_OUTPUT_PROCEDURE_NDC_TABLE_LOCATION}'
TBLPROPERTIES (
  'classification'='parquet', 
  'compressionType'='snappy', 
  'projection.enabled'='false', 
  'typeOfData'='file')"""


LOGICAL_DATE = '{{ ds }}'


with DAG(
  dag_id            = 'procedure_codes_modifier',
  schedule          = timedelta(days=7),
  start_date        = datetime(2024, 4, 1, 3, 0), # min date parser source : 2024-04-13
  catchup           = True,
  max_active_runs   = 1,
  default_args      = {},
  tags              = ['athena', 'analytics'],
) as dag:
  
  start = EmptyOperator(
      task_id='start',
      wait_for_downstream=True,
  )
  
  end = EmptyOperator(
      task_id='end',
      wait_for_downstream=True,
  )
  
  create_procedure_codes_table = PythonOperator(
    task_id = 'create_procedure_codes_table',
    python_callable = athena_query_execution_wr,
    op_kwargs = {
      'sql': ATHENA_PROCEDURE_CODES_DDL,
      'database': 'temp_db',
      's3_output': ATHENA_QUERY_OUTPUT_LOCATION,
    }
  )
  
  create_procedure_modifiers_table = PythonOperator(
    task_id = 'create_procedure_modifiers_table',
    python_callable = athena_query_execution_wr,
    op_kwargs = {
      'sql': ATHENA_PROCEDURE_CODES_MODIFIERS_DDL,
      'database': 'temp_db',
      's3_output': ATHENA_QUERY_OUTPUT_LOCATION,
    }
  )
  
  create_procedure_ndc_table = PythonOperator(
    task_id = 'create_procedure_ndc_table',
    python_callable = athena_query_execution_wr,
    op_kwargs = {
      'sql': ATHENA_PROCEDURE_NDC_DDL,
      'database': 'temp_db',
      's3_output': ATHENA_QUERY_OUTPUT_LOCATION,
    }
  )

  extract_procedures = ContainerOperator_Single_Server(
    name          = 'airflow-dag-procedure-codes',
    image         = ECR_CRAWLER_IMAGE,
    name_prefix   = None,
    cmds          = ["python", "/app/src/procedure_code.py"],
    env_vars      = {
      "ATHENA_QUERY_OUTPUT_LOCATION": ATHENA_QUERY_OUTPUT_LOCATION,
      "ATHENA_OUTPUT_PROCEDURE_CODES_TABLE_SCHEMA": ATHENA_OUTPUT_PROCEDURE_CODES_TABLE_SCHEMA,
      "ATHENA_OUTPUT_PROCEDURE_CODES_TABLE_NAME": ATHENA_OUTPUT_PROCEDURE_CODES_TABLE_NAME,
      "ATHENA_OUTPUT_PROCEDURE_CODES_TABLE_LOCATION": ATHENA_OUTPUT_PROCEDURE_CODES_TABLE_LOCATION,
      "ATHENA_OUTPUT_PROCEDURE_MODIFIERS_TABLE_SCHEMA": ATHENA_OUTPUT_PROCEDURE_MODIFIERS_TABLE_SCHEMA,
      "ATHENA_OUTPUT_PROCEDURE_MODIFIERS_TABLE_NAME": ATHENA_OUTPUT_PROCEDURE_MODIFIERS_TABLE_NAME,
      "ATHENA_OUTPUT_PROCEDURE_MODIFIERS_TABLE_LOCATION": ATHENA_OUTPUT_PROCEDURE_MODIFIERS_TABLE_LOCATION,
      "ATHENA_OUTPUT_PROCEDURE_NDC_TABLE_SCHEMA": ATHENA_OUTPUT_PROCEDURE_NDC_TABLE_SCHEMA,
      "ATHENA_OUTPUT_PROCEDURE_NDC_TABLE_NAME": ATHENA_OUTPUT_PROCEDURE_NDC_TABLE_NAME,
      "ATHENA_OUTPUT_PROCEDURE_NDC_TABLE_LOCATION": ATHENA_OUTPUT_PROCEDURE_NDC_TABLE_LOCATION,
      "LOGICAL_DATE": LOGICAL_DATE,
      "AAPC_SECRET_ID": AAPC_SECRET_ID,
    },
    dag           = dag,
    eks           = RUN_IN_EKS,
    aws_credentials_absolute_path = AWS_CREDENTIALS_ABSOLUTE_PATH,
    retries=3,
  )  
  
  start >> create_procedure_codes_table >> create_procedure_modifiers_table >> create_procedure_ndc_table >> extract_procedures >> end
