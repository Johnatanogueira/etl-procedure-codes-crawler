# Fetch data from https://www.cms.gov/medicare/coding-billing/icd-10-codes

# Downloading and Processing POA 
# 
# (https://lifemed-ai.atlassian.net/jira/software/c/projects/DCAI/boards/20?label=Engineering&selectedIssue=DCAI-721)
# Example URL: https://www.cms.gov/files/zip/2026-poa-exempt-codes.zip
# Example file name: POAexemptCodesFY26.xlsx

# Matches from 2021 to 2026.
# 2021: ['POAexemptCodes2021.xlsx', 'POAexemptCodesJan2021.xlsx']
# 2022: ['POAexemptCodesApr22.xlsx']
# 2023: ['POAexemptCodesApr23.xlsx']
# 2024: ['POAexemptCodesFY24.xlsx']
# 2025: ['POAexemptCodesFY25.xlsx']
# 2026: ['POAexemptCodesFY26.xlsx']

from selenium import webdriver
import numpy as np
import requests
import zipfile
import io
import pandas as pd
import requests
from datetime import datetime
from uuid import uuid4

from utils.cms import get_download_url_dict_per_year
from utils.logger import get_logger
from utils.config import handle_env_vars
from utils.s3 import s3_get_table_location, s3_list_objects, s3_read_parquet, s3_to_parquet

logger = get_logger('poa_exempt_code')

# Env Vars
__ENV_VARS = [
  "LOGICAL_DATE"
]

__OPT_ENV_VARS = [
  'ATHENA_OUTPUT_DB',
  'ATHENA_OUTPUT_TABLE'
]
ENV = handle_env_vars(__ENV_VARS, __OPT_ENV_VARS)

BASE_SITE_URL       = 'https://www.cms.gov/medicare/coding-billing/icd-10-codes'
LOGICAL_DATE        = ENV.get("LOGICAL_DATE")
ATHENA_OUTPUT_DB    = ENV.get('ATHENA_OUTPUT_DB', 'analytics_db')
ATHENA_OUTPUT_TABLE = ENV.get('ATHENA_OUTPUT_TABLE', 'poa_exempt_code')

def s3_poa_parquet(
  df,
  athena_output_db,
  athena_output_table
):
  
  year = df.iloc[0]['year']
  date_parse = df.iloc[0]['date_parse']
  
  s3_output_table_location = s3_get_table_location(
    database=athena_output_db,
    table=athena_output_table
  )
  s3_output_table_location = s3_output_table_location.strip('/')
  s3_output_table_current_year_location = f"{s3_output_table_location}/year={year}"
  
  logger.info(f'Searching previous loading on {athena_output_db}.{athena_output_table}. S3 Path: {s3_output_table_current_year_location}')
  
  previous_loads = s3_list_objects(s3_output_table_current_year_location)
  previous_loads.sort()
  
  version = 0
  # Handle file_path with data as context  
  if(previous_loads):
    previous_version = previous_loads[-1].split('/')[-2]
    previous_version = int(previous_version.split('=')[-1])
    
    columns_to_compare = [c for c in df.columns if c not in ['date_parse', 'version']] # Removing date_parse and version to compare
    
    df_x = s3_read_parquet(previous_loads[-1])
    different_rows = pd.concat([
      df[columns_to_compare],
      df_x[columns_to_compare]
    ]).drop_duplicates(keep=False)
    no_new_data = different_rows.empty
    del different_rows
    
    if( no_new_data ):
      logger.info(f"POA on {year} don't have any updates. Keeping updated existent version {previous_version} on database.")
      return
    
    version = previous_version + 1
    logger.info(f"Found new version of POA on {year}. Feeding version '{version}'")
    
  version = str(version)
  file_path = f"{s3_output_table_location}/year={year}/version={version}/{date_parse}_{uuid4()}.parquet"
  
  df['version'] = version
  logger.info(f"Loading version of POA on {year}. Feeding version '{version}'")
  s3_to_parquet(
    df=df,
    file_path=file_path
  )

if __name__ == '__main__':
  try:
    # Fetch links
    year_download_urls = get_download_url_dict_per_year(url = BASE_SITE_URL)

    year_min = 2021 # First year with file
    year_max = datetime.now().year + 1

    for year in range(2021, year_max):
      year = str(year)
      logger.info(f"Extracting year {year}")

      poa_urls = [ i for i in year_download_urls[year] if 'poa-exempt-codes' in i ]
      
      if(len(poa_urls) == 0):
        logger.error(f'Year {year} poa-exempt-codes download link not found. Available links are :\n{year_download_urls[year]}')
        raise LookupError(f'Search for "{year} POA Exempt Codes (ZIP)" href, expected to be like "%poa-exempt-codes%" on: https://www.cms.gov/medicare/coding-billing/icd-10-codes')
      
      poa_url = poa_urls[0]
      response = requests.get(poa_url)
      
      zip_file_buffer = io.BytesIO(response.content)
      with zipfile.ZipFile(zip_file_buffer) as z:
        logger.info("Opened ZipFile and searching .xlsx POAexemptCodes file")
        matched_file_names = [ i for i in z.namelist() if 'POAexemptCodes'.lower() in i.lower() and i.lower().endswith('xlsx')]
        
        if(len(matched_file_names) == 0):
          logger.error(f"Zip file doesn't has expected file like '%POAexemptCodes%.xlsx'.")
          raise LookupError(f'Check files on {poa_url} zip file.')
        
        # Caso dê match em mais de um arquivo, pegue o que tem menor quantidade de caracteres, pois aí evitará declarações extras de "mês"
        matched_index = int(np.argmin([len(i) for i in matched_file_names]))
        file_name = matched_file_names[matched_index]

        logger.debug(f"Found files in zip: {matched_file_names} and choiced '{file_name}' to fetch")

        with z.open(file_name) as f:
          logger.info(f'Reading {file_name} as dataframe')
          df = pd.read_excel(f)
        
        df['year'] = year
        df['date_parse'] = LOGICAL_DATE
        
        rename_mapping = { k:v for k, v in zip([*df.columns], ['order', 'code', 'description', 'year', 'date_parse'])}
        
        df.rename(columns=rename_mapping, inplace=True)
        
        df['order'] = df['order'].astype(str)
        s3_poa_parquet(
          df = df,
          athena_output_db=ATHENA_OUTPUT_DB,
          athena_output_table=ATHENA_OUTPUT_TABLE
        )
        
        logger.info(f"Success year {year}, {df.shape[0]} rows.")
  except Exception as e:
    logger.critical(e)
    raise

"""
drop table analytics_db.poa_exempt_code;

CREATE EXTERNAL TABLE `analytics_db.poa_exempt_code`(
    `order` string,
    `code` string,
    `description` string
)
PARTITIONED BY ( 
  `year` STRING
)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  's3://claims-management-data-lake/warehouse/analytics_db/poa_exempt_code'
TBLPROPERTIES (
  'parquet.compress'='SNAPPY'
)
"""