import re
import awswrangler as wr
import pandas as pd
import time
from uuid import uuid4
from datetime import datetime

from utils.logger import get_logger

logger = get_logger(__name__)

def s3_list_objects(s3_path: str):
  objects = wr.s3.list_objects( path=s3_path )
  return objects

def s3_delete_path(s3_path: str):
  try:
    objects = s3_list_objects( s3_path=s3_path )
    if objects:
      wr.s3.delete_objects(objects)
      logger.debug(f"Success to delete files on {s3_path}")
  except Exception as e:
    logger.error(f"An error occurred on delete files on: {s3_path}")
    logger.error(e)
    
def s3_extract_bucket_path(uri):
  match = re.match(r"s3:\/\/([^\/]+)\/(.+)", uri)
  if match:
    bucket = match.group(1)
    path = match.group(2)
    if( path[-1] == '/' ):
      path = path[:-1]
    return bucket, path
  else:
    return None, None
  
def s3_athena_load_table_parquet_snappy(df, database, table_name, table_location, partition_cols=None, s3_file_prefix = f'{datetime.now().strftime("%Y%m%d")}_', insert_mode='overwrite') :
  start = time.perf_counter()
      
  if df.shape[0] > 0:
    wr.s3.to_parquet(
      df=df,
      database=database,
      table=table_name,
      filename_prefix=s3_file_prefix,
      dataset=True,
      compression='snappy',
      mode=insert_mode,
      path=table_location,
      index=False,
      partition_cols= partition_cols
    )
      
  elapsed = time.perf_counter() - start
  logger.info(f"Upload completo para o S3 levou: {elapsed} segundos")

def s3_to_parquet(df, file_path):
  wr.s3.to_parquet(
    df=df,
    path=file_path,
    compression="snappy",
    dataset=False
  )

def s3_get_table_location(database, table):
  return wr.catalog.get_table_location(
    database=database,
    table=table
  )

def s3_read_parquet(path):
  return wr.s3.read_parquet(path)