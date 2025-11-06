import types
import awswrangler as wr

import pandas as pd
import time

from utils.logger import get_logger

logger = get_logger(__name__)

def athena_execute_query(query, database = 'temp_db', s3_output = 's3://claims-management-athena-results/ClaimsManagementDataCatalog', wait=True):
  try:
    start = time.perf_counter()
    logger.debug(f"Athena execute query")
    logger.debug(query)
    r = wr.athena.start_query_execution(
      sql=query,
      database=database,
      s3_output=s3_output,
      wait= wait
    )
    elapsed = time.perf_counter() - start
    logger.debug(f"({elapsed}s) Success query on athena")
    return r
  except Exception as e:
    logger.error("Fail to query on Athena")
    logger.error(e)
    raise e

def athena_get_generator(
    athena_query:str, 
    athena_database:str='temp_db', 
    s3_output:str='s3://claims-management-athena-results/ClaimsManagementDataCatalog', 
    chunk_size:int=None
  ):
  try:
    logger.debug(f"Athena get generator from query: \n{athena_query}")
    start = time.perf_counter()
    generator = wr.athena.read_sql_query(
      sql=athena_query,
      database=athena_database,
      s3_output=s3_output,
      chunksize=chunk_size
    )
    elapsed = time.perf_counter() - start
    logger.debug(f"({elapsed}s) Athena generator with chunk size {chunk_size}")
    if(not (isinstance(generator, (pd.DataFrame, types.GeneratorType)))):
      logger.info(f"No result on Athena Input Query")
      return None
    return generator
  except Exception as e:
    logger.error("Fail to create Athena generator")
    logger.error(e)
    raise e