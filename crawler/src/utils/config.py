import os
from utils.logger import get_logger

logger = get_logger(__name__)

LIFEMED_PG_SECRET_ID = 'lifemed-data-db-deepclaim-dev'

PROJECT_PATH = '/app'

def handle_env_vars(required, optional):
  try:
    env_vars = {}
    for env_var_name in  required:
      try:
        env_vars[env_var_name] = os.environ[env_var_name]
      except Exception as e:
        logger.error(f"Missing env var '{env_var_name}'")
        logger.error(e)
        raise e

    for env_var_name in  optional:
      env_var_value = os.environ.get(env_var_name, None)
      if(env_var_value):
        env_vars[env_var_name] = os.environ.get(env_var_name)

    logger.info(f"Success loading variables.")
    
    return env_vars

  except Exception as e:
    logger.error('Fail to handle env vars')
    logger.error(e)
    raise e