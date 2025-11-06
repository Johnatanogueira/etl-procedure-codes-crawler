import requests
import zipfile
import os
import pandas as pd
import logging
import time

from utils.athena import athena_get_generator
from utils.chrome_config import get_headless_chrome_driver
from utils.s3 import s3_athena_load_table_parquet_snappy
from utils.logger import get_logger
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC


logger = get_logger('icd_codes')

YEAR = os.getenv('YEAR')
ATHENA_TABLE_SCHEMA = os.getenv('ATHENA_TABLE_SCHEMA')
ATHENA_TABLE_NAME = os.getenv('ATHENA_TABLE_NAME')
ATHENA_OUTPUT_TABLE_LOCATION = os.getenv('ATHENA_OUTPUT_TABLE_LOCATION')
ATHENA_QUERY_OUTPUT_LOCATION = os.getenv('ATHENA_QUERY_OUTPUT_LOCATION')
BASE_URL="https://www.cms.gov/medicare/coding-billing/icd-10-codes"

ATHENA_SELECT=f"""SELECT code, year, code_type FROM {ATHENA_TABLE_SCHEMA}.{ATHENA_TABLE_NAME} WHERE year = '{YEAR}'"""


def process_pcs_file(year, df_existente, driver, wait, athena_table_schema, athena_table_name, athena_output_table_location):
    xpath_primary = f"//a[contains(text(), '{year} ICD-10-PCS Codes File') and contains(text(), 'ZIP')]"
    xpath_fallback = f"//a[contains(., '{year} ICD-10-PCS Codes File') and contains(., 'ZIP')]"

    elements = driver.find_elements(By.XPATH, xpath_primary)

    if elements:
        pcs_link_element = elements[0]
        logger.info("Link encontrado.")
    else:
        pcs_link_element = wait.until(
            EC.presence_of_element_located((By.XPATH, xpath_fallback))
        )
        logger.info("Link encontrado.")
        
    pcs_relative_url = pcs_link_element.get_attribute("href")
    pcs_full_url = pcs_relative_url if pcs_relative_url.startswith("http") else f"https://www.cms.gov{pcs_relative_url}"

    pcs_output_file = f"{year}_pcs_codes.zip"
    pcs_response = requests.get(pcs_full_url)
    with open(pcs_output_file, "wb") as f:
        f.write(pcs_response.content)
    logger.info(f"Arquivo PCS baixado com sucesso: {pcs_output_file}")

    with zipfile.ZipFile(pcs_output_file, 'r') as zip_ref:
        zip_file_names = zip_ref.namelist()
        logger.info("Arquivos dentro do ZIP (PCS):")
        for file_name in zip_file_names:
            logger.info(f"  {file_name}")

        pcs_target_file = f"icd10pcs_codes_{year}.txt"
        matched_file_path = next((name for name in zip_file_names if name.lower().endswith(pcs_target_file.lower())), None)

        if not matched_file_path:
            raise FileNotFoundError(f"Arquivo {pcs_target_file} não encontrado no ZIP PCS!")

        with zip_ref.open(matched_file_path) as txt_file:
            pcs_file_content = txt_file.read().decode("utf-8")

    pcs_lines = pcs_file_content.splitlines()
    logger.info(f"Número de linhas PCS carregadas: {len(pcs_lines)}")
    os.remove(pcs_output_file)
    logger.info(f"Arquivo ZIP removido: {pcs_output_file}")

    pcs_data = []
    code_type = 'PCS'
    for line in pcs_lines:
        if line.strip():
            parts = line.strip().split(None, 1)
            if len(parts) == 2:
                code, description = parts
                pcs_data.append({
                    'code': code.strip(),
                    'description': description.strip(),
                    'year': year,
                    'code_type': code_type
                })

    df_pcs = pd.DataFrame(pcs_data)
    df_pcs_filtrado = df_pcs.merge(df_existente[['code', 'year', 'code_type']], on=['code', 'year', 'code_type'], how='left', indicator=True)
    df_pcs_filtrado = df_pcs_filtrado[df_pcs_filtrado['_merge'] == 'left_only'].drop(columns=['_merge'])

    logger.info(f"Número de linhas PCS novas: {len(df_pcs_filtrado)}")
    
    
    if not df_pcs_filtrado.empty:
        s3_athena_load_table_parquet_snappy(
            df=df_pcs_filtrado,    
            database=athena_table_schema,
            table_name=athena_table_name,
            table_location=athena_output_table_location,
            s3_file_prefix=f'{datetime.now().strftime("%Y%m%d")}_',
            insert_mode='append',
            partition_cols=["code_type", "year"]
        )
        logger.info(f"{len(df_pcs_filtrado)} novos códigos PCS enviados para o S3.")
    else:
        logger.info("Nenhum código PCS novo encontrado — nada foi enviado.")

if __name__ == "__main__":
    logger.info("Start")
    try:
    
        driver = get_headless_chrome_driver()

        driver.get(BASE_URL)
        logger.info(driver.title)

        wait = WebDriverWait(driver, 5)
        
        
        accordion_container = driver.find_element(By.CSS_SELECTOR,
            "#block-cms-evo-content > div > div > div > div.subsite_body > div > div > div > div > div.block.block-layout-builder.block-field-blocknodesection-pagebody > div > div > div.ckeditor-accordion-container"
        )

        botoes_accordion = accordion_container.find_elements(By.CSS_SELECTOR, "a.ckeditor-accordion-toggler")

        ano_encontrado = any(f"{YEAR} ICD-10" in botao.text for botao in botoes_accordion)

        if not ano_encontrado:
            logger.warning(f"O ano {YEAR} não foi encontrado na lista de botões de expansão.")
        else:
            logger.info(f"Botão para o ano {YEAR} encontrado com sucesso.")
        
        
        accordion_button = wait.until(
            EC.element_to_be_clickable((
                By.XPATH, f"//a[contains(@class, 'ckeditor-accordion-toggler') and contains(text(), '{YEAR} ICD-10')]"
            ))
        )
        driver.execute_script("arguments[0].click();", accordion_button)
        wait.until(lambda d: accordion_button.get_attribute("aria-expanded") == "true")


        zip_link_element = wait.until(
            EC.presence_of_element_located((By.XPATH,
                f"//a[contains(text(), '{YEAR} Code Descriptions') and contains(text(), 'Tabular Order') and contains(text(), 'ZIP')]"
            ))
        )
        relative_url = zip_link_element.get_attribute("href")
        full_url = relative_url if relative_url.startswith("http") else f"https://www.cms.gov{relative_url}"

        output_file = f"{YEAR}_code_descriptions_tabular_order.zip"

        response = requests.get(full_url)
        with open(output_file, "wb") as f:
            f.write(response.content)
        logger.info(f"Arquivo baixado com sucesso: {output_file}")


        with zipfile.ZipFile(output_file, 'r') as zip_ref:
            zip_file_names = zip_ref.namelist()
            logger.info("Arquivos dentro do ZIP:")
            for file_name in zip_file_names:
                logger.info(f"  {file_name}")
            
            target_file_name = f"icd10cm_codes_{YEAR}.txt"
            matched_file_path = next((name for name in zip_file_names if name.lower().endswith(target_file_name.lower())), None)

            if not matched_file_path:
                raise FileNotFoundError(f"Arquivo {target_file_name} não encontrado dentro do ZIP!")

            with zip_ref.open(matched_file_path) as txt_file:
                file_content = txt_file.read().decode("utf-8")

        logger.info("Conteúdo carregado na memória com sucesso!")
        lines = file_content.splitlines()
        logger.info(f"Número de linhas carregadas: {len(lines)}")
        
        os.remove(output_file)
        logger.info(f"Arquivo ZIP removido: {output_file}")
        
        
        data = []
        code_type = 'CM'
        for line in lines:
            if line.strip():
                parts = line.strip().split(None, 1) 
                if len(parts) == 2:
                    code, description = parts
                    data.append({
                        'code': code.strip(),
                        'description': description.strip(),
                        'year': YEAR,
                        'code_type': code_type
                    })
        
        df_novo = pd.DataFrame(data)
        
        
        df_existente         = athena_get_generator(
            athena_query     = ATHENA_SELECT,
            athena_database  = ATHENA_TABLE_SCHEMA,
            s3_output        = ATHENA_QUERY_OUTPUT_LOCATION
        )
        
        
        chaves = ['code', 'year', 'code_type']
        df_filtrado = df_novo.merge(df_existente[chaves], on=chaves, how='left', indicator=True)
        df_filtrado = df_filtrado[df_filtrado['_merge'] == 'left_only'].drop(columns=['_merge'])
        logger.info(f"Número de linhas CM carregadas: {len(df_filtrado)}")
        
        if not df_filtrado.empty:
            s3_athena_load_table_parquet_snappy(
                df=df_filtrado,    
                database=ATHENA_TABLE_SCHEMA,
                table_name=ATHENA_TABLE_NAME,
                table_location=ATHENA_OUTPUT_TABLE_LOCATION,
                s3_file_prefix=f'{datetime.now().strftime("%Y%m%d")}_',
                insert_mode='append',
                partition_cols=["code_type", "year"]
            )
            logger.info(f"{len(df_filtrado)} novos códigos enviados para o S3.")
        else:
            logger.info("Nenhum código novo encontrado — nada foi enviado.")
            
        process_pcs_file(
        year=YEAR,
        df_existente=df_existente,
        driver=driver,
        wait=wait,
        athena_table_schema=ATHENA_TABLE_SCHEMA,
        athena_table_name=ATHENA_TABLE_NAME,
        athena_output_table_location=ATHENA_OUTPUT_TABLE_LOCATION
        )
    finally:
       driver.quit()