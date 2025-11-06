import requests
import zipfile
import io
import pandas as pd
import os
import numpy as np


from utils.athena import athena_get_generator
from utils.chrome_config import get_headless_chrome_driver
from utils.s3 import s3_athena_load_table_parquet_snappy
from utils.logger import get_logger
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC


logger = get_logger('hcpcs_codes')

BASE_URL = 'https://www.cms.gov/medicare/coding-billing/healthcare-common-procedure-system/quarterly-update'
ATHENA_TABLE_SCHEMA = os.getenv('ATHENA_TABLE_SCHEMA')
ATHENA_TABLE_NAME = os.getenv('ATHENA_TABLE_NAME')
ATHENA_OUTPUT_TABLE_LOCATION = os.getenv('ATHENA_OUTPUT_TABLE_LOCATION')
ATHENA_TABLE_SCHEMA_MODIFIERS = os.getenv('ATHENA_TABLE_SCHEMA_MODIFIERS')
ATHENA_TABLE_NAME_MODIFIERS = os.getenv('ATHENA_TABLE_NAME_MODIFIERS')
ATHENA_OUTPUT_TABLE_LOCATION_MODIFIERS = os.getenv('ATHENA_OUTPUT_TABLE_LOCATION_MODIFIERS')


if __name__ == "__main__":
    logger.info("Start")
    try:
        driver = get_headless_chrome_driver()

        driver.get(BASE_URL)
        
        wait = WebDriverWait(driver, 10)
        rxbodyfield_div = wait.until(EC.presence_of_element_located((By.CLASS_NAME, "rxbodyfield")))

        links = rxbodyfield_div.find_elements(By.CSS_SELECTOR,"ul > li > a")
        for link in links:
            logger.info(f"{link.text} -> {link.get_attribute('href')}")
            
        zip_links = [
            (link.get_attribute("href"))
            for link in links
            if link.get_attribute("href") and link.get_attribute("href").endswith(".zip")
        ]
        
        driver.quit()
        first_link = zip_links[0]

        response = requests.get(first_link)
        if response.status_code == 200:
            with zipfile.ZipFile(io.BytesIO(response.content)) as z:
                excel_candidates = [
                    name for name in z.namelist()
                    if "ANWEB" in name.upper() and name.lower().endswith(".xlsx")
                ]

                if not excel_candidates:
                    raise FileNotFoundError("Nenhum arquivo ANWEB .xlsx encontrado no ZIP.")

                excel_file_name = min(excel_candidates, key=len)
                logger.info(f"Arquivo Excel selecionado: {excel_file_name}")

                with z.open(excel_file_name) as excel_file:
                    df = pd.read_excel(excel_file)
            
                date_columns = ["ACT EFF DT", "TERM DT"]

                for col in date_columns:
                    if col in df.columns:
                        df[col] = (
                            df[col]
                            .apply(lambda x: str(int(x)) if pd.notnull(x) else "")
                            .str.strip()
                        )

            columns = [
                "HCPC", "LONG DESCRIPTION", "SHORT DESCRIPTION",
                "LABCERT1", "LABCERT2", "LABCERT3", "LABCERT4", "LABCERT5", "LABCERT6", "LABCERT7", "LABCERT8",
                "XREF1", "XREF2", "XREF3", "XREF4", "XREF5",
                "COV", "BETOS",
                "TOS1", "TOS2", "TOS3", "TOS4", "TOS5", "ANEST_BU",
                "ADD DT", "ACT EFF DT", "TERM DT"
            ]
            
            modifiers_columns = [
            "hcpc",
            "long_description", "short_description",
            "xref1", "xref2", "xref3", "xref4", "xref5",
            "cov", "add_dt", "act_eff_dt", "term_dt"
            ]

            
            text_columns = [
                "HCPC", "LONG DESCRIPTION", "SHORT DESCRIPTION",
                "XREF1", "XREF2",
                "COV", "BETOS",
                "TOS1", "TOS2", "TOS3"
            ]
            
            string_cols = [
                "hcpc", "long_description", "short_description",
                "labcert1","labcert2","labcert3","labcert4","labcert5","labcert6","labcert7","labcert8",
                "xref1","xref2","xref3","xref4","xref5",
                "cov","betos",
                "tos1","tos2","tos3","tos4","tos5",
                "anest_bu","add_dt","act_eff_dt","term_dt"
            ]

            missing_columns = [col for col in columns if col not in df.columns]
            if missing_columns:
                raise ValueError(
                    f"As seguintes colunas esperadas não foram encontradas no arquivo Excel: {missing_columns}"
                )
            hcpcs_df = df[columns].copy()

            if "RECID" not in df.columns:
                raise ValueError("A coluna 'RECID' não foi encontrada no arquivo Excel.")

            df["RECID"] = df["RECID"].astype(str).str.strip()

            hcpcs_codes_df = df[df["RECID"] == "3"][columns].copy()
            modifiers_df = df[df["RECID"] == "7"][columns].copy()

            hcpcs_codes_df.columns = [col.strip().lower().replace(" ", "_") for col in hcpcs_codes_df.columns]
            modifiers_df.columns = [col.strip().lower().replace(" ", "_") for col in modifiers_df.columns]
            
            modifiers_df = modifiers_df[modifiers_columns].copy()
            modifiers_df.rename(columns={"hcpc": "modifiers"}, inplace=True)

            for col in string_cols:
                if col in hcpcs_codes_df.columns:
                    hcpcs_codes_df[col] = hcpcs_codes_df[col].astype("string").str.strip()
                if col in modifiers_df.columns:
                    modifiers_df[col] = modifiers_df[col].astype("string").str.strip()

            logger.info(f"Total de linhas com RECID=3 (HCPCS): {len(hcpcs_codes_df)}")
            logger.info(f"Total de linhas com RECID=7 (Modifiers): {len(modifiers_df)}")  
            
            
            if not hcpcs_codes_df.empty:
                s3_athena_load_table_parquet_snappy(
                    df=hcpcs_codes_df,    
                    database=ATHENA_TABLE_SCHEMA,
                    table_name=ATHENA_TABLE_NAME,
                    table_location=ATHENA_OUTPUT_TABLE_LOCATION,
                    s3_file_prefix=f'{datetime.now().strftime("%Y%m%d")}_',
                    insert_mode='overwrite'
                )
                logger.info(f"{len(hcpcs_codes_df)} códigos HCPCS enviados para a tabela {ATHENA_TABLE_NAME}.")
            else:
                logger.warning("Nenhum registro com RECID=3 encontrado para HCPCS.")
            
            if not modifiers_df.empty:
                s3_athena_load_table_parquet_snappy(
                    df=modifiers_df,
                    database=ATHENA_TABLE_SCHEMA_MODIFIERS,
                    table_name=ATHENA_TABLE_NAME_MODIFIERS,
                    table_location=ATHENA_OUTPUT_TABLE_LOCATION_MODIFIERS,
                    s3_file_prefix=f'{datetime.now().strftime("%Y%m%d")}_',
                    insert_mode='overwrite'
                )
                logger.info(f"{len(modifiers_df)} códigos Modifiers enviados para a tabela {ATHENA_TABLE_NAME}_modifiers.")
            else:
                logger.warning("Nenhum registro com RECID=7 encontrado para Modifiers.")
        
    except Exception as e:
        logger.exception("Erro durante execução")
        raise e 