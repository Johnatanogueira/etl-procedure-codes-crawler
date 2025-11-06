import pandas as pd
from datetime import datetime
from utils.s3 import s3_athena_load_table_parquet_snappy
from utils.logger import get_logger

# Configurações
ATHENA_TABLE_SCHEMA = "temp_db"
ATHENA_TABLE_NAME = "hcpcs_crosswalk"
ATHENA_OUTPUT_TABLE_LOCATION = "s3://claims-management-data-lake/warehouse/temp_db/hcpcs_crosswalk/"

logger = get_logger('hcpcs_modifier')

if __name__ == "__main__":
    logger.info("Start")

    try:
        # Caminho do arquivo Excel
        excel_path = "src/2025_HCPCS_Level-II_Modifier_Crosswalk.Excel.xlsx"

        # 1️⃣ Ler o arquivo Excel (tudo como string)
        df = pd.read_excel(excel_path, dtype=str)

        # 2️⃣ Padronizar nomes: minúsculas + underscore
        df.columns = [
            c.strip().lower().replace(" ", "_").replace("-", "_").replace("(", "").replace(")", "")
            for c in df.columns
        ]

        # 3️⃣ Mapeamento (caso precise ajustar nomes)
        rename_map = {
            "hcpcs_code": "hcpcs_code",
            "hcpcs_code_full_descriptor": "hcpcs_code_full_descriptor",
            "modifier_code": "modifier_code",
            "modifier_descriptor": "modifier_descriptor"
        }
        df = df.rename(columns=rename_map)

        # 4️⃣ Manter colunas na ordem certa
        expected_cols = [
            "hcpcs_code",
            "hcpcs_code_full_descriptor",
            "modifier_code",
            "modifier_descriptor"
        ]
        hcpcs_modifier_df = df[expected_cols].astype(str)

        logger.info(f"Arquivo Excel lido com {len(hcpcs_modifier_df)} linhas e colunas padronizadas.")

        # 5️⃣ Enviar para Athena
        if not hcpcs_modifier_df.empty:
            s3_athena_load_table_parquet_snappy(
                df=hcpcs_modifier_df,
                database=ATHENA_TABLE_SCHEMA,
                table_name=ATHENA_TABLE_NAME,
                table_location=ATHENA_OUTPUT_TABLE_LOCATION,
                s3_file_prefix=f'{datetime.now().strftime("%Y%m%d")}_',
                insert_mode='overwrite'
            )
            logger.info(f"{len(hcpcs_modifier_df)} registros enviados para a tabela {ATHENA_TABLE_NAME}.")
        else:
            logger.warning("O arquivo Excel está vazio — nada a inserir no Athena.")

    except Exception as e:
        logger.exception("Erro durante execução")
        raise e
