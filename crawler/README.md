## Lifemed Crawler
This project contains two crawlers developed in Python for extracting medical codes CPT/HCPCS and ICD-10 (CM/PCS) from the official AAPC and CMS websites, respectively. The collected data is processed, compared with existing data via Athena, and stored in Parquet format on S3.

The Docker image is built based on a Dockerfile, allowing automated and isolated execution of these crawlers.

This image includes all necessary dependencies such as headless Chrome, Selenium, pandas, BeautifulSoup, and AWS integration.

## Note on SQL Queries

The original SQL queries used in this project have been removed for confidentiality reasons, as they contain business-specific logic and database structures from a private environment.
This repository focuses on showcasing the crawler implementation, data processing logic, and integration flow (Athena, S3, Airflow, etc.) while keeping proprietary information secure.

## Docker Image Build
``` bash
docker build -t lifemed-crawler -f Dockerfile .
```

## ICD

Download .zip files containing ICD-10 codes in text format from current year
Extract, filter against existing data (via Athena), and send only new records to S3.

Extraction of ICD-10 Codes (CM/PCS)
Source: AAPC CPT/HCPCS Codes

Feed 1 table:
- analytics_db.icd_10

## Docker command icd
``` bash
docker run --rm -it --env-file ./src/icd.env -v ~/.aws/credentials:/root/.aws/credentials -v ./:/app lifemed-crawler python src/icd.py
```


## Procedure Code

Crawler Overview
aapc_crawler.py – Extraction of CPT/HCPCS Codes
Source: AAPC CPT/HCPCS Codes

Feed 3 tables:
- analytics_db.procedure_codes
- analytics_db.procedure_codes_modifier
- analytics_db.procedure_codes_ndc

``` bash
docker run --rm -it --env-file ./src/procedure_code.env -v ~/.aws/credentials:/root/.aws/credentials -v ./:/app lifemed-crawler python src/procedure_code.py
```
## Airflow Integration

Although this project is fully functional as a standalone crawler (via Docker commands),  
it was also designed to be orchestrated through **Apache Airflow** for automation, scheduling, and monitoring.

### Workflow Overview

```text
+----------+     +-----------+     +---------+     +----------+
|  AAPC    | --> |  Crawler  | --> |   S3    | --> |  Athena  |
+----------+     +-----------+     +---------+     +----------+
         (Triggered and monitored by Apache Airflow)
