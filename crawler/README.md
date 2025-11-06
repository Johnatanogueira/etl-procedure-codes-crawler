## Lifemed Crawler
This project contains two crawlers developed in Python for extracting medical codes CPT/HCPCS and ICD-10 (CM/PCS) from the official AAPC and CMS websites, respectively. The collected data is processed, compared with existing data via Athena, and stored in Parquet format on S3.

The Docker image is built based on a Dockerfile, allowing automated and isolated execution of these crawlers.

This image includes all necessary dependencies such as headless Chrome, Selenium, pandas, BeautifulSoup, and AWS integration.


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
