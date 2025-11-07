## Crawler
This project contains two Python-based crawlers designed for automated data extraction and processing from official public data sources.
The collected data is validated, transformed, compared with existing datasets via AWS Athena, and stored in Parquet format on Amazon S3.

The system is fully containerized using Docker, allowing automated, isolated, and repeatable execution in production environments.

The Docker image includes all necessary dependencies such as:

Headless Chrome (for Selenium automation)

Selenium and Requests for web navigation and file download

Pandas and BeautifulSoup for data parsing and transformation

AWS SDK integration (boto3) for communication with S3 and Athena

## Note on SQL Queries

The original SQL queries used in this project have been removed for confidentiality and compliance reasons, as they contain internal business logic and database structures specific to a private environment.

This repository focuses on demonstrating the crawler architecture, data processing logic, and integration flow (Athena, S3, Airflow, etc.) while ensuring proprietary information remains protected.

+-----------+     +---------+     +----------+
 |  Crawler  | --> |   S3    | --> |  Athena  |
+-----------+     +---------+     +----------+
         (Triggered and monitored by Apache Airflow)
