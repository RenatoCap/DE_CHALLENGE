# Globant's Data Engineer Challenge

## Introduction
This repository outlines my proposed solution for the data engineer challenge. All challenge-related information and files are located in the docs folder. Additional branches were created for proof-of-concept development and testing, with the full commit history available to demonstrate the development process. You'll also find a results folder containing output CSV files and screenshots.


## Solution
Our envisioned cloud architecture, shown in the accompanying image, is built on a tech stack featuring a Flask REST API, Azure SQL Database, Azure Blob Storage for archiving CSV data, and Pandas for robust data processing and cleaning.

![Proposed Architecture](docs/Proposed%20architecture.jpeg)

The solution proposes the following services:
* Azure Blob Storage for storing historical CSV files.
* Azure SQL Database as the relational database, compatible with SQL Server.
* Azure Web Apps for deploying the API.

This solution effectively addresses the requirements delineated in the problem statement. For enhanced scalability, however, it is recommended to implement Azure Data Factory to facilitate a more robust ingestion pipeline, particularly for CSV datasets approaching gigabyte scales. Database backup operations, currently managed via a cron job, can be seamlessly automated through an Azure Scheduler Job for improved operational efficiency.

## Setup
For a local setup, the line app.run should be uncommented and the env variables should be set appropiately:
1. AZURE_STORAGE_CONNECTION_STRING: Connection string for accessing the Azure Storage account.
2. BLOB_CONTAINER_NAME_HISTORIC: Name of the Azure Blob Storage container for historical data.
3. DB_SERVER: Hostname or IP address of the Azure SQL Database server.
4. DB_NAME: Name of the specific database to connect to.
5. DB_USER: Username for database authentication.
6. DB_PASSWORD: Password for database authentication.

For a cloud implementation you should create the following services:
1. Create a Azure SQL database instance.
2. Create a Blob Storage and the containers "historic" for historic files and "backup" to storage the backup.
3. Create a Web Service to deploy API and introduce the environment variables
4. Create a branch to deploy API Rest.
