# Globant's Data Engineer Challenge

## Introduction
This repository outlines my proposed solution for the data engineer challenge. All challenge-related information and files are located in the docs folder. Additional branches were created for proof-of-concept development and testing, with the full commit history available to demonstrate the development process. You'll also find a results folder containing output CSV files and screenshots.


## Solution
Our envisioned cloud architecture, shown in the accompanying image, is built on a tech stack featuring a Flask REST API, Azure SQL Database, Azure Blob Storage for archiving CSV data, and Pandas for robust data processing and cleaning.

The solution proposes the following services:
* Azure blob storage to store historic files in csv format.
* Azure SQL like database who work with SQL Server.
* 