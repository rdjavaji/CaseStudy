Project name: CaseStudy

Created a resource group - Casestudy

Create Storage account for Data Lake storage Gen2: mystoragecd

Create SQL Database: sourcedb
* Data Organization
* Creating tables in the database
* Inserting values to the tables

(Create pipeline for data ingestion into the datalake storage)
* Create Azure Data Factory: source-raw
* Configure GitHub: Created Branch - Dev
* Creating Linked service for SQL Database: ls_casestudy_sqldb
* Creating Linked service for HTTP API: ls_casestudy_http
* Creating Linked service for DataLake Gen2: ls_casestudy_dl
* Creating Datasets for SQL Database: ds_casestudy_sqldb
* Creating Datasets for Azure DataLake Storage gen2: ds_casestudy_dl
* Creating Datasets for HTTP: ds_casestudy_http
* Creating Pipeline for copy activity
* Created parameterised pipelines
* Scheduled Triggers for the same.
* Validate, Debug, Publish all

All the copy activity has been ingested in the raw container. 

Creating Azure databricks
* Cleaning and storing the data in curated container with the following format
* Created a cluster
* Created a folder
* Created a notebook:  Delta Processing
* Reading and transforming tables
* Moving Data to silver container
* Performed activity to the table in different notebook: ETL
* Define the data and columns
* Create a DataFrame
* Define the path for gold container

Createed Azure Synapse Analytics: casestudysynapse
* Launch Synapse studio
* Created External table
* CREATE external data source for the Silver container
* creating external data source for the Gold container
* External table name: dbo.customer_account_summary1
* Run the script and executed
