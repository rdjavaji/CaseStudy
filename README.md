
# Casestudy

This casestudy demonstrate ingesting data from Azure SQL database and http API into the datalake storage account. Performing data cleaning and transformations, and storing the results in an Azure Data Lake house and Synapse Analytics.

It involved several key steps, including setting up the environment, creating databases and tables, Performs integration of various Azure data services, SQL database, HTTP, and Key vaults, Linked services, datasets, 
parameterised pipelines for processing and transforming data. Performing data cleaning and transformations, and storing the results in an Azure Data Lake house and Synapse Analytics and Pull request in GitHub repo.

## GitHub Repository and Data Upload (Considering the data for HTTP API):

- Created a GitHub repository and uploaded a file named ecommers containing customer data. This file served as an HTTP source for the data pipeline in Azure.

## Create SQL Database: sourcedb

- Data Organization
- Creating tables in the database
- Inserting values to the tables

[Query] (https://github.com/rdjavaji/CaseStudy/blob/main/SQL%20Database/query.sql)

## Azure Setup:

- created Resource Group with name "casestudy" (Under this resource group created Azure data factory, Storage account (Azure Data Lake storage Gen2), Azure Data bricks, Key vault.
- Azure Data Lake Storage: Created a storage account named “mystoragecd” and created containers named Raw, Silver, and Gold.
- Key vault: Created key vault with name "casestudy2keyvault".
- Azure Data Factory: Created an instance of Azure Data Factory (ADF) called “source-raw”
- GitHub: Configured Github branch as "Dev" to Azure Data factory
- Linked Services: Configured linked services to SQL database, HTTP, and Azure Data Lake.
- Datasets: Created datasets for SQL database, HTTP, and Azure Data Lake.

## Azure Data Lake Storage:

- Azure Data Lake Storage: A storage account was created with containers named Raw, Silver, and Gold. These containers are used for different stages of data processing
- Raw: Contains unprocessed data.
- Silver: Contains cleaned and transformed data.
- Gold: Contains aggregated and refined data.

## Azure Data Factory 

Azure Data Factory: ADF was used to create data pipelines for transferring data from SQL database and HTTP (from GitHub) to Azure Data Lake Storage (Raw container).
Linked Services: Linked services were set up to connect ADF to SQL database, HTTP (for GitHub), and Azure Data Lake Storage.
- Creating Linked service for SQL Database: ls_casestudy_sqldb
- Creating Linked service for HTTP API: ls_casestudy_http
- Creating Linked service for DataLake Gen2: ls_casestudy_dl
Datasets: Datasets were created for each data source to define the schema and structure of the data.
- Creating Datasets for SQL Database: ds_casestudy_sqldb
- Creating Datasets for Azure DataLake Storage gen2: ds_casestudy_dl
- Creating Datasets for HTTP: ds_casestudy_http

## Pipelines Parameterised in Azure Data Factory:
- Creating Pipeline for copy activity
- Created parameterised pipelines
- Scheduled Triggers for the same.
- Validate, Debug, Publish all
- First Pipeline: Transferred data from SQL database to Azure Data Lake Storage (Raw container).
- Second Pipeline: Transferred data from the HTTP source (Data from GitHub) to the Raw container in Azure Data Lake.
- Data Storage: The Raw container now contained both SQL database data and the HTTP data from the ecommers file.

## Azure Databricks:

- Azure Databricks was created with name "casestudy2_databricks" and created a cluster, folder, Notebook. 
- Retrieved raw data from Azure Data Lake Storage into Azure Databricks.
- Performed data cleaning and transformations in Databricks, storing the cleaned data in the Silver container of Azure Data Lake.

##### Data Cleaning and Transformation:
- Null Value Removal: Null values were removed from the data using dropna().
- Data Skew and Aggregation: Transactions data was analyzed for skew by grouping data by account_id and summing the transaction amounts. Repartitioning was done to reduce data shuffling.
- Salting Technique: Random "salt" was added to the account_id in order to evenly distribute data across partitions and reduce skew.
- Data Type Conversion: Various columns were cast to the appropriate data types, such as converting customer_id to integers and balance to doubles, ensuring proper data types for analysis.
- Renaming Columns: For instance, the column zip was renamed to postal_code for consistency.
- Joins and Grouping: More complex transformations, such as joining tables and grouping data by account, were performed in Databricks. This step aimed to create a unified, business-friendly dataset.
- The transformed data was then stored in the Gold container, which is intended for final consumption by analytics tools.)

##### Delta Lake (Silver Container):

- After cleaning, data was written back to the Silver container in Delta format, using Delta Lake for version control and ACID transaction support.
- Delta tables were created for accounts, customers, loan_payments, loans, and transactions.

##### Data Aggregation and Joins:

- Data was read from the Silver container, and several aggregation operations (e.g., summing transaction amounts) were performed.
- A join operation was executed on the transactions and customers tables using salted keys to minimize skew and ensure better distribution of the data.

##### Saving Final Data (Gold Container):

- After cleaning and aggregating the data, the final result was written to the Gold container, ready for downstream analysis or reporting.

[pyspark] (https://github.com/rdjavaji/CaseStudy/blob/main/Databricks/processing.ipynb)

## Azure Synapse Analytics:

Created an external table in Azure Synapse Analytics to to enable querying the processed data. This step allowed users to run SQL queries over the Gold container's data for reporting, analysis, and decision-making.

- Createed Azure Synapse Analytics with name as "casestudysynapse"
* Launch Synapse studio
* Created External table
* CREATE external data source for the Silver container
* creating external data source for the Gold container
* External table name: dbo.customer_account_summary1
* Run the script and executed

## GitHub Pull Request: 

- Created New resource group with name: my_rg
- Created New Azure data Factory: pr_adf1
- Configured Github branch as "QA" to Azure Data

[Steps for Project] (https://github.com/rdjavaji/CaseStudy/blob/main/Documentation%20on%20Project.pdf)
