


-- Create Database Master Key: This object is used to encrypt the Database Scoped Credential's Secret and store it in the Data Warehouse
Create  Master Key


-- When connecting to Azure Blob Storage, you need to supply the Azure Blob Storage Access key SQL DW so that it can access the data. 
-- This is done via the database scoped credential Secret. The identity is a required field, but it is not used for authentication.
Create Database Scoped Credential Mastdata
with Identity = 'mas',
Secret = 'chYJlpTz/0kAgHqj0+f8DEP9Ty7qHuM423VH6X9Ymu4mf64hY/2Hu3k/9E3kqEFPuFEwQdks/lo246Qam414CQ=='

-- An External Data Source specifies the WASB URI. 
Create External Data Source MastData_stor
WITH 
(
TYPE = HADOOP, 
LOCATION = N'wasbs://demodata@demodatacasey.blob.core.windows.net/', 
CREDENTIAL = Mastdata
)


-- External File Formats describe how the data is stored on WASB. In this case, we have a pipe delimited text file.
CREATE EXTERNAL FILE FORMAT pipe
WITH (FORMAT_TYPE = DELIMITEDTEXT,
      FORMAT_OPTIONS(
          FIELD_TERMINATOR = '|',
          STRING_DELIMITER = '',
          DATE_FORMAT = '',
          USE_TYPE_DEFAULT = False)
)
