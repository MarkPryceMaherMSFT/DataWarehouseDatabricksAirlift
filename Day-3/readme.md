# Problem Statement

Scenario: Adventureworks is a Bicycle company that sells bicycles
worldwide. They would like to use the following data sources to draw
meaningful insights. They approached you to build an end to end solution
that addresses data ingestion, cleansing, preparation, analysis, and
orchestration.

## Partner Sales Data Files

They work with a partner to handle their internet sales. Their partner
exports the sales history on a monthly basis to Amazon S3 storage. The
format of the files is Parquet and contains many attributes, some of
them nested. You can run the following code to see the schema of these
files:

```
orderDate20130508DF =
spark.read.parquet("/mnt/partnerdata-external/internetSales/05/OrderDate=2013-05-08/")

orderDate20130508DF.printSchema()
```

The folder structure for these files is shown below:

```
microsoft-airlift-azure-data-factory-ingestion/internetSales/MM/OrderDate=YYYY-MM-DD
```

## New Products File

Adventureworks is also introducing new products to their portfolio in
the upcoming months. They exported an entire catalog of their products
to a shared BLOB storage location. The format of the files is delimited
text. The structure of the table is shown below.

### Table Schema


```
|-- ProductKey: integer (nullable = true)
|-- ProductAlternateKey: string (nullable = true)
|-- ProductSubcategoryKey: integer (nullable = true)
|-- WeightUnitMeasureCode: string (nullable = true)
|-- SizeUnitMeasureCode: string (nullable = true)
|-- EnglishProductName: string (nullable = true)
|-- SpanishProductName: string (nullable = true)
|-- FrenchProductName: string (nullable = true)
|-- StandardCost: decimal(19,4) (nullable = true)
|-- FinishedGoodsFlag: boolean (nullable = true)
|-- Color: string (nullable = true)
|-- SafetyStockLevel: integer (nullable = true)
|-- ReorderPoint: integer (nullable = true)
|-- ListPrice: decimal(19,4) (nullable = true)
|-- Size: string (nullable = true)
|-- SizeRange: string (nullable = true)
|-- Weight: double (nullable = true)
|-- DaysToManufacture: integer (nullable = true)
|-- ProductLine: string (nullable = true)
|-- DealerPrice: decimal(19,4) (nullable = true)
|-- Class: string (nullable = true)
|-- Style: string (nullable = true)
|-- ModelName: string (nullable = true)
|-- EnglishDescription: string (nullable = true)
|-- FrenchDescription: string (nullable = true)
|-- ChineseDescription: string (nullable = true)
|-- ArabicDescription: string (nullable = true)
|-- HebrewDescription: string (nullable = true)
|-- ThaiDescription: string (nullable = true)
|-- GermanDescription: string (nullable = true)
|-- JapaneseDescription: string (nullable = true)
|-- TurkishDescription: string (nullable = true)
|-- StartDate: timestamp (nullable = true)
|-- EndDate: timestamp (nullable = true)
|-- Status: string (nullable = true)
```

## Assignment

You need build a Modern Data Warehouse (MDW) solution that meets the
following criteria:

**HINT: You will need to create two staging tables in the Data
Warehouse. You can use the Create Table As Commands at the end of this
document. **

1.  Ingest the Parquet data from S3 for the month of May (05) into an
    Azure BLOB container in your Azure subscription.

2.  Perform necessary transformations to the ingested data and load the data into a table 
    named `FactInternetSalesStg` in your SQLDW database.

3.  Load the products dump file into a table named DimProductStg in the
    most efficient way possible.

4.  Write a query to join both tables above to generate a report that looks similar to below:

5.  Create a reporting user named `rptusr` with absolute minimum privileges to execute the query above.

6.  The query observed to be not performing optimally. Investigate the
    reasons for this performance issue and make necessary changes to
    the table structure so that query can perform optimally.

## Bonus

There is data for additional months stored in S3 storage. Develop your
Azure infrastructure to ingest this data from each month into it's
corresponding folder in your Azure BLOB storage, Transform and load this
data into FactInternetSalesStg table.

## Resources

Use the following resources for guidance:

- [https://docs.microsoft.com/en-us/azure/sql-data-warehouse/load-data-wideworldimportersdw](https://docs.microsoft.com/en-us/azure/sql-data-warehouse/load-data-wideworldimportersdw)
- [https://docs.microsoft.com/en-us/azure/data-factory/](https://docs.microsoft.com/en-us/azure/data-factory/)
- [https://docs.microsoft.com/en-us/azure/azure-databricks/](https://docs.microsoft.com/en-us/azure/azure-databricks/)
- [https://docs.microsoft.com/en-us/azure/sql-data-warehouse/sql-data-warehouse-manage-monitor](https://docs.microsoft.com/en-us/azure/sql-data-warehouse/sql-data-warehouse-manage-monitor)
- [https://docs.microsoft.com/en-us/azure/sql-database/sql-database-manage-logins?toc=/azure/sql-data-warehouse/toc.json](https://docs.microsoft.com/en-us/azure/sql-database/sql-database-manage-logins?toc=/azure/sql-data-warehouse/toc.json)

### Create Table As

Hint, this can help you create an empty table with the same schema as
the original table:

```
CREATE TABLE FactInternetSalesStaging

WITH (DISTRIBUTION = ROUND_ROBIN)

AS SELECT * FROM FactInternetSales WHERE 1 = 2;
```

```
CREATE TABLE DimProductStg

WITH (DISTRIBUTION = ROUND_ROBIN)

AS SELECT * FROM FactInternetSales WHERE 1 = 2;
```

### Create External Table hint

```
create schema hack

Create Database Scoped Credential hackdata
with Identity = 'hac',
Secret = 'Nc0nm9Dd1xpb6Ip2AZGtse6AfkRVVnt9HfY1bJ7iTLp0vyLEDVOBpsUgDLXD28IxPwcDPtNayqiG9fzR6u/nfw=='

-- An External Data Source specifies the WASB URI. 
Create External Data Source HackData_stor
WITH 
(
TYPE = HADOOP, 
LOCATION = N'wasbs://data@storagezoddhdfm7p2tm.blob.core.windows.net/', 
CREDENTIAL = hackdata
)

DROP EXTERNAL TABLE  [hack].[importProduct]
DROP EXTERNAL FILE FORMAT csv

CREATE EXTERNAL FILE FORMAT csv
WITH (FORMAT_TYPE = DELIMITEDTEXT,
      FORMAT_OPTIONS(
          FIELD_TERMINATOR = ',',
          STRING_DELIMITER = '',
          DATE_FORMAT = '',
		    FIRST_ROW = 2, 
          USE_TYPE_DEFAULT = True))

CREATE External TABLE [hack].[importProduct]
(
	[ProductKey] [int] NOT NULL,
	[ProductAlternateKey] [nvarchar](25) NULL,
	[ProductSubcategoryKey][nvarchar](50) NULL,
	[WeightUnitMeasureCode] [nchar](3) NULL,
	[SizeUnitMeasureCode] [nchar](3) NULL,
	[EnglishProductName] [nvarchar](50)  NULL,
	[SpanishProductName] [nvarchar](50) NULL,
	[FrenchProductName] [nvarchar](50) NULL,
	[StandardCost] [nvarchar](50) NULL,
	[FinishedGoodsFlag] [nvarchar](50)  NULL,
	[Color] [nvarchar](15)  NULL,
	[SafetyStockLevel]  [nvarchar](50)  NULL,
	[ReorderPoint]  [nvarchar](50)  NULL,
	[ListPrice] [nvarchar](50) NULL,
	[Size] [nvarchar](50) NULL,
	[SizeRange] [nvarchar](50) NULL,
	[Weight] [nvarchar](50) NULL,
	[DaysToManufacture] [nvarchar](50) NULL,
	[ProductLine] [nchar](50) NULL,
	[DealerPrice] [nvarchar](50) NULL,
	[Class] [nchar](50) NULL,
	[Style] [nchar](50) NULL,
	[ModelName] [nvarchar](50) NULL,
	[EnglishDescription] [nvarchar](400) NULL,
	[FrenchDescription] [nvarchar](400) NULL,
	[ChineseDescription] [nvarchar](400) NULL,
	[ArabicDescription] [nvarchar](400) NULL,
	[HebrewDescription] [nvarchar](400) NULL,
	[ThaiDescription] [nvarchar](400) NULL,
	[GermanDescription] [nvarchar](400) NULL,
	[JapaneseDescription] [nvarchar](400) NULL,
	[TurkishDescription] [nvarchar](400) NULL,
	[StartDate] [VARCHAR](400) NULL,
	[EndDate] [VARCHAR](400) NULL,
	[Status] [nvarchar](7) NULL

)
 With (Location = '\DimProduct.csv', 
  Data_Source = hackData_stor,
	File_Format = csv)

select * from [hack].[importProduct];
```
