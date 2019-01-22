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
