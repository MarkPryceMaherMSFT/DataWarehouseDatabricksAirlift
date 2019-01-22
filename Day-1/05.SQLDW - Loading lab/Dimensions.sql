

-- To Load external data, you need to create an external table. 
-- The schema uses SQL data types and is strongly typed
Create External table Aircraft_IMPORT
   ([id] [int] NULL,
	[TailNum] [varchar](15) NULL,
	[Type] [varchar](50) NULL,
	[Manufacturer] [varchar](50) NULL,
	[IssueDate] [varchar](15) NULL,
	[Model] [varchar](20) NULL,
	[Status] [char](5) NULL,
	[AircraftType] [varchar](30) NULL,
	[EngineType] [varchar](20) NULL,
	[Year] [smallint] NULL)
With
(
data_source = MastData_Stor,     -- Reference the External Data Source that you want to read from
File_format = pipe,              -- Reference the File Format that the data is in
location = 'aircraft' -- specify the directory location that you want to read from. PolyBase has traverses all childern directories and files from a stated filepath.
)


-- The external table acts as a pointer to data outside of the DataWarehouse. Use the Create Table as Select (CTAS) to import the data.
-- 
Create Table Dim_Aircraft
WITH
(
Distribution = ROUND_ROBIN, Clustered INDEX (id)                      -- Define the Distribution and index inline.
)
AS SELECT * FROM Aircraft_IMPORT    -- Select all records and all columns from the external table

-- Statistics need to be created or updated any time that data is loaded into SQL DW.
-- Create Statistics on all columns that are used in joins and are frequently used in predicates
CREATE Statistics Aircraft_Stat on Dim_Aircraft(id, type, manufacturer)


Create External table Airport_export
    ([id] [int] NULL,
	[Code] [nvarchar](10) NULL,
	[Name] [varchar](50) NULL,
	[City] [varchar](50) NULL,
	[State] [varchar](15) NULL,
	[Country] [varchar](50) NULL,
	[Latitude] [decimal](9, 6) NULL,
	[Longitude] [decimal](9, 6) NULL,
	[geo] varchar(1000) NULL) 
With
(
data_source = MastData_Stor,
File_format = pipe,
location = 'Airport'
)
 

 CREATE TABLE Dim_Airport
 WITH 
 (
 Distribution = ROUND_ROBIN, Clustered INDEX (id)
 )
 AS 
 SELECT * FROM Airport_export

 CREATE Statistics Airport_Stat on Dim_Airport(id, Name, Country)

 Create External Table Carrier_export
 ([ID] [smallint] NOT NULL,
	[Code] [nvarchar](10) NULL,
	[Description] [varchar](100) NULL)
WITH
(
data_source = MastData_Stor,
File_format = pipe,
location = 'Carrier'
)

 CREATE TABLE Dim_Carrier
 WITH 
 (
 Distribution = ROUND_ROBIN, Clustered INDEX (id)
 )
 AS 
 SELECT * FROM Carrier_export

  CREATE Statistics Carrier_Stat on Dim_Carrier(id, code)



 Create External Table [Date_Export](
	[ID] [int] NOT NULL,
	[Date] [date] NOT NULL,
	[DayOfMonth] [tinyint] NOT NULL,
	[DayofWeek] [tinyint] NOT NULL,
	[DayName] [varchar](9) NOT NULL,
	[MonthOfYear] [tinyint] NOT NULL,
	[MonthName] [varchar](9) NOT NULL,
	[Year] [smallint] NOT NULL,
	[IsWeekend] [bit] NOT NULL,
	[SeasonId] [tinyint] NOT NULL,
	[Season] [varchar](10) NOT NULL)
	With
(
data_source = MastData_Stor,
File_format = pipe,
location = 'date'
)

 CREATE TABLE Dim_Date
 WITH 
 (
 Distribution = ROUND_ROBIN, Clustered INDEX (id)
 )
 AS 
 SELECT * FROM [Date_Export]


  CREATE Statistics Date_Stat on Dim_Date(id, MonthName, Year)


 Create external table 	Time_Export(
    [ID] [smallint] NOT NULL,
	[Time] [time](0) NOT NULL,
	[HourOfDay] [tinyint] NOT NULL,
	[PartofDayId] [tinyint] NOT NULL,
	[PartOfDay] [varchar](10) NOT NULL)
With 
(
data_source = MastData_Stor,
File_format = pipe,
location = 'Time'
)


 CREATE TABLE Dim_Time
 WITH 
 (
 Distribution = ROUND_ROBIN, Clustered INDEX (id)
 )
 AS 
 SELECT * FROM Time_Export


   CREATE Statistics Time_Stat on Dim_Time(id)





