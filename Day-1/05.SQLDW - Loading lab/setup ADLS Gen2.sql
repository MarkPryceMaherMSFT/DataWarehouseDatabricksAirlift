/* clean up script 

drop External Table Fact_flight_ExtADLS
drop External Data Source alDataSource
drop Database Scoped Credential aldata
*/



-- Create Database Master Key: This object is used to encrypt the Database Scoped Credential's Secret and store it in the Data Warehouse
Create  Master Key


-- When connecting to Azure Data Lake Store Gen2 , you need to supply the  Azure Data Lake Storage Access key SQL DW so that it can access the data. 
-- This is done via the database scoped credential Secret. The identity is a required field, but it is not used for authentication.
Create Database Scoped Credential aldata
with Identity = 'mas',
Secret = 'eAPqQxcK0aeSmiVsmDwCB+ZvdrwfeUsiKWmXPV5QDHXLCj4mSjpWzMn8fDAgj4qzy1h/dlahXDTqxoBwLjrtSg=='

-- Location of the  Azure Data Lake Store 
Create External Data Source alDataSource
WITH 
(
TYPE = HADOOP, 
LOCATION = 'abfss://markshdi-2019-01-29t07-38-34-786z@adlsgen2uscentral.dfs.core.windows.net/', 
CREDENTIAL = aldata
)



-- External File Formats describe how the data is stored on abfss In this case, we have a pipe delimited text file.
CREATE EXTERNAL FILE FORMAT pipe
WITH (FORMAT_TYPE = DELIMITEDTEXT,
      FORMAT_OPTIONS(
          FIELD_TERMINATOR = '|',
          STRING_DELIMITER = '',
          DATE_FORMAT = '',
          USE_TYPE_DEFAULT = False)
)

-- Create external table
Create External Table Fact_flight_ExtADLS
					(
					  [DateId] int NULL, 
					  [DepartureTimeId] smallint NULL, 
					  [ScheduledDepartureTimeId] smallint NULL, 
					  [ArrivalTimeId] smallint NULL, 
				      [ScheduledArrivalTimeId] smallint NULL, 
					  [CarrierID] int NULL, 
					  [ElapsedTime] float NULL, 
					  [OriginAirportId] int NULL, 
					  [DestinationAirportId] int NULL, 
					  [Distance] int NOT NULL, 
				      [ArrivalDelay] float NULL, 
					  [DepartureDelay] float NULL, 
					  [AirTime] float NULL, 
					  [LateAircraftDelay] float NULL, 
					  [SecurityDelay] float NULL, 
					  [WeatherDelay] float NULL, 
					  [CarrierDelay] float NULL, 
					  [NASDelay] float NULL, 
					  [AircraftId] float NULL
					)
					 With (Location = 'demodata/Tables/FactFlights/2016', 
						   Data_Source = alDataSource,
						   File_Format = pipe)



 select * from Fact_flight_ExtADLS;
