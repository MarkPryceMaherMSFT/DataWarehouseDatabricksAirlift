DECLARE @Month VARCHAR(10) = '01'
DECLARE @Day VARCHAR(10) = '01'
DECLARE @Year VARCHAR(10) = '2010'

DECLARE @Date INT = CAST(CONCAT(@Year,@Month, @day) AS INT)
DECLARE @Location VARCHAR(100)

--SQL Execution Strings
DECLARE @SQLString NVARCHAR(4000);
DECLARE @ParmDefinition NVARCHAR(500);

-- Loop Counters

DECLARE @YearCounter Int = Cast(@Year AS INT)

DECLARE @StartMonth INT
DECLARE @EndMonth INT

WHILE (SELECT @YearCounter) <= 2016 -- Yearly Loop
BEGIN

	SET @Location = CONCAT('Tables\FactFlights\',@YearCounter)

	SET @SQLString = 
	CONCAT('Create External Table extflight_',@Date,'
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
					 With (Location = ''',@location,''', 
						   Data_Source = MastData_stor,
						   File_Format = pipe)
					')

SET @ParmDefinition = '@Location VARCHAR(100), @Date INT'

EXECUTE SP_ExecuteSQL @SQLString, @ParmDefinition, @Location = @Location, @Date = @Date

-- CTAS into individual staging tables

SET @SQLString = 
 CONCAT('CREATE TABLE flight_',@Date,'
		WITH (DISTRIBUTION = ROUND_ROBIN, HEAP)
		AS SELECT * FROM extflight_',@Date,'
		DROP EXTERNAL TABLE extflight_',@Date);

SELECT @SQLString
	SET @ParmDefinition = '@Date INT'
EXECUTE SP_ExecuteSQL @SQLString, @ParmDefinition, @Date = @Date

SET @YearCounter = @YearCounter + 1
SET @Date = @Date + 10000

END