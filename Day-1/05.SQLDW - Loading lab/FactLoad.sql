Create External Table Fact_flight_Ext
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
					 With (Location = 'Tables/FactFlights/2016', 
						   Data_Source = MastData_stor,
						   File_Format = pipe)


Create Table Fact_Flight_test
WITH
(
CLUSTERED COLUMNSTORE INDEX , Distribution = ROUND_ROBIN 
)
AS 
SELECT * FROM Fact_flight_Ext