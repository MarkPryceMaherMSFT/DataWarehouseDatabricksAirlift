Create Table dbo.Fact_flight
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
					 With (
					 DISTRIBUTION = HASH(OriginAirportId),
					 CLUSTERED COLUMNSTORE INDEX,
					 PARTITION ([DateID] RANGE RIGHT FOR VALUES
					 (20090101, 20100101, 20110101, 20120101, 20130101, 20140101, 20150101, 20160101
					)
					)
					)


					