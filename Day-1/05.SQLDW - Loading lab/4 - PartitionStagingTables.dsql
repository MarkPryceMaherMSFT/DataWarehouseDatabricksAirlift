DECLARE @Month VARCHAR(10) = '01'
DECLARE @Day VARCHAR(10) = '01'
DECLARE @Year VARCHAR(10) = '2010'

DECLARE @Date INT = CAST(CONCAT(@Year,@Month, @day) AS INT)

--SQL Execution Strings
DECLARE @SQLString NVARCHAR(4000);
DECLARE @ParmDefinition NVARCHAR(500);

-- Loop Counters
DECLARE @YearCounter Int = Cast(@Year AS INT)

WHILE (SELECT @YearCounter) <= 2016 -- Yearly Loop
BEGIN

	SET @SQLString = 
	CONCAT('Create Table flight_',@Date,'_partitioned
	 WITH (DISTRIBUTION = HASH(OriginAirportId),
					 CLUSTERED COLUMNSTORE INDEX,
					 PARTITION ([DateID] RANGE RIGHT FOR VALUES
					 (20090101, 20100101, 20110101, 20120101, 20130101, 20140101, 20150101, 20160101
					 )
					 )
					 ) AS
	SELECT * FROM flight_',@Date)

	SET @ParmDefinition = '@Date INT'
	EXECUTE SP_ExecuteSQL @SQLString, @ParmDefinition, @Date = @Date

	SET @YearCounter = @YearCounter + 1
	SET @Date = @Date + 10000

END