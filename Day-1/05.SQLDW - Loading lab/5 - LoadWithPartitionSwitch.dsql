DECLARE @Month VARCHAR(10) = '01'
DECLARE @Day VARCHAR(10) = '01'
DECLARE @Year VARCHAR(10) = '2010'

DECLARE @Date INT = CAST(CONCAT(@Year,@Month, @day) AS INT)

--SQL Execution Strings
DECLARE @SQLString NVARCHAR(4000);
DECLARE @ParmDefinition NVARCHAR(500);

-- Loop Counters
DECLARE @PartitionCount Int = Cast(@Month AS INT)

WHILE (SELECT @PartitionCount) <= 7 -- Partition Loop
BEGIN

SET @SQLString =
CONCAT('ALTER TABLE dbo.flight_',@Date,'_partitioned SWITCH PARTITION ',
@PartitionCount + 2, ' TO dbo.Fact_flight PARTITION ',@PartitionCount + 2)

SELECT @SQLString
SET @ParmDefinition = '@PartitionCount INT'

EXECUTE SP_ExecuteSQL @SQLString, @ParmDefinition, @PartitionCount = @PartitionCount

SET @PartitionCount = @PartitionCount + 1
	SET @Date = @Date + 10000

END

