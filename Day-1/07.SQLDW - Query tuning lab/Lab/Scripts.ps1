param
(
    [ValidateSet("Exercise1","Exercise2","Exercise3","Exercise4","Exercise5","Exercise6")] [string] $Name = "Exercise1",
    [ValidateSet("Slow","Fast")] [string] $Type = "Slow",
    [int] $Iterations,
    [string] $Label,
    [string] $Database,
    [string] $Server,
    [string] $Password = "S3cret!Pass",
    [int] $BackgroundConcurrency = 0
)

################################################################################
# Common Variables
################################################################################
$Server += ".database.windows.net"
$SmallRcUser = "smallrc_user"
$LargeRcUser = "largerc_user"
$ClearCache = "DBCC DROPCLEANBUFFERS;`r`nDBCC FREEPROCCACHE;`r`nGO"
$Demo = @{}
$Path = (Split-Path $myInvocation.MyCommand.Path)

################################################################################
# Common Instructions
################################################################################
$GetPlan = @"
https://azure.microsoft.com/en-us/documentation/articles/sql-data-warehouse-manage-monitor/

Start by looking at the Query Plan

--Find Request ID
select * 
from sys.dm_pdw_exec_requests 
where status not in ('completed','failed','cancelled') and session_id <> session_id()
order by request_id desc;

--Get Query Plan
select * 
from sys.dm_pdw_request_steps 
where request_id  in ('<insert_request_id_here>')
order by step_index;

What step is running?
"@

################################################################################
# Common Queries
################################################################################
$JoinQuery = @"
select
    top 10 *
from
    dbo.orders_<id>,
    dbo.lineitem_<id>
where
    l_orderkey  =  o_orderkey
    and o_orderdate BETWEEN '1997-01-01' AND '1997-12-31'
order by
    o_orderdate, l_shipdate
"@

$TPCHQuery5 = @"
select
    n_name,
    sum(l_extendedprice * (1 - l_discount)) as revenue
from
    dbo.customer_<id>,
    dbo.orders_<id>,
    dbo.lineitem_<id>,
    dbo.supplier_<id>,
    dbo.nation_<id>,
    dbo.region_<id>
where
    c_custkey = o_custkey
    and l_orderkey = o_orderkey
    and l_suppkey = s_suppkey
    and c_nationkey = s_nationkey
    and s_nationkey = n_nationkey
    and n_regionkey = r_regionkey
    and r_name = 'ASIA'
    and o_orderdate >= '1994-01-01'
    and o_orderdate < dateadd(year,1,cast ('1994-01-01'as date))
group by
    n_name
order by
    revenue desc
"@

################################################################################
# Exercise1
################################################################################
$DemoName = "Exercise1"

$SQL = @"
SELECT COUNT_BIG(*) 
FROM dbo.lineitem_<id>
WHERE l_shipdate  =  '1997-12-01' ;
"@

$SlowId = "0"
$FastId = "3"

$Parameters = @{
    Server = $Server
    Database = $Database
    BackgroundConcurrency = if ($BackgroundConcurrency -gt 0) {$BackgroundConcurrency} else {0}
    ClearCache = $false
    SlowUser = $SmallRcUser
    FastUser = $SmallRcUser
    SlowSQL = $SQL.replace("<id>",$SlowId)
    FastSQL = $SQL.replace("<id>",$FastId)
    Label = 'Count One Date'
}
$Demo+= @{"Exercise1" = $Parameters}

################################################################################
# Exercise2
################################################################################
$DemoName = "Exercise2"

$SQL = $TPCHQuery5
$SlowId = "2"
$FastId = "3"

$Parameters = @{
    Server = $Server
    Database = $Database
    BackgroundConcurrency = if ($BackgroundConcurrency -gt 0) {$BackgroundConcurrency} else {0}
    ClearCache = $false
    SlowUser = $SmallRcUser
    FastUser = $SmallRcUser
    SlowSQL = $SQL.replace("<id>",$SlowId)
    FastSQL = $SQL.replace("<id>",$FastId)
    Label = 'TPCH Query 5'
}
$Demo+= @{$DemoName = $Parameters}

################################################################################
# Exercise3
################################################################################
$DemoName = "Exercise3"

$SQL = $TPCHQuery5
$SlowId = "1"
$FastId = "4"

$Parameters = @{
    Server = $Server
    Database = $Database
    BackgroundConcurrency = if ($BackgroundConcurrency -gt 0) {$BackgroundConcurrency} else {0}
    ClearCache = $false
    SlowUser = $SmallRcUser
    FastUser = $SmallRcUser
    SlowSQL = $SQL.replace("<id>",$SlowId)
    FastSQL = $SQL.replace("<id>",$FastId)
    Label = 'TPCH Query 5'
}
$Demo+= @{$DemoName = $Parameters}

################################################################################
# Exercise4
################################################################################
$DemoName = "Exercise4"

$SQL = $JoinQuery
$SlowId = "5"
$FastId = "5"

$Parameters = @{
    Server = $Server
    Database = $Database
    BackgroundConcurrency = if ($BackgroundConcurrency -gt 0) {$BackgroundConcurrency} else {0}
    ClearCache = $false
    SlowUser = $SmallRcUser
    FastUser = $LargeRcUser
    SlowSQL = $SQL.replace("<id>",$SlowId)
    FastSQL = $SQL.replace("<id>",$FastId)
    Label = 'Large Join Query'
}
$Demo+= @{$DemoName = $Parameters}

################################################################################
# Exercise5 
################################################################################
$DemoName = "Exercise5"

$SQL = "select count_big(*) from region_<id>"
$SlowId = "3"
$FastId = "3"

$Parameters = @{
    Server = $Server
    Database = $Database
    BackgroundConcurrency = if ($BackgroundConcurrency -gt 0) {$BackgroundConcurrency} else {20}
    ClearCache = $false
    SlowUser = $LargeRcUser
    FastUser = $SmallRcUser
    SlowSQL = $SQL.replace("<id>",$SlowId)
    FastSQL = $SQL.replace("<id>",$FastId)
    Label = 'Large Join Query'
}
$Demo+= @{$DemoName = $Parameters}

################################################################################
# Start up background concurrency
################################################################################
if ($Demo.$Name.BackgroundConcurrency -gt 0)
{
    Write-Host "Starting $($Demo.$Name.BackgroundConcurrency) Concurrent Queries" -ForegroundColor Yellow

    for ($i=1;$i -le $Demo.$Name.BackgroundConcurrency;$i++)
    {
        $Cmd = "&'$Path\Scripts.ps1' -Server:$($ServerName) -Database:$($Demo.$Name.Database) -Name Exercise1 -Type Slow -Label:'Other User $i' >`$null 2>&1"
        $SB = $executioncontext.InvokeCommand.NewScriptBlock($Cmd)
        Start-Job -ScriptBlock $SB
    }

    Write-Host "Waiting for 60 seconds for concurrent queries to start"
    Start-Sleep 60
}

################################################################################
# Run Demo
################################################################################
$Server = $Demo.$Name.Server
$Database = $Demo.$Name.Database
$User = if ($Type -eq "Slow") {$Demo.$Name.SlowUser} else {$Demo.$Name.FastUser}
$Label = if ($Label) {"$Label | $Type | $($Demo.$Name.Label)"} else {"$Type | $($Demo.$Name.Label)"}
$SQL = @()
$SQL += if ($Demo.$Name.ClearCache) {$ClearCache}
$SQL += if ($Type -eq "Slow") {$Demo.$Name.SlowSQL} else {$Demo.$Name.FastSQL}
$SQL = $SQL.replace(";","")

for ($Iteration = 1; !$Iterations -or ($Iteration -le $Iterations); $Iteration++)
{
    Write-Host
    Write-Host ("=" * 80)
    #Write-Host "$(Get-Date -Format "ddd MM/dd/yyy HH:mm:ss.fff") Iteration #: $Iteration" -ForegroundColor Yellow
    Write-Host "Demo: $Name" -ForegroundColor Yellow
    Write-Host "Type: $Type" -ForegroundColor Yellow
    Write-Host "Label: $Label ($Iteration)" -ForegroundColor Yellow
    if ($Demo.$Name.BackgroundConcurrency) { Write-Host "BackgroundConcurrency: $($Demo.$Name.BackgroundConcurrency)" -ForegroundColor Yellow}
    Write-Host ("-" * 60)
    #Write-Host "sqlcmd -S $Server -I -p -U $User -P $Password -d $Database"
    $IterationSQL = $SQL + "`r`nOPTION (LABEL = '$Label ($Iteration)');"
    $IterationSQL += "`r`nGO"
    #Write-Host $IterationSQL
    #Write-Host ("-" * 80)

    $IterationSQL | sqlcmd -S $Server -I -p -U $User -P $Password -d $Database -e
    Write-Host ("=" * 80)
    Write-Host
}
