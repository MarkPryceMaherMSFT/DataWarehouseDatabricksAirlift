param
(
    [string]$ServerName,
    [string]$DatabaseName,
    [string]$UserName,
    [string]$Password
)
function GetName([string]$Prompt)
{
	$result = Read-host "`n$Prompt"
	return $result
}
function GetPassword([string]$Prompt)
{
	$securePassword = Read-Host $Prompt -AsSecureString
	$result = [Runtime.InteropServices.Marshal]::PtrToStringAuto([Runtime.InteropServices.Marshal]::SecureStringToBSTR($securePassword))
	return $result
}
function CheckCredentials([string]$ServerName,[string]$DatabaseName,[string]$UserName,[string]$Password)
{
	try
    {
		Write-Host "`nChecking credentials... (server:'$ServerName.database.windows.net', database:'$DatabaseName', user:'$UserName')"

		$result = sqlcmd -S "$ServerName.database.windows.net" -d "$DatabaseName" -U "$UserName" -P "$Password" -Q "select @@version" -I

		if ($result | Select-String "Microsoft Azure SQL Data Warehouse" ) 
		{
			Write-Host -ForegroundColor Green "Credentials Verified!"
			return $true
		}
		else
		{
			return $false
		}
	}
    catch
    {
        Write-Host -ForegroundColor Red $_.Exception
        return $false
    }
}
function ExecuteScript([string]$ServerName,[string]$DatabaseName,[string]$UserName,[string]$Password,[string]$ScriptFile)
{
	try
    {
        Write-Host "`nExecuting query... (user:'$UserName', script:'$ScriptFile')"

		$result = sqlcmd -S "$ServerName.database.windows.net" -d "$DatabaseName" -U "$UserName" -P "$Password" -i "$ScriptFile" -I

        return $results
	}
    catch
    {
        Write-Host -ForegroundColor Red $_.Exception
    }
}
#=====================================================================================================================================

Write-Host -ForegroundColor Yellow "`nBeginning...`n"

if (!$ServerName)
{
	$ServerName=GetName -Prompt "Enter server name (without .database.windows.net)"
}

if (!$DatabaseName)
{
	$DatabaseName=GetName -Prompt "Enter database name"
}

if (!$UserName)
{
	$UserName=GetName -Prompt "Enter user name"
}

if (!$Password)
{
	$Password=GetPassword -Prompt "Enter password"
}

$check=CheckCredentials -ServerName $ServerName -DatabaseName $DatabaseName -UserName $UserName -Password $Password

if (!$check)
{
    Write-Host -ForegroundColor Cyan "`nPlease check credentials`n`n"
    return
}

#--------------------------------------

ExecuteScript -ServerName $ServerName -DatabaseName "master" -UserName $UserName -Password $Password -ScriptFile ".\CreateLogins.sql"

ExecuteScript -ServerName $ServerName -DatabaseName $DatabaseName -UserName $UserName -Password $Password -ScriptFile ".\CreateUsers.sql"

$check=CheckCredentials -ServerName $ServerName -DatabaseName $DatabaseName -UserName "largerc_user" -Password "S3cret!Pass"

if (!$check)
{
    Write-Host -ForegroundColor Cyan "`nPlease check largerc_user credentials`n`n"
    return
}

$check=CheckCredentials -ServerName $ServerName -DatabaseName $DatabaseName -UserName "smallrc_user" -Password "S3cret!Pass"

if (!$check)
{
    Write-Host -ForegroundColor Cyan "`nPlease check smallrc_user credentials`n`n"
    return
}

#--------------------------------------

ExecuteScript -ServerName $ServerName -DatabaseName $DatabaseName -UserName "largerc_user" -Password "S3cret!Pass" -ScriptFile ".\LoadDataFromBlob.sql"

ExecuteScript -ServerName $ServerName -DatabaseName $DatabaseName -UserName "smallrc_user" -Password "S3cret!Pass" -ScriptFile ".\CreatePartitionTables.sql"

Write-Host -ForegroundColor Green "`nFinished!`n`n"