taskkill.exe /im wsmprovhost.exe /f
taskkill.exe /im sqlcmd.exe /f
Write-Host "Close this window to ensure all demos have been killed" -ForegroundColor Yellow
$RESPONSE=Read-Host
