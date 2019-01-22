param
(
    [ValidateSet("Exercise1","Exercise2","Exercise3","Exercise4","Exercise5","Exercise6")] [string] $Name = "Exercise1",
    [ValidateSet("Slow","Fast")] [string] $Type = "Slow"
)

$ServerName = "<your_server>"
$DatabaseName = "<your_database>"

switch ($Name)
{
    "Exercise1" { .\Scripts.ps1 -Server $ServerName -Database $DatabaseName -Name Exercise1 -Type $Type -Label Exercise1 -Iterations 1 }
    "Exercise2" { .\Scripts.ps1 -Server $ServerName -Database $DatabaseName -Name Exercise2 -Type $Type -Label Exercise2 -Iterations 1 }
    "Exercise3" { .\Scripts.ps1 -Server $ServerName -Database $DatabaseName -Name Exercise3 -Type $Type -Label Exercise3 -Iterations 1 }
    "Exercise4" { .\Scripts.ps1 -Server $ServerName -Database $DatabaseName -Name Exercise4 -Type $Type -Label Exercise4 -Iterations 1 }
    "Exercise5" { .\Scripts.ps1 -Server $ServerName -Database $DatabaseName -Name Exercise5 -Type $Type -Label Exercise5 -Iterations 5 }
}
