<#
Purpose: Smoke test – start local cluster, upload/download a file, compare, simulate a node failure, then restart it.
Inputs: None (assumes default ports and local ZooKeeper)
#>

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

function Get-DsRoot {
  $here = Split-Path -Parent $MyInvocation.MyCommand.Path
  return (Resolve-Path (Join-Path $here '..' '..')).Path
}

function Start-BackgroundRunLocal {
  param([string]$dsRoot)
  $ps1 = Join-Path $dsRoot 'scripts\windows\run-local.ps1'
  Start-Job -ScriptBlock { param($p) & powershell -ExecutionPolicy Bypass -File $p } -ArgumentList $ps1 | Out-Null
}

function Kill-StorageByPort([int]$port) {
  $wmi = Get-CimInstance Win32_Process | Where-Object { $_.CommandLine -match "StorageNode" -and $_.CommandLine -match "$port" }
  if ($wmi) { $wmi | ForEach-Object { Stop-Process -Id $_.ProcessId -Force -ErrorAction SilentlyContinue } }
}

$dsRoot = Get-DsRoot
Set-Location $dsRoot

# Ensure build exists
& mvn -q -DskipTests package | Out-Null

Start-BackgroundRunLocal -dsRoot $dsRoot
Start-Sleep -Seconds 5

Write-Host "Uploading file..."
& java -cp "client\target\*;common\target\*" com.ds.client.DsClient put .\test.txt /demo/test.txt

Write-Host "Downloading file..."
& java -cp "client\target\*;common\target\*" com.ds.client.DsClient get /demo/test.txt .\downloaded.txt

Write-Host "Comparing..."
$same = (Get-FileHash .\test.txt).Hash -eq (Get-FileHash .\downloaded.txt).Hash
if ($same) { Write-Host "✅ Files match" } else { throw "Files differ" }

Write-Host "Simulating node failure..."
Kill-StorageByPort 8002
Start-Sleep -Seconds 10

Write-Host "Restarting node..."
Start-Process -FilePath java -ArgumentList @('-cp', 'storage\target\*;common\target\*', 'com.ds.storage.StorageNode', '--port', '8002', '--data', '.\data\node2', '--zone', 'z1', '--zk', 'localhost:2181') -NoNewWindow | Out-Null

Start-Sleep -Seconds 5
Write-Host "✅ Test completed successfully."

