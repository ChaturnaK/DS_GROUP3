<#
Purpose: Build the project, ensure ZooKeeper is available (if running), then launch MetadataServer and three StorageNode instances on free ports. Keeps processes attached until stopped.
Inputs:
- Optional: -ZkHost (default: localhost:2181)
- Optional: -Replication (default: 3)
#>

param(
  [string]$ZkHost = "localhost:2181",
  [int]$Replication = 3
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

function Get-RepoRoot {
  $here = Split-Path -Parent $MyInvocation.MyCommand.Path
  # .../ds/scripts/windows -> ds -> repo root
  return (Resolve-Path (Join-Path $here '..' '..' '..')).Path
}

function Get-DsRoot {
  $here = Split-Path -Parent $MyInvocation.MyCommand.Path
  return (Resolve-Path (Join-Path $here '..' '..')).Path
}

function Test-PortOpen([string]$host, [int]$port) {
  try {
    $client = New-Object System.Net.Sockets.TcpClient
    $iar = $client.BeginConnect($host, $port, $null, $null)
    $ok = $iar.AsyncWaitHandle.WaitOne(400)
    $client.Close()
    return $ok
  } catch { return $false }
}

function Get-FreePort([int]$start) {
  $p = $start
  while (Test-PortOpen '127.0.0.1' $p) { $p++ }
  return $p
}

function Start-Proc([string]$FilePath, [string[]]$ArgumentList) {
  return Start-Process -FilePath $FilePath -ArgumentList $ArgumentList -NoNewWindow -PassThru
}

$dsRoot = Get-DsRoot
Set-Location $dsRoot

Write-Host "Starting ZooKeeper (if available)..."
if (Test-Path "$dsRoot/scripts/start-zk.sh") {
  Write-Host "Hint: start-zk.sh detected, please start it via WSL/Git Bash if needed."
}

Write-Host -NoNewline "Waiting for ZooKeeper on $ZkHost"
$zkHost, $zkPortStr = $ZkHost.Split(':',2)
$zkPort = [int]($zkPortStr)
$ready = $false
for ($i=1; $i -le 30; $i++) {
  if (Test-PortOpen $zkHost $zkPort) { $ready = $true; break }
  Write-Host -NoNewline "."
  Start-Sleep -Seconds 1
}
if ($ready) { Write-Host " - ready" } else { Write-Host "`nZooKeeper did not become ready. Proceeding anyway..." }

& mvn -q -DskipTests clean package | Out-Null

Write-Host "Launching Stage 2 services..."
$COMMON_CP = "common\target\classes;common\target\*"

$metaPort = Get-FreePort 7000
Write-Host "Using MetadataServer port: $metaPort"

$metaArgs = @('-cp', "metadata\target\classes;metadata\target\*;$COMMON_CP",
  'com.ds.metadata.MetadataServer', '--port', "$metaPort", '--zk', $ZkHost, '--replication', "$Replication")
$meta = Start-Proc 'java' $metaArgs

# Pick three free storage ports starting from 8001
$p1 = Get-FreePort 8001
$p2 = Get-FreePort ($p1+1)
$p3 = Get-FreePort ($p2+1)
Write-Host "Using StorageNode ports: $p1, $p2, $p3"

New-Item -ItemType Directory -Force -Path "$dsRoot/data/node1","$dsRoot/data/node2","$dsRoot/data/node3" | Out-Null

$storageArgs1 = @('-cp', "storage\target\classes;storage\target\*;$COMMON_CP",
  'com.ds.storage.StorageNode', '--port', "$p1", '--data', ".\data\node1", '--zone', 'z1', '--zk', $ZkHost)
$s1 = Start-Proc 'java' $storageArgs1

$storageArgs2 = @('-cp', "storage\target\classes;storage\target\*;$COMMON_CP",
  'com.ds.storage.StorageNode', '--port', "$p2", '--data', ".\data\node2", '--zone', 'z2', '--zk', $ZkHost)
$s2 = Start-Proc 'java' $storageArgs2

$storageArgs3 = @('-cp', "storage\target\classes;storage\target\*;$COMMON_CP",
  'com.ds.storage.StorageNode', '--port', "$p3", '--data', ".\data\node3", '--zone', 'z3', '--zk', $ZkHost)
$s3 = Start-Proc 'java' $storageArgs3

Write-Host "Press Ctrl+C to stop all services."
Wait-Process -Id @($meta.Id, $s1.Id, $s2.Id, $s3.Id)

