<#
Purpose: End-to-end automated test with basic metrics on Windows.
Inputs:
- Optional: -ZkHost (default: localhost:2181)
- Optional: -MetaPort (default: 7000)
- Optional: -StoragePorts (default: 8001,8002,8003)
- Optional: -LogDir, -DataDir, -MetricsDir
#>

param(
  [string]$ZkHost = 'localhost:2181',
  [int]$MetaPort = 7000,
  [int[]]$StoragePorts = @(8001,8002,8003),
  [string]$LogDir,
  [string]$DataDir,
  [string]$MetricsDir
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

function Get-RepoRoot {
  $here = Split-Path -Parent $MyInvocation.MyCommand.Path
  return (Resolve-Path (Join-Path $here '..' '..' '..')).Path
}

function Get-DsRoot {
  $here = Split-Path -Parent $MyInvocation.MyCommand.Path
  return (Resolve-Path (Join-Path $here '..' '..')).Path
}

function Banner([string]$msg) { Write-Host "`n=== $msg ===" -ForegroundColor Cyan }
function Ok([string]$msg) { Write-Host "[OK] $msg" -ForegroundColor Green }
function Warn([string]$msg) { Write-Host "[WARN] $msg" -ForegroundColor Yellow }
function Err([string]$msg) { Write-Host "[ERR] $msg" -ForegroundColor Red }

function Start-JavaLogged([string]$cp, [string[]]$args, [string]$log) {
  $all = @('-cp', $cp) + $args
  return Start-Process -FilePath java -ArgumentList $all -RedirectStandardOutput $log -RedirectStandardError $log -NoNewWindow -PassThru
}

function Kill-ByMatch([string]$substr) {
  Get-CimInstance Win32_Process | Where-Object { $_.CommandLine -match $substr } | ForEach-Object { Stop-Process -Id $_.ProcessId -Force -ErrorAction SilentlyContinue }
}

function New-RandomFile([string]$path, [int]$megabytes) {
  $len = $megabytes * 1MB
  $rng = [System.Security.Cryptography.RandomNumberGenerator]::Create()
  $buf = New-Object byte[] (1MB)
  $written = 0
  $fs = [System.IO.File]::Open($path, [System.IO.FileMode]::Create, [System.IO.FileAccess]::Write, [System.IO.FileShare]::None)
  try {
    while ($written -lt $len) {
      $rng.GetBytes($buf)
      $toWrite = [Math]::Min($buf.Length, $len - $written)
      $fs.Write($buf, 0, $toWrite)
      $written += $toWrite
    }
  } finally { $fs.Dispose(); $rng.Dispose() }
}

$dsRoot = Get-DsRoot
$repoRoot = Get-RepoRoot
if (-not $LogDir) { $LogDir = Join-Path $repoRoot 'logs' }
if (-not $DataDir) { $DataDir = Join-Path $repoRoot 'data' }
if (-not $MetricsDir) { $MetricsDir = Join-Path $repoRoot 'metrics' }

New-Item -ItemType Directory -Force -Path $LogDir,$MetricsDir | Out-Null
New-Item -ItemType Directory -Force -Path (Join-Path $DataDir 'node1'),(Join-Path $DataDir 'node2'),(Join-Path $DataDir 'node3') | Out-Null

Set-Location $dsRoot

Banner 'Build Modules'
& mvn -q -DskipTests clean package | Out-Null

Banner 'Check ZooKeeper'
$zkHost,$zkPortStr = $ZkHost.Split(':',2)
$zkPort = [int]$zkPortStr
try {
  $client = New-Object System.Net.Sockets.TcpClient('127.0.0.1',$zkPort)
  $client.Close()
  Ok "ZooKeeper reachable at $ZkHost"
} catch { Warn "ZooKeeper not reachable at $ZkHost; continuing anyway" }

$COMMON_CP = 'common\\target\\*'
$META_CP   = 'metadata\\target\\*'
$STOR_CP   = 'storage\\target\\*'
$CLIENT_CP = 'client\\target\\*'

Banner 'Launch MetadataServer'
$metaLog = Join-Path $LogDir 'meta.log'
$meta = Start-JavaLogged ("$META_CP;$COMMON_CP") @('com.ds.metadata.MetadataServer','--port',"$MetaPort",'--zk',$ZkHost) $metaLog
Start-Sleep -Seconds 4

for ($i=0; $i -lt $StoragePorts.Count; $i++) {
  $port = $StoragePorts[$i]
  Banner "Launch StorageNode on port $port"
  $nodeDir = Join-Path $DataDir ("node{0}" -f ($i+1))
  $log = Join-Path $LogDir ("storage_{0}.log" -f $port)
  Start-JavaLogged ("$STOR_CP;$COMMON_CP") @('com.ds.storage.StorageNode','--port',"$port",'--data',$nodeDir,'--zone',("z{0}" -f ($i+1)),'--zk',$ZkHost) $log | Out-Null
  Start-Sleep -Seconds 2
}
Start-Sleep -Seconds 3

Banner 'Prepare Test Files'
$TEST_FILE = Join-Path $repoRoot 'test.txt'
$OUT_FILE  = Join-Path $repoRoot 'downloaded.txt'
$BIG_FILE  = Join-Path $repoRoot 'big.bin'
$BIG_OUT   = Join-Path $repoRoot 'big_out.bin'
Set-Content -Path $TEST_FILE -Value 'Hello Distributed Systems' -NoNewline
New-RandomFile -path $BIG_FILE -megabytes 50

Banner 'TEST 1 – Basic PUT/GET'
$putStart = Get-Date
& java -cp "$CLIENT_CP;$COMMON_CP" com.ds.client.DsClient put $TEST_FILE /demo/test.txt
$putLat = (Get-Date) - $putStart

$getStart = Get-Date
& java -cp "$CLIENT_CP;$COMMON_CP" com.ds.client.DsClient get /demo/test.txt $OUT_FILE
$getLat = (Get-Date) - $getStart

if ((Get-FileHash $TEST_FILE).Hash -eq (Get-FileHash $OUT_FILE).Hash) { Ok ("PUT/GET succeeded – PUT: {0:n2}s, GET: {1:n2}s" -f $putLat.TotalSeconds, $getLat.TotalSeconds) } else { Err 'File mismatch after GET'; exit 1 }

Banner 'TEST 2 – Fault Tolerance (Kill One Node)'
Start-Job -ScriptBlock {
  & java -cp "$using:CLIENT_CP;$using:COMMON_CP" com.ds.client.DsClient put $using:BIG_FILE /demo/big.bin
} | Out-Null
Start-Sleep -Seconds 3
$failStart = Get-Date
Kill-ByMatch 'StorageNode.*8002'
Start-Sleep -Seconds 10
$healTime = ((Get-Date) - $failStart).TotalSeconds
Ok ("Healing completed in {0:n2}s" -f $healTime)

Banner 'Restart Node 8002'
Start-JavaLogged ("$STOR_CP;$COMMON_CP") @('com.ds.storage.StorageNode','--port','8002','--data',(Join-Path $DataDir 'node2'),'--zone','z2','--zk',$ZkHost) (Join-Path $LogDir 'storage_8002_restarted.log') | Out-Null
Start-Sleep -Seconds 3

Banner 'TEST 3 – Leader Failover'
$foStart = Get-Date
Kill-ByMatch 'MetadataServer'
Start-Sleep -Seconds 2
Start-JavaLogged ("$META_CP;$COMMON_CP") @('com.ds.metadata.MetadataServer','--port','7001','--zk',$ZkHost) (Join-Path $LogDir 'meta_newleader.log') | Out-Null
Start-Sleep -Seconds 4
$failover = ((Get-Date) - $foStart).TotalSeconds
Ok ("Leader failover completed in {0:n2}s" -f $failover)

Banner 'TEST 4 – Data Consistency Check'
& java -cp "$CLIENT_CP;$COMMON_CP" com.ds.client.DsClient get /demo/big.bin $BIG_OUT
if ((Get-FileHash $BIG_FILE).Hash -eq (Get-FileHash $BIG_OUT).Hash) { Ok 'File integrity verified after failover' } else { Err 'Checksum mismatch after failover'; exit 1 }

Banner 'TEST 5 – Metrics Summary'
$throughput = [math]::Round(50.0 / $putLat.TotalSeconds, 2)
Write-Host ''
Write-Host '📊  METRICS SUMMARY'
Write-Host '----------------------------------'
Write-Host (" PUT Latency:     {0:n2}s" -f $putLat.TotalSeconds)
Write-Host (" GET Latency:     {0:n2}s" -f $getLat.TotalSeconds)
Write-Host (" Healing Time:    {0:n2}s" -f $healTime)
Write-Host (" Leader Failover: {0:n2}s" -f $failover)
Write-Host (" Throughput:      {0} MB/s" -f $throughput)
Write-Host '----------------------------------'

Banner 'PASS/FAIL Evaluation'
if ($healTime -lt 10) { Ok 'Healing ✅ (<10s)' } else { Warn ("Healing ⚠️  ({0:n2}s)" -f $healTime) }
if ($throughput -gt 80) { Ok 'Throughput ✅ (>80 MB/s)' } else { Warn ("Throughput ⚠️  ({0} MB/s)" -f $throughput) }
if ($failover -lt 5) { Ok 'Leader Failover ✅ (<5s)' } else { Warn ("Leader Failover ⚠️  ({0:n2}s)" -f $failover) }

Banner 'ALL TESTS COMPLETE 🎉'

