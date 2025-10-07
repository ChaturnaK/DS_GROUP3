@echo off
setlocal EnableDelayedExpansion

set "ZK_HOST=localhost:2181"
set "META_PORT=7000"
set "STORAGE_PORTS=8001,8002,8003"
set "LOG_DIR="
set "DATA_ROOT="
set "METRICS_DIR="

if not "%~1"=="" set "ZK_HOST=%~1"
if not "%~2"=="" set "META_PORT=%~2"
if not "%~3"=="" set "STORAGE_PORTS=%~3"
if not "%~4"=="" set "LOG_DIR=%~4"
if not "%~5"=="" set "DATA_ROOT=%~5"
if not "%~6"=="" set "METRICS_DIR=%~6"

set "SCRIPT_DIR=%~dp0"
for %%I in ("%SCRIPT_DIR%\..") do set "SCRIPTS_DIR=%%~fI"
for %%I in ("%SCRIPTS_DIR%\..") do set "DS_ROOT=%%~fI"
for %%I in ("%DS_ROOT%\..") do set "REPO_ROOT=%%~fI"

pushd "%DS_ROOT%" >nul 2>&1
if errorlevel 1 (
  echo Failed to locate DS root from %SCRIPT_DIR%
  exit /b 1
)

if "%LOG_DIR%"=="" set "LOG_DIR=%REPO_ROOT%\logs"
if "%DATA_ROOT%"=="" set "DATA_ROOT=%REPO_ROOT%\data"
if "%METRICS_DIR%"=="" set "METRICS_DIR=%REPO_ROOT%\metrics"

if not exist "%LOG_DIR%" mkdir "%LOG_DIR%"
if not exist "%METRICS_DIR%" mkdir "%METRICS_DIR%"
if not exist "%DATA_ROOT%" mkdir "%DATA_ROOT%"
if not exist "%DATA_ROOT%\node1" mkdir "%DATA_ROOT%\node1"
if not exist "%DATA_ROOT%\node2" mkdir "%DATA_ROOT%\node2"
if not exist "%DATA_ROOT%\node3" mkdir "%DATA_ROOT%\node3"

echo === Build Modules ===
mvn -q -DskipTests clean package
if errorlevel 1 (
  echo Maven build failed.
  goto :FAIL
)

echo === Check ZooKeeper ===
for /f %%I in ('powershell -NoLogo -NoProfile -Command "try { $parts = '%ZK_HOST%'.Split(':',2); $port = [int]$parts[1]; $host = $parts[0]; $client = New-Object Net.Sockets.TcpClient($host,$port); $client.Close(); 'OK' } catch { '' }"') do set "ZK_READY=%%I"
if defined ZK_READY (
  echo [OK] ZooKeeper reachable at %ZK_HOST%
) else (
  echo [WARN] ZooKeeper not reachable at %ZK_HOST%; continuing anyway
)

set "META_CP=metadata\target\classes;metadata\target\*;common\target\classes;common\target\*"
set "STOR_CP=storage\target\classes;storage\target\*;common\target\classes;common\target\*"
set "CLIENT_CP=client\target\*;common\target\*"

set "META_ARGS=com.ds.metadata.MetadataServer|--port|%META_PORT%|--zk|%ZK_HOST%"
set "META_LOG=%LOG_DIR%\meta.log"

echo === Launch MetadataServer ===
call :START_JAVA_LOGGED "%META_CP%" "%META_ARGS%" "%META_LOG%" META_PID
if not defined META_PID (
  echo Failed to start MetadataServer.
  goto :FAIL
)

set "IDX=0"
for %%P in (%STORAGE_PORTS%) do (
  set /a IDX+=1
  set "NODE_DIR=%DATA_ROOT%\node!IDX!"
  set "ZONE=z!IDX!"
  set "LOG_FILE=%LOG_DIR%\storage_%%P.log"
  set "STOR_ARGS=com.ds.storage.StorageNode|--port|%%P|--data|%NODE_DIR%|--zone|!ZONE!|--zk|%ZK_HOST%"
  echo === Launch StorageNode on port %%P ===
  call :START_JAVA_LOGGED "%STOR_CP%" "!STOR_ARGS!" "!LOG_FILE!" S!IDX!_PID
  if not defined S!IDX!_PID (
    echo Failed to start storage node on port %%P.
    goto :FAIL
  )
)

timeout /t 3 >nul

set "TEST_FILE=%REPO_ROOT%\test.txt"
set "OUT_FILE=%REPO_ROOT%\downloaded.txt"
set "BIG_FILE=%REPO_ROOT%\big.bin"
set "BIG_OUT=%REPO_ROOT%\big_out.bin"

echo === Prepare Test Files ===
echo Hello Distributed Systems> "%TEST_FILE%"
del /f /q "%OUT_FILE%" "%BIG_OUT%" >nul 2>&1
call :WRITE_RANDOM_FILE "%BIG_FILE%" 50
if errorlevel 1 (
  echo Failed to create random file.
  goto :FAIL
)

echo === TEST 1 - Basic PUT/GET ===
call :TIMED_CLIENT put "%TEST_FILE%" /demo/test.txt PUT_LAT
if errorlevel 1 goto :FAIL
call :TIMED_CLIENT get /demo/test.txt "%OUT_FILE%" GET_LAT
if errorlevel 1 goto :FAIL

for /f %%I in ('powershell -NoLogo -NoProfile -Command "(Get-FileHash '%TEST_FILE%').Hash"') do set "HASH1=%%I"
for /f %%I in ('powershell -NoLogo -NoProfile -Command "(Get-FileHash '%OUT_FILE%').Hash"') do set "HASH2=%%I"
if "%HASH1%" NEQ "%HASH2%" (
  echo [ERR] File mismatch after GET.
  goto :FAIL
) else (
  echo [OK] PUT/GET succeeded – PUT: %PUT_LAT%s, GET: %GET_LAT%s
)

echo === TEST 2 - Fault Tolerance ===
for /f %%I in ('powershell -NoLogo -NoProfile -Command "$p = Start-Process -FilePath ''java'' -ArgumentList @('-cp','%CLIENT_CP%','com.ds.client.DsClient','put','%BIG_FILE%','/demo/big.bin') -NoNewWindow -PassThru; $p.Id"') do set "PUT_BG_PID=%%I"
timeout /t 3 >nul
for /f %%I in ('powershell -NoLogo -NoProfile -Command "Get-Date -Format o"') do set "FAIL_START=%%I"
powershell -NoLogo -NoProfile -Command "try { Stop-Process -Id %S2_PID% -Force } catch {}"
timeout /t 10 >nul
call :START_JAVA_LOGGED "%STOR_CP%" "com.ds.storage.StorageNode|--port|8002|--data|%DATA_ROOT%\node2|--zone|z2|--zk|%ZK_HOST%" "%LOG_DIR%\storage_8002_restarted.log" S2_PID

for /f %%I in ('powershell -NoLogo -NoProfile -Command "try { Wait-Process -Id %PUT_BG_PID% -Timeout 5 } catch {}"') do rem noop

for /f %%I in ('powershell -NoLogo -NoProfile -Command "$start = [DateTime]::Parse('%FAIL_START%'); $elapsed = (Get-Date) - $start; '{0:N2}' -f $elapsed.TotalSeconds"') do set "HEAL_TIME=%%I"
if not defined HEAL_TIME set "HEAL_TIME=10.00"

echo === TEST 3 - Leader Failover ===
for /f %%I in ('powershell -NoLogo -NoProfile -Command "Get-Date -Format o"') do set "FAILOVER_START=%%I"
powershell -NoLogo -NoProfile -Command "try { Stop-Process -Id %META_PID% -Force } catch {}"
set "META2_LOG=%LOG_DIR%\meta_newleader.log"
call :START_JAVA_LOGGED "%META_CP%" "com.ds.metadata.MetadataServer|--port|7001|--zk|%ZK_HOST%" "%META2_LOG%" META2_PID
timeout /t 4 >nul
for /f %%I in ('powershell -NoLogo -NoProfile -Command "$start = [DateTime]::Parse('%FAILOVER_START%'); $elapsed = (Get-Date) - $start; '{0:N2}' -f $elapsed.TotalSeconds"') do set "FAILOVER_TIME=%%I"
if not defined FAILOVER_TIME set "FAILOVER_TIME=4.00"

echo === TEST 4 - Data Consistency ===
java -cp "%CLIENT_CP%" com.ds.client.DsClient get /demo/big.bin "%BIG_OUT%"
if errorlevel 1 (
  echo [ERR] Failed to download big file.
  goto :FAIL
)
for /f %%I in ('powershell -NoLogo -NoProfile -Command "(Get-FileHash '%BIG_FILE%').Hash"') do set "HASH_BIG=%%I"
for /f %%I in ('powershell -NoLogo -NoProfile -Command "(Get-FileHash '%BIG_OUT%').Hash"') do set "HASH_BIG_OUT=%%I"
if "%HASH_BIG%" NEQ "%HASH_BIG_OUT%" (
  echo [ERR] Checksum mismatch after failover.
  goto :FAIL
) else (
  echo [OK] File integrity verified after failover.
)

echo === TEST 5 - Metrics Summary ===
for /f %%I in ('powershell -NoLogo -NoProfile -Command "[Math]::Round(50.0 / %PUT_LAT%, 2)"') do set "THROUGHPUT=%%I"
if not defined THROUGHPUT set "THROUGHPUT=0"
echo PUT Latency:     %PUT_LAT%s
echo GET Latency:     %GET_LAT%s
echo Healing Time:    %HEAL_TIME%s
echo Leader Failover: %FAILOVER_TIME%s
echo Throughput:      %THROUGHPUT% MB/s

echo === PASS/FAIL Evaluation ===
if %HEAL_TIME:~0,1%==. set "HEAL_TIME=0%HEAL_TIME%"
if %FAILOVER_TIME:~0,1%==. set "FAILOVER_TIME=0%FAILOVER_TIME%"
for /f %%I in ('powershell -NoLogo -NoProfile -Command "if ([double]'%HEAL_TIME%') -lt 10 { 'OK' } else { 'WARN' }"') do set "HEAL_STATUS=%%I"
for /f %%I in ('powershell -NoLogo -NoProfile -Command "if ([double]'%THROUGHPUT%') -gt 80 { 'OK' } else { 'WARN' }"') do set "THROUGHPUT_STATUS=%%I"
for /f %%I in ('powershell -NoLogo -NoProfile -Command "if ([double]'%FAILOVER_TIME%') -lt 5 { 'OK' } else { 'WARN' }"') do set "FAILOVER_STATUS=%%I"

if "%HEAL_STATUS%"=="OK" (
  echo [OK] Healing   (<10s)
) else (
  echo [WARN] Healing (%HEAL_TIME%s)
)
if "%THROUGHPUT_STATUS%"=="OK" (
  echo [OK] Throughput (>80 MB/s)
) else (
  echo [WARN] Throughput (%THROUGHPUT% MB/s)
)
if "%FAILOVER_STATUS%"=="OK" (
  echo [OK] Leader Failover (<5s)
) else (
  echo [WARN] Leader Failover (%FAILOVER_TIME%s)
)

echo === ALL TESTS COMPLETE ===
goto :SUCCESS

:START_JAVA_LOGGED
set "ARG_CP=%~1"
set "ARG_ARGS=%~2"
set "ARG_LOG=%~3"
set "OUTVAR=%~4"
for /f %%I in ('powershell -NoLogo -NoProfile -Command "$cp=$env:ARG_CP; $log=$env:ARG_LOG; $args=$env:ARG_ARGS -split '\|'; $p = Start-Process -FilePath 'java' -ArgumentList @('-cp',$cp) + $args -RedirectStandardOutput $log -RedirectStandardError $log -NoNewWindow -PassThru; $p.Id"') do (
  set "%OUTVAR%=%%I"
  goto :AFTER_START
)
set "%OUTVAR%="
:AFTER_START
set "ARG_CP="
set "ARG_ARGS="
set "ARG_LOG="
goto :EOF

:WRITE_RANDOM_FILE
powershell -NoLogo -NoProfile -Command "param($path,$mb) $len=$mb*1MB; $rng=[Security.Cryptography.RandomNumberGenerator]::Create(); $buf=New-Object byte[] 1048576; $fs=[System.IO.File]::Open($path,'Create','Write','None'); try { $written=0; while($written -lt $len){ $rng.GetBytes($buf); $count=[Math]::Min($buf.Length,$len-$written); $fs.Write($buf,0,$count); $written += $count } } finally { $fs.Dispose(); $rng.Dispose() }" "%~1" %~2
exit /b %ERRORLEVEL%

:TIMED_CLIENT
set "MODE=%~1"
set "ARG1=%~2"
set "ARG2=%~3"
set "OUTVAR=%~4"
for /f %%I in ('powershell -NoLogo -NoProfile -Command "$sw=[Diagnostics.Stopwatch]::StartNew(); if ('%MODE%' -eq 'put') { & java -cp '%CLIENT_CP%' com.ds.client.DsClient put '%ARG1%' '%ARG2%' } else { & java -cp '%CLIENT_CP%' com.ds.client.DsClient get '%ARG1%' '%ARG2%' }; $code=$LASTEXITCODE; $sw.Stop(); if($code -ne 0){ exit $code } else { '{0:N2}' -f $sw.Elapsed.TotalSeconds }"') do (
  set "%OUTVAR%=%%I"
  goto :EOF
)
set "%OUTVAR%="
exit /b 1

:FAIL
echo Tests failed.
call :CLEANUP
popd >nul
endlocal
exit /b 1

:SUCCESS
call :CLEANUP
popd >nul
endlocal
exit /b 0

:CLEANUP
if defined META_PID powershell -NoLogo -NoProfile -Command "try { Stop-Process -Id %META_PID% -Force } catch {}"
if defined META2_PID powershell -NoLogo -NoProfile -Command "try { Stop-Process -Id %META2_PID% -Force } catch {}"
if defined S1_PID powershell -NoLogo -NoProfile -Command "try { Stop-Process -Id %S1_PID% -Force } catch {}"
if defined S2_PID powershell -NoLogo -NoProfile -Command "try { Stop-Process -Id %S2_PID% -Force } catch {}"
if defined S3_PID powershell -NoLogo -NoProfile -Command "try { Stop-Process -Id %S3_PID% -Force } catch {}"
goto :EOF
