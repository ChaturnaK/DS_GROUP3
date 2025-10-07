@echo off
setlocal EnableDelayedExpansion

set "SCRIPT_DIR=%~dp0"
for %%I in ("%SCRIPT_DIR%\..") do set "SCRIPTS_DIR=%%~fI"
for %%I in ("%SCRIPTS_DIR%\..") do set "DS_ROOT=%%~fI"

pushd "%DS_ROOT%" >nul 2>&1
if errorlevel 1 (
  echo Failed to locate DS root from %SCRIPT_DIR%
  exit /b 1
)

set "ZK_HOST=localhost:2181"

echo Building modules...
mvn -q -DskipTests package
if errorlevel 1 (
  echo Maven build failed.
  goto :FAIL
)

call :GET_FREE_PORT 7000 META_PORT
if not defined META_PORT (
  echo Could not acquire metadata port.
  goto :FAIL
)

call :GET_FREE_PORT 8001 S1_PORT
if not defined S1_PORT (
  echo Could not acquire first storage port.
  goto :FAIL
)
set /a NEXT=S1_PORT+1
call :GET_FREE_PORT %NEXT% S2_PORT
if not defined S2_PORT (
  echo Could not acquire second storage port.
  goto :FAIL
)
set /a NEXT=S2_PORT+1
call :GET_FREE_PORT %NEXT% S3_PORT
if not defined S3_PORT (
  echo Could not acquire third storage port.
  goto :FAIL
)

if not exist "data" mkdir "data"
if not exist "data\node1" mkdir "data\node1"
if not exist "data\node2" mkdir "data\node2"
if not exist "data\node3" mkdir "data\node3"

echo Starting MetadataServer on port %META_PORT%...
call :START_METADATA %META_PORT% "%ZK_HOST%" 3 META_PID
if not defined META_PID (
  echo Failed to start MetadataServer.
  goto :FAIL
)

echo Starting StorageNode on port %S1_PORT%...
call :START_STORAGE %S1_PORT% ".\data\node1" z1 "%ZK_HOST%" S1_PID
if not defined S1_PID (
  echo Failed to start storage node on %S1_PORT%.
  goto :FAIL
)

echo Starting StorageNode on port %S2_PORT%...
call :START_STORAGE %S2_PORT% ".\data\node2" z2 "%ZK_HOST%" S2_PID
if not defined S2_PID (
  echo Failed to start storage node on %S2_PORT%.
  goto :FAIL
)

echo Starting StorageNode on port %S3_PORT%...
call :START_STORAGE %S3_PORT% ".\data\node3" z3 "%ZK_HOST%" S3_PID
if not defined S3_PID (
  echo Failed to start storage node on %S3_PORT%.
  goto :FAIL
)

echo Waiting for services to initialize...
timeout /t 5 >nul

if not exist "test.txt" echo sample>test.txt
del /f /q "downloaded.txt" >nul 2>&1

echo Uploading file...
java -cp "client\target\*;common\target\*" com.ds.client.DsClient put .\test.txt /demo/test.txt
if errorlevel 1 (
  echo Upload failed.
  goto :FAIL
)

echo Downloading file...
java -cp "client\target\*;common\target\*" com.ds.client.DsClient get /demo/test.txt .\downloaded.txt
if errorlevel 1 (
  echo Download failed.
  goto :FAIL
)

for /f %%I in ('powershell -NoLogo -NoProfile -Command "(Get-FileHash 'test.txt').Hash"') do set "HASH1=%%I"
for /f %%I in ('powershell -NoLogo -NoProfile -Command "(Get-FileHash 'downloaded.txt').Hash"') do set "HASH2=%%I"
if not defined HASH1 goto :FAIL
if "%HASH1%" NEQ "%HASH2%" (
  echo File mismatch detected.
  goto :FAIL
)

echo Simulating node failure on port %S2_PORT%...
powershell -NoLogo -NoProfile -Command "try { Stop-Process -Id %S2_PID% -Force } catch {}"
timeout /t 10 >nul

echo Restarting storage node on port %S2_PORT%...
call :START_STORAGE %S2_PORT% ".\data\node2" z2 "%ZK_HOST%" S2_PID
if not defined S2_PID (
  echo Failed to restart storage node on %S2_PORT%.
  goto :FAIL
)

timeout /t 5 >nul
echo Tests completed successfully.
goto :SUCCESS

:GET_FREE_PORT
for /f %%I in ('powershell -NoLogo -NoProfile -Command "param([int]$start) for($p=$start; $true; $p++){ try { $listener = [System.Net.Sockets.TcpListener]::new([System.Net.IPAddress]::Loopback, $p); $listener.Start(); $listener.Stop(); Write-Output $p; break } catch {} }" %1') do (
  set "%2=%%I"
  goto :EOF
)
set "%2="
goto :EOF

:START_METADATA
set "PORT=%~1"
set "ZK=%~2"
set "REPL=%~3"
set "OUTVAR=%~4"
for /f %%I in ('powershell -NoLogo -NoProfile -Command "$p = Start-Process -FilePath ''java'' -ArgumentList @('-cp','metadata\target\classes;metadata\target\*;common\target\classes;common\target\*','com.ds.metadata.MetadataServer','--port','%PORT%','--zk','%ZK%','--replication','%REPL%') -NoNewWindow -PassThru; $p.Id"') do (
  set "%OUTVAR%=%%I"
  goto :EOF
)
set "%OUTVAR%="
goto :EOF

:START_STORAGE
set "PORT=%~1"
set "DATA=%~2"
set "ZONE=%~3"
set "ZK=%~4"
set "OUTVAR=%~5"
for /f %%I in ('powershell -NoLogo -NoProfile -Command "$p = Start-Process -FilePath ''java'' -ArgumentList @('-cp','storage\target\classes;storage\target\*;common\target\classes;common\target\*','com.ds.storage.StorageNode','--port','%PORT%','--data','%DATA%','--zone','%ZONE%','--zk','%ZK%') -NoNewWindow -PassThru; $p.Id"') do (
  set "%OUTVAR%=%%I"
  goto :EOF
)
set "%OUTVAR%="
goto :EOF

:FAIL
echo Test run failed.
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
if defined S1_PID powershell -NoLogo -NoProfile -Command "try { Stop-Process -Id %S1_PID% -Force } catch {}"
if defined S2_PID powershell -NoLogo -NoProfile -Command "try { Stop-Process -Id %S2_PID% -Force } catch {}"
if defined S3_PID powershell -NoLogo -NoProfile -Command "try { Stop-Process -Id %S3_PID% -Force } catch {}"
goto :EOF
