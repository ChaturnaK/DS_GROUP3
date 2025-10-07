@echo off
setlocal EnableDelayedExpansion

rem Defaults
set "ZK_HOST=localhost:2181"
set "REPLICATION=3"

if not "%~1"=="" set "ZK_HOST=%~1"
if not "%~2"=="" set "REPLICATION=%~2"

set "SCRIPT_DIR=%~dp0"
for %%I in ("%SCRIPT_DIR%\..") do set "SCRIPTS_DIR=%%~fI"
for %%I in ("%SCRIPTS_DIR%\..") do set "DS_ROOT=%%~fI"

pushd "%DS_ROOT%" >nul 2>&1
if errorlevel 1 (
  echo Failed to locate DS root from %SCRIPT_DIR%
  exit /b 1
)

echo ZooKeeper host: %ZK_HOST%
call :WAIT_FOR_ZK "%ZK_HOST%"

echo.
echo Building modules...
mvn -q -DskipTests clean package
if errorlevel 1 (
  echo Maven build failed.
  goto :FAIL
)

set "COMMON_CP=common\target\classes;common\target\*"

call :GET_FREE_PORT 7000 META_PORT
if "%META_PORT%"=="" (
  echo Failed to find free port for MetadataServer.
  goto :FAIL
)
echo Using MetadataServer port: %META_PORT%

call :GET_FREE_PORT 8001 S1_PORT
if "%S1_PORT%"=="" (
  echo Failed to find first storage port.
  goto :FAIL
)
set /a NEXT_START=S1_PORT+1
call :GET_FREE_PORT %NEXT_START% S2_PORT
if "%S2_PORT%"=="" (
  echo Failed to find second storage port.
  goto :FAIL
)
set /a NEXT_START=S2_PORT+1
call :GET_FREE_PORT %NEXT_START% S3_PORT
if "%S3_PORT%"=="" (
  echo Failed to find third storage port.
  goto :FAIL
)
echo Using StorageNode ports: %S1_PORT%, %S2_PORT%, %S3_PORT%

if not exist "data" mkdir "data"
if not exist "data\node1" mkdir "data\node1"
if not exist "data\node2" mkdir "data\node2"
if not exist "data\node3" mkdir "data\node3"

call :START_METADATA %META_PORT% "%ZK_HOST%" %REPLICATION% META_PID
if "%META_PID%"=="" (
  echo Failed to launch MetadataServer.
  goto :FAIL
)

call :START_STORAGE %S1_PORT% ".\data\node1" z1 "%ZK_HOST%" S1_PID
if "%S1_PID%"=="" (
  echo Failed to launch StorageNode on %S1_PORT%.
  goto :FAIL
)

call :START_STORAGE %S2_PORT% ".\data\node2" z2 "%ZK_HOST%" S2_PID
if "%S2_PID%"=="" (
  echo Failed to launch StorageNode on %S2_PORT%.
  goto :FAIL
)

call :START_STORAGE %S3_PORT% ".\data\node3" z3 "%ZK_HOST%" S3_PID
if "%S3_PID%"=="" (
  echo Failed to launch StorageNode on %S3_PORT%.
  goto :FAIL
)

echo.
echo Press Ctrl+C to stop all services.
powershell -NoLogo -NoProfile -Command "Wait-Process -Id %META_PID%,%S1_PID%,%S2_PID%,%S3_PID%"
goto :SUCCESS

:WAIT_FOR_ZK
set "ADDR=%~1"
set "READY="
for /f "tokens=1* delims=:" %%A in ("%ADDR%") do (
  set "ZK_HOST_ONLY=%%A"
  set "ZK_PORT_ONLY=%%B"
)
if not defined ZK_PORT_ONLY set "ZK_PORT_ONLY=2181"
set /a ATTEMPT=0
setlocal enabledelayedexpansion
<nul set /p="Waiting for ZooKeeper on !ZK_HOST_ONLY!:!ZK_PORT_ONLY!"
endlocal
:WAIT_LOOP
if !ATTEMPT! geq 30 goto :NO_ZK
set "READY="
for /f %%I in ('powershell -NoLogo -NoProfile -Command "try { $client = New-Object Net.Sockets.TcpClient('!ZK_HOST_ONLY!', !ZK_PORT_ONLY!); $client.Close(); 'ready' } catch { }"') do set "READY=%%I"
if defined READY (
  echo  - ready
  goto :EOF
)
<nul set /p="."
timeout /t 1 >nul
set /a ATTEMPT+=1
goto :WAIT_LOOP
:NO_ZK
echo.
echo ZooKeeper did not become ready. Proceeding anyway...
goto :EOF

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
if defined META_PID powershell -NoLogo -NoProfile -Command "try { Stop-Process -Id %META_PID% -Force } catch {}"
if defined S1_PID powershell -NoLogo -NoProfile -Command "try { Stop-Process -Id %S1_PID% -Force } catch {}"
if defined S2_PID powershell -NoLogo -NoProfile -Command "try { Stop-Process -Id %S2_PID% -Force } catch {}"
if defined S3_PID powershell -NoLogo -NoProfile -Command "try { Stop-Process -Id %S3_PID% -Force } catch {}"
popd >nul
endlocal
exit /b 1

:SUCCESS
popd >nul
endlocal
exit /b 0
