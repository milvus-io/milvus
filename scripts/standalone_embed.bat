@REM  Licensed to the LF AI & Data foundation under one
@REM  or more contributor license agreements. See the NOTICE file
@REM  distributed with this work for additional information
@REM  regarding copyright ownership. The ASF licenses this file
@REM  to you under the Apache License, Version 2.0 (the
@REM  "License"); you may not use this file except in compliance
@REM  with the License. You may obtain a copy of the License at
@REM 
@REM    http://www.apache.org/licenses/LICENSE-2.0
@REM 
@REM  Unless required by applicable law or agreed to in writing, software
@REM  distributed under the License is distributed on an "AS IS" BASIS,
@REM  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
@REM  See the License for the specific language governing permissions and
@REM  limitations under the License.

@echo off
setlocal enabledelayedexpansion

:main
if "%1"=="" (
    echo Please use standalone_embed.bat restart^|start^|stop^|delete
    echo Note: 'delete' will remove ALL data and containers.
    exit /b 1
)

if "%1"=="restart" (
    call :stop
    call :start
) else if "%1"=="start" (
    call :start
) else if "%1"=="stop" (
    call :stop
) else if "%1"=="delete" (
    call :delete
) else (
    echo Unknown command.
    echo Please use standalone_embed.bat restart^|start^|stop^|delete
    echo Note: 'delete' will remove ALL data and containers.
    exit /b 1
)
goto :eof

:run_embed
(
echo listen-client-urls: http://0.0.0.0:2379
echo advertise-client-urls: http://0.0.0.0:2379
echo quota-backend-bytes: 4294967296
echo auto-compaction-mode: revision
echo auto-compaction-retention: '1000'
) > embedEtcd.yaml

(
echo # Extra config to override default milvus.yaml
) > user.yaml

docker run -d ^
    --name milvus-standalone ^
    --security-opt seccomp:unconfined ^
    -e ETCD_USE_EMBED=true ^
    -e ETCD_DATA_DIR=/var/lib/milvus/etcd ^
    -e ETCD_CONFIG_PATH=/milvus/configs/embedEtcd.yaml ^
    -e COMMON_STORAGETYPE=local ^
    -v "%cd%\volumes\milvus:/var/lib/milvus" ^
    -v "%cd%\embedEtcd.yaml:/milvus/configs/embedEtcd.yaml" ^
    -v "%cd%\user.yaml:/milvus/configs/user.yaml" ^
    -p 19530:19530 ^
    -p 9091:9091 ^
    -p 2379:2379 ^
    --health-cmd="curl -f http://localhost:9091/healthz" ^
    --health-interval=30s ^
    --health-start-period=90s ^
    --health-timeout=20s ^
    --health-retries=3 ^
    milvusdb/milvus:v2.4.13 ^
    milvus run standalone >nul
if %errorlevel% neq 0 (
    echo Failed to start Milvus container.
    exit /b 1
)

goto :eof

:wait_for_milvus_running
echo Wait for Milvus Starting...
:wait_loop
for /f "tokens=*" %%A in ('docker ps ^| findstr "milvus-standalone" ^| findstr "healthy"') do set running=1
if "!running!"=="1" (
    echo Start successfully.
    echo To change the default Milvus configuration, edit user.yaml and restart the service.
    goto :eof
)
timeout /t 1 >nul
goto wait_loop

:start
for /f "tokens=*" %%A in ('docker ps ^| findstr "milvus-standalone" ^| findstr "healthy"') do (
    echo Milvus is running.
    exit /b 0
)

for /f "tokens=*" %%A in ('docker ps -a ^| findstr "milvus-standalone"') do set container_exists=1
if defined container_exists (
    docker start milvus-standalone >nul
) else (
    call :run_embed
)

if %errorlevel% neq 0 (
    echo Start failed.
    exit /b 1
)

call :wait_for_milvus_running
goto :eof

:stop
docker stop milvus-standalone >nul
if %errorlevel% neq 0 (
    echo Stop failed.
    exit /b 1
)
echo Stop successfully.
goto :eof

:delete_container
for /f "tokens=*" %%A in ('docker ps ^| findstr "milvus-standalone"') do (
    echo Please stop Milvus service before delete.
    exit /b 1
)

rem Check if container exists before trying to remove it
for /f "tokens=*" %%A in ('docker ps -a ^| findstr "milvus-standalone"') do set container_exists=1
if not defined container_exists (
    echo Container milvus-standalone does not exist.
    exit /b 0
)

docker rm milvus-standalone >nul
if %errorlevel% neq 0 (
    echo Delete Milvus container failed.
    exit /b 1
)
echo Delete Milvus container successfully.
goto :eof

:delete
set /p check="WARNING: This will permanently delete all Milvus data and containers. Are you sure? [y/N] > "
if /i "%check%"=="y" (
    call :delete_container
    rmdir /s /q "%cd%\volumes"
    del /q embedEtcd.yaml
    del /q user.yaml
    echo Delete successfully.
) else (
    echo Delete operation cancelled.
    exit /b 0
)
goto :eof

:EOF