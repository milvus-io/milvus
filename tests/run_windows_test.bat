@echo off
REM Milvus Windows 功能测试运行脚本
REM ================================

echo ========================================
echo Milvus Windows 功能测试
echo ========================================
echo.

REM 检查 Python 是否安装
python --version >nul 2>&1
if %errorlevel% neq 0 (
    echo [错误] 未检测到 Python，请先安装 Python 3.8 或更高版本
    echo 下载地址: https://www.python.org/downloads/
    pause
    exit /b 1
)

echo [1/3] 检查 Python 版本...
python --version

echo.
echo [2/3] 检查并安装依赖...
echo 正在检查 pymilvus 是否已安装...

REM 检查 pymilvus 是否已安装
pip show pymilvus >nul 2>&1
if %errorlevel% neq 0 (
    echo pymilvus 未安装，正在安装...
    pip install pymilvus
) else (
    echo pymilvus 已安装
)

echo.
echo [3/3] 运行测试...
echo ----------------------------------------
python windows_simple_test.py

if %errorlevel% equ 0 (
    echo.
    echo ========================================
    echo 测试成功完成！
    echo ========================================
) else (
    echo.
    echo ========================================
    echo 测试失败，请检查错误信息
    echo ========================================
)

echo.
pause
