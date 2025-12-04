# Milvus Windows 功能测试运行脚本 (PowerShell)
# ================================================

Write-Host "========================================" -ForegroundColor Cyan
Write-Host "Milvus Windows 功能测试" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan
Write-Host ""

# 检查 Python 是否安装
Write-Host "[1/3] 检查 Python 版本..." -ForegroundColor Yellow
try {
    $pythonVersion = python --version 2>&1
    Write-Host $pythonVersion -ForegroundColor Green
} catch {
    Write-Host "[错误] 未检测到 Python，请先安装 Python 3.8 或更高版本" -ForegroundColor Red
    Write-Host "下载地址: https://www.python.org/downloads/" -ForegroundColor Red
    Read-Host "按任意键退出"
    exit 1
}

Write-Host ""
Write-Host "[2/3] 检查并安装依赖..." -ForegroundColor Yellow
Write-Host "正在检查 pymilvus 是否已安装..."

# 检查 pymilvus 是否已安装
$pipShow = pip show pymilvus 2>&1
if ($LASTEXITCODE -ne 0) {
    Write-Host "pymilvus 未安装，正在安装..." -ForegroundColor Yellow
    pip install pymilvus
    if ($LASTEXITCODE -ne 0) {
        Write-Host "[错误] pymilvus 安装失败" -ForegroundColor Red
        Read-Host "按任意键退出"
        exit 1
    }
} else {
    Write-Host "pymilvus 已安装" -ForegroundColor Green
}

Write-Host ""
Write-Host "[3/3] 运行测试..." -ForegroundColor Yellow
Write-Host "----------------------------------------"

# 运行测试脚本
python windows_simple_test.py

if ($LASTEXITCODE -eq 0) {
    Write-Host ""
    Write-Host "========================================" -ForegroundColor Green
    Write-Host "测试成功完成！" -ForegroundColor Green
    Write-Host "========================================" -ForegroundColor Green
} else {
    Write-Host ""
    Write-Host "========================================" -ForegroundColor Red
    Write-Host "测试失败，请检查错误信息" -ForegroundColor Red
    Write-Host "========================================" -ForegroundColor Red
}

Write-Host ""
Read-Host "按回车键退出"
