# Milvus Windows 功能测试指南

这是一个简单的 Milvus 功能测试脚本，专门为 Windows 用户设计，用于快速验证 Milvus 的基本功能。

## 📋 测试内容

测试脚本会执行以下操作：

1. **连接 Milvus** - 连接到本地或远程 Milvus 实例
2. **创建集合** - 创建一个测试向量集合
3. **插入数据** - 插入 1000 条测试向量数据
4. **向量搜索** - 执行向量相似度搜索
5. **过滤查询** - 使用标量字段进行过滤查询
6. **统计信息** - 获取集合统计信息
7. **清理数据** - 删除测试集合

## 🔧 环境要求

- **操作系统**: Windows 10/11
- **Python**: 3.8 或更高版本
- **依赖包**: pymilvus (脚本会自动安装)

## 🚀 快速开始

### 方法 1: 使用批处理脚本 (.bat) - 推荐

1. 打开文件资源管理器，进入 `tests` 目录
2. 双击运行 `run_windows_test.bat`
3. 等待测试完成

### 方法 2: 使用 PowerShell 脚本 (.ps1)

1. 右键点击 `run_windows_test.ps1`
2. 选择 "使用 PowerShell 运行"
3. 如果出现执行策略错误，在 PowerShell 中运行:
   ```powershell
   Set-ExecutionPolicy -ExecutionPolicy RemoteSigned -Scope CurrentUser
   ```
4. 再次运行脚本

### 方法 3: 手动运行

1. 打开命令提示符 (cmd) 或 PowerShell
2. 切换到 tests 目录:
   ```bash
   cd path\to\milvus\tests
   ```
3. 安装依赖:
   ```bash
   pip install pymilvus
   ```
4. 运行测试:
   ```bash
   python windows_simple_test.py
   ```

## 📝 配置选项

### 使用 Milvus Lite (默认)

默认配置使用 Milvus Lite，无需启动任何服务，数据保存在本地文件中：

```python
uri = "milvus_demo.db"  # 本地文件，无需服务
```

### 连接到 Milvus 服务器

如果你已经启动了 Milvus 服务器（通过 Docker 或其他方式），可以修改 `windows_simple_test.py` 中的 URI：

```python
# 连接到本地 Milvus 服务器
uri = "http://localhost:19530"

# 或连接到远程 Milvus 服务器
uri = "http://your-milvus-server:19530"
```

### 使用 Docker 运行 Milvus

如果你想在 Windows 上使用完整的 Milvus 服务，可以通过 Docker 启动：

```bash
# 1. 安装 Docker Desktop for Windows
# 下载: https://www.docker.com/products/docker-desktop

# 2. 启动 Milvus (在 PowerShell 或 cmd 中运行)
docker run -d --name milvus-standalone ^
  -p 19530:19530 -p 9091:9091 ^
  -v //c/milvus:/var/lib/milvus ^
  milvusdb/milvus:latest

# 3. 修改测试脚本中的 URI
# uri = "http://localhost:19530"

# 4. 运行测试
python windows_simple_test.py
```

## 🎯 预期输出

成功运行后，你应该看到类似以下的输出：

```
============================================================
Milvus Windows 功能测试
============================================================

正在连接到 Milvus: milvus_demo.db
✓ 连接成功

正在创建集合: test_collection_1234567890
✓ 集合创建成功: test_collection_1234567890

正在插入 1000 条数据...
✓ 数据插入成功
  - 插入数量: 1000
  - 耗时: 0.15 秒

正在执行向量搜索 (查询数: 5, Top-K: 5)...
✓ 搜索成功
  - 查询数量: 5
  - 耗时: 0.03 秒

正在执行过滤查询...
✓ 查询成功
  - 过滤条件: subject == "主题_0"
  - 结果数量: 10
  - 耗时: 0.02 秒

正在获取集合统计信息...
✓ 集合统计信息:
  - 集合名称: test_collection_1234567890
  - 实体数量: 1000

正在清理测试数据...
✓ 集合 test_collection_1234567890 已删除

============================================================
测试总结
============================================================
连接 Milvus      : ✓ 通过
创建集合         : ✓ 通过
插入数据         : ✓ 通过
向量搜索         : ✓ 通过
过滤查询         : ✓ 通过
统计信息         : ✓ 通过
清理数据         : ✓ 通过
------------------------------------------------------------
总计: 7/7 测试通过
============================================================
```

## 🔍 常见问题

### 1. Python 未找到

**错误**: `'python' 不是内部或外部命令...`

**解决方案**:
- 安装 Python: https://www.python.org/downloads/
- 安装时勾选 "Add Python to PATH"
- 或手动添加 Python 到系统环境变量

### 2. pip 命令失败

**错误**: `pip install pymilvus` 失败

**解决方案**:
```bash
# 升级 pip
python -m pip install --upgrade pip

# 使用国内镜像源
pip install pymilvus -i https://pypi.tuna.tsinghua.edu.cn/simple
```

### 3. 连接失败

**错误**: `连接失败: ...`

**解决方案**:
- 使用默认的 Milvus Lite 模式（uri = "milvus_demo.db"）
- 如果连接服务器，确保 Milvus 服务正在运行
- 检查防火墙设置
- 验证 URI 地址和端口是否正确

### 4. PowerShell 执行策略错误

**错误**: `无法加载文件 run_windows_test.ps1，因为在此系统上禁止运行脚本`

**解决方案**:
```powershell
# 在 PowerShell 中运行（以管理员身份）
Set-ExecutionPolicy -ExecutionPolicy RemoteSigned -Scope CurrentUser
```

## 📚 更多信息

- **Milvus 官方文档**: https://milvus.io/docs
- **PyMilvus 文档**: https://milvus.io/api-reference/pymilvus/v2.4.x/About.md
- **Milvus GitHub**: https://github.com/milvus-io/milvus
- **问题反馈**: https://github.com/milvus-io/milvus/issues

## 📦 测试文件说明

- `windows_simple_test.py` - 主测试脚本（Python）
- `run_windows_test.bat` - Windows 批处理启动脚本
- `run_windows_test.ps1` - PowerShell 启动脚本
- `WINDOWS_TEST_README.md` - 本说明文档

## 🎓 测试说明

这个测试脚本是基于 Milvus 官方文档和示例创建的简化版本，主要用于：

- ✅ 快速验证 Milvus 基本功能
- ✅ 学习 Milvus 的基本操作
- ✅ 作为开发前的环境验证
- ✅ 作为入门教程的实践代码

如需更全面的测试，请参考 `tests/python_client/` 目录下的完整测试套件。

## 💡 提示

- 首次运行建议使用 Milvus Lite 模式，无需额外配置
- 测试数据会在完成后自动清理
- 可以根据需要修改测试参数（数据量、维度等）
- 生产环境请使用完整的 Milvus 服务器部署

---

**祝测试顺利！** 🎉
