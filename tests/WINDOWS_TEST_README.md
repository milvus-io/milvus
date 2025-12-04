# Milvus Windows 功能测试指南 v2.0

这是一个功能完整的 Milvus 测试脚本，专门为 Windows 用户设计，包含 **2048 维向量**、**丰富的中文数据**和**多种标量类型**，全面验证 Milvus 的核心功能。

## ✨ 新特性

- 🎯 **2048 维向量搜索** - 支持高维度向量测试
- 🇨🇳 **丰富的中文数据** - 真实的商品信息，每条数据都不同
- 📊 **多种标量类型** - VARCHAR, INT64, FLOAT, BOOL, JSON 完整测试
- 🔍 **多样化查询** - 测试各种标量过滤条件
- 📈 **数据统计分析** - 自动生成数据分布统计

## 📋 测试内容

测试脚本会执行以下操作：

1. **连接 Milvus** - 连接到本地或远程 Milvus 实例
2. **创建集合** - 创建包含 2048 维向量和 10 个标量字段的集合
3. **插入数据** - 插入 1000 条差异化的中文商品数据
4. **向量搜索** - 执行 2048 维向量相似度搜索
5. **标量查询** - 测试 VARCHAR、FLOAT、INT64、BOOL 类型的过滤
6. **统计信息** - 获取集合统计和数据分布
7. **清理数据** - 删除测试集合

## 🗂️ 数据模型

每条测试数据包含以下字段：

| 字段名 | 类型 | 说明 | 示例 |
|--------|------|------|------|
| `id` | INT64 | 主键（自动生成） | 449837520435832832 |
| `vector` | FLOAT_VECTOR | 2048 维向量 | [0.123, 0.456, ...] |
| `product_name` | VARCHAR | 商品名称 | "华为 小米智能手机 第123代" |
| `description` | VARCHAR | 商品描述 | "性能卓越，功能强大..." |
| `category` | VARCHAR | 商品分类 | "手机数码" |
| `brand` | VARCHAR | 品牌 | "华为" |
| `price` | FLOAT | 价格 | 2999.99 |
| `stock` | INT64 | 库存 | 568 |
| `rating` | FLOAT | 评分 | 4.5 |
| `is_available` | BOOL | 是否有货 | true |
| `sales_count` | INT64 | 销量 | 12580 |
| `metadata` | JSON | 元数据 | {"tags": ["热销", "新品"], ...} |

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
╔════════════════════════════════════════════════════════════╗
║          Milvus Windows 功能测试脚本 v2.0                   ║
╚════════════════════════════════════════════════════════════╝

测试特性：
---------
✓ 2048 维向量搜索
✓ 丰富的中文测试数据（商品信息）
✓ 多种标量类型：VARCHAR, INT64, FLOAT, BOOL, JSON
✓ 标量过滤查询测试
✓ 完整的 CRUD 操作演示

============================================================
Milvus Windows 功能测试
============================================================

正在连接到 Milvus: milvus_demo.db
✓ 连接成功

正在创建集合: test_collection_1733308800
  - 向量维度: 2048
  - 包含标量字段: int64, float, varchar, bool, json
✓ 集合创建成功: test_collection_1733308800
  - 字段数量: 12 个（1个向量字段 + 11个标量字段）

正在插入 1000 条数据...
  - 每条数据包含 2048 维向量和 10 个标量字段
✓ 数据插入成功
  - 插入数量: 1000
  - 耗时: 1.25 秒

数据示例（第一条）:
  - 商品名称: 华为 小米智能手机 第0代
  - 描述: 性能卓越，功能强大，适合专业人士使用。小米智能手机的典范之作...
  - 品牌: 华为, 分类: 手机数码
  - 价格: ¥2999.99, 库存: 568
  - 评分: 4.5★, 销量: 12580
  - 有货: True
  - 标签: 热销, 新品, 爆款

正在执行向量搜索 (查询数: 5, Top-K: 5)...
✓ 搜索成功
  - 查询数量: 5
  - 耗时: 0.08 秒

第一个查询的 Top-3 结果:
  1. 相似度距离: 45.2341
     商品: 索尼 罗技无线鼠标 第456代
     品牌: 索尼, 分类: 外设产品
     价格: ¥199.99, 评分: 4.2★
     销量: 8520, 有货: True

正在执行标量过滤查询...

测试: 按品牌查询（VARCHAR）
  过滤条件: brand == "华为"
  ✓ 结果数量: 102, 耗时: 0.015 秒
    [1] 华为 华为笔记本电脑 第23代
        价格: ¥5699.00, 库存: 234, 评分: 4.8★

测试: 按价格范围查询（FLOAT）
  过滤条件: price >= 1000.0 and price <= 3000.0
  ✓ 结果数量: 195, 耗时: 0.012 秒
    [1] 小米 大疆无人机 第89代
        价格: ¥2599.00, 库存: 45, 评分: 4.6★

测试: 按库存查询（INT64）
  过滤条件: stock > 500
  ✓ 结果数量: 483, 耗时: 0.011 秒

测试: 按可用性查询（BOOL）
  过滤条件: is_available == true
  ✓ 结果数量: 892, 耗时: 0.010 秒

测试: 组合查询（多个条件）
  过滤条件: category == "电脑办公" and rating >= 4.5 and is_available == true
  ✓ 结果数量: 58, 耗时: 0.013 秒

✓ 所有标量查询测试完成

正在获取集合统计信息...
✓ 集合统计信息:
  - 集合名称: test_collection_1733308800
  - 实体数量: 1000
  - 向量维度: 2048
  - 标量字段: 10 个 (VARCHAR×4, FLOAT×2, INT64×2, BOOL×1, JSON×1)

  数据分析:
  - 有货商品数: 892
  - 品牌分布: 华为(102条), 苹果(98条), 小米(105条)

正在清理测试数据...
✓ 集合 test_collection_1733308800 已删除

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
