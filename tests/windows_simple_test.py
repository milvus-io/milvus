"""
Milvus Windows 简单功能测试脚本
=================================
这是一个简单的 Milvus 功能测试脚本，适用于 Windows 环境。
测试包括：创建集合、插入数据、创建索引、搜索和查询等基本操作。

使用方法：
1. 安装依赖：pip install pymilvus
2. 确保 Milvus 服务正在运行（可以使用 Docker 或 Milvus Lite）
3. 运行脚本：python windows_simple_test.py
"""

import sys
import time
import random
import json
from pymilvus import MilvusClient, DataType
from pymilvus import CollectionSchema, FieldSchema
import numpy as np


class MilvusSimpleTest:
    """Milvus 简单功能测试类"""

    # 中文测试数据库
    CHINESE_PRODUCTS = [
        "小米智能手机", "华为笔记本电脑", "苹果平板电脑", "联想台式机", "戴尔显示器",
        "三星固态硬盘", "金士顿内存条", "罗技无线鼠标", "雷蛇机械键盘", "索尼耳机",
        "佳能数码相机", "尼康单反相机", "大疆无人机", "GoPro运动相机", "小米手环",
        "华为智能手表", "苹果AirPods", "Beats音箱", "漫步者音响", "飞利浦台灯"
    ]

    CHINESE_DESCRIPTIONS = [
        "性能卓越，功能强大，适合专业人士使用",
        "轻薄便携，续航持久，商务办公首选",
        "外观时尚，操作简便，学生党的最爱",
        "性价比高，质量可靠，家用办公两相宜",
        "创新设计，品质保证，科技感十足",
        "高清画质，色彩艳丽，视觉享受绝佳",
        "智能语音，便捷控制，未来生活体验",
        "环保材质，节能省电，绿色低碳选择",
        "专业级性能，满足各种创作需求",
        "多功能集成，一机多用，提升效率"
    ]

    CHINESE_CATEGORIES = [
        "电脑办公", "手机数码", "家用电器", "智能穿戴", "影音娱乐",
        "摄影摄像", "网络设备", "电脑配件", "外设产品", "智能家居"
    ]

    CHINESE_BRANDS = [
        "小米", "华为", "苹果", "三星", "联想", "戴尔", "惠普", "华硕", "索尼", "松下"
    ]

    CHINESE_TAGS = [
        "热销", "新品", "促销", "推荐", "爆款", "限时优惠", "精选", "口碑爆款", "品质之选", "畅销榜"
    ]

    def __init__(self, uri="milvus_demo.db"):
        """
        初始化测试客户端

        参数:
            uri: Milvus 连接地址
                - 使用本地文件: "milvus_demo.db" (Milvus Lite)
                - 使用服务器: "http://localhost:19530"
                - 使用远程服务: "http://<your-milvus-server>:19530"
        """
        self.uri = uri
        self.client = None
        self.collection_name = f"test_collection_{int(time.time())}"
        self.dimension = 2048  # 使用 2048 维度

    def connect(self):
        """连接到 Milvus"""
        print(f"正在连接到 Milvus: {self.uri}")
        try:
            self.client = MilvusClient(uri=self.uri)
            print("✓ 连接成功")
            return True
        except Exception as e:
            print(f"✗ 连接失败: {e}")
            return False

    def create_collection(self):
        """创建测试集合（包含多种标量字段类型）"""
        print(f"\n正在创建集合: {self.collection_name}")
        print(f"  - 向量维度: {self.dimension}")
        print(f"  - 包含标量字段: int64, float, varchar, bool, json")
        try:
            # 检查集合是否已存在
            if self.client.has_collection(self.collection_name):
                print(f"集合 {self.collection_name} 已存在，正在删除...")
                self.client.drop_collection(self.collection_name)

            # 定义集合 Schema（包含多种常见标量类型）
            schema = MilvusClient.create_schema(
                auto_id=True,  # 自动生成 ID
                enable_dynamic_field=True  # 支持动态字段
            )

            # 添加字段
            schema.add_field(field_name="id", datatype=DataType.INT64, is_primary=True, auto_id=True)
            schema.add_field(field_name="vector", datatype=DataType.FLOAT_VECTOR, dim=self.dimension)

            # 各种常见标量类型
            schema.add_field(field_name="product_name", datatype=DataType.VARCHAR, max_length=200)  # 商品名称
            schema.add_field(field_name="description", datatype=DataType.VARCHAR, max_length=500)   # 商品描述
            schema.add_field(field_name="category", datatype=DataType.VARCHAR, max_length=100)      # 分类
            schema.add_field(field_name="brand", datatype=DataType.VARCHAR, max_length=100)         # 品牌
            schema.add_field(field_name="price", datatype=DataType.FLOAT)                           # 价格（浮点型）
            schema.add_field(field_name="stock", datatype=DataType.INT64)                           # 库存（整型）
            schema.add_field(field_name="rating", datatype=DataType.FLOAT)                          # 评分（浮点型）
            schema.add_field(field_name="is_available", datatype=DataType.BOOL)                     # 是否有货（布尔型）
            schema.add_field(field_name="sales_count", datatype=DataType.INT64)                     # 销量（整型）
            schema.add_field(field_name="metadata", datatype=DataType.JSON)                         # 元数据（JSON）

            # 创建索引参数
            index_params = MilvusClient.prepare_index_params()
            index_params.add_index(
                field_name="vector",
                index_type="FLAT",  # 使用 FLAT 索引（适合小数据集）
                metric_type="L2"
            )

            # 创建集合
            self.client.create_collection(
                collection_name=self.collection_name,
                schema=schema,
                index_params=index_params
            )

            print(f"✓ 集合创建成功: {self.collection_name}")
            print(f"  - 字段数量: 12 个（1个向量字段 + 11个标量字段）")
            return True
        except Exception as e:
            print(f"✗ 集合创建失败: {e}")
            return False

    def generate_chinese_data(self, index):
        """
        生成有差异性的中文测试数据

        参数:
            index: 数据索引

        返回:
            包含所有字段的数据字典
        """
        # 随机选择商品信息
        product = random.choice(self.CHINESE_PRODUCTS)
        description = random.choice(self.CHINESE_DESCRIPTIONS)
        category = random.choice(self.CHINESE_CATEGORIES)
        brand = random.choice(self.CHINESE_BRANDS)

        # 生成随机价格和库存
        price = round(random.uniform(99.0, 9999.0), 2)
        stock = random.randint(0, 1000)
        rating = round(random.uniform(3.0, 5.0), 1)
        is_available = stock > 0
        sales_count = random.randint(0, 100000)

        # 生成 JSON 元数据
        metadata = {
            "tags": random.sample(self.CHINESE_TAGS, k=random.randint(2, 4)),
            "release_year": random.randint(2020, 2024),
            "warranty_months": random.choice([6, 12, 24, 36]),
            "origin": random.choice(["中国", "美国", "日本", "韩国", "德国"]),
            "shipping": {
                "free": random.choice([True, False]),
                "days": random.randint(1, 7)
            }
        }

        # 组合完整的商品名称和描述
        full_product_name = f"{brand} {product} 第{index}代"
        full_description = f"{description}。{product}的典范之作，编号#{index:04d}。"

        return {
            "product_name": full_product_name,
            "description": full_description,
            "category": category,
            "brand": brand,
            "price": price,
            "stock": stock,
            "rating": rating,
            "is_available": is_available,
            "sales_count": sales_count,
            "metadata": metadata
        }

    def insert_data(self, num_entities=1000):
        """
        插入测试数据（包含丰富的中文内容和多种标量类型）

        参数:
            num_entities: 要插入的实体数量
        """
        print(f"\n正在插入 {num_entities} 条数据...")
        print(f"  - 每条数据包含 2048 维向量和 10 个标量字段")
        try:
            # 生成随机向量数据（2048维）
            vectors = np.random.rand(num_entities, self.dimension).tolist()

            # 准备数据
            data = []
            for i in range(num_entities):
                item = self.generate_chinese_data(i)
                item["vector"] = vectors[i]
                data.append(item)

            # 插入数据
            start_time = time.time()
            insert_result = self.client.insert(
                collection_name=self.collection_name,
                data=data
            )
            elapsed_time = time.time() - start_time

            print(f"✓ 数据插入成功")
            print(f"  - 插入数量: {insert_result['insert_count']}")
            print(f"  - 耗时: {elapsed_time:.2f} 秒")

            # 显示第一条数据示例
            if data:
                sample = data[0]
                print(f"\n数据示例（第一条）:")
                print(f"  - 商品名称: {sample['product_name']}")
                print(f"  - 描述: {sample['description'][:50]}...")
                print(f"  - 品牌: {sample['brand']}, 分类: {sample['category']}")
                print(f"  - 价格: ¥{sample['price']}, 库存: {sample['stock']}")
                print(f"  - 评分: {sample['rating']}★, 销量: {sample['sales_count']}")
                print(f"  - 有货: {sample['is_available']}")
                print(f"  - 标签: {', '.join(sample['metadata']['tags'])}")

            return True
        except Exception as e:
            print(f"✗ 数据插入失败: {e}")
            return False

    def search_data(self, num_queries=5, top_k=5):
        """
        执行向量搜索

        参数:
            num_queries: 查询向量数量
            top_k: 返回最相似的 top_k 个结果
        """
        print(f"\n正在执行向量搜索 (查询数: {num_queries}, Top-K: {top_k})...")
        try:
            # 生成随机查询向量（2048维）
            query_vectors = np.random.rand(num_queries, self.dimension).tolist()

            # 执行搜索
            start_time = time.time()
            search_results = self.client.search(
                collection_name=self.collection_name,
                data=query_vectors,
                limit=top_k,
                output_fields=["product_name", "description", "category", "brand",
                              "price", "rating", "is_available", "sales_count"]
            )
            elapsed_time = time.time() - start_time

            print(f"✓ 搜索成功")
            print(f"  - 查询数量: {len(search_results)}")
            print(f"  - 耗时: {elapsed_time:.2f} 秒")

            # 显示第一个查询的结果
            if search_results and len(search_results[0]) > 0:
                print(f"\n第一个查询的 Top-{min(3, top_k)} 结果:")
                for i, result in enumerate(search_results[0][:3]):
                    entity = result['entity']
                    print(f"  {i+1}. 相似度距离: {result['distance']:.4f}")
                    print(f"     商品: {entity.get('product_name', 'N/A')}")
                    print(f"     品牌: {entity.get('brand', 'N/A')}, 分类: {entity.get('category', 'N/A')}")
                    print(f"     价格: ¥{entity.get('price', 0):.2f}, 评分: {entity.get('rating', 0)}★")
                    print(f"     销量: {entity.get('sales_count', 0)}, 有货: {entity.get('is_available', False)}")

            return True
        except Exception as e:
            print(f"✗ 搜索失败: {e}")
            return False

    def query_data(self):
        """执行标量过滤查询（测试多种数据类型的过滤）"""
        print(f"\n正在执行标量过滤查询...")
        try:
            # 测试多种标量类型的查询
            queries = [
                {
                    "name": "按品牌查询（VARCHAR）",
                    "filter": 'brand == "华为"',
                    "limit": 5
                },
                {
                    "name": "按价格范围查询（FLOAT）",
                    "filter": "price >= 1000.0 and price <= 3000.0",
                    "limit": 5
                },
                {
                    "name": "按库存查询（INT64）",
                    "filter": "stock > 500",
                    "limit": 5
                },
                {
                    "name": "按可用性查询（BOOL）",
                    "filter": "is_available == true",
                    "limit": 3
                },
                {
                    "name": "组合查询（多个条件）",
                    "filter": 'category == "电脑办公" and rating >= 4.5 and is_available == true',
                    "limit": 3
                }
            ]

            all_success = True
            for query_info in queries:
                print(f"\n测试: {query_info['name']}")
                print(f"  过滤条件: {query_info['filter']}")

                start_time = time.time()
                query_results = self.client.query(
                    collection_name=self.collection_name,
                    filter=query_info['filter'],
                    output_fields=["product_name", "brand", "category", "price",
                                  "stock", "rating", "is_available", "sales_count"],
                    limit=query_info['limit']
                )
                elapsed_time = time.time() - start_time

                print(f"  ✓ 结果数量: {len(query_results)}, 耗时: {elapsed_time:.3f} 秒")

                # 显示前2个结果
                if query_results:
                    for i, result in enumerate(query_results[:2]):
                        print(f"    [{i+1}] {result.get('product_name', 'N/A')[:40]}")
                        print(f"        价格: ¥{result.get('price', 0):.2f}, 库存: {result.get('stock', 0)}, "
                              f"评分: {result.get('rating', 0)}★")

            print(f"\n✓ 所有标量查询测试完成")
            return True
        except Exception as e:
            print(f"✗ 查询失败: {e}")
            return False

    def get_collection_stats(self):
        """获取集合统计信息"""
        print(f"\n正在获取集合统计信息...")
        try:
            # 获取集合信息
            stats = self.client.get_collection_stats(collection_name=self.collection_name)

            print(f"✓ 集合统计信息:")
            print(f"  - 集合名称: {self.collection_name}")
            print(f"  - 实体数量: {stats['row_count']}")
            print(f"  - 向量维度: {self.dimension}")
            print(f"  - 标量字段: 10 个 (VARCHAR×4, FLOAT×2, INT64×2, BOOL×1, JSON×1)")

            # 查询一些统计数据
            try:
                # 统计有货商品数量
                available_count = len(self.client.query(
                    collection_name=self.collection_name,
                    filter="is_available == true",
                    output_fields=["id"],
                    limit=10000
                ))

                # 统计品牌分布（示例）
                brands = ["华为", "苹果", "小米"]
                brand_stats = {}
                for brand in brands:
                    count = len(self.client.query(
                        collection_name=self.collection_name,
                        filter=f'brand == "{brand}"',
                        output_fields=["id"],
                        limit=10000
                    ))
                    brand_stats[brand] = count

                print(f"\n  数据分析:")
                print(f"  - 有货商品数: {available_count}")
                print(f"  - 品牌分布: {', '.join([f'{k}({v}条)' for k, v in brand_stats.items()])}")

            except Exception as e:
                print(f"  (统计分析跳过: {e})")

            return True
        except Exception as e:
            print(f"✗ 获取统计信息失败: {e}")
            return False

    def cleanup(self):
        """清理测试数据"""
        print(f"\n正在清理测试数据...")
        try:
            if self.client and self.client.has_collection(self.collection_name):
                self.client.drop_collection(self.collection_name)
                print(f"✓ 集合 {self.collection_name} 已删除")
            return True
        except Exception as e:
            print(f"✗ 清理失败: {e}")
            return False

    def run_all_tests(self):
        """运行所有测试"""
        print("=" * 60)
        print("Milvus Windows 功能测试")
        print("=" * 60)

        results = []

        # 1. 连接测试
        results.append(("连接 Milvus", self.connect()))
        if not results[-1][1]:
            print("\n测试终止：无法连接到 Milvus")
            return False

        # 2. 创建集合
        results.append(("创建集合", self.create_collection()))
        if not results[-1][1]:
            print("\n测试终止：无法创建集合")
            return False

        # 3. 插入数据
        results.append(("插入数据", self.insert_data(num_entities=1000)))

        # 4. 向量搜索
        results.append(("向量搜索", self.search_data(num_queries=5, top_k=5)))

        # 5. 过滤查询
        results.append(("过滤查询", self.query_data()))

        # 6. 统计信息
        results.append(("统计信息", self.get_collection_stats()))

        # 7. 清理
        results.append(("清理数据", self.cleanup()))

        # 打印测试总结
        print("\n" + "=" * 60)
        print("测试总结")
        print("=" * 60)

        passed = sum(1 for _, result in results if result)
        total = len(results)

        for test_name, result in results:
            status = "✓ 通过" if result else "✗ 失败"
            print(f"{test_name:15s} : {status}")

        print("-" * 60)
        print(f"总计: {passed}/{total} 测试通过")
        print("=" * 60)

        return passed == total


def main():
    """主函数"""
    # 默认使用 Milvus Lite（本地文件）
    # 如果你想连接到 Milvus 服务器，请修改 uri
    uri = "milvus_demo.db"  # Milvus Lite
    # uri = "http://localhost:19530"  # 本地 Milvus 服务器

    print("""
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

使用说明：
---------
1. 确保已安装 pymilvus: pip install pymilvus
2. 默认使用 Milvus Lite (无需启动服务)
3. 如需连接 Milvus 服务器，请修改脚本中的 uri 参数
    """)

    # 创建测试实例
    test = MilvusSimpleTest(uri=uri)

    # 运行所有测试
    success = test.run_all_tests()

    # 返回退出码
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
