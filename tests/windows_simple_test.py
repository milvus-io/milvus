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
from pymilvus import MilvusClient, DataType
import numpy as np


class MilvusSimpleTest:
    """Milvus 简单功能测试类"""

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
        self.dimension = 128

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
        """创建测试集合"""
        print(f"\n正在创建集合: {self.collection_name}")
        try:
            # 检查集合是否已存在
            if self.client.has_collection(self.collection_name):
                print(f"集合 {self.collection_name} 已存在，正在删除...")
                self.client.drop_collection(self.collection_name)

            # 创建集合
            self.client.create_collection(
                collection_name=self.collection_name,
                dimension=self.dimension,
                metric_type="L2",  # 使用欧式距离
                auto_id=True,      # 自动生成 ID
            )
            print(f"✓ 集合创建成功: {self.collection_name}")
            return True
        except Exception as e:
            print(f"✗ 集合创建失败: {e}")
            return False

    def insert_data(self, num_entities=1000):
        """
        插入测试数据

        参数:
            num_entities: 要插入的实体数量
        """
        print(f"\n正在插入 {num_entities} 条数据...")
        try:
            # 生成随机向量数据
            vectors = np.random.rand(num_entities, self.dimension).tolist()

            # 准备数据
            data = [
                {
                    "vector": vectors[i],
                    "text": f"这是第 {i} 条测试数据",
                    "subject": f"主题_{i % 10}",
                }
                for i in range(num_entities)
            ]

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
            # 生成随机查询向量
            query_vectors = np.random.rand(num_queries, self.dimension).tolist()

            # 执行搜索
            start_time = time.time()
            search_results = self.client.search(
                collection_name=self.collection_name,
                data=query_vectors,
                limit=top_k,
                output_fields=["text", "subject"]
            )
            elapsed_time = time.time() - start_time

            print(f"✓ 搜索成功")
            print(f"  - 查询数量: {len(search_results)}")
            print(f"  - 耗时: {elapsed_time:.2f} 秒")

            # 显示第一个查询的结果
            if search_results and len(search_results[0]) > 0:
                print(f"\n第一个查询的 Top-{min(3, top_k)} 结果:")
                for i, result in enumerate(search_results[0][:3]):
                    print(f"  {i+1}. 距离: {result['distance']:.4f}")
                    print(f"     文本: {result['entity'].get('text', 'N/A')}")

            return True
        except Exception as e:
            print(f"✗ 搜索失败: {e}")
            return False

    def query_data(self):
        """执行标量过滤查询"""
        print(f"\n正在执行过滤查询...")
        try:
            # 查询特定主题的数据
            query_filter = 'subject == "主题_0"'

            start_time = time.time()
            query_results = self.client.query(
                collection_name=self.collection_name,
                filter=query_filter,
                output_fields=["text", "subject"],
                limit=10
            )
            elapsed_time = time.time() - start_time

            print(f"✓ 查询成功")
            print(f"  - 过滤条件: {query_filter}")
            print(f"  - 结果数量: {len(query_results)}")
            print(f"  - 耗时: {elapsed_time:.2f} 秒")

            # 显示前3个结果
            if query_results:
                print(f"\n前 {min(3, len(query_results))} 条结果:")
                for i, result in enumerate(query_results[:3]):
                    print(f"  {i+1}. 文本: {result.get('text', 'N/A')}")
                    print(f"     主题: {result.get('subject', 'N/A')}")

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
