# Quick start

## 运行

### 运行示例：

`python3 main.py --image=registry.zilliz.com/milvus/engine:branch-0.3.1-release --run-count=2 --run-type=performance`

### 运行参数：

--image: 容器模式，传入镜像名称，如传入，则运行测试时，会先进行pull image，基于image生成milvus server容器

--local: 与image参数互斥，本地模式，连接使用本地启动的milvus server进行测试

--run-count: 重复运行次数

--suites: 测试集配置文件，默认使用suites.yaml

--run-type: 测试类型，包括性能--performance、准确性测试--accuracy以及稳定性--stability

### 测试集配置文件：

`operations:

  insert:

​    [
​      {"table.index_type": "ivf_flat", "server.index_building_threshold": 300, "table.size": 2000000, "table.ni": 100000, "table.dim": 512},
​    ]

  build: []

  query:

​    [
​      {"dataset": "ip_ivfsq8_1000", "top_ks": [10], "nqs": [10, 100], "server.nprobe": 1, "server.use_blas_threshold": 800},
​      {"dataset": "ip_ivfsq8_1000", "top_ks": [10], "nqs": [10, 100], "server.nprobe": 10, "server.use_blas_threshold": 20},
​    ]`

## 测试结果：

性能：

`INFO:milvus_benchmark.runner:Start warm query, query params: top-k: 1, nq: 1

INFO:milvus_benchmark.client:query run in 19.19s
INFO:milvus_benchmark.runner:Start query, query params: top-k: 64, nq: 10, actually length of vectors: 10
INFO:milvus_benchmark.runner:Start run query, run 1 of 1
INFO:milvus_benchmark.client:query run in 0.2s
INFO:milvus_benchmark.runner:Avarage query time: 0.20
INFO:milvus_benchmark.runner:[[0.2]]`

**│          10 │         0.2 │**

准确率：

`INFO:milvus_benchmark.runner:Avarage accuracy: 1.0`