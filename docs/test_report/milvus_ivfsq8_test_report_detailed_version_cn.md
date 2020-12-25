# milvus_ivfsq8_test_report_detailed_version_cn

## 概述

本文描述了ivfsq8索引在milvus单机部署方式下的测试结果。



## 测试目标

参数不同情况下的查询时间和召回率。



## 测试方法

### 软硬件环境

操作系统：CentOS Linux release 7.6.1810 (Core) 

CPU：Intel(R) Xeon(R) CPU E5-2678 v3 @ 2.50GHz

GPU0：GeForce GTX 1080

GPU1：GeForce GTX 1080

内存：503GB

Docker版本：18.09

NVIDIA Driver版本：430.34

Milvus版本：0.5.3

SDK接口：Python 3.6.8

pymilvus版本：0.2.5



### 数据模型

本测试中用到的主要数据:

- 数据来源: sift1b
- 数据类型: hdf5

关于该数据集的详细信息请参考 : http://corpus-texmex.irisa.fr/ 。



### 测试指标

- Query Elapsed Time：数据库查询所有向量的时间（以秒计）。影响Query Elapsed Time的变量：

  - nq (被查询向量的数量)

  > 备注：在向量查询测试中，我们会测试下面参数不同的取值来观察结果：
  >
  > 被查询向量的数量nq将按照 [1, 5, 10,  200, 400, 600, 800, 1000]的数量分组。 

- Recall：实际返回的正确结果占总数之比。影响Recall的变量：

  - nq (被查询向量的数量)
  - topk (单条查询中最相似的K个结果)

  > 备注：在向量准确性测试中，我们会测试下面参数不同的取值来观察结果：
  >
  > 被查询向量的数量nq将按照 [10,  200, 400, 600, 800, 1000]的数量分组，
  >
  > 单条查询中最相似的K个结果topk将按照[1, 10, 100]的数量分组。



## 测试报告

### 测试环境

数据集：sift1b-1,000,000,000向量，128维

表格属性：

- nlist: 16384
- metric_type: L2

查询设置：

- nprobe: 32

Milvus设置：

- cpu_cache_capacity: 150
- gpu_cache_capacity: 6
- use_blas_threshold: 1100

Milvus设置的详细定义可以参考 https://milvus.io/docs/en/reference/milvus_config/。

测试方法

通过一次仅改变一个参数的值，测试查询向量时间和召回率。

- 查询后是否重启Milvus：否



### 性能测试

#### 数据查询

测试结果

Query Elapsed Time 

topk : 100

search_resources: gpu0, gpu1

| nq/topk | topk=100 |
| :-----: | :------: |
|  nq=1   |  15.57   |
|  nq=10  |  15.80   |
| nq=200  |  15.72   |
| nq=400  |  15.94   |
| nq=600  |  16.58   |
| nq=800  |  16.71   |
| nq=1000 |  16.91   |

当nq为1000时，在GPU模式下查询一条128维向量需要耗时约17毫秒。 



topk : 100

search_resources: cpu, gpu0

| nq/topk | topk=100 |
| :-----: | :------: |
|  nq=1   |   1.12   |
|  nq=10  |   2.89   |
| nq=200  |   8.10   |
| nq=400  |  12.36   |
| nq=600  |  17.81   |
| nq=800  |  23.24   |
| nq=1000 |  27.41   |

当nq为1000时，在CPU模式下查询一条128维向量需要耗时约27毫秒。 



**总结**

在CPU模式下查询耗时随nq的增长快速增大，而在GPU模式下查询耗时的增大则缓慢许多。当nq较小时，CPU模式比GPU模式耗时更少。但当nq足够大时，GPU模式则更具有优势。

在GPU模式下的查询耗时由两部分组成：（1）索引从CPU到GPU的拷贝时间；（2）所有分桶的查询时间。当nq小于500时，索引从CPU到GPU 的拷贝时间无法被有效均摊，此时CPU模式时一个更优的选择；当nq大于500时，选择GPU模式更合理。

和CPU相比，GPU具有更多的核数和更强的算力。当nq较大时，GPU在计算上的优势能被更好地被体现。



### 召回率测试

**测试结果**

topk = 1 : recall - recall@1

topk = 10 : recall - recall@10

topk = 100 : recall - recall@100

我们利用sift1b数据集中的ground_truth来计算查询结果的召回率。



Recall of GPU Mode

search_resources: gpu0, gpu1

| nq/topk | topk=1 | topk=10 | topk=100 |
| :-----: | :----: | :-----: | :------: |
|  nq=10  | 0.900  |  0.910  |  0.939   |
| nq=200  | 0.955  |  0.941  |  0.929   |
| nq=400  | 0.958  |  0.944  |  0.932   |
| nq=600  | 0.952  |  0.946  |  0.934   |
| nq=800  | 0.941  |  0.943  |  0.930   |
| nq=1000 | 0.938  |  0.942  |  0.930   |



Recall of CPU Mode

search_resources: cpu, gpu0

| nq/topk | topk=1 | topk=10 | topk=100 |
| :-----: | :----: | :-----: | :------: |
|  nq=10  | 0.900  |  0.910  |  0.939   |
| nq=200  | 0.955  |  0.941  |  0.929   |
| nq=400  | 0.958  |  0.944  |  0.932   |
| nq=600  | 0.952  |  0.946  |  0.934   |
| nq=800  | 0.941  |  0.943  |  0.930   |
| nq=1000 | 0.938  |  0.942  |  0.930   |



**总结**

随着nq的增大，召回率逐渐稳定至93%以上。

