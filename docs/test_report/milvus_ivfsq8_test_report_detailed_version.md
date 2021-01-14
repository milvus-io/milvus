# milvus_ivfsq8_test_report_detailed_version

## Summary

This document contains the test reports of IVF_SQ8 index on Milvus single server.



## Test objectives

The time cost and recall when searching with different parameters.



## Test method

### Hardware/Software requirements

Operating System: CentOS Linux release 7.6.1810 (Core) 

CPU: Intel(R) Xeon(R) CPU E5-2678 v3 @ 2.50GHz

GPU0: GeForce GTX 1080

GPU1: GeForce GTX 1080

Memory: 503GB

Docker version: 18.09

NVIDIA Driver version: 430.34

Milvus version: 0.5.3

SDK interface: Python 3.6.8

pymilvus version: 0.2.5



### Data model

The data used in the tests are:

- Data source: sift1b
- Data type: hdf5

For details on this dataset, you can check : http://corpus-texmex.irisa.fr/ .



### Measures

- Query Elapsed Time: Time cost (in seconds) to run a query. Variables that affect Query Elapsed Time:

  - nq (Number of queried vectors)

  > Note: In the query test of query elapsed time, we will test the following parameters with different values:
  >
  > nq - grouped by: [1, 5, 10,  200, 400, 600, 800, 1000], 

- Recall: The fraction of the total amount of relevant instances that were actually retrieved . Variables that affect Recall:

  - nq (Number of queried vectors)
  - topk (Top k result of a query)

  > Note: In the query test of recall, we will test the following parameters with different values:
  >
  > nq - grouped by: [10,  200, 400, 600, 800, 1000], 
  >
  > topk - grouped by: [1, 10, 100]



## Test reports

### Test environment

Data base: sift1b-1,000,000,000 vectors, 128-dimension

Table Attributes

- nlist: 16384
- metric_type: L2

Query configuration 

- nprobe: 32

Milvus configuration 

- cpu_cache_capacity: 150
- gpu_cache_capacity: 6
- use_blas_threshold: 1100

The definitions of Milvus configuration are on [Milvus Server Configuration](https://milvus.io/docs/milvus_config.md).

Test method

Test the query elapsed time and recall with several parameters, and once only change one parameter.

- Whether to restart Milvus after each query: No



### Performance test

#### Data query

**Test result**

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

When nq is 1000, the query time cost of a 128-dimension vector is around 17ms in GPU Mode. 



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

When nq is 1000, the query time cost of a 128-dimension vector is around 27ms in CPU Mode. 



**Conclusion**

The query elapsed time in CPU Mode increases quickly with nq, while in GPU Mode query elapsed time  increases much slower. When nq is small,  CPU Mode consumes less time than GPU Mode. However, as nq becomes larger, GPU Mode shows its advantage against CPU Mode. 

The query elapsed time in GPU Mode consists of two parts: (1) index CPU-to-GPU copy time; (2) nprobe buckets search time. When nq is smaller than 500, index CPU-to-GPU copy time cannot be amortized efficiently, CPU Mode is a better choice; when nq is larger than 500, choosing GPU Mode is better.

Compared with CPU, GPU has much more cores and stronger computing capability. When nq is large, it can better reflect GPU's advantages on computing.



### Recall test

**Test result**

topk = 1 : recall - recall@1

topk = 10 : recall - recall@10

topk = 100 : recall - recall@100

We use the ground_truth in sift1b dataset to calculate the recall of query results.



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



**Conclusion**

As nq increases, the recall gradually stabilizes to over 93%.