# milvus_ivfsq8h_test_report_detailed_version

## Summary

This document contains the test reports of IVF_SQ8H index on Milvus single server.



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

For details on this dataset, please check : http://corpus-texmex.irisa.fr/ .



### Measures

- Query Elapsed Time: Time cost (in seconds) to run a query. Variables that affect Query Elapsed Time:

  - nq (Number of queried vectors)
  
> Note: In the query test of query elapsed time, we will test the following parameters with different values:
  >
  > nq - grouped by: [1, 5, 10, 50, 100, 200, 400, 600, 800, 1000, 1200, 1400, 1600, 1800].
  >

- Recall: The fraction of the total amount of relevant instances that were actually retrieved . Variables that affect Recall:

  - nq (Number of queried vectors)
  - topk (Top k result of a query)

  > Note: In the query test of recall, we will test the following parameters with different values:
  >
  > nq - grouped by: [10, 50, 100, 200, 400, 600, 800, 1000, 1200, 1400, 1600, 1800], 
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
- gpu_search_threshold: 1200
- search_resources: cpu, gpu0, gpu1

The definitions of Milvus configuration are on https://milvus.io/docs/milvus_config.md.

Test method

Test the query elapsed time and recall with several parameters, and once only change one parameter.

- Whether to restart Milvus after each query: No



### Performance test

#### Data query

**Test result**

Query Elapsed Time 

topk = 100

| nq/topk | topk=100 |
| :-----: | :------: |
|  nq=1   |   0.34   |
|  nq=5   |   0.72   |
|  nq=10  |   0.91   |
|  nq=50  |   1.51   |
| nq=100  |   2.49   |
| nq=200  |   4.09   |
| nq=400  |   7.32   |
| nq=600  |  10.63   |
| nq=800  |  13.84   |
| nq=1000 |  16.83   |
| nq=1200 |  18.20   |
| nq=1400 |   20.1   |
| nq=1600 |   20.0   |
| nq=1800 |  19.86   |

When nq is 1800, the query time cost of a 128-dimension vector is around 11ms. 



**Conclusion**

When nq < 1200, the query elapsed time increases quickly with nq; when nq > 1200, the query elapsed time increases much slower. It is because gpu_search_threshold is set to 1200, when nq < 1200, CPU is  chosen to do the query, otherwise GPU is chosen. Compared with CPU, GPU has much more cores and stronger computing capability. When nq is large, it can better reflect GPU's advantages on computing.

The query elapsed time consists of two parts: (1) index CPU-to-GPU copy time; (2) nprobe buckets search time. When nq is larger enough, index CPU-to-GPU copy time can be amortized efficiently. So Milvus performs well through setting suitable gpu_search_threshold.



### Recall test

**Test result**

topk = 1 : recall - recall@1

topk = 10 : recall - recall@10

topk = 100 : recall - recall@100

We use the ground_truth in sift1b dataset to calculate the recall of query results.

| nq/topk | topk=1 | topk=10 | topk=100 |
| :-----: | :----: | :-----: | :------: |
|  nq=10  | 0.900  |  0.910  |  0.939   |
|  nq=50  | 0.980  |  0.950  |  0.941   |
| nq=100  | 0.970  |  0.937  |  0.931   |
| nq=200  | 0.955  |  0.941  |  0.929   |
| nq=400  | 0.958  |  0.944  |  0.932   |
| nq=600  | 0.952  |  0.946  |  0.934   |
| nq=800  | 0.941  |  0.943  |  0.930   |
| nq=1000 | 0.938  |  0.942  |  0.930   |
| nq=1200 | 0.937  |  0.943  |  0.931   |
| nq=1400 | 0.939  |  0.945  |  0.931   |
| nq=1600 | 0.936  |  0.945  |  0.931   |
| nq=1800 | 0.937  |  0.946  |  0.932   |



**Conclusion**

As nq increases, the recall gradually stabilizes to over 93%. The usage of CPU or GPU and different topk are not related to recall.