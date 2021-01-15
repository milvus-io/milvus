# ivffalt_test_report_en

## Summary

This document contains the test reports of IVF_FLAT index on Milvus single server.



## Test objectives

The time cost and recall when searching with different parameters.`



## Test method

### Hardware/Software requirements

Operating System: Ubuntu 18.04

CPU: Intel(R) Xeon(R) Platinum 8163 CPU @ 2.50GHz

GPU0: GeForce RTX 2080Ti 11GB

GPU1: GeForce RTX 2080Ti 11GB

GPU2: GeForce RTX 2080Ti 11GB

GPU3: GeForce RTX 2080Ti 11GB

Memory: 768GB

Docker version: 19.03

NVIDIA Driver version: 430.50

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
  > nq - grouped by: [1, 5, 10,  100, 200, 400, 600, 800, 1000], 

- Recall: The fraction of the total amount of relevant instances that were actually retrieved . Variables that affect Recall:

  - nq (Number of queried vectors)
  - topk (Top k result of a query)

  > Note: In the query test of recall, we will test the following parameters with different values:
  >
  > nq - grouped by: [50, 200, 400, 600, 800, 1000], 
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

- cpu_cache_capacity: 600
- gpu_cache_capacity: 6
- use_blas_threshold: 2100

The definitions of Milvus configuration are on [Milvus Server Configuration](https://milvus.io/docs/milvus_config.md).

Test method

Test the query elapsed time and recall with several parameters, and once only change one parameter.

- Whether to restart Milvus after each query: No



### Performance test

#### Data query

**Test result**

Query Elapsed Time 

topk : 100

search_resources: cpu, gpu0, gpu1, gpu2, gpu3

| nq/topk | topk=100 |
| :-----: | :------: |
|  nq=1   |  0.649   |
|  nq=5   |  0.911   |
|  nq=10  |  1.393   |
| nq=100  |  2.189   |
| nq=200  |  6.134   |
| nq=400  |  9.480   |
| nq=600  |  16.616  |
| nq=800  |  22.225  |
| nq=1000 |  25.901  |

When nq is 1000, the query time cost of a 128-dimension vector is around 26ms in CPU Mode. 



topk : 100

search_resources: gpu0, gpu1, gpu2, gpu3

| nq/topk | topk=100 |
| :-----: | :------: |
|  nq=1   |  14.348  |
|  nq=5   |  14.326  |
|  nq=10  |  14.387  |
| nq=100  |  14.684  |
| nq=200  |  14.665  |
| nq=400  |  14.750  |
| nq=600  |  15.009  |
| nq=800  |  15.350  |
| nq=1000 |  15.336  |

When nq is 1000, the query time cost of a 128-dimension vector is around 15ms in GPU Mode. 



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



Recall of CPU Mode

search_resources: cpu, gpu0, gpu1, gpu2, gpu3

| nq/topk | topk=1 | topk=10 | topk=100 |
| :-----: | :----: | :-----: | :------: |
|  nq=50  | 0.960  |  0.952  |  0.936   |
| nq=200  | 0.975  |  0.964  |  0.939   |
| nq=400  | 0.983  |  0.967  |  0.937   |
| nq=600  | 0.970  |  0.964  |  0.939   |
| nq=800  | 0.970  |  0.960  |  0.939   |
| nq=1000 | 0.976  |  0.961  |  0.941   |



Recall of GPU Mode

search_resources: gpu0, gpu1, gpu2, gpu3

| nq/topk | topk=1 | topk=10 | topk=100 |
| :-----: | :----: | :-----: | :------: |
|  nq=50  | 0.980  |  0.952  |  0.946   |
| nq=200  | 0.970  |  0.962  |  0.934   |
| nq=400  | 0.975  |  0.953  |  0.939   |
| nq=600  | 0.970  |  0.957  |  0.939   |
| nq=800  | 0.981  |  0.963  |  0.941   |
| nq=1000 | 0.979  |  0.964  |  0.938   |



**Conclusion**

As nq increases, the recall gradually stabilizes to over 93%.