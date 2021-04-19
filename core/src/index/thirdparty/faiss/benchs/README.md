
# Benchmarking scripts

This directory contains benchmarking scripts that can reproduce the
numbers reported in the two papers

```
@inproceedings{DJP16,
  Author = {Douze, Matthijs and J{\'e}gou, Herv{\'e} and Perronnin, Florent},
  Booktitle = "ECCV",
  Organization = {Springer},
  Title = {Polysemous codes},
  Year = {2016}
}
```
and

```
@inproceedings{JDJ17,
   Author = {Jeff Johnson and Matthijs Douze and Herv{\'e} J{\'e}gou},
   journal= {arXiv:1702.08734},,
   Title = {Billion-scale similarity search with GPUs},
   Year = {2017},
}
```

Note that the numbers (especially timings) change slightly due to changes in the implementation, different machines, etc.

The scripts are self-contained. They depend only on Faiss and external training data that should be stored in sub-directories.

## SIFT1M experiments

The script [`bench_polysemous_sift1m.py`](bench_polysemous_sift1m.py) reproduces the numbers in
Figure 3 from the "Polysemous" paper.

### Getting SIFT1M

To run it, please download the ANN_SIFT1M dataset from

http://corpus-texmex.irisa.fr/

and unzip it to the subdirectory sift1M.

### Result

The output looks like:

```
PQ training on 100000 points, remains 0 points: training polysemous on centroids
add vectors to index
PQ baseline        7.517 ms per query, R@1 0.4474
Polysemous 64      9.875 ms per query, R@1 0.4474
Polysemous 62      8.358 ms per query, R@1 0.4474
Polysemous 58      5.531 ms per query, R@1 0.4474
Polysemous 54      3.420 ms per query, R@1 0.4478
Polysemous 50      2.182 ms per query, R@1 0.4475
Polysemous 46      1.621 ms per query, R@1 0.4408
Polysemous 42      1.448 ms per query, R@1 0.4174
Polysemous 38      1.331 ms per query, R@1 0.3563
Polysemous 34      1.334 ms per query, R@1 0.2661
Polysemous 30      1.272 ms per query, R@1 0.1794
```


## Experiments on 1B elements dataset

The script [`bench_polysemous_1bn.py`](bench_polysemous_1bn.py) reproduces a few experiments on
two datasets of size 1B from the Polysemous codes" paper.


### Getting BIGANN

Download the four files of ANN_SIFT1B from
http://corpus-texmex.irisa.fr/ to subdirectory bigann/

### Getting Deep1B

The ground-truth and queries are available here 

https://yadi.sk/d/11eDCm7Dsn9GA

For the learning and database vectors, use the script

https://github.com/arbabenko/GNOIMI/blob/master/downloadDeep1B.py

to download the data to subdirectory deep1b/, then concatenate the
database files to base.fvecs and the training files to learn.fvecs

### Running the experiments

These experiments are quite long. To support resuming, the script
stores the result of training to a temporary directory, `/tmp/bench_polysemous`.

The script `bench_polysemous_1bn.py` takes at least two arguments:

- the dataset name: SIFT1000M (aka SIFT1B, aka BIGANN) or Deep1B. SIFT1M, SIFT2M,... are also supported to make subsets of for small experiments (note that SIFT1M as a subset of SIFT1B is not the same as the SIFT1M above)

- the type of index to build, which should be a valid [index_factory key](https://github.com/facebookresearch/faiss/wiki/High-level-interface-and-auto-tuning#index-factory) (see below for examples)

- the remaining arguments are parsed as search-time parameters.

### Experiments of Table 2

The `IMI*+PolyD+ADC` results in Table 2 can be reproduced with (for 16 bytes):

```
python bench_polysemous_1bn.par SIFT1000M IMI2x12,PQ16 nprobe=16,max_codes={10000,30000},ht={44..54}
```

Training takes about 2 minutes and adding vectors to the dataset
takes 3.1 h. These operations are multithreaded. Note that in the command
above, we use bash's [brace expansion](https://www.gnu.org/software/bash/manual/html_node/Brace-Expansion.html) to set a grid of parameters.

The search is *not* multithreaded, and the output looks like:

```
                                        R@1    R@10   R@100     time    %pass
nprobe=16,max_codes=10000,ht=44         0.1779 0.2994 0.3139    0.194   12.45
nprobe=16,max_codes=10000,ht=45         0.1859 0.3183 0.3339    0.197   14.24
nprobe=16,max_codes=10000,ht=46         0.1930 0.3366 0.3543    0.202   16.22
nprobe=16,max_codes=10000,ht=47         0.1993 0.3550 0.3745    0.209   18.39
nprobe=16,max_codes=10000,ht=48         0.2033 0.3694 0.3917    0.640   20.77
nprobe=16,max_codes=10000,ht=49         0.2070 0.3839 0.4077    0.229   23.36
nprobe=16,max_codes=10000,ht=50         0.2101 0.3949 0.4205    0.232   26.17
nprobe=16,max_codes=10000,ht=51         0.2120 0.4042 0.4310    0.239   29.21
nprobe=16,max_codes=10000,ht=52         0.2134 0.4113 0.4402    0.245   32.47
nprobe=16,max_codes=10000,ht=53         0.2157 0.4184 0.4482    0.250   35.96
nprobe=16,max_codes=10000,ht=54         0.2170 0.4240 0.4546    0.256   39.66
nprobe=16,max_codes=30000,ht=44         0.1882 0.3327 0.3555    0.226   11.29
nprobe=16,max_codes=30000,ht=45         0.1964 0.3525 0.3771    0.231   13.05
nprobe=16,max_codes=30000,ht=46         0.2039 0.3713 0.3987    0.236   15.01
nprobe=16,max_codes=30000,ht=47         0.2103 0.3907 0.4202    0.245   17.19
nprobe=16,max_codes=30000,ht=48         0.2145 0.4055 0.4384    0.251   19.60
nprobe=16,max_codes=30000,ht=49         0.2179 0.4198 0.4550    0.257   22.25
nprobe=16,max_codes=30000,ht=50         0.2208 0.4305 0.4681    0.268   25.15
nprobe=16,max_codes=30000,ht=51         0.2227 0.4402 0.4791    0.275   28.30
nprobe=16,max_codes=30000,ht=52         0.2241 0.4473 0.4884    0.284   31.70
nprobe=16,max_codes=30000,ht=53         0.2265 0.4544 0.4965    0.294   35.34
nprobe=16,max_codes=30000,ht=54         0.2278 0.4601 0.5031    0.303   39.20
```

The result reported in table 2 is the one for which the %pass (percentage of code comparisons that pass the Hamming check) is around 20%, which occurs for Hamming threshold `ht=48`.

The 8-byte results can be reproduced with the factory key `IMI2x12,PQ8`

### Experiments of the appendix

The experiments in the appendix are only in the ArXiv version of the paper (table 3). 

```
python bench_polysemous_1bn.py SIFT1000M OPQ8_64,IMI2x13,PQ8 nprobe={1,2,4,8,16,32,64,128},ht={20,24,26,28,30}

               	R@1    R@10   R@100     time    %pass
nprobe=1,ht=20 	0.0351 0.0616 0.0751    0.158   19.01
...
nprobe=32,ht=28 	0.1256 0.3563 0.5026    0.561   52.61
...
```
Here again the runs are not exactly the same but the original result was obtained from nprobe=32,ht=28.

For Deep1B, we used a simple version of [auto-tuning](https://github.com/facebookresearch/faiss/wiki/High-level-interface-and-auto-tuning/_edit#auto-tuning-the-runtime-parameters) to sweep through the set of operating points:

```
python bench_polysemous_1bn.py Deep1B OPQ20_80,IMI2x14,PQ20 autotune
...
Done in 4067.555 s, available OPs:
Parameters                                1-R@1     time
                                          0.0000    0.000
nprobe=1,ht=22,max_codes=256              0.0215    3.115
nprobe=1,ht=30,max_codes=256              0.0381    3.120
...
nprobe=512,ht=68,max_codes=524288         0.4478   36.903
nprobe=1024,ht=80,max_codes=131072        0.4557   46.363
nprobe=1024,ht=78,max_codes=262144        0.4616   61.939
...
```
The original results were obtained with `nprobe=1024,ht=66,max_codes=262144`.


## GPU experiments

The benchmarks below run 1 or 4 Titan X GPUs and reproduce the results of the "GPU paper". They are also a good starting point on how to use GPU Faiss. 

### Search on SIFT1M

See above on how to get SIFT1M into subdirectory sift1M/. The script [`bench_gpu_sift1m.py`](bench_gpu_sift1m.py) reproduces the "exact k-NN time" plot in the ArXiv paper, and the SIFT1M numbers. 

The output is:
```
============ Exact search
add vectors to index
warmup
benchmark
k=1 0.715 s, R@1 0.9914
k=2 0.729 s, R@1 0.9935
k=4 0.731 s, R@1 0.9935
k=8 0.732 s, R@1 0.9935
k=16 0.742 s, R@1 0.9935
k=32 0.737 s, R@1 0.9935
k=64 0.753 s, R@1 0.9935
k=128 0.761 s, R@1 0.9935
k=256 0.799 s, R@1 0.9935
k=512 0.975 s, R@1 0.9935
k=1024 1.424 s, R@1 0.9935
============ Approximate search
train
WARNING clustering 100000 points to 4096 centroids: please provide at least 159744 training points
add vectors to index
WARN: increase temp memory to avoid cudaMalloc, or decrease query/add size (alloc 256000000 B, highwater 256000000 B)
warmup
benchmark
nprobe=   1 0.043 s recalls= 0.3909 0.4312 0.4312
nprobe=   2 0.040 s recalls= 0.5041 0.5636 0.5636
nprobe=   4 0.048 s recalls= 0.6048 0.6897 0.6897
nprobe=   8 0.064 s recalls= 0.6879 0.8028 0.8028
nprobe=  16 0.088 s recalls= 0.7534 0.8940 0.8940
nprobe=  32 0.134 s recalls= 0.7957 0.9549 0.9550
nprobe=  64 0.224 s recalls= 0.8125 0.9833 0.9834
nprobe= 128 0.395 s recalls= 0.8205 0.9953 0.9954
nprobe= 256 0.717 s recalls= 0.8227 0.9993 0.9994
nprobe= 512 1.348 s recalls= 0.8228 0.9999 1.0000
```
The run produces two warnings:

- the clustering complains that it does not have enough training data, there is not much we can do about this.

- the add() function complains that there is an inefficient memory allocation, but this is a concern only when it happens often, and we are not benchmarking the add time anyways.

To index small datasets, it is more efficient to use a `GpuIVFFlat`, which just stores the full vectors in the inverted lists. We did not mention this in the the paper because it is not as scalable. To experiment with this setting, change the `index_factory` string from "IVF4096,PQ64" to "IVF16384,Flat". This gives:

```
nprobe=   1 0.025 s recalls= 0.4084 0.4105 0.4105
nprobe=   2 0.033 s recalls= 0.5235 0.5264 0.5264
nprobe=   4 0.033 s recalls= 0.6332 0.6367 0.6367
nprobe=   8 0.040 s recalls= 0.7358 0.7403 0.7403
nprobe=  16 0.049 s recalls= 0.8273 0.8324 0.8324
nprobe=  32 0.068 s recalls= 0.8957 0.9024 0.9024
nprobe=  64 0.104 s recalls= 0.9477 0.9549 0.9549
nprobe= 128 0.174 s recalls= 0.9760 0.9837 0.9837
nprobe= 256 0.299 s recalls= 0.9866 0.9944 0.9944
nprobe= 512 0.527 s recalls= 0.9907 0.9987 0.9987
```

### Clustering on MNIST8m

To get the "infinite MNIST dataset", follow the instructions on [Léon Bottou's website](http://leon.bottou.org/projects/infimnist). The script assumes the file `mnist8m-patterns-idx3-ubyte` is in subdirectory `mnist8m`

The script [`kmeans_mnist.py`](kmeans_mnist.py) produces the following output: 

```
python kmeans_mnist.py 1 256
...
Clustering 8100000 points in 784D to 256 clusters, redo 1 times, 20 iterations
  Preprocessing in 7.94526 s
  Iteration 19 (131.697 s, search 114.78 s): objective=1.44881e+13 imbalance=1.05963 nsplit=0        
final objective: 1.449e+13
total runtime: 140.615 s
```

### search on SIFT1B

The script [`bench_gpu_1bn.py`](bench_gpu_1bn.py) runs multi-gpu searches on the two 1-billion vector datasets we considered. It is more complex than the previous scripts, because it supports many search options and decomposes the dataset build process in Python to exploit the best possible CPU/GPU parallelism and GPU distribution.

Even on multiple GPUs, building the 1B datasets can last several hours. It is often a good idea to validate that everything is working fine on smaller datasets like SIFT1M, SIFT2M, etc.

The search results on SIFT1B in the "GPU paper" can be obtained with 

<!-- see P57124181 -->

```
python bench_gpu_1bn.py SIFT1000M OPQ8_32,IVF262144,PQ8 -nnn 10 -ngpu 1 -tempmem $[1536*1024*1024]
...
0/10000 (0.024 s)      probe=1  : 0.161 s 1-R@1: 0.0752 1-R@10: 0.1924
0/10000 (0.005 s)      probe=2  : 0.150 s 1-R@1: 0.0964 1-R@10: 0.2693
0/10000 (0.005 s)      probe=4  : 0.153 s 1-R@1: 0.1102 1-R@10: 0.3328
0/10000 (0.005 s)      probe=8  : 0.170 s 1-R@1: 0.1220 1-R@10: 0.3827
0/10000 (0.005 s)      probe=16 : 0.196 s 1-R@1: 0.1290 1-R@10: 0.4151
0/10000 (0.006 s)      probe=32 : 0.244 s 1-R@1: 0.1314 1-R@10: 0.4345
0/10000 (0.006 s)      probe=64 : 0.353 s 1-R@1: 0.1332 1-R@10: 0.4461
0/10000 (0.005 s)      probe=128: 0.587 s 1-R@1: 0.1341 1-R@10: 0.4502
0/10000 (0.006 s)      probe=256: 1.160 s 1-R@1: 0.1342 1-R@10: 0.4511
```

We use the `-tempmem` option to reduce the temporary memory allocation to 1.5G, otherwise the dataset does not fit in GPU memory

### search on Deep1B

The same script generates the GPU search results on Deep1B. 

```
python bench_gpu_1bn.py  Deep1B OPQ20_80,IVF262144,PQ20 -nnn 10 -R 2 -ngpu 4 -altadd -noptables -tempmem $[1024*1024*1024]
...

0/10000 (0.115 s)      probe=1  : 0.239 s 1-R@1: 0.2387 1-R@10: 0.3420
0/10000 (0.006 s)      probe=2  : 0.103 s 1-R@1: 0.3110 1-R@10: 0.4623
0/10000 (0.005 s)      probe=4  : 0.105 s 1-R@1: 0.3772 1-R@10: 0.5862
0/10000 (0.005 s)      probe=8  : 0.116 s 1-R@1: 0.4235 1-R@10: 0.6889
0/10000 (0.005 s)      probe=16 : 0.133 s 1-R@1: 0.4517 1-R@10: 0.7693
0/10000 (0.005 s)      probe=32 : 0.168 s 1-R@1: 0.4713 1-R@10: 0.8281
0/10000 (0.005 s)      probe=64 : 0.238 s 1-R@1: 0.4841 1-R@10: 0.8649
0/10000 (0.007 s)      probe=128: 0.384 s 1-R@1: 0.4900 1-R@10: 0.8816
0/10000 (0.005 s)      probe=256: 0.736 s 1-R@1: 0.4933 1-R@10: 0.8912
```

Here we are a bit tight on memory so we disable precomputed tables (`-noptables`) and restrict the amount of temporary memory. The `-altadd` option avoids GPU memory overflows during add.


### knn-graph on Deep1B

The same script generates the KNN-graph on Deep1B. Note that the inverted file from above will not be re-used because the training sets are different. For the knngraph, the script will first do a pass over the whole dataset to compute the ground-truth knn for a subset of 10k nodes, for evaluation.

```
python bench_gpu_1bn.py Deep1B OPQ20_80,IVF262144,PQ20 -nnn 10 -altadd -knngraph  -R 2 -noptables -tempmem $[1<<30] -ngpu 4
...
CPU index contains 1000000000 vectors, move to GPU
Copy CPU index to 2 sharded GPU indexes
   dispatch to GPUs 0:2
IndexShards shard 0 indices 0:500000000
  IndexIVFPQ size 500000000 -> GpuIndexIVFPQ indicesOptions=0 usePrecomputed=0 useFloat16=0 reserveVecs=0
IndexShards shard 1 indices 500000000:1000000000
  IndexIVFPQ size 500000000 -> GpuIndexIVFPQ indicesOptions=0 usePrecomputed=0 useFloat16=0 reserveVecs=0
   dispatch to GPUs 2:4
IndexShards shard 0 indices 0:500000000
  IndexIVFPQ size 500000000 -> GpuIndexIVFPQ indicesOptions=0 usePrecomputed=0 useFloat16=0 reserveVecs=0
IndexShards shard 1 indices 500000000:1000000000
  IndexIVFPQ size 500000000 -> GpuIndexIVFPQ indicesOptions=0 usePrecomputed=0 useFloat16=0 reserveVecs=0
move to GPU done in 151.535 s
search...
999997440/1000000000 (8389.961 s, 0.3379)      probe=1  : 8389.990 s rank-10 intersection results: 0.3379
999997440/1000000000 (9205.934 s, 0.4079)      probe=2  : 9205.966 s rank-10 intersection results: 0.4079
999997440/1000000000 (9741.095 s, 0.4722)      probe=4  : 9741.128 s rank-10 intersection results: 0.4722
999997440/1000000000 (10830.420 s, 0.5256)      probe=8  : 10830.455 s rank-10 intersection results: 0.5256
999997440/1000000000 (12531.716 s, 0.5603)      probe=16 : 12531.758 s rank-10 intersection results: 0.5603
999997440/1000000000 (15922.519 s, 0.5825)      probe=32 : 15922.571 s rank-10 intersection results: 0.5825
999997440/1000000000 (22774.153 s, 0.5950)      probe=64 : 22774.220 s rank-10 intersection results: 0.5950
999997440/1000000000 (36717.207 s, 0.6015)      probe=128: 36717.309 s rank-10 intersection results: 0.6015
999997440/1000000000 (70616.392 s, 0.6047)      probe=256: 70616.581 s rank-10 intersection results: 0.6047
```
