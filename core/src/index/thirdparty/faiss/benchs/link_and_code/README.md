


README for the link & code implementation
=========================================

What is this?
-------------

Link & code is an indexing method that combines HNSW indexing with
compression and exploits the neighborhood structure of the similarity
graph to improve the reconstruction. It is described in

```
@inproceedings{link_and_code,
   author = {Matthijs Douze and Alexandre Sablayrolles and Herv\'e J\'egou},
   title = {Link and code: Fast indexing with graphs and compact regression codes},
   booktitle = {CVPR},
   year = {2018}
}
```

ArXiV [here](https://arxiv.org/abs/1804.09996)

Code structure
--------------

The test runs with 3 files:

- `bench_link_and_code.py`: driver script

- `datasets.py`: code to load the datasets. The example code runs on the
  deep1b and bigann datasets. See the [toplevel README](../README.md)
  on how to downlod them. They should be put in a directory, edit
  datasets.py to set the path.

- `neighbor_codec.py`: this is where the representation is trained.

The code runs on top of Faiss. The HNSW index can be extended with a
`ReconstructFromNeighbors` C++ object that refines the distances. The
training is implemented in Python.


Reproducing Table 2 in the paper
--------------------------------

The results of table 2 (accuracy on deep100M) in the paper can be
obtained with:

```
python bench_link_and_code.py \
   --db deep100M \
   --M0 6 \
   --indexkey OPQ36_144,HNSW32_PQ36 \
   --indexfile $bdir/deep100M_PQ36_L6.index \
   --beta_nsq 4  \
   --beta_centroids $bdir/deep100M_PQ36_L6_nsq4.npy \
   --neigh_recons_codes $bdir/deep100M_PQ36_L6_nsq4_codes.npy \
   --k_reorder 0,5 --efSearch 1,1024
```

Set `bdir` to a scratch directory.

Explanation of the flags:

- `--db deep1M`: dataset to process

- `--M0 6`: number of links on the base level (L6)

- `--indexkey OPQ36_144,HNSW32_PQ36`: Faiss index key to construct the
  HNSW structure. It means that vectors are transformed by OPQ and
  encoded with PQ 36x8 (with an intermediate size of 144D). The HNSW
  level>0 nodes have 32 links (theses ones are "cheap" to store
  because there are fewer nodes in the upper levels.

- `--indexfile $bdir/deep1M_PQ36_M6.index`: name of the index file
  (without information for the L&C extension)

- `--beta_nsq 4`: number of bytes to allocate for the codes (M in the
  paper)

- `--beta_centroids $bdir/deep1M_PQ36_M6_nsq4.npy`: filename to store
  the trained beta centroids

- `--neigh_recons_codes $bdir/deep1M_PQ36_M6_nsq4_codes.npy`: filename
  for the encoded weights (beta) of the combination

- `--k_reorder 0,5`: number of restults to reorder. 0 = baseline
  without reordering, 5 = value used throughout the paper

- `--efSearch 1,1024`: number of nodes to visit (T in the paper)

The script will proceed with the following steps:

0. load dataset (and possibly compute the ground-truth if the
ground-truth file is not provided)

1. train the OPQ encoder

2. build the index and store it

3. compute the residuals and train the beta vocabulary to do the reconstuction

4. encode the vertices

5. search and evaluate the search results.

With option `--exhaustive` the results of the exhaustive column can be
obtained.

The run above should output:
```
...
setting k_reorder=5
...
efSearch=1024      0.3132 ms per query,  R@1: 0.4283 R@10: 0.6337 R@100: 0.6520 ndis 40941919 nreorder 50000

```
which matches the paper's table 2.

Note that in multi-threaded mode, the building of the HNSW strcuture
is not deterministic. Therefore, the results across runs may not be exactly the same.

Reproducing Figure 5 in the paper
---------------------------------

Figure 5 just evaluates the combination of HNSW and PQ. For example,
the operating point L6&OPQ40 can be obtained with

```
python bench_link_and_code.py \
   --db deep1M \
   --M0 6 \
   --indexkey OPQ40_160,HNSW32_PQ40 \
   --indexfile $bdir/deep1M_PQ40_M6.index \
   --beta_nsq 1 --beta_k 1  \
   --beta_centroids $bdir/deep1M_PQ40_M6_nsq0.npy \
   --neigh_recons_codes $bdir/deep1M_PQ36_M6_nsq0_codes.npy \
   --k_reorder 0 --efSearch 16,64,256,1024
```

The arguments are similar to the previous table. Note that nsq = 0 is
simulated by setting beta_nsq = 1 and beta_k = 1 (ie a code with a single
reproduction value).

The output should look like:

```
setting k_reorder=0
efSearch=16        0.0147 ms per query,  R@1: 0.3409 R@10: 0.4388 R@100: 0.4394 ndis 2629735 nreorder 0
efSearch=64        0.0122 ms per query,  R@1: 0.4836 R@10: 0.6490 R@100: 0.6509 ndis 4623221 nreorder 0
efSearch=256       0.0344 ms per query,  R@1: 0.5730 R@10: 0.7915 R@100: 0.7951 ndis 11090176 nreorder 0
efSearch=1024      0.2656 ms per query,  R@1: 0.6212 R@10: 0.8722 R@100: 0.8765 ndis 33501951 nreorder 0
```

The results with k_reorder=5 are not reported in the paper, they
represent the performance of a "free coding" version of the algorithm.
