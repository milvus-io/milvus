# Benchmark of IVF variants

This is a benchmark of IVF index variants, looking at compression vs. speed vs. accuracy. 
The results are in [this wiki chapter](https://github.com/facebookresearch/faiss/wiki/Indexing-1G-vectors)


The code is organized as: 

- `datasets.py`: code to access the datafiles, compute the ground-truth and report accuracies

- `bench_all_ivf.py`: evaluate one type of inverted file

- `run_on_cluster_generic.bash`: call `bench_all_ivf.py` for all tested types of indices. 
Since the number of experiments is quite large the script is structued so that the benchmark can be run on a cluster.

- `parse_bench_all_ivf.py`: make nice tradeoff plots from all the results. 

The code depends on Faiss and can use 1 to 8 GPUs to do the k-means clustering for large vocabularies. 

It was run in October 2018 for the results in the wiki. 
