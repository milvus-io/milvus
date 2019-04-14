# Faiss 

Faiss is a library for efficient similarity search and clustering of dense vectors. It contains algorithms that search in sets of vectors of any size, up to ones that possibly do not fit in RAM. It also contains supporting code for evaluation and parameter tuning. Faiss is written in C++ with complete wrappers for Python/numpy. Some of the most useful algorithms are implemented on the GPU. It is developed by [Facebook AI Research](https://research.fb.com/category/facebook-ai-research-fair/).

## NEWS

*NEW: version 1.5.0 (2018-12-19) GPU binary flat index and binary HNSW index*

*NEW: version 1.4.0 (2018-08-30) no more crashes in pure Python code*

*NEW: version 1.3.0 (2018-07-12) support for binary indexes*

*NEW: latest commit (2018-02-22) supports on-disk storage of inverted indexes, see demos/demo_ondisk_ivf.py*

*NEW: latest commit (2018-01-09) includes an implementation of the HNSW indexing method, see benchs/bench_hnsw.py*

*NEW: there is now a Facebook public discussion group for Faiss users at https://www.facebook.com/groups/faissusers/*

*NEW: on 2017-07-30, the license on Faiss was relaxed to BSD from CC-BY-NC. Read LICENSE for details.*

## Introduction

Faiss contains several methods for similarity search. It assumes that the instances are represented as vectors and are identified by an integer, and that the vectors can be compared with L2 distances or dot products. Vectors that are similar to a query vector are those that have the lowest L2 distance or the highest dot product with the query vector. It also supports cosine similarity, since this is a dot product on normalized vectors.

Most of the methods, like those based on binary vectors and compact quantization codes, solely use a compressed representation of the vectors and do not require to keep the original vectors. This generally comes at the cost of a less precise search but these methods can scale to billions of vectors in main memory on a single server. 

The GPU implementation can accept input from either CPU or GPU memory. On a server with GPUs, the GPU indexes can be used a drop-in replacement for the CPU indexes (e.g., replace `IndexFlatL2` with `GpuIndexFlatL2`) and copies to/from GPU memory are handled automatically. Results will be faster however if both input and output remain resident on the GPU. Both single and multi-GPU usage is supported.

## Building 

The library is mostly implemented in C++, with optional GPU support provided via CUDA, and an optional Python interface. The CPU version requires a BLAS library. It compiles with a Makefile and can be packaged in a docker image. See [INSTALL.md](INSTALL.md) for details.

## How Faiss works

Faiss is built around an index type that stores a set of vectors, and provides a function to search in them with L2 and/or dot product vector comparison. Some index types are simple baselines, such as exact search. Most of the available indexing structures correspond to various trade-offs with respect to

- search time
- search quality
- memory used per index vector 
- training time
- need for external data for unsupervised training

The optional GPU implementation provides what is likely (as of March 2017) the fastest exact and approximate (compressed-domain) nearest neighbor search implementation for high-dimensional vectors, fastest Lloyd's k-means, and fastest small k-selection algorithm known. [The implementation is detailed here](https://arxiv.org/abs/1702.08734).

## Full documentation of Faiss

The following are entry points for documentation: 

- the full documentation, including a [tutorial](https://github.com/facebookresearch/faiss/wiki/Getting-started), a [FAQ](https://github.com/facebookresearch/faiss/wiki/FAQ) and a [troubleshooting section](https://github.com/facebookresearch/faiss/wiki/Troubleshooting) can be found on the [wiki page](http://github.com/facebookresearch/faiss/wiki)
- the [doxygen documentation](http://rawgithub.com/facebookresearch/faiss/master/docs/html/annotated.html) gives per-class information
- to reproduce results from our research papers, [Polysemous codes](https://arxiv.org/abs/1609.01882) and [Billion-scale similarity search with GPUs](https://arxiv.org/abs/1702.08734), refer to the [benchmarks README](benchs/README.md). For [
Link and code: Fast indexing with graphs and compact regression codes](https://arxiv.org/abs/1804.09996), see the [link_and_code README](benchs/link_and_code)

## Authors

The main authors of Faiss are:
- [Hervé Jégou](https://github.com/jegou) initiated the Faiss project and wrote its first implementation
- [Matthijs Douze](https://github.com/mdouze) implemented most of the CPU Faiss
- [Jeff Johnson](https://github.com/wickedfoo) implemented all of the GPU Faiss
- [Lucas Hosseini](https://github.com/beauby) implemented the binary indexes

## Reference

Reference to cite when you use Faiss in a research paper:

```
@article{JDH17,
  title={Billion-scale similarity search with GPUs},
  author={Johnson, Jeff and Douze, Matthijs and J{\'e}gou, Herv{\'e}},
  journal={arXiv preprint arXiv:1702.08734},
  year={2017}
}
```

## Join the Faiss community

For public discussion of Faiss or for questions, there is a Facebook public discussion group at https://www.facebook.com/groups/faissusers/

We monitor the [issues page](http://github.com/facebookresearch/faiss/issues) of the repository. You can report bugs, ask questions, etc.

## License

Faiss  is BSD-licensed. We also provide an additional patent grant.

