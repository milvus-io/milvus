# import numpy as np

# d = 64                           # dimension
# nb = 100000                      # database size
# nq = 10000                       # nb of queries
# np.random.seed(1234)             # make reproducible
# xb = np.random.random((nb, d)).astype('float32')
# xb[:, 0] += np.arange(nb) / 1000.
# xq = np.random.random((nq, d)).astype('float32')
# xq[:, 0] += np.arange(nq) / 1000.
#
# import faiss                    # make faiss available
#
# res = faiss.StandardGpuResources()  # use a single GPU
#
# ## Using a flat index
#
# index_flat = faiss.IndexFlatL2(d)  # build a flat (CPU) index
#
# # make it a flat GPU index
# gpu_index_flat = faiss.index_cpu_to_gpu(res, 0, index_flat)
#
# gpu_index_flat.add(xb)         # add vectors to the index
# print(gpu_index_flat.ntotal)
#
# k = 4                          # we want to see 4 nearest neighbors
# D, I = gpu_index_flat.search(xq, k)  # actual search
# print(I[:5])                   # neighbors of the 5 first queries
# print(I[-5:])                  # neighbors of the 5 last queries
#
#
# ## Using an IVF index
#
# nlist = 100
# quantizer = faiss.IndexFlatL2(d)  # the other index
# index_ivf = faiss.IndexIVFFlat(quantizer, d, nlist, faiss.METRIC_L2)
# # here we specify METRIC_L2, by default it performs inner-product search
#
# # make it an IVF GPU index
# gpu_index_ivf = faiss.index_cpu_to_gpu(res, 0, index_ivf)
#
# assert not gpu_index_ivf.is_trained
# gpu_index_ivf.train(xb)        # add vectors to the index
# assert gpu_index_ivf.is_trained
#
# gpu_index_ivf.add(xb)          # add vectors to the index
# print(gpu_index_ivf.ntotal)
#
# k = 4                          # we want to see 4 nearest neighbors
# D, I = gpu_index_ivf.search(xq, k)  # actual search
# print(I[:5])                   # neighbors of the 5 first queries
# print(I[-5:])


import numpy as np
import pytest

@pytest.mark.skip(reason="Not for pytest")
def basic_test():
    d = 64                           # dimension
    nb = 100000                      # database size
    nq = 10000                       # nb of queries
    np.random.seed(1234)             # make reproducible
    xb = np.random.random((nb, d)).astype('float32')
    xb[:, 0] += np.arange(nb) / 1000.
    xc = np.random.random((nb, d)).astype('float32')
    xc[:, 0] += np.arange(nb) / 1000.
    xq = np.random.random((nq, d)).astype('float32')
    xq[:, 0] += np.arange(nq) / 1000.

    import faiss                   # make faiss available
    index = faiss.IndexFlatL2(d)   # build the index
    print(index.is_trained)
    index.add(xb)                  # add vectors to the index
    print(index.ntotal)
    #faiss.write_index(index, "/tmp/faiss/tempfile_1")

    writer = faiss.VectorIOWriter()
    faiss.write_index(index, writer)
    ar_data = faiss.vector_to_array(writer.data)
    import pickle
    pickle.dump(ar_data, open("/tmp/faiss/ser_1", "wb"))

    #index_3 = pickle.load("/tmp/faiss/ser_1")


    # index_2 = faiss.IndexFlatL2(d)   # build the index
    # print(index_2.is_trained)
    # index_2.add(xc)                  # add vectors to the index
    # print(index_2.ntotal)
    # faiss.write_index(index, "/tmp/faiss/tempfile_2")
    #
    # index_3 = faiss.read_index

    # k = 4                          # we want to see 4 nearest neighbors
    # D, I = index.search(xb[:5], k) # sanity check
    # print(I)
    # print(D)
    # D, I = index.search(xq, k)     # actual search
    # print(I[:5])                   # neighbors of the 5 first queries
    # print(I[-5:])                  # neighbors of the 5 last queries

if __name__ == '__main__':
    basic_test()