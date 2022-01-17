import faiss
import numpy as np

d = 128
n = 100

rs = np.random.RandomState(1337)
x = rs.rand(n, d).astype(np.float32)

index = faiss.IndexFlatL2(d)

res = faiss.StandardGpuResources()
gpu_index = faiss.index_cpu_to_gpu(res, 0, index)
gpu_index.add(x)

D, I = index.search(x, 10)
