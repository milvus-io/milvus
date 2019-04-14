import faiss
import numpy as np

d = 128
# NOTE: BLAS kicks in only when n > distance_compute_blas_threshold = 20
n = 100

rs = np.random.RandomState(1337)
x = rs.rand(n, d).astype(np.float32)

index = faiss.IndexFlatL2(d)
index.add(x)

D, I = index.search(x, 10)
