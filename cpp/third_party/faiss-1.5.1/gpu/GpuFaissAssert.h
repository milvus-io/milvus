/**
 * Copyright (c) 2015-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD+Patents license found in the
 * LICENSE file in the root directory of this source tree.
 */


#ifndef GPU_FAISS_ASSERT_INCLUDED
#define GPU_FAISS_ASSERT_INCLUDED

#include "../FaissAssert.h"
#include <cuda.h>

///
/// Assertions
///

#ifdef __CUDA_ARCH__
#define GPU_FAISS_ASSERT(X) assert(X)
#define GPU_FAISS_ASSERT_MSG(X, MSG) assert(X)
#define GPU_FAISS_ASSERT_FMT(X, FMT, ...) assert(X)
#else
#define GPU_FAISS_ASSERT(X) FAISS_ASSERT(X)
#define GPU_FAISS_ASSERT_MSG(X, MSG) FAISS_ASSERT_MSG(X, MSG)
#define GPU_FAISS_ASSERT_FMT(X, FMT, ...) FAISS_ASSERT_FMT(X, FMT, __VA_ARGS)
#endif // __CUDA_ARCH__

#endif
