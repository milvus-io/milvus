/**
 * Copyright (c) 2015-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD+Patents license found in the
 * LICENSE file in the root directory of this source tree.
 */


#pragma once

#include "Float16.cuh"

#ifndef __HALF2_TO_UI
// cuda_fp16.hpp doesn't export this
#define __HALF2_TO_UI(var) *(reinterpret_cast<unsigned int *>(&(var)))
#endif


//
// Templated wrappers to express load/store for different scalar and vector
// types, so kernels can have the same written form but can operate
// over half and float, and on vector types transparently
//

namespace faiss { namespace gpu {

template <typename T>
struct LoadStore {
  static inline __device__ T load(void* p) {
    return *((T*) p);
  }

  static inline __device__ void store(void* p, const T& v) {
    *((T*) p) = v;
  }
};

#ifdef FAISS_USE_FLOAT16

template <>
struct LoadStore<Half4> {
  static inline __device__ Half4 load(void* p) {
    Half4 out;
#if CUDA_VERSION >= 9000
    asm("ld.global.v2.u32 {%0, %1}, [%2];" :
        "=r"(__HALF2_TO_UI(out.a)), "=r"(__HALF2_TO_UI(out.b)) : "l"(p));
#else
    asm("ld.global.v2.u32 {%0, %1}, [%2];" :
        "=r"(out.a.x), "=r"(out.b.x) : "l"(p));
#endif
    return out;
  }

  static inline __device__ void store(void* p, Half4& v) {
#if CUDA_VERSION >= 9000
    asm("st.v2.u32 [%0], {%1, %2};" : : "l"(p),
        "r"(__HALF2_TO_UI(v.a)), "r"(__HALF2_TO_UI(v.b)));
#else
    asm("st.v2.u32 [%0], {%1, %2};" : : "l"(p), "r"(v.a.x), "r"(v.b.x));
#endif
  }
};

template <>
struct LoadStore<Half8> {
  static inline __device__ Half8 load(void* p) {
    Half8 out;
#if CUDA_VERSION >= 9000
    asm("ld.global.v4.u32 {%0, %1, %2, %3}, [%4];" :
        "=r"(__HALF2_TO_UI(out.a.a)), "=r"(__HALF2_TO_UI(out.a.b)),
        "=r"(__HALF2_TO_UI(out.b.a)), "=r"(__HALF2_TO_UI(out.b.b)) : "l"(p));
#else
    asm("ld.global.v4.u32 {%0, %1, %2, %3}, [%4];" :
        "=r"(out.a.a.x), "=r"(out.a.b.x),
        "=r"(out.b.a.x), "=r"(out.b.b.x) : "l"(p));
#endif
    return out;
  }

  static inline __device__ void store(void* p, Half8& v) {
#if CUDA_VERSION >= 9000
    asm("st.v4.u32 [%0], {%1, %2, %3, %4};"
        : : "l"(p), "r"(__HALF2_TO_UI(v.a.a)), "r"(__HALF2_TO_UI(v.a.b)),
          "r"(__HALF2_TO_UI(v.b.a)), "r"(__HALF2_TO_UI(v.b.b)));
#else
    asm("st.v4.u32 [%0], {%1, %2, %3, %4};"
        : : "l"(p), "r"(v.a.a.x), "r"(v.a.b.x), "r"(v.b.a.x), "r"(v.b.b.x));
#endif
  }
};

#endif // FAISS_USE_FLOAT16

} } // namespace
