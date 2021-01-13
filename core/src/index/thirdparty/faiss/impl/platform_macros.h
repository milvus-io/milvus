/**
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once


#ifdef _MSC_VER

/*******************************************************
 * Windows specific macros
 *******************************************************/


#ifdef FAISS_MAIN_LIB
#define FAISS_API __declspec(dllexport)
#else // _FAISS_MAIN_LIB
#define FAISS_API __declspec(dllimport)
#endif // FAISS_MAIN_LIB

#define __PRETTY_FUNCTION__ __FUNCSIG__

#define posix_memalign(p, a, s) (((*(p)) = _aligned_malloc((s), (a))), *(p) ?0 :errno)
#define posix_memalign_free _aligned_free

// aligned should be in front of the declaration
#define ALIGNED(x) __declspec(align(x))

// redefine the GCC intrinsics with Windows equivalents

#include <intrin.h>

inline int __builtin_ctzll(uint64_t x) {
    unsigned long ret;
    _BitScanForward64(&ret, x);
    return (int)ret;
}

inline int __builtin_ctz(unsigned long x) {
    unsigned long ret;
    _BitScanForward(&ret, x);
    return (int)ret;
}

inline int __builtin_clzll(uint64_t x) {
    return (int)__lzcnt64(x);
}

#define __builtin_popcountl __popcnt64

#else
/*******************************************************
 * Linux and OSX
 *******************************************************/

#define FAISS_API
#define posix_memalign_free free

// aligned should be *in front* of the declaration, for compatibility with windows
#define ALIGNED(x) __attribute__ ((aligned(x)))

#endif // _MSC_VER



