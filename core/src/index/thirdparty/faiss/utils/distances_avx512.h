
// -*- c++ -*-

/* All distance functions for L2 and IP distances.
 * The actual functions are implemented in distances_simd_avx512.cpp */

#pragma once

#include <stddef.h>

namespace faiss {

/*********************************************************
 * Optimized distance/norm/inner prod computations
 *********************************************************/

/// Squared L2 distance between two vectors
float fvec_L2sqr_avx512 (
        const float * x,
        const float * y,
        size_t d);

/// inner product
float  fvec_inner_product_avx512 (
        const float * x,
        const float * y,
        size_t d);

/// L1 distance
float fvec_L1_avx512 (
        const float * x,
        const float * y,
        size_t d);

float fvec_Linf_avx512 (
        const float * x,
        const float * y,
        size_t d);

} // namespace faiss
