
// -*- c++ -*-

/* All distance functions for L2 and IP distances.
 * The actual functions are implemented in distances_simd_avx512.cpp */

#pragma once

#include <stddef.h>
#include <stdint.h>

namespace faiss {

/*********************************************************
 * Optimized distance/norm/inner prod computations
 *********************************************************/

/// Squared L2 distance between two vectors
float
fvec_L2sqr_avx512(const float* x, const float* y, size_t d);

/// inner product
float
fvec_inner_product_avx512(const float * x, const float * y, size_t d);

/// L1 distance
float
fvec_L1_avx512(const float* x, const float* y, size_t d);

float
fvec_Linf_avx512(const float* x, const float* y, size_t d);

/// popcnt
int
popcnt_AVX512VBMI_lookup(const uint8_t* data, const size_t n);

/// binary distance
int
xor_popcnt_AVX512VBMI_lookup(const uint8_t* data1, const uint8_t* data2, const size_t n);

int
or_popcnt_AVX512VBMI_lookup(const uint8_t* data1, const uint8_t* data2, const size_t n);

int
and_popcnt_AVX512VBMI_lookup(const uint8_t* data1, const uint8_t* data2, const size_t n);

float
jaccard__AVX512(const uint8_t * a, const uint8_t * b, size_t n);

} // namespace faiss
