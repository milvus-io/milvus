/**
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

// -*- c++ -*-

#pragma once

#include <stdint.h>

#include <faiss/utils/BitsetView.h>


namespace faiss {

 /*********************************************************
 * Optimized distance/norm/inner prod computations
 *********************************************************/

float fvec_L2sqr_ref (
        const float * x,
        const float * y,
        size_t d);

float fvec_inner_product_ref (
        const float * x,
        const float * y,
        size_t d);

float fvec_L1_ref (
        const float * x,
        const float * y,
        size_t d);

float fvec_Linf_ref (
        const float * x,
        const float * y,
        size_t d);

#ifdef __SSE__
float fvec_L2sqr_sse (
        const float * x,
        const float * y,
        size_t d);

float  fvec_inner_product_sse (
        const float * x,
        const float * y,
        size_t d);

float fvec_L1_sse (
        const float * x,
        const float * y,
        size_t d);

float fvec_Linf_sse (
        const float * x,
        const float * y,
        size_t d);
#endif

/* compute ny square L2 distance bewteen x and a set of contiguous y vectors */
void fvec_L2sqr_ny (
        float * dis,
        const float * x,
        const float * y,
        size_t d, size_t ny);


/** squared norm of a vector */
float fvec_norm_L2sqr (const float * x,
                       size_t d);

} // namespace faiss
