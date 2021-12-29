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
#include <faiss/impl/AuxIndexStructures.h>


namespace faiss {

/***************************************************************************
 * Range search
 ***************************************************************************/

/// Forward declaration, see AuxIndexStructures.h
struct RangeSearchResult;

/** Return the k nearest neighors of each of the nx vectors x among the ny
 *  vector y, w.r.t to max inner product
 *
 * @param x      query vectors, size nx * d
 * @param y      database vectors, size ny * d
 * @param radius search radius around the x vectors
 * @param result result structure
 */
void range_search_L2sqr (
        const float * x,
        const float * y,
        size_t d, size_t nx, size_t ny,
        float radius,
        std::vector<RangeSearchPartialResult*> &result,
        size_t buffer_size,
        const BitsetView &bitset = nullptr);

/// same as range_search_L2sqr for the inner product similarity
void range_search_inner_product (
    const float * x,
    const float * y,
    size_t d, size_t nx, size_t ny,
    float radius,
    std::vector<RangeSearchPartialResult*> &result,
    size_t buffer_size,
    const BitsetView &bitset = nullptr);

} // namespace faiss
