/**
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

// -*- c++ -*-

#pragma once

#include <vector>

#include <faiss/IndexPQ.h>
#include <faiss/IndexIVF.h>

namespace faiss {

struct IndexIVFPQ;


/** Same as an IndexIVFPQ without the inverted lists: codes are stored sequentially
 *
 * The class is mainly inteded to store encoded vectors that can be
 * accessed randomly, the search function is not implemented.
 */
struct Index2Layer: Index {
    /// first level quantizer
    Level1Quantizer q1;

    /// second level quantizer is always a PQ
    ProductQuantizer pq;

    /// Codes. Size ntotal * code_size.
    std::vector<uint8_t> codes;

    /// size of the code for the first level (ceil(log8(q1.nlist)))
    size_t code_size_1;

    /// size of the code for the second level
    size_t code_size_2;

    /// code_size_1 + code_size_2
    size_t code_size;

    Index2Layer (Index * quantizer, size_t nlist,
                 int M, int nbit = 8,
                 MetricType metric = METRIC_L2);

    Index2Layer ();
    ~Index2Layer ();

    void train(idx_t n, const float* x) override;

    void add(idx_t n, const float* x) override;

    /// not implemented
    void search(
        idx_t n,
        const float* x,
        idx_t k,
        float* distances,
        idx_t* labels) const override;

    void reconstruct_n(idx_t i0, idx_t ni, float* recons) const override;

    void reconstruct(idx_t key, float* recons) const override;

    void reset() override;

    DistanceComputer * get_distance_computer() const override;

    /// transfer the flat codes to an IVFPQ index
    void transfer_to_IVFPQ(IndexIVFPQ & other) const;


    /* The standalone codec interface */
    size_t sa_code_size () const override;
    void sa_encode (idx_t n, const float *x, uint8_t *bytes) const override;
    void sa_decode (idx_t n, const uint8_t *bytes, float *x) const override;

};


} // namespace faiss
