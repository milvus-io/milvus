/**
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

// -*- c++ -*-

#pragma once

#include <vector>

#include <faiss/IndexIVFPQ.h>


namespace faiss {



/** Index with an additional level of PQ refinement */
struct IndexIVFPQR: IndexIVFPQ {
    ProductQuantizer refine_pq;           ///< 3rd level quantizer
    std::vector <uint8_t> refine_codes;   ///< corresponding codes

    /// factor between k requested in search and the k requested from the IVFPQ
    float k_factor;

    IndexIVFPQR (
            Index * quantizer, size_t d, size_t nlist,
            size_t M, size_t nbits_per_idx,
            size_t M_refine, size_t nbits_per_idx_refine);

    void reset() override;

    size_t remove_ids(const IDSelector& sel) override;

    /// trains the two product quantizers
    void train_residual(idx_t n, const float* x) override;

    void add_with_ids(idx_t n, const float* x, const idx_t* xids) override;

    /// same as add_with_ids, but optionally use the precomputed list ids
    void add_core (idx_t n, const float *x, const idx_t *xids,
                     const idx_t *precomputed_idx = nullptr);

    void reconstruct_from_offset (int64_t list_no, int64_t offset,
                                  float* recons) const override;

    void merge_from (IndexIVF &other, idx_t add_id) override;


    void search_preassigned (idx_t n, const float *x, idx_t k,
                             const idx_t *assign,
                             const float *centroid_dis,
                             float *distances, idx_t *labels,
                             bool store_pairs,
                             const IVFSearchParameters *params=nullptr
                             ) const override;

    IndexIVFPQR();
};


} // namespace faiss
