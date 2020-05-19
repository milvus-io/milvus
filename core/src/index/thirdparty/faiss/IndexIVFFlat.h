/**
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

// -*- c++ -*-

#ifndef FAISS_INDEX_IVF_FLAT_H
#define FAISS_INDEX_IVF_FLAT_H

#include <unordered_map>
#include <stdint.h>

#include <faiss/IndexIVF.h>


namespace faiss {

/** Inverted file with stored vectors. Here the inverted file
 * pre-selects the vectors to be searched, but they are not otherwise
 * encoded, the code array just contains the raw float entries.
 */
struct IndexIVFFlat: IndexIVF {

    IndexIVFFlat (
            Index * quantizer, size_t d, size_t nlist_,
            MetricType = METRIC_L2);

    /// same as add_with_ids, with precomputed coarse quantizer
    virtual void add_core (idx_t n, const float * x, const int64_t *xids,
                   const int64_t *precomputed_idx);

    /// implemented for all IndexIVF* classes
    void add_with_ids(idx_t n, const float* x, const idx_t* xids) override;

    void encode_vectors(idx_t n, const float* x,
                        const idx_t *list_nos,
                        uint8_t * codes,
                        bool include_listnos=false) const override;


    InvertedListScanner *get_InvertedListScanner (bool store_pairs)
        const override;


    void reconstruct_from_offset (int64_t list_no, int64_t offset,
                                  float* recons) const override;

    void sa_decode (idx_t n, const uint8_t *bytes,
                            float *x) const override;

    IndexIVFFlat () {}
};


struct IndexIVFFlatDedup: IndexIVFFlat {

    /** Maps ids stored in the index to the ids of vectors that are
     *  the same. When a vector is unique, it does not appear in the
     *  instances map */
    std::unordered_multimap <idx_t, idx_t> instances;

    IndexIVFFlatDedup (
            Index * quantizer, size_t d, size_t nlist_,
            MetricType = METRIC_L2);

    /// also dedups the training set
    void train(idx_t n, const float* x) override;

    /// implemented for all IndexIVF* classes
    void add_with_ids(idx_t n, const float* x, const idx_t* xids) override;

    void search_preassigned (idx_t n, const float *x, idx_t k,
                             const idx_t *assign,
                             const float *centroid_dis,
                             float *distances, idx_t *labels,
                             bool store_pairs,
                             const IVFSearchParameters *params=nullptr
                             ) const override;

    size_t remove_ids(const IDSelector& sel) override;

    /// not implemented
    void range_search(
        idx_t n,
        const float* x,
        float radius,
        RangeSearchResult* result) const override;

    /// not implemented
    void update_vectors (int nv, const idx_t *idx, const float *v) override;

    /// not implemented
    void reconstruct_from_offset (int64_t list_no, int64_t offset,
                                  float* recons) const override;

    IndexIVFFlatDedup () {}


};



} // namespace faiss

#endif
