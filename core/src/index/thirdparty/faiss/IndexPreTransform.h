/**
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

// -*- c++ -*-

#pragma once



#include <faiss/Index.h>
#include <faiss/VectorTransform.h>

namespace faiss {

/** Index that applies a LinearTransform transform on vectors before
 *  handing them over to a sub-index */
struct IndexPreTransform: Index {

    std::vector<VectorTransform *> chain;  ///! chain of tranforms
    Index * index;            ///! the sub-index

    bool own_fields;          ///! whether pointers are deleted in destructor

    explicit IndexPreTransform (Index *index);

    IndexPreTransform ();

    /// ltrans is the last transform before the index
    IndexPreTransform (VectorTransform * ltrans, Index * index);

    void prepend_transform (VectorTransform * ltrans);

    void train(idx_t n, const float* x) override;

    void add(idx_t n, const float* x) override;

    void add_with_ids(idx_t n, const float* x, const idx_t* xids) override;

    void reset() override;

    /** removes IDs from the index. Not supported by all indexes.
     */
    size_t remove_ids(const IDSelector& sel) override;

    void search(
        idx_t n,
        const float* x,
        idx_t k,
        float* distances,
        idx_t* labels) const override;


    /* range search, no attempt is done to change the radius */
    void range_search (idx_t n, const float* x, float radius,
                       RangeSearchResult* result) const override;


    void reconstruct (idx_t key, float * recons) const override;

    void reconstruct_n (idx_t i0, idx_t ni, float *recons)
        const override;

    void search_and_reconstruct (idx_t n, const float *x, idx_t k,
                                 float *distances, idx_t *labels,
                                 float *recons) const override;

    /// apply the transforms in the chain. The returned float * may be
    /// equal to x, otherwise it should be deallocated.
    const float * apply_chain (idx_t n, const float *x) const;

    /// Reverse the transforms in the chain. May not be implemented for
    /// all transforms in the chain or may return approximate results.
    void reverse_chain (idx_t n, const float* xt, float* x) const;


    /* standalone codec interface */
    size_t sa_code_size () const override;
    void sa_encode (idx_t n, const float *x,
                          uint8_t *bytes) const override;
    void sa_decode (idx_t n, const uint8_t *bytes,
                            float *x) const override;

    ~IndexPreTransform() override;
};


} // namespace faiss
