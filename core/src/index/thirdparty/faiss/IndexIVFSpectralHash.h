/**
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

// -*- c++ -*-

#ifndef FAISS_INDEX_IVFSH_H
#define FAISS_INDEX_IVFSH_H


#include <vector>

#include <faiss/IndexIVF.h>


namespace faiss {

struct VectorTransform;

/** Inverted list that stores binary codes of size nbit. Before the
 * binary conversion, the dimension of the vectors is transformed from
 * dim d into dim nbit by vt (a random rotation by default).
 *
 * Each coordinate is subtracted from a value determined by
 * threshold_type, and split into intervals of size period. Half of
 * the interval is a 0 bit, the other half a 1.
 */
struct IndexIVFSpectralHash: IndexIVF {

    VectorTransform *vt; // transformation from d to nbit dim
    bool own_fields;

    int nbit;
    float period;

    enum ThresholdType {
        Thresh_global,
        Thresh_centroid,
        Thresh_centroid_half,
        Thresh_median
    };
    ThresholdType threshold_type;

    // size nlist * nbit or 0 if Thresh_global
    std::vector<float> trained;

    IndexIVFSpectralHash (Index * quantizer, size_t d, size_t nlist,
                          int nbit, float period);

    IndexIVFSpectralHash ();

    void train_residual(idx_t n, const float* x) override;

    void encode_vectors(idx_t n, const float* x,
                        const idx_t *list_nos,
                        uint8_t * codes,
                        bool include_listnos = false) const override;

    InvertedListScanner *get_InvertedListScanner (bool store_pairs)
        const override;

    ~IndexIVFSpectralHash () override;

};




}; // namespace faiss


#endif
