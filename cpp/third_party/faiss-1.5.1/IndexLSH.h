/**
 * Copyright (c) 2015-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD+Patents license found in the
 * LICENSE file in the root directory of this source tree.
 */

// -*- c++ -*-

#ifndef INDEX_LSH_H
#define INDEX_LSH_H

#include <vector>

#include "Index.h"
#include "VectorTransform.h"

namespace faiss {


/** The sign of each vector component is put in a binary signature */
struct IndexLSH:Index {
    typedef unsigned char uint8_t;

    int nbits;              ///< nb of bits per vector
    int bytes_per_vec;      ///< nb of 8-bits per encoded vector
    bool rotate_data;       ///< whether to apply a random rotation to input
    bool train_thresholds;  ///< whether we train thresholds or use 0

    RandomRotationMatrix rrot; ///< optional random rotation

    std::vector <float> thresholds; ///< thresholds to compare with

    /// encoded dataset
    std::vector<uint8_t> codes;

    IndexLSH (
            idx_t d, int nbits,
            bool rotate_data = true,
            bool train_thresholds = false);

    /** Preprocesses and resizes the input to the size required to
     * binarize the data
     *
     * @param x input vectors, size n * d
     * @return output vectors, size n * bits. May be the same pointer
     *         as x, otherwise it should be deleted by the caller
     */
    const float *apply_preprocess (idx_t n, const float *x) const;

    void train(idx_t n, const float* x) override;

    void add(idx_t n, const float* x) override;

    void search(
        idx_t n,
        const float* x,
        idx_t k,
        float* distances,
        idx_t* labels) const override;

    void reset() override;

    /// transfer the thresholds to a pre-processing stage (and unset
    /// train_thresholds)
    void transfer_thresholds (LinearTransform * vt);

    ~IndexLSH() override {}

    IndexLSH ();
};


}


#endif
