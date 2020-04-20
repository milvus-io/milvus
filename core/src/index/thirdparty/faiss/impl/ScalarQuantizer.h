/**
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

// -*- c++ -*-

#pragma once

#include <faiss/IndexIVF.h>
#include <faiss/impl/ScalarQuantizerOp.h>


namespace faiss {

/**
 * The uniform quantizer has a range [vmin, vmax]. The range can be
 * the same for all dimensions (uniform) or specific per dimension
 * (default).
 */

struct ScalarQuantizer {

    QuantizerType qtype;

    /** The uniform encoder can estimate the range of representable
     * values of the unform encoder using different statistics. Here
     * rs = rangestat_arg */

    RangeStat rangestat;
    float rangestat_arg;

    /// dimension of input vectors
    size_t d;

    /// bytes per vector
    size_t code_size;

    /// trained values (including the range)
    std::vector<float> trained;

    ScalarQuantizer (size_t d, QuantizerType qtype);
    ScalarQuantizer ();

    void train (size_t n, const float *x);

    /// Used by an IVF index to train based on the residuals
    void train_residual (size_t n,
                         const float *x,
                         Index *quantizer,
                         bool by_residual,
                         bool verbose);

    /// same as compute_code for several vectors
    void compute_codes (const float * x,
                        uint8_t * codes,
                        size_t n) const ;

    /// decode a vector from a given code (or n vectors if third argument)
    void decode (const uint8_t *code, float *x, size_t n) const;


    /*****************************************************
     * Objects that provide methods for encoding/decoding, distance
     * computation and inverted list scanning
     *****************************************************/

    Quantizer * select_quantizer() const;

    SQDistanceComputer *get_distance_computer (MetricType metric = METRIC_L2)
        const;

    InvertedListScanner *select_InvertedListScanner
        (MetricType mt, const Index *quantizer, bool store_pairs,
         bool by_residual=false) const;

};



} // namespace faiss
