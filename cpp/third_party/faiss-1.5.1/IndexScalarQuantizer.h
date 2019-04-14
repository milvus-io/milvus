/**
 * Copyright (c) 2015-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD+Patents license found in the
 * LICENSE file in the root directory of this source tree.
 */

// -*- c++ -*-

#ifndef FAISS_INDEX_SCALAR_QUANTIZER_H
#define FAISS_INDEX_SCALAR_QUANTIZER_H

#include <stdint.h>


#include <vector>


#include "IndexIVF.h"


namespace faiss {

/**
 * The uniform quantizer has a range [vmin, vmax]. The range can be
 * the same for all dimensions (uniform) or specific per dimension
 * (default).
 */

struct SQDistanceComputer;

struct ScalarQuantizer {

    enum QuantizerType {
        QT_8bit,             ///< 8 bits per component
        QT_4bit,             ///< 4 bits per component
        QT_8bit_uniform,     ///< same, shared range for all dimensions
        QT_4bit_uniform,
        QT_fp16,
        QT_8bit_direct,      /// fast indexing of uint8s
    };

    QuantizerType qtype;

    /** The uniform encoder can estimate the range of representable
     * values of the unform encoder using different statistics. Here
     * rs = rangestat_arg */

    // rangestat_arg.
    enum RangeStat {
        RS_minmax,           ///< [min - rs*(max-min), max + rs*(max-min)]
        RS_meanstd,          ///< [mean - std * rs, mean + std * rs]
        RS_quantiles,        ///< [Q(rs), Q(1-rs)]
        RS_optim,            ///< alternate optimization of reconstruction error
    };

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


    /// same as compute_code for several vectors
    void compute_codes (const float * x,
                        uint8_t * codes,
                        size_t n) const ;

    /// decode a vector from a given code (or n vectors if third argument)
    void decode (const uint8_t *code, float *x, size_t n) const;


    SQDistanceComputer *get_distance_computer (MetricType metric = METRIC_L2)
        const;

};

struct DistanceComputer;

struct IndexScalarQuantizer: Index {
    /// Used to encode the vectors
    ScalarQuantizer sq;

    /// Codes. Size ntotal * pq.code_size
    std::vector<uint8_t> codes;

    size_t code_size;

    /** Constructor.
     *
     * @param d      dimensionality of the input vectors
     * @param M      number of subquantizers
     * @param nbits  number of bit per subvector index
     */
    IndexScalarQuantizer (int d,
                          ScalarQuantizer::QuantizerType qtype,
                          MetricType metric = METRIC_L2);

    IndexScalarQuantizer ();

    void train(idx_t n, const float* x) override;

    void add(idx_t n, const float* x) override;

    void search(
        idx_t n,
        const float* x,
        idx_t k,
        float* distances,
        idx_t* labels) const override;

    void reset() override;

    void reconstruct_n(idx_t i0, idx_t ni, float* recons) const override;

    void reconstruct(idx_t key, float* recons) const override;

    DistanceComputer *get_distance_computer () const;

};


 /** An IVF implementation where the components of the residuals are
 * encoded with a scalar uniform quantizer. All distance computations
 * are asymmetric, so the encoded vectors are decoded and approximate
 * distances are computed.
 */

struct IndexIVFScalarQuantizer: IndexIVF {
    ScalarQuantizer sq;
    bool by_residual;

    IndexIVFScalarQuantizer(Index *quantizer, size_t d, size_t nlist,
                            ScalarQuantizer::QuantizerType qtype,
                            MetricType metric = METRIC_L2);

    IndexIVFScalarQuantizer();

    void train_residual(idx_t n, const float* x) override;

    void encode_vectors(idx_t n, const float* x,
                        const idx_t *list_nos,
                        uint8_t * codes) const override;

    void add_with_ids(idx_t n, const float* x, const long* xids) override;

    InvertedListScanner *get_InvertedListScanner (bool store_pairs)
        const override;


    void reconstruct_from_offset (long list_no, long offset,
                                  float* recons) const override;

};


}


#endif
