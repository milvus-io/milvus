/**
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

// -*- c++ -*-

#pragma once

#include <cstdio>
#include <algorithm>

#include <omp.h>

#include <faiss/utils/utils.h>
#include <faiss/impl/FaissAssert.h>
#include <faiss/impl/AuxIndexStructures.h>

namespace faiss {

typedef Index::idx_t idx_t;

enum class QuantizerType {
    QT_8bit = 0,         ///< 8 bits per component
    QT_4bit,             ///< 4 bits per component
    QT_8bit_uniform,     ///< same, shared range for all dimensions
    QT_4bit_uniform,
    QT_fp16,
    QT_8bit_direct,      /// fast indexing of uint8s
    QT_6bit,             ///< 6 bits per component
};

// rangestat_arg.
enum class RangeStat {
    RS_minmax = 0,       ///< [min - rs*(max-min), max + rs*(max-min)]
    RS_meanstd,          ///< [mean - std * rs, mean + std * rs]
    RS_quantiles,        ///< [Q(rs), Q(1-rs)]
    RS_optim,            ///< alternate optimization of reconstruction error
};

struct Quantizer {
    // encodes one vector. Assumes code is filled with 0s on input!
    virtual void encode_vector(const float *x, uint8_t *code) const = 0;
    virtual void decode_vector(const uint8_t *code, float *x) const = 0;

    virtual ~Quantizer() {}
};

struct SQDistanceComputer: DistanceComputer {
    const float *q;
    const uint8_t *codes;
    size_t code_size;

    SQDistanceComputer (): q(nullptr), codes (nullptr), code_size (0)
    {}
};

extern uint16_t encode_fp16 (float x);
extern float decode_fp16 (uint16_t x);

extern void train_Uniform(RangeStat rs, float rs_arg,
                   idx_t n, int k, const float *x,
                   std::vector<float> & trained);
extern void train_NonUniform(RangeStat rs, float rs_arg,
                      idx_t n, int d, int k, const float *x,
                      std::vector<float> & trained);


} // namespace faiss
