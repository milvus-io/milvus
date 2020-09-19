/**
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

// -*- c++ -*-

#include <faiss/impl/ScalarQuantizerDC_avx.h>
#include <faiss/impl/ScalarQuantizerCodec_avx.h>

namespace faiss {

/*******************************************************************
 * ScalarQuantizer Distance Computer
 ********************************************************************/

SQDistanceComputer *
sq_get_distance_computer_avx (MetricType metric, QuantizerType qtype, size_t dim, const std::vector<float>& trained) {
    if (metric == METRIC_L2) {
        if (dim % 8 == 0) {
            return select_distance_computer_avx<SimilarityL2_avx<8>>(qtype, dim, trained);
        } else {
            return select_distance_computer_avx<SimilarityL2_avx<1>>(qtype, dim, trained);
        }
    } else {
        if (dim % 8 == 0) {
            return select_distance_computer_avx<SimilarityIP_avx<8>>(qtype, dim, trained);
        } else {
            return select_distance_computer_avx<SimilarityIP_avx<1>>(qtype, dim, trained);
        }
    }
}

Quantizer *
sq_select_quantizer_avx (QuantizerType qtype, size_t dim, const std::vector<float>& trained) {
    if (dim % 8 == 0) {
        return select_quantizer_1_avx<8>(qtype, dim, trained);
    } else {
        return select_quantizer_1_avx<1> (qtype, dim, trained);
    }
}

InvertedListScanner*
sq_select_inverted_list_scanner_avx (MetricType mt, const ScalarQuantizer *sq, const Index *quantizer, size_t dim, bool store_pairs, bool by_residual) {
    if (dim % 8 == 0) {
        return sel0_InvertedListScanner_avx<8> (mt, sq, quantizer, store_pairs, by_residual);
    } else {
        return sel0_InvertedListScanner_avx<1> (mt, sq, quantizer, store_pairs, by_residual);
    }
}

} // namespace faiss
