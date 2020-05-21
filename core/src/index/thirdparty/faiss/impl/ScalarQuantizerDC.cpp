/**
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

// -*- c++ -*-

#include <faiss/impl/ScalarQuantizerDC.h>
#include <faiss/impl/ScalarQuantizerCodec.h>

namespace faiss {

/*******************************************************************
 * ScalarQuantizer Distance Computer
 ********************************************************************/

/* SSE */
SQDistanceComputer *
sq_get_distance_computer_L2_sse (QuantizerType qtype, size_t dim, const std::vector<float>& trained) {
    return select_distance_computer<SimilarityL2<1>> (qtype, dim, trained);
}

SQDistanceComputer *
sq_get_distance_computer_IP_sse (QuantizerType qtype, size_t dim, const std::vector<float>& trained) {
    return select_distance_computer<SimilarityIP<1>> (qtype, dim, trained);
}

Quantizer *
sq_select_quantizer_sse (QuantizerType qtype, size_t dim, const std::vector<float>& trained) {
    return select_quantizer_1<1> (qtype, dim, trained);
}

} // namespace faiss
