/**
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

// -*- c++ -*-

#pragma once

#include <vector>
#include <faiss/impl/ScalarQuantizer.h>
#include <faiss/MetricType.h>

namespace faiss {

SQDistanceComputer *
sq_get_distance_computer_ref(
        MetricType metric,
        QuantizerType qtype,
        size_t dim,
        const std::vector<float>& trained);

Quantizer *
sq_select_quantizer_ref(
        QuantizerType qtype,
        size_t dim,
        const std::vector<float>& trained);

InvertedListScanner*
sq_select_inverted_list_scanner_ref(
        MetricType mt,
        const ScalarQuantizer *sq,
        const Index *quantizer,
        size_t dim,
        bool store_pairs,
        bool by_residual);

} // namespace faiss
