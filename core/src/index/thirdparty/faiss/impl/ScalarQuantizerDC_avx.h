/**
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

// -*- c++ -*-

#pragma once

#include <vector>
#include <faiss/impl/ScalarQuantizerOp.h>
#include <faiss/MetricType.h>

namespace faiss {


SQDistanceComputer *
sq_get_distance_computer_avx(MetricType metric, QuantizerType qtype, size_t dim, const std::vector<float>& trained);

Quantizer *
sq_select_quantizer_avx(QuantizerType qtype, size_t dim, const std::vector<float>& trained);

} // namespace faiss
