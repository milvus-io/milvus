/**
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

// -*- c++ -*-

#pragma once

#include <faiss/impl/ScalarQuantizerOp.h>

namespace faiss {

SQDistanceComputer *
sq_get_distance_computer_L2_sse(QuantizerType qtype, size_t dim, const std::vector<float>& trained);

SQDistanceComputer *
sq_get_distance_computer_IP_sse(QuantizerType qtype, size_t dim, const std::vector<float>& trained);

Quantizer *
sq_select_quantizer_sse(QuantizerType qtype, size_t dim, const std::vector<float>& trained);

} // namespace faiss
