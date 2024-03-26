// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#include <cstddef>
#include <cstdint>

#include <folly/FBVector.h>

#include "bitset/bitset.h"
#include "bitset/common.h"
#include "bitset/detail/element_vectorized.h"
#include "bitset/detail/platform/dynamic.h"

namespace milvus {

namespace {

using vectorized_type = milvus::bitset::detail::VectorizedDynamic;
using policy_type =
    milvus::bitset::detail::VectorizedElementWiseBitsetPolicy<uint64_t,
                                                              vectorized_type>;
using container_type = folly::fbvector<uint8_t>;
// temporary enable range check
using bitset_type = milvus::bitset::Bitset<policy_type, container_type, true>;
// temporary enable range check
using bitset_view = milvus::bitset::BitsetView<policy_type, true>;

}  // namespace

using CustomBitset = bitset_type;
using CustomBitsetView = bitset_view;

}  // namespace milvus
