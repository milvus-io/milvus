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

#include <cstdint>
#include <string_view>

namespace milvus::index::sort_format {

inline constexpr std::string_view kIndexData = "index_data";
inline constexpr std::string_view kIndexLength = "index_length";
inline constexpr std::string_view kLegacyNumRows = "index_num_rows";
inline constexpr std::string_view kNumRows = "num_rows";
inline constexpr std::string_view kLegacyNested = "is_nested_index";
inline constexpr std::string_view kNested = "is_nested";
inline constexpr std::string_view kValidBitset = "valid_bitset";
inline constexpr std::string_view kIdxToOffsets = "idx_to_offsets";
inline constexpr std::string_view kVersion = "version";

inline constexpr uint32_t kStringVersion = 1;
inline constexpr uint64_t kStringMagic = 0x5354524E47534F52ULL;

}  // namespace milvus::index::sort_format
