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
#include <string>

// Canonical registry family names. They are selected from runtime configuration
// and existing format selectors; they are not written as new artifact metadata.
//
// These are NOT the same vocabulary as `index/Meta.h`'s user-facing index type
// names ("INVERTED", "STL_SORT", "Trie", "AUTOINDEX", ...). The user-facing
// names are a plan/proto-level concept with aliases and legacy spellings; the
// mapping from a user-facing name to a family happens once, in the index-type
// adapter, not inside any family.

namespace milvus::index::families {

inline constexpr const char* kInverted = "inverted";
inline constexpr const char* kBitmap = "bitmap";
inline constexpr const char* kSort = "sort";
inline constexpr const char* kMarisa = "marisa";
inline constexpr const char* kFmIndex = "fmindex";
inline constexpr const char* kText = "text";
inline constexpr const char* kNgram = "ngram";
inline constexpr const char* kRTree = "rtree";
inline constexpr const char* kJsonFlat = "json_flat";
inline constexpr const char* kVectorMem = "vector_mem";
inline constexpr const char* kVectorDisk = "vector_disk";

// Not a family with a reader of its own. The builder emits the existing HYBRID
// selector, and the loader resolves that selector before choosing a reader.
inline constexpr const char* kHybrid = "hybrid";

}  // namespace milvus::index::families

namespace milvus::index {

// Persisted by the legacy HYBRID format as one byte. Ordinals are part of the
// wire format and must not change.
enum class ScalarIndexType : uint8_t {
    NONE = 0,
    BITMAP,
    STLSORT,
    MARISA,
    INVERTED,
    HYBRID,
    JSONSTATS,
    RTREE,
    NGRAM,
    FMINDEX,
};

inline std::string
ToString(ScalarIndexType type) {
    switch (type) {
        case ScalarIndexType::NONE:
            return "NONE";
        case ScalarIndexType::BITMAP:
            return "BITMAP";
        case ScalarIndexType::STLSORT:
            return "STLSORT";
        case ScalarIndexType::MARISA:
            return "MARISA";
        case ScalarIndexType::INVERTED:
            return "INVERTED";
        case ScalarIndexType::HYBRID:
            return "HYBRID";
        case ScalarIndexType::JSONSTATS:
            return "JSONSTATS";
        case ScalarIndexType::RTREE:
            return "RTREE";
        case ScalarIndexType::NGRAM:
            return "NGRAM";
        case ScalarIndexType::FMINDEX:
            return "FMINDEX";
    }
    return "UNKNOWN";
}

}  // namespace milvus::index
