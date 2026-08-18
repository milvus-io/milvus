// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#include <roaring/roaring.hh>

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string_view>
#include <utility>
#include <vector>

namespace milvus {

class RoaringMembership {
 public:
    struct ValidationSummary {
        uint64_t cardinality;
        uint64_t body_size;
        uint64_t high_container_count;
        uint64_t low_container_count;
        uint64_t estimated_decoded_bytes;
    };

    // The MRB1 envelope and admission limits below are one contract with the
    // Go SDK builder (client/v3/roaringfilter) and the Go proxy validator
    // (pkg/v3/util/roaringfilter). Nothing in the build links the three, so
    // TestRoaringSegcoreConstantsMatch (internal/parser/planparserv2) pins each
    // declaration to its Go counterpart by exact text. That check builds the
    // expected text from the Go constant rather than evaluating this one, which
    // is why these are plain literals: the readable form goes in the trailing
    // comment. Keep the shape `static constexpr <type> <name> = <literal>;`.
    static constexpr std::string_view kMagic = "MRB1";
    static constexpr uint16_t kVersion = 1;
    static constexpr uint16_t kFormatPortableRoaring64 = 1;
    static constexpr size_t kHeaderSize = 32;
    static constexpr size_t kMaxBodySize = 134217728;                // 128 MiB
    static constexpr uint64_t kMaxHighContainerCount = 262144;       // 1 << 18
    static constexpr uint64_t kMaxEstimatedDecodedBytes = 67108864;  // 64 MiB
    static constexpr uint64_t kEstimatedHighContainerOverheadBytes = 128;
    static constexpr uint64_t kEstimatedLowContainerOverheadBytes = 64;

    // Validates MRB1 and reports its allocation shape without constructing any
    // CRoaring object. One bitmap may have at most a 64 MiB decoded estimate.
    static ValidationSummary
    Validate(std::string_view blob);

    static std::shared_ptr<const RoaringMembership>
    Parse(std::string_view blob);

    bool
    Contains(int64_t value) const;

    uint64_t
    cardinality() const;

    size_t
    serialized_size() const;

 private:
    RoaringMembership(
        std::vector<std::pair<uint32_t, roaring::Roaring>> bitmaps,
        uint64_t cardinality,
        size_t serialized_size);

    std::vector<std::pair<uint32_t, roaring::Roaring>> bitmaps_;
    uint64_t cardinality_;
    size_t serialized_size_;
};

}  // namespace milvus
