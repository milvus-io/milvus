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

#include "common/MRB1Limits.generated.h"

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

    // The MRB1 envelope and admission limits are one contract with the Go SDK
    // builder (client/v3/roaringfilter) and the Go proxy validator
    // (pkg/v3/util/roaringfilter): the SDK pre-rejects what the proxy would
    // reject, and the proxy pre-rejects what segcore would reject. Nothing in
    // the build linked the three, so a value edited on one side alone compiled
    // and passed CI, and the symptom was the proxy admitting a blob every
    // querynode refuses.
    //
    // These are aliases, not a second copy: the values are generated from the
    // Go ones into MRB1Limits.generated.h, so there is nothing here that can
    // diverge from them. To change a limit, change the Go constant and run
    // `make generate-cpp-constants`; editing a number here is not possible
    // without editing a file that says DO NOT EDIT, and a stale generated
    // header fails TestGeneratedHeaderIsCurrent.
    //
    // They stay members so that RoaringMembership::kFoo keeps working at the
    // call sites, and `auto` so the declared type is the generated one -- a
    // narrowing spelled here would be a value this class does not actually use.
    static constexpr auto kMagic = mrb1::kMagic;
    static constexpr auto kVersion = mrb1::kVersion;
    static constexpr auto kFormatPortableRoaring64 =
        mrb1::kFormatPortableRoaring64;
    static constexpr auto kHeaderSize = mrb1::kHeaderSize;
    static constexpr auto kMaxBodySize = mrb1::kMaxBodySize;
    static constexpr auto kMaxHighContainerCount = mrb1::kMaxHighContainerCount;
    static constexpr auto kMaxEstimatedDecodedBytes =
        mrb1::kMaxEstimatedDecodedBytes;
    static constexpr auto kEstimatedHighContainerOverheadBytes =
        mrb1::kEstimatedHighContainerOverheadBytes;
    static constexpr auto kEstimatedLowContainerOverheadBytes =
        mrb1::kEstimatedLowContainerOverheadBytes;

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
