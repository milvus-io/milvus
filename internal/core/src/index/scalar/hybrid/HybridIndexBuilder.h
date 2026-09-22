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
#include <string>

#include "common/Types.h"
#include "index/contracts/build/IArtifactBuilder.h"
#include "index/contracts/build/ScalarBuildInput.h"
#include "index/contracts/Registry.h"
#include "storage/artifact/Artifact.h"

// Cardinality-based hybrid build strategy. It selects a concrete family and
// builds it from the same complete stable input. Loading opens the selected
// reader directly, so this directory needs no forwarding reader.

namespace milvus::index {

struct HybridBuildParams {
    // Distinct values at or above this count select the high-cardinality
    // family. `BITMAP_INDEX_CARDINALITY_LIMIT` (index/Meta.h:80).
    int32_t cardinality_limit{0};

    // Which families the two sides map to. ARRAY inputs use bitmap for low
    // cardinality and inverted for high cardinality. Scalar inputs use the
    // configured families when supported by their engine version; otherwise
    // low cardinality uses bitmap, while high cardinality uses inverted for
    // strings, sort for integral types, and inverted for other types.
    std::string low_cardinality_family;
    std::string high_cardinality_family;

    DataType element_type{DataType::NONE};
    DataType value_type{DataType::NONE};

    // Passed unchanged to the selected family's registry factory. Selection
    // must not discard normalized field/version/staging parameters.
    BuildParams delegate_params;
};

template <typename T>
class HybridIndexBuilder final
    : public IArtifactBuilder<ScalarBuildInput<T>> {
 public:
    explicit HybridIndexBuilder(HybridBuildParams params);

    ~HybridIndexBuilder() override;

    storage::ArtifactPtr
        Build(const ScalarBuildInput<T>& input) &&
        override;

 private:
    // Select the concrete family from the observed cardinality.
    std::string
    SelectFamily(size_t distinct_count) const;

    HybridBuildParams params_;
};

}  // namespace milvus::index
