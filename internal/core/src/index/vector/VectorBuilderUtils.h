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

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <string_view>

#include "common/EasyAssert.h"
#include "common/Types.h"
#include "index/Meta.h"
#include "index/Utils.h"
#include "index/contracts/build/IArtifactBuilder.h"
#include "knowhere/index/index.h"

namespace milvus::index::vector_builder {

inline size_t
CheckedSize(int64_t value, std::string_view label) {
    AssertInfo(value >= 0, "{} is negative: {}", label, value);
    AssertInfo(static_cast<uint64_t>(value) <=
                   static_cast<uint64_t>(std::numeric_limits<size_t>::max()),
               "{} exceeds size_t: {}",
               label,
               value);
    return static_cast<size_t>(value);
}

inline BuilderInputSpec
DeriveInputSpec(const Config& build_params,
                const knowhere::Index<knowhere::IndexNode>& native_index) {
    BuilderInputSpec spec;
    const auto opt_fields =
        GetValueFromConfig<OptFieldT>(build_params, VEC_OPT_FIELDS);
    const auto partition_isolation =
        GetValueFromConfig<bool>(build_params, PARTITION_KEY_ISOLATION_KEY)
            .value_or(false);
    if (opt_fields.has_value() &&
        native_index.IsAdditionalScalarSupported(partition_isolation)) {
        spec.side_inputs.reserve(opt_fields->size());
        for (const auto& [field_id, _] : *opt_fields) {
            spec.side_inputs.emplace_back(field_id);
        }
        std::sort(spec.side_inputs.begin(), spec.side_inputs.end());
    }
    return spec;
}

}  // namespace milvus::index::vector_builder
