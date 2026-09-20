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

#include <optional>
#include <vector>

#include "common/OpContext.h"
#include "common/Types.h"
#include "segcore/SegmentInterface.h"

namespace milvus::exec {

// `base_filter` uses vector-search semantics: one means invalid. The returned
// membership bitmap uses scalar-index semantics: one means that the eligible
// row belongs to one of the requested groups.
template <typename T>
std::optional<TargetBitmap>
BuildGroupMembership(milvus::OpContext* op_ctx,
                     const segcore::SegmentInternalInterface& segment,
                     FieldId field_id,
                     int64_t row_count,
                     const std::vector<std::optional<T>>& groups,
                     const TargetBitmap* base_filter);

}  // namespace milvus::exec
