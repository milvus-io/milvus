// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file to you under
// the Apache License, Version 2.0 (the "License"); you may not use this file
// except in compliance with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#include <span>

#include "common/ValidityView.h"

namespace milvus::index {

// One stable, non-owning scalar batch. Values are logical-row aligned, including
// nullable rows. An empty validity view means every row is valid; callers and
// builders must test `!validity || validity[row]` before indexing the view.
// Variable-length value views and all storage they reference must remain valid
// for the complete IArtifactBuilder::Build call.
template <typename T>
struct ScalarBuildBatch {
    std::span<const T> values;
    ValidityView validity;
};

// Complete scalar input. Batches and their transitively referenced storage are
// stable for the complete Build call, so a builder may traverse them repeatedly
// without asking the caller to replay or rematerialize the source.
template <typename T>
struct ScalarBuildInput {
    std::span<const ScalarBuildBatch<T>> batches;
};

}  // namespace milvus::index
