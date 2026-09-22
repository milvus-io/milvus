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

#include <vector>

#include "common/Types.h"
#include "storage/artifact/Artifact.h"

// One-shot builders consume a caller-materialized complete input. The input type
// describes its physical shape; scalar batches, vector tensors, and prepared
// local files do not share a forced common layout.

namespace milvus::index {

struct BuilderInputSpec {
    // Additional fields that must be materialized into the concrete Input before
    // Build. The input type defines their value channel and representation.
    std::vector<FieldId> side_inputs;
};

template <typename Input>
class IArtifactBuilder {
 public:
    virtual ~IArtifactBuilder() = default;

    // Stable for this builder's lifetime. The complete input is materialized
    // once after reading these requirements. Most builders need no side input;
    // builders with additional requirements override this default.
    virtual BuilderInputSpec
    InputSpec() const {
        return {};
    }

    // The builder borrows the complete input and everything referenced by its
    // views for this synchronous call. It may traverse the input more than once,
    // but neither the builder nor the returned Artifact may retain borrowed input
    // storage. Invoking Build consumes the one-shot builder and returns a complete
    // Artifact. Direct conversion is an optional capability of that artifact.
    virtual storage::ArtifactPtr
    Build(const Input& input) && = 0;
};

}  // namespace milvus::index
