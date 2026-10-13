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

#include <memory>

#include "common/FieldData.h"
#include "index/contracts/Registry.h"
#include "index/contracts/build/IArtifactBuilder.h"
#include "storage/artifact/Artifact.h"

namespace milvus::indexbuilder {

// Type erasure exists only around caller-side ownership. Implementations
// accumulate stable typed batches and invoke the selected typed builder once.
class BuildInputMaterializer {
 public:
    virtual ~BuildInputMaterializer() = default;

    virtual const index::BuilderInputSpec&
    InputSpec() const = 0;

    virtual void
    Add(const FieldDataPtr& batch) = 0;

    virtual storage::ArtifactPtr
    Build() && = 0;
};

using BuildInputMaterializerPtr = std::unique_ptr<BuildInputMaterializer>;

BuildInputMaterializerPtr
MakeScalarBuildInputMaterializer(DataType source_type,
                                 DataType value_type,
                                 int64_t expected_rows,
                                 const index::IndexFamily& family,
                                 const index::BuildParams& params);

}  // namespace milvus::indexbuilder
