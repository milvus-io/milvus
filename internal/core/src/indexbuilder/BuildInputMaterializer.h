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
#include <optional>

#include "common/FieldData.h"
#include "index/contracts/Registry.h"
#include "index/contracts/build/IArtifactBuilder.h"
#include "knowhere/build_scheduler.h"
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

    // False leaves the input intact and allows a later retry. A successful
    // default reservation is held until BuildSession destroys the materializer.
    virtual bool
    TryReserveBuild() {
        if (!build_reservation_) {
            build_reservation_ =
                knowhere::BuildScheduler::Instance().TryReserve();
        }
        return build_reservation_.has_value();
    }

    // After loading, input remains resident through spill until explicitly released.
    virtual bool
    HasResidentInput() const {
        return true;
    }

    // True means a complete file is available; memory is still retained.
    // False means spilling is unsupported or was canceled before completion.
    virtual bool
    SpillInput() {
        return false;
    }

    // May run concurrently with SpillInput. Requests cancellation, not a join.
    virtual void
    StopSpill() {
    }

    // Only after SpillInput returned true and the caller joined its writer.
    virtual void
    ReleaseInputMemory() {
    }

    virtual storage::ArtifactPtr
    Build() && = 0;

 private:
    std::optional<knowhere::BuildScheduler::Reservation> build_reservation_;
};

using BuildInputMaterializerPtr = std::unique_ptr<BuildInputMaterializer>;

BuildInputMaterializerPtr
MakeScalarBuildInputMaterializer(DataType source_type,
                                 DataType value_type,
                                 int64_t expected_rows,
                                 const index::IndexFamily& family,
                                 const index::BuildParams& params);

}  // namespace milvus::indexbuilder
