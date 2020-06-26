// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License.

#pragma once

#include "db/snapshot/Snapshot.h"
#include "server/context/Context.h"
#include "utils/Log.h"

#include <memory>

namespace milvus {
namespace engine {

struct LoadVectorFieldElementHandler : public snapshot::IterateHandler<snapshot::FieldElement> {
    using ResourceT = snapshot::FieldElement;
    LoadVectorFieldElementHandler(const std::shared_ptr<server::Context>& context, snapshot::ScopedSnapshotT ss,
                                  const snapshot::FieldPtr& field);

    Status
    Handle(const typename ResourceT::Ptr&) override;

    const std::shared_ptr<server::Context>& context_;
    snapshot::ScopedSnapshotT ss_;
    const snapshot::FieldPtr& field_;
};

struct LoadVectorFieldHandler : public snapshot::IterateHandler<snapshot::Field> {
    using ResourceT = snapshot::Field;
    LoadVectorFieldHandler(const std::shared_ptr<server::Context>& context, snapshot::ScopedSnapshotT ss);

    Status
    Handle(const typename ResourceT::Ptr&) override;

    const std::shared_ptr<server::Context>& context_;
    snapshot::ScopedSnapshotT ss_;
};

}  // namespace engine
}  // namespace milvus
